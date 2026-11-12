/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
)

// ResultSink durably checkpoints one result: it stores the result object and
// commits it to the batch's entry store (first-wins).
type ResultSink interface {
	// Checkpoint stores the marshaled result line and commits the entry.
	// It returns false with nil error when the entry is already terminal —
	// a duplicate delivery whose result must be dropped without storing.
	// It returns db.ErrConflict when batch ownership was lost.
	Checkpoint(ctx context.Context, msg *ResultItem, line []byte, sha256 string, size int64) (bool, error)
}

// ResultCheckpointer is the durable pipeline's terminal actor: it enriches
// each result, checkpoints it through the sink, and resolves the pending
// entry only after the checkpoint succeeded. It mirrors the legacy collector
// loop (metrics, sticky first error, persistence-failure abort) with one
// addition: an ownership conflict aborts the job without a terminal write —
// the job now belongs to another processor.
type ResultCheckpointer struct {
	sink                 ResultSink
	pending              *PendingRequests
	tracker              *ProgressTracker
	logger               logr.Logger
	onPersistenceFailure func()
}

func NewResultCheckpointer(sink ResultSink, pending *PendingRequests, tracker *ProgressTracker, logger logr.Logger) *ResultCheckpointer {
	return &ResultCheckpointer{
		sink:    sink,
		pending: pending,
		tracker: tracker,
		logger:  logger,
	}
}

// setPersistenceFailureHook arms the executor's persistence-failure abort.
func (c *ResultCheckpointer) setPersistenceFailureHook(fn func()) {
	c.onPersistenceFailure = fn
}

// Drain reads results until resultCh is closed. Like the legacy collector it
// keeps reading after a write error (the dispatcher blocks if nobody reads)
// and returns the first error after the channel closes. A result whose
// checkpoint fails stays pending: its entry is not terminal, so a later
// resume re-submits it. Durable deliveries are acknowledged only once
// accounted for — a checkpointed result (or a duplicate whose earlier
// delivery was checkpointed) is acked; a failed one is left unacked so its
// lease redelivers it instead of replaying the inference.
func (c *ResultCheckpointer) Drain(ctx context.Context, resultCh <-chan ResultItem) error {
	var firstErr error
	var conflictErr error
	for msg := range resultCh {
		if !c.pending.Enrich(&msg) {
			// A broadcast result that belongs to another job sharing the
			// client: its owner acknowledges after its own checkpoint, so
			// leave the delivery alone here.
			continue
		}
		if !msg.SubmittedAt.IsZero() {
			metrics.DecProcessorInflightRequests()
			metrics.DecModelInflightRequests(msg.ModelID)
			metrics.RecordModelRequestExecutionDuration(time.Since(msg.SubmittedAt), msg.ModelID)
		}
		if firstErr != nil || conflictErr != nil {
			// Not accounted for: leave the delivery unacked so lease expiry
			// redelivers it (to this run's retry or a resumed incarnation).
			continue
		}
		if err := c.checkpoint(ctx, &msg); err != nil {
			if errors.Is(err, db.ErrConflict) {
				conflictErr = err
				c.logger.Error(err, "Ownership lost during checkpoint; aborting without a terminal write",
					"requestID", msg.RequestID)
				// Abort dispatch like a persistence failure: entries left
				// pending would otherwise block the dispatcher forever.
				if c.onPersistenceFailure != nil {
					c.onPersistenceFailure()
				}
				continue
			}
			firstErr = err
			c.logger.Error(err, "Persistence failure, skipping further writes")
			if c.onPersistenceFailure != nil {
				c.onPersistenceFailure()
			}
			continue
		}
		// Durably accounted for (committed, or a duplicate whose earlier
		// delivery was): acknowledging releases the delivery; a failed ack
		// only risks a duplicate redelivery, which the first-wins entry
		// commit drops.
		if msg.Ack != nil {
			if err := msg.Ack(ctx); err != nil {
				c.logger.Info("Result delivery acknowledgement failed; its lease will redeliver it and the duplicate is dropped on commit",
					"requestID", msg.RequestID, "err", err)
			}
		}
	}
	if conflictErr != nil {
		return conflictErr
	}
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		c.logger.Info("Context cancelled after drain completed", "err", ctx.Err())
	}
	return nil
}

// checkpoint stores one result and resolves its pending entry on success.
func (c *ResultCheckpointer) checkpoint(ctx context.Context, msg *ResultItem) error {
	line, sha256Hex, size, err := MarshalResultLine(msg)
	if err != nil {
		return err
	}
	committed, err := c.sink.Checkpoint(ctx, msg, line, sha256Hex, size)
	if err != nil {
		return err
	}
	c.pending.Resolve(msg)
	if committed {
		c.record(*msg)
	}
	return nil
}

// record mirrors the legacy collector's post-write bookkeeping.
func (c *ResultCheckpointer) record(msg ResultItem) {
	if msg.Succeeded() {
		c.tracker.RecordSuccess(msg)
		if msg.Response != nil {
			recordTokenUsage(msg.Response.Body, msg.ModelID, c.logger)
		}
		return
	}
	c.tracker.RecordFailure(fmt.Errorf("%s: %s", msg.RequestID, msg.ErrorCode()))
	metrics.RecordRequestError(msg.ModelID)
}

func recordTokenUsage(body map[string]any, model string, logger logr.Logger) {
	usage, ok := body["usage"].(map[string]any)
	if !ok {
		return
	}
	prompt, pOK := toFloat64(usage["prompt_tokens"])
	completion, cOK := toFloat64(usage["completion_tokens"])
	if !pOK && !cOK {
		return
	}
	if prompt < 0 || completion < 0 {
		logger.Info("Negative token values, skipping metrics",
			"prompt_tokens", prompt, "completion_tokens", completion)
		return
	}
	metrics.RecordTokenUsage(prompt, completion, model)
}

func toFloat64(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}
