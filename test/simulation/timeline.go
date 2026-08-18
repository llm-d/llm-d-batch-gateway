//go:build simulation

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

package simulation

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// legalEdges is the allowed status transition graph from
// docs/design/batch_processor_architecture.md section 2.
var legalEdges = map[openai.BatchStatus][]openai.BatchStatus{
	openai.BatchStatusValidating: {
		openai.BatchStatusInProgress, openai.BatchStatusFailed,
		openai.BatchStatusExpired, openai.BatchStatusCancelled,
		openai.BatchStatusCancelling,
	},
	openai.BatchStatusInProgress: {
		openai.BatchStatusFinalizing, openai.BatchStatusFailed,
		openai.BatchStatusExpired, openai.BatchStatusCancelling,
	},
	openai.BatchStatusFinalizing: {
		openai.BatchStatusCompleted, openai.BatchStatusFailed,
		openai.BatchStatusCancelled,
	},
	openai.BatchStatusCancelling: {
		openai.BatchStatusCancelled, openai.BatchStatusFailed,
	},
}

// reachable reports whether `to` is reachable from `from` in the legal graph.
// The observer polls, so consecutive observations may skip intermediate
// states; an observed pair is legal if any legal path connects it.
func reachable(from, to openai.BatchStatus) bool {
	seen := map[openai.BatchStatus]bool{from: true}
	frontier := []openai.BatchStatus{from}
	for len(frontier) > 0 {
		next := frontier[0]
		frontier = frontier[1:]
		for _, edge := range legalEdges[next] {
			if edge == to {
				return true
			}
			if !seen[edge] {
				seen[edge] = true
				frontier = append(frontier, edge)
			}
		}
	}
	return false
}

// observation is one observed status with the time it was first seen.
type observation struct {
	status openai.BatchStatus
	at     time.Time
}

// timeline records the sequence of distinct statuses observed for one batch.
type timeline struct {
	mu         sync.Mutex
	batchID    string
	obs        []observation
	lastCounts openai.BatchRequestCounts
	rec        *recorder
}

// recordCounts mirrors request-count changes into the event stream so the
// report can plot progress alongside status transitions.
func (tl *timeline) recordCounts(counts openai.BatchRequestCounts) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if counts == tl.lastCounts {
		return
	}
	tl.lastCounts = counts
	if tl.rec != nil {
		tl.rec.event("counts-observed", map[string]any{
			"batch": tl.batchID, "total": counts.Total,
			"completed": counts.Completed, "failed": counts.Failed,
		})
	}
}

// observe starts polling the batch status every 100ms until ctx is done,
// recording each distinct status in order of first appearance. Transitions
// are mirrored into the recorder's event stream.
func observe(ctx context.Context, client *apiClient, batchID string, rec *recorder) *timeline {
	// A short-timeout client keeps polls from hanging on the dead port proxy
	// while a scenario has the apiserver killed; a hung poll would blind the
	// observer through the interesting window.
	client = client.withTimeout(2 * time.Second)
	tl := &timeline{batchID: batchID, rec: rec}
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				batch, err := client.getBatch(batchID)
				if err != nil {
					continue // apiserver may be down mid-scenario
				}
				tl.record(batch.Status)
				tl.recordCounts(batch.RequestCounts)
			}
		}
	}()
	return tl
}

func (tl *timeline) record(status openai.BatchStatus) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if n := len(tl.obs); n > 0 && tl.obs[n-1].status == status {
		return
	}
	tl.obs = append(tl.obs, observation{status: status, at: time.Now()})
	if tl.rec != nil {
		tl.rec.event("status-observed", map[string]any{"batch": tl.batchID, "status": string(status)})
	}
}

// statuses returns the observed distinct status sequence.
func (tl *timeline) statuses() []openai.BatchStatus {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	out := make([]openai.BatchStatus, len(tl.obs))
	for i, o := range tl.obs {
		out[i] = o.status
	}
	return out
}

// violation describes one observed invariant breach.
type violation struct {
	kind string
	msg  string
}

func (v violation) String() string { return v.kind + ": " + v.msg }

// checkTransitions returns a violation for every observed consecutive status
// pair that the legal graph cannot produce, and for every transition out of a
// terminal state.
func (tl *timeline) checkTransitions() []violation {
	seq := tl.statuses()
	var out []violation
	for i := 1; i < len(seq); i++ {
		from, to := seq[i-1], seq[i]
		if from.IsTerminal() {
			out = append(out, violation{
				kind: "terminal-transition",
				msg:  fmt.Sprintf("batch %s left terminal state %s for %s", tl.batchID, from, to),
			})
			continue
		}
		if !reachable(from, to) {
			out = append(out, violation{
				kind: "illegal-transition",
				msg:  fmt.Sprintf("batch %s observed %s -> %s, not reachable in the legal graph", tl.batchID, from, to),
			})
		}
	}
	return out
}

// waitForStatus polls until the batch reaches one of the wanted statuses or
// the timeout elapses. It returns the final batch either way; ok reports
// whether a wanted status was reached.
func waitForStatus(client *apiClient, batchID string, timeout time.Duration, wanted ...openai.BatchStatus) (openai.Batch, bool) {
	client = client.withTimeout(2 * time.Second)
	deadline := time.Now().Add(timeout)
	var last openai.Batch
	for time.Now().Before(deadline) {
		batch, err := client.getBatch(batchID)
		if err == nil {
			last = batch
			for _, w := range wanted {
				if batch.Status == w {
					return batch, true
				}
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return last, false
}
