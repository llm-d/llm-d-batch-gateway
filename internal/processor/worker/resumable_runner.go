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

package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
)

// activateResumable flips the batch into the durable pipeline: it stores the
// rejected lines' error objects, then performs the fenced validating →
// in_progress transition that inserts the entry set and takes the ownership
// lease. The error objects are PUT first (create-only store, deterministic
// content), so a crash before activation leaves orphans — never a
// half-activated batch.
func (p *Processor) activateResumable(ctx context.Context, params *jobExecutionParams, manifest *requestManifest) (*batchResultStore, error) {
	sink, err := newBatchResultStore(p.files.storage, p.resumable, params.jobItem.ID, params.jobInfo.TenantID, p.processorID, params.jobItem.Epoch)
	if err != nil {
		return nil, err
	}
	for _, e := range manifest.Entries {
		if !e.Rejected {
			continue
		}
		if err := sink.putRejectedObject(ctx, e); err != nil {
			return nil, err
		}
	}

	err = p.resumable.ActivateResumableBatch(ctx, &db.ResumableActivation{
		BatchID:     params.jobItem.ID,
		ProcessorID: params.jobItem.ProcessorID,
		Epoch:       params.jobItem.Epoch,
		LeaseTTL:    p.cfg.Resumable.LeaseDuration,
		Entries:     manifest.activationEntries(),
	})
	if err != nil {
		return nil, fmt.Errorf("activate resumable batch: %w", err)
	}
	return sink, nil
}

// renewBatchLease extends the batch's ownership lease at half the lease
// interval until done closes or ctx ends. A fenced-out renewal means
// ownership moved: it trips the neutral ownership-lost abort so runJob stops
// without a terminal write. Transient errors are logged and retried on the
// next tick — the lease outlives any single failed renewal.
func (p *Processor) renewBatchLease(ctx context.Context, jobItem *db.BatchItem, onLost func(), done <-chan struct{}) {
	logger := logr.FromContextOrDiscard(ctx)
	ticker := time.NewTicker(p.cfg.Resumable.LeaseDuration / 2)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := p.resumable.RenewResumableBatchLease(ctx, jobItem.ID, p.processorID, jobItem.Epoch, p.cfg.Resumable.LeaseDuration)
			if err == nil {
				continue
			}
			if errors.Is(err, db.ErrConflict) {
				logger.Info("Lease renewal fenced out; batch ownership lost", "jobID", jobItem.ID)
				if onLost != nil {
					onLost()
				}
				return
			}
			logger.Error(err, "Lease renewal failed; retrying next interval", "jobID", jobItem.ID)
		}
	}
}

// markEntryInFlightHook returns the dispatcher chain's pre-submit hook: each
// request is fenced into in_flight before dispatch, so a zombie that lost
// the lease stops at its next submit instead of streaming the remaining
// requests into inference. An ownership conflict also trips the neutral
// ownership-lost abort; other errors surface per request.
func (p *Processor) markEntryInFlightHook(ctx context.Context, params *jobExecutionParams) func(*pipeline.RequestItem) (bool, error) {
	jobID := params.jobInfo.JobID
	epoch := int64(0)
	if params.jobItem != nil {
		epoch = params.jobItem.Epoch
	}
	logger := logr.FromContextOrDiscard(ctx)
	return func(msg *pipeline.RequestItem) (bool, error) {
		submit, err := p.resumable.MarkEntryInFlight(ctx, jobID, msg.RequestID, p.processorID, epoch)
		if err == nil {
			return submit, nil
		}
		if errors.Is(err, db.ErrConflict) {
			logger.Info("Pre-submit ownership check fenced out; batch ownership lost", "jobID", jobID)
			if params.onOwnershipLost != nil {
				params.onOwnershipLost()
			}
		}
		return false, err
	}
}
