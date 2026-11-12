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
	"fmt"

	"github.com/go-logr/logr"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

func (p *Processor) watchCancel(ctx context.Context, params *jobExecutionParams) {
	logger := logr.FromContextOrDiscard(ctx)
	for {
		select {
		case <-ctx.Done():
			logger.V(logging.DEBUG).Info("watchCancel: context done")
			return

		case event, ok := <-params.eventWatcher.Events:
			if !ok {
				logger.V(logging.DEBUG).Info("watchCancel: event channel closed")
				return
			}

			if event.Type == db.BatchEventCancel {
				logger.V(logging.INFO).Info("watchCancel: cancel event received")

				// Trip the user-cancel layer so batchctx.Cause attributes the
				// stop to a user cancellation.
				params.cancelUser()

				// We don't update the DB status to 'cancelling' here because
				// the API server already wrote 'cancelling' before sending this event.
			}
		}
	}
}

// handleCancelled finalizes a user-cancelled job.
// When called after executeJob (execution), requestCounts is non-nil and partial results are
// uploaded. When called before executeJob (ingestion), requestCounts is nil — only cleanup
// and status transition are performed. jobInfo is always non-nil on the normal runJob path
// (it is populated before the job goroutine is launched).
//
// Uses a detached context so that a concurrent SIGTERM cannot abort the upload or DB write.
func (p *Processor) handleCancelled(ctx context.Context, params *jobExecutionParams) error {
	ioCtx, ioSpan := uotel.DetachedContext(ctx, "handle-cancelled")
	ioCtx, ioCancel := context.WithTimeout(ioCtx, finalizationTimeout)
	defer ioCancel()
	defer ioSpan.End()

	logger := logr.FromContextOrDiscard(ctx)

	if err := params.updater.UpdateCancelledStatus(ioCtx, params.jobItem, params.requestCounts, "", ""); err != nil {
		ioSpan.RecordError(err)
		logger.Error(err, "Failed to update cancelled status, falling back to failed with file IDs preserved")
		if failErr := params.updater.UpdateFailedStatus(ioCtx, params.jobItem, params.requestCounts, "", ""); failErr != nil {
			ioSpan.RecordError(failErr)
			return fmt.Errorf("failed to update job status to cancelled (%w) and fallback to failed also failed: %w", err, failErr)
		}
		// Cleanup after fallback succeeded so startup recovery doesn't re-process.
		p.cleanupJobArtifacts(ioCtx, params.jobItem.ID, params.jobItem.TenantID)
		return fmt.Errorf("cancelled status write failed: %w", errFinalizeFailedOver)
	}

	// Cleanup after terminal status write: if the write failed above, the local
	// files survive for startup recovery to re-upload.
	p.cleanupJobArtifacts(ioCtx, params.jobItem.ID, params.jobItem.TenantID)

	setRequestCountAttrs(ctx, params.requestCounts)

	recordE2ELatency(params.jobInfo, metrics.E2EStatusCancelled)

	if params.requestCounts != nil {
		metrics.RecordCancellation(metrics.CancelPhaseInProgress)
	} else {
		metrics.RecordCancellation(metrics.CancelPhaseQueued)
	}
	metrics.RecordJobProcessed(metrics.ResultSuccess, metrics.ReasonNone)
	logger.V(logging.INFO).Info("Job cancelled handled")
	return nil
}
