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
	"maps"
	"runtime/debug"
	"slices"
	"time"

	"github.com/go-logr/logr"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

// panicRecoveryTimeout bounds how long handlePanicRecovery waits for DB
// operations before giving up. This prevents an unreachable database from
// blocking wg.Done() and release() indefinitely, which would leak the
// worker token and eventually deadlock Stop().
// Declared as var (not const) so tests can shorten it.
var panicRecoveryTimeout = time.Minute

func (p *Processor) runJob(ctx context.Context, params *jobExecutionParams) {
	// Restore parent trace context propagated from the apiserver via batch tags
	if len(params.jobInfo.TraceContext) > 0 {
		propagator := otel.GetTextMapPropagator()
		ctx = propagator.Extract(ctx, propagation.MapCarrier(params.jobInfo.TraceContext))
	}

	spanAttrs := []attribute.KeyValue{
		attribute.String(uotel.AttrBatchID, params.jobItem.ID),
		attribute.String(uotel.AttrTenantID, params.jobItem.TenantID),
	}
	if params.jobInfo.BatchJob != nil {
		spanAttrs = append(spanAttrs, attribute.String(uotel.AttrInputFileID, params.jobInfo.BatchJob.InputFileID))
	}
	if len(params.jobInfo.PassThroughHeaders) > 0 {
		spanAttrs = append(spanAttrs, attribute.StringSlice(uotel.AttrPassThroughHeaders, slices.Sorted(maps.Keys(params.jobInfo.PassThroughHeaders))))
	}
	ctx, span := uotel.StartSpan(ctx, "process-batch",
		trace.WithAttributes(spanAttrs...),
	)
	defer span.End()

	logger := logr.FromContextOrDiscard(ctx)

	defer p.wg.Done()
	defer p.release()
	// Declared before the deferred recover so the panic handler can inspect
	// how far execution progressed and attempt partial-result preservation.
	var requestCounts *openai.BatchRequestCounts
	// Note: recover() only catches panics on this goroutine. Panics in child
	// goroutines (watchCancel, per-model/per-request goroutines in executeJob)
	// are not caught here and will crash the process.
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		stackTrace := debug.Stack()
		logr.FromContextOrDiscard(ctx).Error(fmt.Errorf("panic: %v\n%s", r, stackTrace), "Panic recovered")
		span.RecordError(fmt.Errorf("panic: %v", r))
		span.SetStatus(codes.Error, "panic recovered")

		p.handlePanicRecovery(ctx, params, requestCounts)
	}()

	metrics.IncActiveWorkers()
	defer metrics.DecActiveWorkers()

	jobStart := time.Now()

	// A requeued resumable task holds its ownership lease from the dequeue,
	// and the pickup routing below can finalize immediately: artifact
	// assembly is bounded by finalizeTimeout, which can far exceed the lease
	// TTL, so the renewal must already be running or a large assembly loses
	// the lease before the fenced terminal write and the finalization loops
	// through requeues. There is nothing abortable before the execution
	// phase, so this pickup renewal runs detached from the abort causes and
	// without an ownership-lost callback — the first fenced write (entry
	// mark, checkpoint, or the terminal transition itself) catches a lost
	// lease. Once the batch is activated below, it is swapped for the
	// execution-phase renewal wired to the neutral abort.
	var stopPickupRenewal chan struct{}
	defer func() {
		if stopPickupRenewal != nil {
			close(stopPickupRenewal)
		}
	}()
	if params.task != nil && params.task.Resumable {
		stopPickupRenewal = make(chan struct{})
		go p.renewBatchLease(context.WithoutCancel(ctx), params.jobItem, nil, stopPickupRenewal)
	}

	// A requeued resumable task converges here from the unified dequeue and
	// the startup requeue. Terminal-bound states (cancelling, SLO-expired,
	// recovery budget exhausted) finalize with partial artifacts before any
	// execution; everything else resumes through the normal pipeline.
	if params.task != nil && params.task.Resumable {
		if p.routeResumedPickup(ctx, params) {
			return
		}
	}

	// event watcher for cancel event
	eventWatcher, err := p.event.ECConsumerGetChannel(ctx, params.jobInfo.JobID)
	if err != nil {
		logger.Error(err, "Failed to get event watcher")
		span.RecordError(err)
		span.SetStatus(codes.Error, "event watcher failed")
		// Re-enqueue best-effort. Use a detached context because ctx may already be
		// cancelled (e.g. pod shutdown) and we don't want the enqueue call to be short-circuited.
		if params.task != nil {
			bgCtx, bgSpan := uotel.DetachedContext(ctx, "re-enqueue")
			defer bgSpan.End()
			if params.task.Resumable {
				// PQEnqueue refuses resumable rows; hand the batch back the
				// way the reconciler does so the dequeue can reclaim it.
				if reqErr := p.resumable.RequeueResumableOrphan(bgCtx, params.task.ID, params.jobItem.ProcessorID, params.jobItem.Epoch, false, true); reqErr != nil {
					logger.Error(reqErr, "Failed to requeue the resumable job; leaving it to the reconciler")
				} else {
					metrics.RecordJobProcessed(metrics.ResultReEnqueued, metrics.ReasonSystemError)
				}
			} else if enqErr := p.poller.enqueueOne(bgCtx, params.task); enqErr != nil {
				logger.Error(enqErr, "Failed to re-enqueue the job to the queue")
				if failErr := p.handleFailed(bgCtx, params.updater, params.jobItem, nil, params.jobInfo); failErr != nil {
					logger.Error(failErr, "Failed to mark job as failed after re-enqueue failure")
				}
			} else {
				metrics.RecordJobProcessed(metrics.ResultReEnqueued, metrics.ReasonSystemError)
			}
		}
		return
	}
	defer eventWatcher.CloseFn()

	// abortCtx is the single context that drives every phase. It carries the
	// process-batch span + logger values but records *why* it was cancelled via
	// context.Cause, so the terminal state can be classified from one signal:
	//   - SLO deadline  -> context.DeadlineExceeded (from WithDeadline below)
	//   - user cancel    -> batchctx.ErrCancelled (watchCancel)
	//   - SIGTERM        -> batchctx.ErrShutdown (AfterFunc on the parent ctx)
	//   - reconciler/IO  -> context.Canceled (heartbeat / fatal I/O; neutral)
	// The deadline is placed on WithoutCancel(ctx) so SIGTERM cannot mask an SLO
	// expiry as a plain cancellation, and vice versa.
	abortBase := context.WithoutCancel(ctx)
	sloStop := context.CancelFunc(func() {})
	if params.task != nil && !params.task.SLO.IsZero() {
		abortBase, sloStop = context.WithDeadline(abortBase, params.task.SLO)
	}
	defer sloStop()

	// One cancel-cause func drives every phase; each source is a no-arg closure
	// that bakes its own cause. First call wins — batchctx.Cause reads it back to
	// classify the terminal state. The cleanup defer uses the neutral
	// context.Canceled so a normal return is not mistaken for an abort.
	abortCtx, abortCause := context.WithCancelCause(abortBase)
	defer abortCause(context.Canceled)
	params.cancelUser = func() { abortCause(batchctx.ErrCancelled) }
	// A fenced progress write (jobProgressUpdater) trips this with the neutral
	// cause context.Canceled. It is deliberately NOT a terminal user/shutdown/
	// expiry cause: we lost ownership, so the new owner owns the job's terminal
	// state and we must not write one ourselves.
	params.onOwnershipLost = func() { abortCause(context.Canceled) }

	// SIGTERM / pod shutdown propagates from the original ctx as ErrShutdown.
	stopShutdown := context.AfterFunc(ctx, func() { abortCause(batchctx.ErrShutdown) })
	defer stopShutdown()

	// watch for cancel event
	params.eventWatcher = eventWatcher
	go p.watchCancel(ctx, params)

	// ingestion: pre-process job (rejects unregistered-model requests early).
	ingestCtx, ingestSpan := uotel.StartSpan(abortCtx, "ingest-and-plan")
	// The manifest feeds the resumable activation.
	manifest, err := p.preProcessJob(ingestCtx, params.jobInfo)
	if err != nil && !batchctx.IsTerminal(err) {
		ingestSpan.RecordError(err)
		ingestSpan.SetStatus(codes.Error, "pre-process failed")
	}
	ingestSpan.End()
	if err != nil {
		// Terminal states (expired/cancelled/shutdown) are expected, not system errors.
		if !batchctx.IsTerminal(err) {
			logger.Error(err, "Pre-processing failed")
		}
		p.handleJobError(ctx, params, err)
		// No RecordJobProcessingDuration here: preprocessing is ingestion (parsing, plan
		// building), not inference execution. Recording elapsed time would pollute the
		// processing-duration metric with ingestion overhead.
		return
	}

	// Take ownership of the batch with the fenced activation (entries +
	// lease) instead of a plain status write; losing that race means another
	// incarnation owns the batch. A requeued task skips the activation — its
	// entry set already exists — and rebuilds the sink and resume seed from
	// the checkpointed entries instead.
	var (
		sink *batchResultStore
		seed *db.ResumableResumeState
	)
	if params.task != nil && params.task.Resumable {
		sink, seed, err = p.recoverResumable(abortCtx, params, manifest)
	} else {
		sink, err = p.activateResumable(abortCtx, params, manifest)
	}
	if err != nil {
		if errors.Is(err, db.ErrConflict) {
			p.handleOwnershipLost(ctx, params)
			return
		}
		logger.Error(err, "Failed to activate or resume resumable batch")
		span.RecordError(err)
		span.SetStatus(codes.Error, "activation failed")
		if params.task != nil && params.task.Resumable {
			// A requeued batch already carries its checkpointed entry set:
			// finalize through it (partial artifacts, entry counts) instead
			// of the plain failed write, which would drop the artifacts and
			// leave the resumable flag set on a terminal row.
			if finErr := p.finalizeResumableAs(ctx, params.updater, params.jobItem, params.jobInfo,
				openai.BatchStatusFailed, batch_types.ErrCodeBatchFailed,
				p.getJobStatus(params.jobItem)); finErr != nil {
				logger.Error(finErr, "Failed to finalize failed resumable pickup")
			}
		} else if failErr := p.handleFailed(ctx, params.updater, params.jobItem, nil, params.jobInfo); failErr != nil {
			logger.Error(failErr, "Failed to handle failed event")
		}
		return
	}
	params.resultSink = sink
	params.resumeSeed = seed
	// The activation CTE writes in_progress (and its timestamp) directly in
	// Postgres; refresh the in-memory status so later status documents build
	// on the row's actual state instead of the stale validating JSON.
	if fresh, ferr := p.poller.fetchJobItemByID(ctx, params.jobItem.ID); ferr == nil && fresh != nil {
		params.jobItem.Status = fresh.Status
	}
	// Keep the ownership lease alive until runJob returns. A requeued task
	// renewed from its pickup above: stop that goroutine and rewire the
	// renewal to the neutral ownership-lost abort for the execution phase.
	// A fresh task's lease is created by the activation, so its renewal
	// starts here. Like the finalization it protects, the renewal runs on a
	// context detached from the abort causes: a cancel or SIGTERM during
	// execution must not stop it while the (equally detached) finalization
	// is still assembling artifacts, or the lease lapses and the fenced
	// terminal write is refused — the row is then requeued and the
	// finalization retried by another owner. The goroutine stops when
	// runJob returns (the deferred close fires before the deferred neutral
	// abort cause, LIFO).
	if stopPickupRenewal != nil {
		close(stopPickupRenewal)
		stopPickupRenewal = nil
	}
	stopLeaseRenewal := make(chan struct{})
	defer close(stopLeaseRenewal)
	go p.renewBatchLease(context.WithoutCancel(ctx), params.jobItem, params.onOwnershipLost, stopLeaseRenewal)

	// execution: execute inference requests
	execCtx, execSpan := uotel.StartSpan(abortCtx, "execute-job")
	var execErr error
	requestCounts, execErr = p.executeJob(execCtx, params)
	if execErr != nil && !batchctx.IsTerminal(execErr) {
		execSpan.RecordError(execErr)
		execSpan.SetStatus(codes.Error, "execution failed")
	}
	execSpan.End()
	params.requestCounts = requestCounts
	// A neutral abort (context.Canceled — i.e. not expired/cancelled/shutdown)
	// means a fenced progress write revealed we lost ownership; so does an
	// ErrConflict surfaced by a fenced checkpoint. Stop here WITHOUT a
	// terminal write: the job now belongs to another processor, which owns the
	// terminal transition.
	if (abortCtx.Err() != nil && batchctx.Cause(abortCtx) == nil) || errors.Is(execErr, db.ErrConflict) {
		p.handleOwnershipLost(ctx, params)
		if requestCounts != nil {
			metrics.RecordJobProcessingDuration(time.Since(jobStart), metrics.GetSizeBucket(int(requestCounts.Total)))
		}
		return
	}
	if execErr != nil {
		p.handleJobError(ctx, params, execErr)
		// Record processing duration for any job that ran (partially or fully).
		// executeJob always returns non-nil counts alongside its sentinel errors
		// (batchctx.ErrExpired, batchctx.ErrCancelled, batchctx.ErrShutdown, system errors) because partial
		// work was done. The nil guard remains defensive for unexpected future paths.
		if requestCounts != nil {
			metrics.RecordJobProcessingDuration(time.Since(jobStart), metrics.GetSizeBucket(int(requestCounts.Total)))
		}
		return
	}

	// finalization: final counts and artifacts are derived from the entry set.
	finErr := p.finalizeResumableJob(abortCtx, params.updater, params.jobItem, params.jobInfo)
	if err := finErr; err != nil {
		if errors.Is(err, db.ErrConflict) {
			// A fenced finalization write means another incarnation owns the
			// batch; it owns the terminal transition.
			p.handleOwnershipLost(ctx, params)
			if requestCounts != nil {
				metrics.RecordJobProcessingDuration(time.Since(jobStart), metrics.GetSizeBucket(int(requestCounts.Total)))
			}
			return
		}
		if errors.Is(err, batchctx.ErrCancelled) {
			// Cancel arrived during finalization — DB already updated to cancelled.
			// Treat as successful cancellation (same as handleCancelled).
			// Use background context: ctx may be cancelled (SIGTERM) and cleanup is local I/O only.
			p.cleanupJobArtifacts(context.Background(), params.jobItem.ID, params.jobItem.TenantID)
			metrics.RecordJobProcessingDuration(time.Since(jobStart), metrics.GetSizeBucket(int(requestCounts.Total)))
			recordE2ELatency(params.jobInfo, metrics.E2EStatusCancelled)
			metrics.RecordCancellation(metrics.CancelPhaseFinalizing)
			metrics.RecordJobProcessed(metrics.ResultSuccess, metrics.ReasonNone)
			logger.V(logging.INFO).Info("Job cancelled during finalization")
			return
		}
		logger.Error(err, "Failed to finalize job")
		span.RecordError(err)
		span.SetStatus(codes.Error, "finalize failed")
		// The finalization failed before its terminal write and its
		// fallbacks: the row is still non-terminal and re-finalization is
		// idempotent, so leave it to the reconciler requeue instead of a
		// degraded failed write.
		logger.Info("Leaving job to the reconciler after a failed finalization")
		metrics.RecordJobProcessingDuration(time.Since(jobStart), metrics.GetSizeBucket(int(requestCounts.Total)))
		metrics.RecordJobProcessed(metrics.ResultReEnqueued, metrics.ReasonSystemError)
		return
	}

	// cleanup local artifacts (best-effort); use background context since ctx may be cancelled.
	p.cleanupJobArtifacts(context.Background(), params.jobItem.ID, params.jobItem.TenantID)
	metrics.RecordJobProcessingDuration(time.Since(jobStart), metrics.GetSizeBucket(int(requestCounts.Total)))
	recordE2ELatency(params.jobInfo, metrics.E2EStatusCompleted)
	metrics.RecordJobProcessed(metrics.ResultSuccess, metrics.ReasonNone)
	logger.V(logging.INFO).Info("Job completed successfully")
}

// handlePanicRecovery contains the damage of a panic in runJob. A batch on
// the durable pipeline — activated here, or resumed from a requeue and
// already carrying its entry set — is left non-terminal: its checkpointed
// entries survive in the entry store, the ownership lease stops renewing as
// runJob unwinds, and the reconciler requeue hands the batch to a replacement
// that retries under the fence. An unfenced failed write here would terminalize
// the batch without artifact IDs and strand the checkpointed output on a row
// that is no longer eligible for recovery. A fresh batch that has not
// activated yet has nothing checkpointed, so it keeps the plain failed write.
// A secondary recover guard prevents a double-panic from crashing the process.
func (p *Processor) handlePanicRecovery(
	ctx context.Context,
	params *jobExecutionParams,
	requestCounts *openai.BatchRequestCounts,
) {
	defer func() {
		if r := recover(); r != nil {
			logr.FromContextOrDiscard(ctx).Error(fmt.Errorf("panic in handlePanicRecovery: %v\n%s", r, debug.Stack()),
				"Double panic: recovery handler itself panicked")
		}
	}()

	logger := logr.FromContextOrDiscard(ctx)

	if params == nil || params.updater == nil || params.jobItem == nil {
		logger.Error(fmt.Errorf("params, updater, or jobItem is nil"), "Cannot recover job")
		return
	}

	if params.resultSink != nil || (params.task != nil && params.task.Resumable) {
		// Leave the row non-terminal for the reconciler requeue: the
		// recovery budget bounds the retries, and the fenced finalization
		// rebuilds the artifacts from the checkpoints on the next pickup.
		logger.Info("Panic on a durable batch; leaving it non-terminal for the reconciler requeue",
			"jobID", params.jobItem.ID, "tenantID", params.jobItem.TenantID)
		metrics.RecordJobProcessed(metrics.ResultReEnqueued, metrics.ReasonSystemError)
		return
	}

	// Use context.Background() because the original ctx may be cancelled (e.g. pod shutdown)
	// and we must ensure the DB update is not short-circuited.
	// Bound the recovery with a timeout so that an unreachable DB cannot block
	// wg.Done() and release() indefinitely, which would leak the worker token
	// and eventually deadlock Stop().
	bgCtx, bgCancel := context.WithTimeout(context.Background(), panicRecoveryTimeout)
	defer bgCancel()
	bgCtx = logr.NewContext(bgCtx, logger)
	if err := p.handleFailed(bgCtx, params.updater, params.jobItem, requestCounts, params.jobInfo); err != nil {
		logger.Error(err, "Failed to mark job as failed after panic — job will remain in_progress until startup recovery runs",
			"jobID", params.jobItem.ID, "tenantID", params.jobItem.TenantID)
	}
}

// handleJobError routes an error from preProcessJob or executeJob to the appropriate handler.
// It is the single decision point for all job-level error routing.
func (p *Processor) handleJobError(ctx context.Context, params *jobExecutionParams, err error) {
	logger := logr.FromContextOrDiscard(ctx)

	// A mid-run error on the durable pipeline finalizes through the entry
	// store: partial artifacts come from the checkpoints and the terminal
	// write is fenced. The expected status is what the row holds at that
	// point: the API server flips a cancelling row before the cancel event
	// is delivered, while an expiry or system error leaves the row at
	// in_progress (activation wrote it; the in-memory dbJob is stale).
	// A resumed pickup (task.Resumable) carries its persisted entry set from
	// the earlier incarnation, so its ingestion-phase errors — before
	// recoverResumable constructs the sink — finalize through the entry set
	// too; the legacy write would drop the checkpointed artifacts' IDs and
	// leave the terminal row unrecoverable. resultSink is set only after
	// activation succeeds, so a fresh batch's ingestion-phase errors (no
	// entries, nothing checkpointed) keep the legacy routing.
	resumed := params.task != nil && params.task.Resumable
	if params.resultSink != nil || resumed {
		switch {
		case errors.Is(err, batchctx.ErrCancelled):
			if finErr := p.finalizeResumableTerminal(ctx, params, openai.BatchStatusCancelled, batch_types.ErrCodeBatchCancelled, openai.BatchStatusCancelling); finErr != nil {
				logger.Error(finErr, "Failed to finalize cancelled resumable job")
			}
		case errors.Is(err, batchctx.ErrExpired):
			if finErr := p.finalizeResumableTerminal(ctx, params, openai.BatchStatusExpired, batch_types.ErrCodeBatchExpired, openai.BatchStatusInProgress); finErr != nil {
				logger.Error(finErr, "Failed to finalize expired resumable job")
			}
		case errors.Is(err, batchctx.ErrShutdown):
			// SIGTERM received — leave the job for the orphan reconciler, same
			// as the legacy pipeline.
			logger.V(logging.INFO).Info("SIGTERM received, leaving resumable job for the reconciler")
		default:
			if finErr := p.finalizeResumableTerminal(ctx, params, openai.BatchStatusFailed, batch_types.ErrCodeBatchFailed, openai.BatchStatusInProgress); finErr != nil {
				logger.Error(finErr, "Failed to finalize failed resumable job")
			}
		}
		return
	}

	switch {
	case errors.Is(err, batchctx.ErrCancelled):
		// User-initiated cancel at any phase. requestCounts is nil for ingestion-phase cancels
		// (preProcessJob returns batchctx.ErrCancelled before execution begins) and non-nil for
		// execution-phase cancels (executeJob returns partial counts). handleCancelled
		// uses requestCounts == nil as the signal to skip partial-output upload.
		if cancelErr := p.handleCancelled(ctx, params); cancelErr != nil {
			logger.Error(cancelErr, "Failed to handle cancelled event")
			if errors.Is(cancelErr, errFinalizeFailedOver) {
				recordE2ELatency(params.jobInfo, metrics.E2EStatusFailed)
				metrics.RecordJobProcessed(metrics.ResultFailed, metrics.ReasonSystemError)
			}
		}

	case errors.Is(err, batchctx.ErrExpired):
		// SLO deadline reached. requestCounts may be nil if SLO expired during preprocessing
		// (before executeJob ran). handleExpired and UpdateExpiredStatus both tolerate nil
		// counts — the DB field is left at zero, which is correct since no requests were processed.
		if expiredErr := p.handleExpired(ctx, params.updater, params.jobItem, params.jobInfo, params.requestCounts); expiredErr != nil {
			logger.Error(expiredErr, "Failed to finalize expired job")
		}

	case errors.Is(err, batchctx.ErrShutdown):
		// SIGTERM received — leave the job in its current state for the orphan
		// reconciler to handle. The reconciler detects non-terminal jobs that
		// are not in the queue and have a stale (or missing) in-flight entry,
		// then transitions them to a terminal state (failed, cancelled, etc.).
		//
		// The in-flight entry is cleaned up by the defer at the top of runJob.
		// If the process is killed (SIGKILL) before that defer runs, the entry
		// remains and becomes stale, which the reconciler also handles.
		logger.V(logging.INFO).Info("SIGTERM received, leaving job for orphan reconciler")

	default:
		if failErr := p.handleFailed(ctx, params.updater, params.jobItem, params.requestCounts, params.jobInfo); failErr != nil {
			logger.Error(failErr, "Failed to handle failed event")
		}
	}
}

// handleExpired finalizes a job whose SLO deadline fired.
// Three cases reach here:
// (1) deadline expired during ingestion (preProcessJob) — no output files exist yet;
//
//	uploadPartialResults uploads nothing, requestCounts is nil, DB counts remain zero.
//
// (2) deadline expired before or at dispatch start — the pipeline runs with an
//
//	already-cancelled context: PreDispatcher drains all requests as batch_expired
//	errors to error.jsonl. requestCounts has Failed == Total.
//
// (3) deadline expired during execution — completed requests remain in the output file
//
//	and undispatched entries were drained as batch_expired by the dispatcher chain.
//
// In all cases, this function uploads whatever files exist and transitions the job to expired status.
// Uses a detached context so that a concurrent SIGTERM cannot abort the upload or DB write.
func (p *Processor) handleExpired(
	ctx context.Context,
	updater *StatusUpdater,
	dbJob *db.BatchItem,
	jobInfo *batch_types.JobInfo,
	requestCounts *openai.BatchRequestCounts,
) error {
	ioCtx, ioSpan := uotel.DetachedContext(ctx, "handle-expired")
	ioCtx, ioCancel := context.WithTimeout(ioCtx, finalizationTimeout)
	defer ioCancel()
	defer ioSpan.End()

	logger := logr.FromContextOrDiscard(ctx)
	logger.V(logging.INFO).Info("Job SLO expired, finalizing as expired")

	if err := updater.UpdateExpiredStatus(ioCtx, dbJob, requestCounts, "", ""); err != nil {
		logger.Error(err, "Failed to update status to expired")
		return err
	}

	// Cleanup after terminal status write: if the write failed above, the local
	// artifacts survive for startup recovery to clean up.
	p.cleanupJobArtifacts(ioCtx, dbJob.ID, dbJob.TenantID)

	setRequestCountAttrs(ctx, requestCounts)

	recordE2ELatency(jobInfo, metrics.E2EStatusExpired)
	metrics.RecordJobProcessed(metrics.ResultExpired, metrics.ReasonExpiredExecution)
	logger.V(logging.INFO).Info("Job expired handled")
	return nil
}

// handleFailed finalizes a failed job by transitioning it to failed status.
//
// Records E2E latency as failed when jobInfo is available (nil-safe).
// Uses a detached context so that a concurrent SIGTERM cannot abort the DB write.
// If the caller already has a tighter deadline (e.g. panic recovery), that deadline is respected.
func (p *Processor) handleFailed(
	ctx context.Context,
	updater *StatusUpdater,
	jobItem *db.BatchItem,
	requestCounts *openai.BatchRequestCounts,
	jobInfo *batch_types.JobInfo,
) error {
	timeout := finalizationTimeout
	if d, ok := ctx.Deadline(); ok {
		if remaining := time.Until(d); remaining > 0 && remaining < timeout {
			timeout = remaining
		}
	}

	ioCtx, ioSpan := uotel.DetachedContext(ctx, "handle-failed")
	ioCtx, ioCancel := context.WithTimeout(ioCtx, timeout)
	defer ioCancel()
	defer ioSpan.End()

	logger := logr.FromContextOrDiscard(ctx)

	if err := updater.UpdateFailedStatus(ioCtx, jobItem, requestCounts, "", ""); err != nil {
		logger.Error(err, "Failed to update status to failed")
		return err
	}

	p.cleanupJobArtifacts(ioCtx, jobItem.ID, jobItem.TenantID)

	setRequestCountAttrs(ctx, requestCounts)

	recordE2ELatency(jobInfo, metrics.E2EStatusFailed)
	metrics.RecordJobProcessed(metrics.ResultFailed, metrics.ReasonSystemError)
	logger.V(logging.INFO).Info("Job failed handled")
	return nil
}

// handleOwnershipLost is invoked when a fenced progress write reveals this
// processor lost ownership of the job (a reclaimer bumped its epoch). It must
// NOT write a terminal status or upload results: the job now belongs to another
// processor, which owns the terminal transition. It only releases local
// resources and records a metric, leaving the DB state to the new owner and the
// orphan reconciler.
func (p *Processor) handleOwnershipLost(ctx context.Context, params *jobExecutionParams) {
	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("Lost job ownership (progress write fenced out by a reclaimer); aborting without a terminal write",
		"jobID", params.jobInfo.JobID)

	// Local cleanup only. A detached context so a concurrent abort cannot
	// interrupt the local artifact removal.
	ioCtx, ioCancel := context.WithTimeout(context.WithoutCancel(ctx), finalizationTimeout)
	defer ioCancel()
	p.cleanupJobArtifacts(ioCtx, params.jobItem.ID, params.jobItem.TenantID)

	metrics.RecordJobProcessed(metrics.ResultLostOwnership, metrics.ReasonNone)
}

// recordE2ELatency records the full lifecycle duration from batch submission to terminal state.
// No-op if jobInfo, BatchJob, or CreatedAt is missing (e.g. DB conversion failure).
func recordE2ELatency(jobInfo *batch_types.JobInfo, status string) {
	if jobInfo == nil || jobInfo.BatchJob == nil || jobInfo.BatchJob.CreatedAt == 0 {
		return
	}
	createdAt := time.Unix(jobInfo.BatchJob.CreatedAt, 0)
	metrics.RecordJobE2ELatency(time.Since(createdAt), status)
}

func setRequestCountAttrs(ctx context.Context, counts *openai.BatchRequestCounts) {
	if counts == nil {
		return
	}
	uotel.SetAttr(ctx,
		attribute.Int64(uotel.AttrRequestTotal, counts.Total),
		attribute.Int64(uotel.AttrRequestCompleted, counts.Completed),
		attribute.Int64(uotel.AttrRequestFailed, counts.Failed),
	)
}
