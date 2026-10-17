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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batch_utils"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	recoveryActionFinalized = "finalized"
	recoveryActionCancelled = "cancelled"
	recoveryActionFailed    = "failed"
	recoveryActionExpired   = "expired"
	recoveryActionReEnqueue = "re_enqueued"
	recoveryActionCleanedUp = "cleaned_up"
	recoveryActionError     = "error"

	recoveryUnknownStatus openai.BatchStatus = "unknown"
)

// recoveryResult carries the outcome of a recover* function back to recoverJob
// so that cleanup, metrics, and fallback logic are handled in a single place.
type recoveryResult struct {
	action      string // recoveryAction* constant for RecordStartupRecovery
	e2eStatus   string // metrics.E2EStatus* constant for recordE2ELatency
	statusLabel string // DB status label for RecordStartupRecovery (e.g. "finalizing")
	counts      *openai.BatchRequestCounts
	cancelPhase string // non-empty → RecordCancellation is called
}

// recoverOwnedJobs claims every non-terminal job owned by this processor
// (via processor_id) and recovers them. This handles both container-level crashes
// (where emptyDir survives) and pod-level restarts within a StatefulSet (where
// the processor identity is preserved across restarts).
//
// The claim bumps the fencing epoch and the recovery attempt counter of each
// job in one atomic UPDATE. recoverJob re-reads the row afterwards, so recovery
// writes at the new epoch and a zombie holding the previous one is fenced out.
//
// Resumable rows are handed back to the queue (recoverResumableOwned) instead
// of recovered in place: the polling loop's unified dequeue re-claims them
// with a fresh lease and routes them through the same resume path a
// reconciler requeue converges on.
//
// Runs once at startup before the polling loop.
func (p *Processor) recoverOwnedJobs(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)

	tasks, err := p.poller.claimOwned(ctx)
	if err != nil {
		return fmt.Errorf("claim owned jobs: %w", err)
	}

	if len(tasks) == 0 {
		logger.V(logging.DEBUG).Info("Startup recovery: no owned jobs found")
		return nil
	}

	logger.V(logging.INFO).Info("Startup recovery: found owned jobs", "count", len(tasks))

	var grp errgroup.Group
	grp.SetLimit(p.cfg.Concurrency.Recovery)

	for _, task := range tasks {
		grp.Go(func() error {
			jlogger := logger.WithValues("jobId", task.ID, "recoveryAttempts", task.RecoveryAttempts)
			jctx := logr.NewContext(ctx, jlogger)
			if task.Resumable {
				if err := p.recoverResumableOwned(jctx, task); err != nil {
					jlogger.Error(err, "Startup recovery: failed to hand resumable job to the polling loop")
					return err
				}
				return nil
			}
			if task.RecoveryAttempts > maxRecoveryAttempts {
				if failErr := p.recoverExhausted(jctx, task.ID); failErr != nil {
					jlogger.Error(failErr, "Startup recovery: failed to fail job past its recovery budget")
				}
				return nil
			}
			if recoverErr := p.recoverJob(jctx, task.ID); recoverErr != nil {
				jlogger.Error(recoverErr, "Startup recovery: failed to recover owned job")
			}
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return fmt.Errorf("recover owned jobs: %w", err)
	}
	return nil
}

// maxRecoveryAttempts bounds how often a batch may be recovered — claimed
// again after losing or handing back its owner — before it is failed with
// whatever partial results survived. One restart costs one attempt (the
// startup claim), and each GC requeue of a dead or wedged owner counts one.
const maxRecoveryAttempts = 3

// recoverResumableOwned handles a resumable row claimed at startup. A
// processor without the durable pipeline keeps the fail-closed behavior (a
// mixed sync/resumable topology is unsupported). A durable processor hands
// the row back through the queue: the polling loop's unified dequeue
// re-claims it with a fresh lease and routes it through the resume path,
// which owns every terminal decision — recovery budget included.
func (p *Processor) recoverResumableOwned(ctx context.Context, task *db.BatchJobPriority) error {
	logger := logr.FromContextOrDiscard(ctx)

	dbItem, err := p.poller.fetchJobItemByID(ctx, task.ID)
	if err != nil {
		return err
	}
	if dbItem == nil {
		logger.Info("Startup recovery: resumable job not found in DB, cleaning up stale directory")
		metrics.RecordStartupRecovery(string(recoveryUnknownStatus), recoveryActionCleanedUp)
		p.cleanupStaleJobDir(ctx, task.ID)
		return nil
	}
	// The claim at startup already counted this restart against the
	// recovery budget (PQClaimOwned), so the requeue must not add a second
	// attempt: one restart costs one attempt, not two.
	if err := p.resumable.RequeueResumableOrphan(ctx, task.ID, dbItem.ProcessorID, dbItem.Epoch, false, false); err != nil {
		return fmt.Errorf("requeue resumable job: %w", err)
	}
	metrics.RecordStartupRecovery(string(p.getJobStatus(dbItem)), recoveryActionReEnqueue)
	logger.V(logging.INFO).Info("Startup recovery: requeued resumable job for the polling loop")
	return nil
}

// recoverExhausted fails a job whose recovery budget is spent.
func (p *Processor) recoverExhausted(ctx context.Context, jobID string) error {
	dbItem, err := p.poller.fetchJobItemByID(ctx, jobID)
	if err != nil {
		return err
	}
	if dbItem == nil {
		return nil
	}
	jobInfo, _ := batch_utils.FromDBItemToJobInfoObject(dbItem)
	logr.FromContextOrDiscard(ctx).Info("Startup recovery: recovery budget exhausted, failing job")
	return p.recoverWithFailed(ctx, dbItem, fmt.Errorf("recovery attempts exhausted after %d", maxRecoveryAttempts), nil, jobInfo)
}

func (p *Processor) recoverJob(ctx context.Context, jobID string) error {
	err := p.recoverJobOnce(ctx, jobID)
	if !errors.Is(err, db.ErrConflict) {
		return err
	}

	logr.FromContextOrDiscard(ctx).Info("Startup recovery: state changed during recovery, retrying", "jobId", jobID)
	return p.recoverJobOnce(ctx, jobID)
}

// recoverJobOnce is the single routing point for one startup recovery attempt.
// Each recover* function returns (*recoveryResult, nil) on success, or
// (*recoveryResult, error) on failure — where the result may be non-nil
// (carrying partial file IDs for fallback) or nil when no partial state was
// created. It handles fallback, cleanup, and metrics recording uniformly.
func (p *Processor) recoverJobOnce(ctx context.Context, jobID string) error {
	logger := logr.FromContextOrDiscard(ctx)

	dbItem, err := p.poller.fetchJobItemByID(ctx, jobID)
	// DB unreachable — can't read status or mark as failed. Leave workdir on disk so the
	// next container restart retries. If the pod is evicted (emptyDir destroyed), this job
	// becomes an orphan that only an external entity can detect.
	if err != nil {
		metrics.RecordStartupRecovery(string(recoveryUnknownStatus), recoveryActionError)
		return err
	}

	// Job not in DB — can't update status, but we can clean up the stale directory.
	// tenantID is unknown (directory uses SHA256 hash), so we glob for the jobID.
	if dbItem == nil {
		logger.Info("Startup recovery: job not found in DB, cleaning up stale directory")
		metrics.RecordStartupRecovery(string(recoveryUnknownStatus), recoveryActionCleanedUp)
		p.cleanupStaleJobDir(ctx, jobID)
		return nil
	}

	jobInfo, err := batch_utils.FromDBItemToJobInfoObject(dbItem)
	if err != nil {
		logger.Error(err, "Startup recovery: failed to convert DB item")
		return p.recoverWithFailed(ctx, dbItem, err, nil, nil)
	}

	status := jobInfo.BatchJob.Status
	logger.V(logging.INFO).Info("Startup recovery: recovering job", "status", string(status))

	var result *recoveryResult
	switch status {
	case openai.BatchStatusValidating:
		result, err = p.recoverReEnqueue(ctx, dbItem, jobInfo, false)

	case openai.BatchStatusInProgress, openai.BatchStatusFinalizing, openai.BatchStatusCancelling:
		// A non-resumable row in an execution status predates the unified
		// pipeline (which activates every batch before execution); its
		// progress is not recoverable, so fail it closed.
		logger.Info("Startup recovery: legacy non-resumable row in an execution status, failing closed",
			"status", string(status))
		return p.recoverWithFailed(ctx, dbItem, fmt.Errorf("non-resumable %s row is not recoverable", status), nil, jobInfo)

	default:
		if status.IsTerminal() {
			logger.V(logging.INFO).Info("Startup recovery: job already terminal, cleaning up")
			metrics.RecordStartupRecovery(string(status), recoveryActionCleanedUp)
			p.cleanupJobArtifacts(ctx, dbItem.ID, dbItem.TenantID)
			return nil
		}
		logger.Info("Startup recovery: unexpected status, marking as failed", "status", string(status))
		return p.recoverWithFailed(ctx, dbItem, nil, nil, jobInfo)
	}

	if err != nil {
		logger.Error(err, "Startup recovery: primary action failed, falling back to failed")
		return p.recoverWithFailed(ctx, dbItem, err, result, jobInfo)
	}

	// Primary action succeeded — record metrics and clean up.
	p.cleanupJobArtifacts(ctx, dbItem.ID, dbItem.TenantID)
	if result.e2eStatus != "" {
		recordE2ELatency(jobInfo, result.e2eStatus)
	}
	metrics.RecordStartupRecovery(result.statusLabel, result.action)
	if result.cancelPhase != "" {
		metrics.RecordCancellation(result.cancelPhase)
	}
	logger.V(logging.INFO).Info("Startup recovery: completed", "action", result.action)
	return nil
}

// ---------------------------------------------------------------------------
// Phase-specific recovery functions
//
// Each returns (*recoveryResult, nil) on success. On failure, the result may
// be non-nil (carrying file IDs/counts for recoverWithFailed to preserve in
// the terminal failed status) or nil when no partial state was created
// (e.g. SLO extraction or re-enqueue failed before any uploads).
// ---------------------------------------------------------------------------

// recoverReEnqueue re-enqueues a job for retry. Used for both in_progress (no
// partial output) and validating statuses. When resetToValidating is true
// (in_progress path), the DB status is first reset to validating so the next
// worker runs the full ingestion→execution flow.
func (p *Processor) recoverReEnqueue(ctx context.Context, dbItem *db.BatchItem, jobInfo *batch_types.JobInfo, resetToValidating bool) (*recoveryResult, error) {
	statusLabel := string(openai.BatchStatusValidating)
	if resetToValidating {
		statusLabel = string(openai.BatchStatusInProgress)
	}

	slo, err := p.extractRecoverySLO(dbItem, jobInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to recover SLO for re-enqueue: %w", err)
	}

	if time.Now().After(*slo) {
		return p.recoverExpired(ctx, dbItem, jobInfo, statusLabel)
	}

	if resetToValidating {
		if err := p.updater.UpdatePersistentStatus(ctx, dbItem, openai.BatchStatusValidating, nil, slo); err != nil {
			return nil, fmt.Errorf("failed to reset status to validating: %w", err)
		}
	}

	task, err := p.buildRecoveryTask(dbItem, slo, openai.BatchStatusValidating)
	if err != nil {
		return nil, fmt.Errorf("failed to build recovery task: %w", err)
	}
	if err := p.poller.enqueueOne(ctx, task); err != nil {
		return nil, fmt.Errorf("failed to re-enqueue job: %w", err)
	}

	return &recoveryResult{
		action:      recoveryActionReEnqueue,
		statusLabel: statusLabel,
	}, nil
}

// recoverExpired transitions a job to expired status (a re-enqueue whose SLO
// deadline already passed).
func (p *Processor) recoverExpired(ctx context.Context, dbItem *db.BatchItem, jobInfo *batch_types.JobInfo, statusLabel string) (*recoveryResult, error) {
	counts := p.extractRequestCounts(dbItem)

	if err := p.updater.UpdateExpiredStatus(ctx, dbItem, counts, "", ""); err != nil {
		return &recoveryResult{
			counts: counts,
		}, fmt.Errorf("failed to update expired status: %w", err)
	}

	return &recoveryResult{
		action:      recoveryActionExpired,
		e2eStatus:   metrics.E2EStatusExpired,
		statusLabel: statusLabel,
		counts:      counts,
	}, nil
}

// ---------------------------------------------------------------------------
// Resumable pickup
//
// A requeued resumable task converges on runJob from two entry points: the
// unified dequeue (GC-reconciler requeues) and the startup requeue
// (recoverResumableOwned). The pickup routing owns every terminal decision;
// everything else resumes through the normal durable pipeline.
// ---------------------------------------------------------------------------

// routeResumedPickup applies the terminal-bound routing for a requeued
// resumable task before any execution: a cancelling row finalizes as
// cancelled, a row past its SLO as expired, and a row past its recovery
// budget as failed — each with partial artifacts (non-terminal entries
// become not-executed error lines). It returns true when the job was
// terminalized (or the attempt failed and the row was left for the
// reconciler); false means execution should resume.
func (p *Processor) routeResumedPickup(ctx context.Context, params *jobExecutionParams) bool {
	logger := logr.FromContextOrDiscard(ctx)

	// The row was fetched fresh at dequeue, so its status is the fence's
	// expected status: the pickup terminalizes what it just read.
	expected := p.getJobStatus(params.jobItem)

	var status openai.BatchStatus
	var notExecutedCode batch_types.BatchErrorCode
	switch {
	case expected == openai.BatchStatusCancelling:
		status, notExecutedCode = openai.BatchStatusCancelled, batch_types.ErrCodeBatchCancelled
	case batch_utils.IsJobExpired(params.task):
		status, notExecutedCode = openai.BatchStatusExpired, batch_types.ErrCodeBatchExpired
	case params.jobItem.RecoveryAttempts > maxRecoveryAttempts:
		status, notExecutedCode = openai.BatchStatusFailed, batch_types.ErrCodeBatchFailed
	default:
		return false
	}

	logger.V(logging.INFO).Info("Resumable pickup routed to terminal finalization", "status", status)
	if finErr := p.finalizeResumableTerminal(ctx, params, status, notExecutedCode, expected); finErr != nil {
		logger.Error(finErr, "Failed to terminalize resumed pickup; leaving it for the reconciler")
	}
	return true
}

// recoverResumable rebuilds the durable execution state of a requeued batch:
// it verifies the persisted entry set still matches the manifest re-derived
// from the immutable input file, then returns a result sink at the claimed
// epoch plus the resume seed (checkpointed counts and terminal request IDs)
// for the progress tracker and the source skip set. The resumed execution
// flows through the normal pipeline.
func (p *Processor) recoverResumable(
	ctx context.Context,
	params *jobExecutionParams,
	manifest *requestManifest,
) (*batchResultStore, *db.ResumableResumeState, error) {
	entries, err := p.listResumableEntries(ctx, params.jobItem.ID)
	if err != nil {
		return nil, nil, fmt.Errorf("list batch entries: %w", err)
	}
	if err := checkManifestIntegrity(manifest, entries); err != nil {
		return nil, nil, fmt.Errorf("batch entry set diverged from the input file: %w", err)
	}

	seed := &db.ResumableResumeState{}
	for _, e := range entries {
		switch e.Status {
		case db.ResumableEntryCompleted:
			seed.Completed++
			seed.TerminalRequestIDs = append(seed.TerminalRequestIDs, e.RequestID)
		case db.ResumableEntryFailed:
			seed.Failed++
			seed.TerminalRequestIDs = append(seed.TerminalRequestIDs, e.RequestID)
		case db.ResumableEntryInFlight:
			seed.InFlight++
			// The submitter's per-processor result route holds the entry's
			// unacknowledged delivery; the resuming owner recovers it from
			// there instead of re-running the inference.
			if e.SubmittedBy != "" {
				if seed.InFlightSubmittedBy == nil {
					seed.InFlightSubmittedBy = make(map[string]string)
				}
				seed.InFlightSubmittedBy[e.RequestID] = e.SubmittedBy
			}
		default:
			seed.Pending++
		}
	}

	sink, err := newBatchResultStore(p.files.storage, p.resumable, params.jobItem.ID, params.jobInfo.TenantID, p.processorID, params.jobItem.Epoch)
	if err != nil {
		return nil, nil, err
	}
	return sink, seed, nil
}

// checkManifestIntegrity verifies that the persisted entry set matches the
// manifest re-derived from the immutable input file: same length, and each
// entry's identity triple (request ID, ordinal, custom ID) equal at its
// index. An activation-rejected entry must appear as failed with zero
// attempts — rejected entries are inserted directly as failed while only
// MarkEntryInFlight increments attempts — so allowlist drift between
// incarnations is caught in both directions.
func checkManifestIntegrity(manifest *requestManifest, entries []*db.ResumableEntry) error {
	if len(entries) != len(manifest.Entries) {
		return fmt.Errorf("entry set has %d entries, manifest has %d", len(entries), len(manifest.Entries))
	}
	for i, e := range entries {
		m := manifest.Entries[i]
		if e.RequestID != m.RequestID || e.Ordinal != m.Ordinal || e.CustomID != m.CustomID {
			return fmt.Errorf("entry %d identity mismatch: persisted (%s, %d, %s), manifest (%s, %d, %s)",
				i, e.RequestID, e.Ordinal, e.CustomID, m.RequestID, m.Ordinal, m.CustomID)
		}
		rejected := e.Status == db.ResumableEntryFailed && e.Attempts == 0
		if rejected != m.Rejected {
			return fmt.Errorf("entry %d (%s) rejected-set mismatch: persisted %v, manifest %v",
				i, e.RequestID, rejected, m.Rejected)
		}
	}
	return nil
}

// listResumableEntries returns the full entry set in ordinal order.
func (p *Processor) listResumableEntries(ctx context.Context, jobID string) ([]*db.ResumableEntry, error) {
	var all []*db.ResumableEntry
	afterOrdinal := int64(-1)
	for {
		entries, err := p.resumable.ListBatchEntries(ctx, jobID, afterOrdinal, resumableEntriesPageSize)
		if err != nil {
			return nil, err
		}
		if len(entries) == 0 {
			return all, nil
		}
		all = append(all, entries...)
		afterOrdinal = entries[len(entries)-1].Ordinal
		if len(entries) < resumableEntriesPageSize {
			return all, nil
		}
	}
}

// ---------------------------------------------------------------------------
// Fallback
// ---------------------------------------------------------------------------

// recoverWithFailed is the terminal fallback: mark the job as failed so it
// doesn't stay stuck. If result is non-nil, its counts and file IDs are
// preserved in the failed status.
func (p *Processor) recoverWithFailed(ctx context.Context, dbItem *db.BatchItem, cause error, result *recoveryResult, jobInfo *batch_types.JobInfo) error {
	logger := logr.FromContextOrDiscard(ctx)

	var counts *openai.BatchRequestCounts
	if result != nil {
		counts = result.counts
	}

	if err := p.updater.UpdateFailedStatus(ctx, dbItem, counts, "", ""); err != nil {
		logger.Error(err, "Startup recovery: failed to mark job as failed (job will remain stuck)")
		return err
	}

	p.cleanupJobArtifacts(ctx, dbItem.ID, dbItem.TenantID)
	recordE2ELatency(jobInfo, metrics.E2EStatusFailed)
	metrics.RecordStartupRecovery(string(p.getJobStatus(dbItem)), recoveryActionFailed)
	logger.Info("Startup recovery: marked as failed (recovery action failed)", "cause", cause)
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// extractRequestCounts parses RequestCounts from the DB status JSON.
func (p *Processor) extractRequestCounts(dbItem *db.BatchItem) *openai.BatchRequestCounts {
	if len(dbItem.Status) == 0 {
		return nil
	}
	var info openai.BatchStatusInfo
	if err := json.Unmarshal(dbItem.Status, &info); err != nil {
		return nil
	}
	if info.RequestCounts.Total == 0 {
		return nil
	}
	return &info.RequestCounts
}

// extractRecoverySLO recovers the exact SLO deadline for queue re-enqueue.
func (p *Processor) extractRecoverySLO(dbItem *db.BatchItem, jobInfo *batch_types.JobInfo) (*time.Time, error) {
	if dbItem != nil && dbItem.Priority > 0 {
		slo := time.UnixMicro(dbItem.Priority).UTC()
		return &slo, nil
	}

	if jobInfo.BatchJob.ExpiresAt != nil {
		slo := time.Unix(*jobInfo.BatchJob.ExpiresAt, 0).UTC()
		return &slo, nil
	}

	return nil, fmt.Errorf("missing recovery SLO for job %s", dbItem.ID)
}

// buildRecoveryTask constructs a BatchJobPriority for re-enqueue.
func (p *Processor) buildRecoveryTask(dbItem *db.BatchItem, slo *time.Time, expectedStatus openai.BatchStatus) (*db.BatchJobPriority, error) {
	if slo == nil || slo.IsZero() {
		return nil, fmt.Errorf("missing recovery SLO for job %s", dbItem.ID)
	}

	task := &db.BatchJobPriority{
		ID:             dbItem.ID,
		SLO:            slo.UTC(),
		Epoch:          dbItem.Epoch,
		ProcessorID:    dbItem.ProcessorID,
		ExpectedStatus: string(expectedStatus),
	}
	return task, nil
}

// getJobStatus parses the status from a BatchItem's Status JSON.
func (p *Processor) getJobStatus(dbItem *db.BatchItem) openai.BatchStatus {
	if len(dbItem.Status) == 0 {
		return recoveryUnknownStatus
	}
	var info openai.BatchStatusInfo
	if err := json.Unmarshal(dbItem.Status, &info); err != nil {
		return recoveryUnknownStatus
	}
	return info.Status
}

// cleanupStaleJobDir removes a job directory when tenantID is unknown (job not in DB).
// Scans all tenant hash directories under workdir to find the matching jobID.
func (p *Processor) cleanupStaleJobDir(ctx context.Context, jobID string) {
	logger := logr.FromContextOrDiscard(ctx)
	pattern := filepath.Join(p.cfg.WorkDir, "*", jobsDirName, jobID)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		logger.Error(err, "Failed to glob for stale job directory")
		return
	}
	for _, dir := range matches {
		if err := os.RemoveAll(dir); err != nil {
			logger.Error(err, "Failed to remove stale job directory", "path", dir)
		} else {
			logger.V(logging.INFO).Info("Removed stale job directory", "path", dir)
		}
	}
}
