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
	"github.com/llm-d/llm-d-batch-gateway/internal/util/failpoint"
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
	action       string // recoveryAction* constant for RecordStartupRecovery
	e2eStatus    string // metrics.E2EStatus* constant for recordE2ELatency
	statusLabel  string // DB status label for RecordStartupRecovery (e.g. "finalizing")
	counts       *openai.BatchRequestCounts
	outputFileID string
	errorFileID  string
	cancelPhase  string // non-empty → RecordCancellation is called
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
// Runs once at startup before the polling loop.
func (p *Processor) recoverOwnedJobs(ctx context.Context) {
	logger := logr.FromContextOrDiscard(ctx)

	tasks, err := p.poller.claimOwned(ctx)
	if err != nil {
		logger.Error(err, "Startup recovery: failed to claim owned jobs")
		return
	}

	if len(tasks) == 0 {
		logger.V(logging.DEBUG).Info("Startup recovery: no owned jobs found")
		return
	}

	logger.V(logging.INFO).Info("Startup recovery: found owned jobs", "count", len(tasks))

	failpoint.Inject("processor/recovery-found-job")

	var grp errgroup.Group
	grp.SetLimit(p.cfg.Concurrency.Recovery)

	for _, task := range tasks {
		grp.Go(func() error {
			jlogger := logger.WithValues("jobId", task.ID, "recoveryAttempts", task.RecoveryAttempts)
			jctx := logr.NewContext(ctx, jlogger)
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
	_ = grp.Wait()
}

// maxRecoveryAttempts bounds how often one ownership may recover a job before
// it is failed with whatever partial results survived.
const maxRecoveryAttempts = 3

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

// recoverJob is the single routing point for startup recovery. Each recover*
// function returns (*recoveryResult, nil) on success, or (*recoveryResult, error)
// on failure — where the result may be non-nil (carrying partial file IDs for
// fallback) or nil when no partial state was created. recoverJob handles
// fallback, cleanup, and metrics recording uniformly.
func (p *Processor) recoverJob(ctx context.Context, jobID string) error {
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
	case openai.BatchStatusFinalizing:
		result, err = p.recoverFinalizing(ctx, dbItem, jobInfo)

	case openai.BatchStatusCancelling:
		result, err = p.recoverCancelling(ctx, dbItem, jobInfo)

	case openai.BatchStatusInProgress:
		result, err = p.recoverInProgress(ctx, dbItem, jobInfo)

	case openai.BatchStatusValidating:
		result, err = p.recoverReEnqueue(ctx, dbItem, jobInfo, false)

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

// recoverFinalizing completes a job that crashed during the upload phase.
// Output files should be complete on disk since execution finished before finalizing.
func (p *Processor) recoverFinalizing(ctx context.Context, dbItem *db.BatchItem, jobInfo *batch_types.JobInfo) (*recoveryResult, error) {
	counts := p.extractRequestCounts(dbItem)
	outputFileID, errorFileID := p.uploadPartialResults(ctx, jobInfo, dbItem)

	result := &recoveryResult{
		action:       recoveryActionFinalized,
		e2eStatus:    metrics.E2EStatusCompleted,
		statusLabel:  string(openai.BatchStatusFinalizing),
		counts:       counts,
		outputFileID: outputFileID,
		errorFileID:  errorFileID,
	}

	if err := p.updater.UpdateCompletedStatus(ctx, dbItem, counts, outputFileID, errorFileID); err != nil {
		return result, err
	}
	return result, nil
}

// recoverCancelling completes a cancellation that was interrupted by crash.
func (p *Processor) recoverCancelling(ctx context.Context, dbItem *db.BatchItem, jobInfo *batch_types.JobInfo) (*recoveryResult, error) {
	counts := p.extractRequestCounts(dbItem)
	outputFileID, errorFileID := p.uploadPartialResults(ctx, jobInfo, dbItem)

	cancelPhase := metrics.CancelPhaseQueued
	if counts != nil {
		cancelPhase = metrics.CancelPhaseInProgress
	}

	result := &recoveryResult{
		action:       recoveryActionCancelled,
		e2eStatus:    metrics.E2EStatusCancelled,
		statusLabel:  string(openai.BatchStatusCancelling),
		counts:       counts,
		outputFileID: outputFileID,
		errorFileID:  errorFileID,
		cancelPhase:  cancelPhase,
	}

	if err := p.updater.UpdateCancelledStatus(ctx, dbItem, counts, outputFileID, errorFileID); err != nil {
		return result, err
	}
	return result, nil
}

// recoverInProgress handles a job that crashed during inference execution.
// If the output file exists and has non-zero size, inference made meaningful progress
// — upload partial results and mark as failed.
// If output is empty or absent, inference barely started — re-enqueue for retry.
func (p *Processor) recoverInProgress(ctx context.Context, dbItem *db.BatchItem, jobInfo *batch_types.JobInfo) (*recoveryResult, error) {
	logger := logr.FromContextOrDiscard(ctx)

	hasOutput, err := p.outputFileHasContent(dbItem.ID, dbItem.TenantID)
	if err != nil {
		logger.Error(err, "Startup recovery: failed to check output file, treating as empty")
	}

	if hasOutput {
		return p.recoverInProgressWithPartial(ctx, dbItem, jobInfo)
	}
	return p.recoverReEnqueue(ctx, dbItem, jobInfo, true)
}

func (p *Processor) recoverInProgressWithPartial(ctx context.Context, dbItem *db.BatchItem, jobInfo *batch_types.JobInfo) (*recoveryResult, error) {
	counts := p.extractRequestCounts(dbItem)
	outputFileID, errorFileID := p.uploadPartialResults(ctx, jobInfo, dbItem)

	result := &recoveryResult{
		action:       recoveryActionFailed,
		e2eStatus:    metrics.E2EStatusFailed,
		statusLabel:  string(openai.BatchStatusInProgress),
		counts:       counts,
		outputFileID: outputFileID,
		errorFileID:  errorFileID,
	}

	if err := p.updater.UpdateFailedStatus(ctx, dbItem, counts, outputFileID, errorFileID); err != nil {
		return result, err
	}
	return result, nil
}

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

	task, err := p.buildRecoveryTask(dbItem, slo)
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

// recoverExpired uploads any surviving partial files and transitions a job to expired status.
func (p *Processor) recoverExpired(ctx context.Context, dbItem *db.BatchItem, jobInfo *batch_types.JobInfo, statusLabel string) (*recoveryResult, error) {
	counts := p.extractRequestCounts(dbItem)
	outputFileID, errorFileID := p.uploadPartialResults(ctx, jobInfo, dbItem)

	if err := p.updater.UpdateExpiredStatus(ctx, dbItem, counts, outputFileID, errorFileID); err != nil {
		return &recoveryResult{
			counts:       counts,
			outputFileID: outputFileID,
			errorFileID:  errorFileID,
		}, fmt.Errorf("failed to update expired status: %w", err)
	}

	return &recoveryResult{
		action:       recoveryActionExpired,
		e2eStatus:    metrics.E2EStatusExpired,
		statusLabel:  statusLabel,
		counts:       counts,
		outputFileID: outputFileID,
		errorFileID:  errorFileID,
	}, nil
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
	var outputFileID, errorFileID string
	if result != nil {
		counts = result.counts
		outputFileID = result.outputFileID
		errorFileID = result.errorFileID
	}

	if err := p.updater.UpdateFailedStatus(ctx, dbItem, counts, outputFileID, errorFileID); err != nil {
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

// outputFileHasContent checks whether the output.jsonl file exists and has content.
func (p *Processor) outputFileHasContent(jobID, tenantID string) (bool, error) {
	outputPath, err := p.jobOutputFilePath(jobID, tenantID)
	if err != nil {
		return false, err
	}
	stat, err := os.Stat(outputPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return stat.Size() > 0, nil
}

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
func (p *Processor) buildRecoveryTask(dbItem *db.BatchItem, slo *time.Time) (*db.BatchJobPriority, error) {
	if slo == nil || slo.IsZero() {
		return nil, fmt.Errorf("missing recovery SLO for job %s", dbItem.ID)
	}

	task := &db.BatchJobPriority{
		ID:  dbItem.ID,
		SLO: slo.UTC(),
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
