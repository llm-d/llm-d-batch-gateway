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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"strconv"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"

	"github.com/google/uuid"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

// resumableEntriesPageSize bounds one ListBatchEntries page during artifact
// assembly (keyset pagination by ordinal).
const resumableEntriesPageSize = 500

// finalizeResumableJob finalizes a durable batch: it streams every entry's
// checkpointed result object into the output and error artifacts and
// transitions the batch to completed. Both the finalizing and terminal
// writes are fenced — the finalizing write on the persisted resumable flag,
// the terminal write on ownership — so only the incarnation that still owns
// the activated batch may terminalize it, and a crashed incarnation's
// committed artifacts make re-finalization idempotent. Artifact assembly
// and the fenced terminal write are retryable work: any failure returns the
// error without a fallback terminal write, leaving the row non-terminal so
// the reconciler requeue retries the finalization under a current lease —
// an unfenced failed write would discard the checkpointed work and let an
// owner that lost its lease terminalize the batch anyway.
func (p *Processor) finalizeResumableJob(
	ctx context.Context,
	updater *StatusUpdater,
	dbJob *db.BatchItem,
	jobInfo *batch_types.JobInfo,
) error {
	logger := logr.FromContextOrDiscard(ctx)
	logger.V(logging.INFO).Info("Starting finalization: resumable job")

	// Detached so a concurrent SIGTERM cannot abort artifact assembly or the
	// final status write; bounded by the configured finalization timeout.
	ioCtx, ioSpan := uotel.DetachedContext(ctx, "finalize-resumable")
	ioCtx, ioCancel := context.WithTimeout(ioCtx, p.cfg.Resumable.FinalizeTimeout)
	defer ioCancel()
	defer ioSpan.End()

	resumable := true
	dbJob.ExpectedResumable = &resumable

	// in_progress → finalizing, before artifact assembly so the API server
	// can reject cancel requests once finalization has begun.
	if err := updater.UpdatePersistentStatus(ioCtx, dbJob, openai.BatchStatusFinalizing, nil, nil); err != nil {
		return fmt.Errorf("failed to update job status to finalizing: %w", err)
	}

	counts, outputFileID, errorFileID, err := p.assembleResumableArtifacts(ioCtx, dbJob, jobInfo, "")
	if err != nil {
		// Artifact assembly is retryable work: a committed artifact is
		// re-derived and verified on the retry, so leave the row non-terminal
		// for the reconciler requeue instead of an unfenced failed write that
		// discards the checkpointed work.
		return fmt.Errorf("assemble resumable artifacts: %w", err)
	}

	// Best-effort: honour a user cancel that arrived during finalization —
	// same contract as the legacy finalizer.
	if errors.Is(context.Cause(ctx), batchctx.ErrCancelled) {
		logger.V(logging.INFO).Info("Cancel requested during finalization; finalizing as cancelled")
		if err := p.finalizeResumableStatus(ioCtx, updater, dbJob, openai.BatchStatusFinalizing, openai.BatchStatusCancelled, counts, outputFileID, errorFileID); err != nil {
			// The row stays non-terminal: the reconciler requeue retries the
			// finalization, and a pickup routes a cancelling row back here.
			return fmt.Errorf("cancelled status write failed: %w", err)
		}
		setRequestCountAttrs(ctx, counts)
		return batchctx.ErrCancelled
	}

	// finalizing → completed
	if err := p.finalizeResumableStatus(ioCtx, updater, dbJob, openai.BatchStatusFinalizing, openai.BatchStatusCompleted, counts, outputFileID, errorFileID); err != nil {
		// The row stays non-terminal for the reconciler requeue: the fenced
		// write is the only terminal transition, and re-finalization is
		// idempotent (deterministic artifact keys, existing-artifact
		// verification).
		return fmt.Errorf("completed status write failed: %w", err)
	}

	setRequestCountAttrs(ctx, counts)
	logger.V(logging.INFO).Info("Resumable finalization completed", "outputFileID", outputFileID, "errorFileID", errorFileID)
	return nil
}

// finalizeResumableAs terminalizes a durable batch that must not resume
// execution — a requeued row that is cancelling, past its SLO, or past its
// recovery budget, or an activated batch whose run aborted (cancel, expiry,
// system error). It streams the checkpointed artifacts (non-terminal entries
// become not-executed error lines) and writes the given terminal status.
// expected is the row status the finalizer believes it is terminalizing —
// the fenced terminal write is refused when the row disagrees, so a stale
// belief cannot overwrite a row another writer moved. Cleanup and metrics
// belong to the caller. Any failure — artifact assembly or the fenced
// terminal write — returns the error with the row left non-terminal: the
// reconciler requeues it, so the finalization is retried under a current
// lease instead of degraded by an unfenced failed write.
func (p *Processor) finalizeResumableAs(
	ctx context.Context,
	updater *StatusUpdater,
	dbJob *db.BatchItem,
	jobInfo *batch_types.JobInfo,
	status openai.BatchStatus,
	notExecutedCode batch_types.BatchErrorCode,
	expected openai.BatchStatus,
) error {
	logger := logr.FromContextOrDiscard(ctx)
	logger.V(logging.INFO).Info("Terminalizing resumable job without resuming execution", "status", status)

	ioCtx, ioSpan := uotel.DetachedContext(ctx, "finalize-resumable-as")
	ioCtx, ioCancel := context.WithTimeout(ioCtx, p.cfg.Resumable.FinalizeTimeout)
	defer ioCancel()
	defer ioSpan.End()

	resumable := true
	dbJob.ExpectedResumable = &resumable

	counts, outputFileID, errorFileID, err := p.assembleResumableArtifacts(ioCtx, dbJob, jobInfo, notExecutedCode)
	if err != nil {
		// Artifact assembly is retryable work: leave the row non-terminal for
		// the reconciler requeue instead of an unfenced failed write that
		// discards the checkpointed work.
		return fmt.Errorf("assemble resumable artifacts: %w", err)
	}

	if err := p.finalizeResumableStatus(ioCtx, updater, dbJob, expected, status, counts, outputFileID, errorFileID); err != nil {
		return fmt.Errorf("update job status to %s: %w", status, err)
	}

	setRequestCountAttrs(ctx, counts)
	logger.V(logging.INFO).Info("Resumable job terminalized", "status", status,
		"outputFileID", outputFileID, "errorFileID", errorFileID)
	return nil
}

// finalizeResumableTerminal terminalizes a durable batch and records the
// outcome: finalization as the given status with partial artifacts (via
// notExecutedCode for non-terminal entries), local-artifact cleanup once the
// row is terminal, and the per-status metrics. It is the shared tail of the
// pickup routing and the durable runJob error branches. An error leaves the
// row non-terminal for the reconciler requeue, so the finalization is
// retried under a current lease.
func (p *Processor) finalizeResumableTerminal(
	ctx context.Context,
	params *jobExecutionParams,
	status openai.BatchStatus,
	notExecutedCode batch_types.BatchErrorCode,
	expected openai.BatchStatus,
) error {
	err := p.finalizeResumableAs(ctx, params.updater, params.jobItem, params.jobInfo, status, notExecutedCode, expected)

	// Cleanup after the terminal write: if it failed, the local files survive
	// for the reconciler requeue to retry the finalization.
	if err == nil {
		p.cleanupJobArtifacts(context.WithoutCancel(ctx), params.jobItem.ID, params.jobItem.TenantID)
	}
	if err != nil {
		// Not logged here: every caller already logs the returned error.
		return err
	}

	switch status {
	case openai.BatchStatusCancelled:
		recordE2ELatency(params.jobInfo, metrics.E2EStatusCancelled)
		metrics.RecordCancellation(metrics.CancelPhaseInProgress)
		metrics.RecordJobProcessed(metrics.ResultSuccess, metrics.ReasonNone)
	case openai.BatchStatusExpired:
		recordE2ELatency(params.jobInfo, metrics.E2EStatusExpired)
		metrics.RecordJobProcessed(metrics.ResultExpired, metrics.ReasonExpiredExecution)
	default:
		recordE2ELatency(params.jobInfo, metrics.E2EStatusFailed)
		metrics.RecordJobProcessed(metrics.ResultFailed, metrics.ReasonSystemError)
	}
	return nil
}

// finalizeResumableStatus performs the fenced terminal transition of a
// durable batch: it builds the terminal status document from dbJob's current
// status and hands it to FinalizeResumableBatch, which commits the document
// atomically with the ownership release (resumable cleared, owner and lease
// dropped). expected is the row status the transition is fenced on. A
// conflict propagates wrapped, so runJob can route it to the
// ownership-lost path.
func (p *Processor) finalizeResumableStatus(
	ctx context.Context,
	updater *StatusUpdater,
	dbJob *db.BatchItem,
	expected, terminal openai.BatchStatus,
	counts *openai.BatchRequestCounts,
	outputFileID, errorFileID string,
) error {
	doc, err := updater.BuildStatus(dbJob, terminal, counts, nil, withFileIDs(outputFileID, errorFileID))
	if err != nil {
		return err
	}
	if err := p.resumable.FinalizeResumableBatch(ctx, &db.ResumableFinalization{
		BatchID:        dbJob.ID,
		ProcessorID:    p.processorID,
		Epoch:          dbJob.Epoch,
		ExpectedStatus: string(expected),
		Status:         doc,
	}); err != nil {
		return fmt.Errorf("fenced terminal status write (%s → %s): %w", expected, terminal, err)
	}
	dbJob.Status = doc

	// The entry objects are baked into the final artifacts and cascade with
	// the row; purge the result prefix so the objects do not outlive the
	// batch. Best-effort — the GC's prefix purge backstops a process that
	// dies between the terminal write and this call.
	p.purgeResultObjects(ctx, dbJob.ID, dbJob.TenantID)
	return nil
}

// purgeResultObjects deletes the batch's checkpointed result objects.
func (p *Processor) purgeResultObjects(ctx context.Context, jobID, tenantID string) {
	logger := logr.FromContextOrDiscard(ctx)
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		logger.Error(err, "Failed to resolve result prefix for purge")
		return
	}
	prefix := "results/" + jobID + "/"
	if _, err := p.files.storage.DeletePrefix(ctx, folder, prefix); err != nil {
		logger.Error(err, "Failed to purge result objects", "prefix", prefix)
	}
}

// assembleResumableArtifacts streams every entry's result object, in input
// order, into the batch's output and error artifacts and returns the final
// request counts and file IDs. Each page's objects are prefetched and
// verified (size and SHA-256) with bounded concurrency — resumable.
// finalize_concurrency parallel fetches — and then written in ordinal order.
// Artifact file IDs are derived from the batch and the ownership epoch, so
// a re-finalization after a requeue (which bumps the epoch) writes fresh
// keys instead of colliding with an earlier generation's committed
// artifacts.
//
// notExecutedCode renders non-terminal entries as synthetic not-executed
// error lines, for terminalizing a batch that must not resume execution; an
// empty code demands every entry be terminal (the completed path).
func (p *Processor) assembleResumableArtifacts(
	ctx context.Context,
	dbJob *db.BatchItem,
	jobInfo *batch_types.JobInfo,
	notExecutedCode batch_types.BatchErrorCode,
) (*openai.BatchRequestCounts, string, string, error) {
	jobID := jobInfo.JobID
	folder, err := ucom.GetFolderNameByTenantID(jobInfo.TenantID)
	if err != nil {
		return nil, "", "", fmt.Errorf("resolve artifact folder: %w", err)
	}

	outputFileID := stableFileID(jobID, metrics.FileTypeOutput, dbJob.Epoch)
	output := &artifactWriter{
		storage: p.files.storage,
		folder:  folder,
		key:     ucom.FileStorageName(outputFileID, jobOutputStorageName(jobID)),
		fileID:  outputFileID,
	}
	errorFileID := stableFileID(jobID, metrics.FileTypeError, dbJob.Epoch)
	errorW := &artifactWriter{
		storage: p.files.storage,
		folder:  folder,
		key:     ucom.FileStorageName(errorFileID, jobErrorStorageName(jobID)),
		fileID:  errorFileID,
	}
	// Abort is a no-op for a writer that never opened.
	defer func() {
		_ = output.abort(ctx)
		_ = errorW.abort(ctx)
	}()

	counts := &openai.BatchRequestCounts{}
	afterOrdinal := int64(-1)
	for {
		entries, err := p.resumable.ListBatchEntries(ctx, jobID, afterOrdinal, resumableEntriesPageSize)
		if err != nil {
			return nil, "", "", fmt.Errorf("list batch entries: %w", err)
		}
		if len(entries) == 0 {
			break
		}
		// Prefetch the page's result objects with bounded concurrency; the
		// writes below stay in ordinal order.
		fetched := make([][]byte, len(entries))
		g, gctx := errgroup.WithContext(ctx)
		g.SetLimit(p.cfg.Resumable.FinalizeConcurrency)
		for i, e := range entries {
			if !e.Status.IsTerminal() {
				continue
			}
			g.Go(func() error {
				buf, err := fetchResult(gctx, p.files.storage, folder, e)
				if err != nil {
					return err
				}
				fetched[i] = buf
				return nil
			})
		}
		if err := g.Wait(); err != nil {
			return nil, "", "", err
		}

		for i, e := range entries {
			afterOrdinal = e.Ordinal
			counts.Total++
			var w *artifactWriter
			switch e.Status {
			case db.ResumableEntryCompleted:
				counts.Completed++
				w = output
			case db.ResumableEntryFailed:
				counts.Failed++
				// A failed entry without an error object preserved a non-2xx
				// HTTP response — an output line, matching the sync
				// collector's routing.
				w = output
				if e.ErrorCode != "" {
					w = errorW
				}
			default:
				if notExecutedCode == "" {
					// Execution cannot succeed with unresolved entries; the
					// checkpointer resolves everything it drains.
					return nil, "", "", fmt.Errorf("entry %s is still %s", e.RequestID, e.Status)
				}
				counts.Failed++
				if err := errorW.writeNotExecuted(ctx, e, notExecutedCode); err != nil {
					return nil, "", "", err
				}
				continue
			}
			if err := w.writeResult(ctx, fetched[i]); err != nil {
				return nil, "", "", err
			}
		}
		if len(entries) < resumableEntriesPageSize {
			break
		}
	}

	outputFileID, err = output.commit(ctx, p, dbJob, jobInfo, jobOutputStorageName(jobID))
	if err != nil {
		return nil, "", "", err
	}
	errorFileID, err = errorW.commit(ctx, p, dbJob, jobInfo, jobErrorStorageName(jobID))
	if err != nil {
		// The output artifact may already be committed with a file record;
		// surface the partial results so the failed-status fallback can
		// reference them instead of dropping an uploaded artifact.
		return counts, outputFileID, "", err
	}
	return counts, outputFileID, errorFileID, nil
}

// artifactWriter lazily opens and finally commits one finalization artifact.
// An artifact with no entries is never created; its file ID stays empty.
//
// A same-generation retry of a finalization that already committed the
// artifact (an ambiguous terminal-write response within one ownership
// epoch — the entry set is frozen once finalization began) re-derives the
// same key, so OpenStream refuses with ErrFileExists: the writer then runs
// in existing mode — no bytes are written, but what this incarnation would
// write is hashed and counted, then verified against the stored object at
// commit. A retry from a later generation derives a different (epoch-scoped)
// key and writes its own artifact; the earlier generation's object is a
// loser that no terminal document references.
type artifactWriter struct {
	storage  filesapi.BatchFilesClient
	folder   string
	key      string
	fileID   string
	writer   filesapi.StreamWriter
	existing bool
	hasher   hash.Hash
	count    int64
}

// Write implements io.Writer, routing to the open stream or, in existing
// mode, to the digest accumulator.
func (w *artifactWriter) Write(p []byte) (int, error) {
	if w.existing {
		w.hasher.Write(p)
		w.count += int64(len(p))
		return len(p), nil
	}
	return w.writer.Write(p)
}

// ensure opens the stream writer on first use, or enters existing mode when
// the artifact is already committed.
func (w *artifactWriter) ensure(ctx context.Context) error {
	if w.writer != nil || w.existing {
		return nil
	}
	writer, err := w.storage.OpenStream(ctx, w.key, w.folder)
	if errors.Is(err, filesapi.ErrFileExists) {
		w.existing = true
		w.hasher = sha256.New()
		return nil
	}
	if err != nil {
		return fmt.Errorf("open artifact stream %s: %w", w.key, err)
	}
	w.writer = writer
	return nil
}

// fetchResult retrieves one entry's result object and verifies it against
// the entry row's recorded size and SHA-256.
func fetchResult(ctx context.Context, storage filesapi.BatchFilesClient, folder string, e *db.ResumableEntry) ([]byte, error) {
	reader, _, err := storage.Retrieve(ctx, e.ResultKey, folder)
	if err != nil {
		return nil, fmt.Errorf("retrieve result %s: %w", e.ResultKey, err)
	}
	defer reader.Close()

	buf, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read result %s: %w", e.ResultKey, err)
	}
	if int64(len(buf)) != e.ResultBytes {
		return nil, fmt.Errorf("result %s failed integrity check: got %d bytes, want %d", e.ResultKey, len(buf), e.ResultBytes)
	}
	sum := sha256.Sum256(buf)
	if hex.EncodeToString(sum[:]) != e.ResultSHA256 {
		return nil, fmt.Errorf("result %s failed integrity check: digest mismatch", e.ResultKey)
	}
	return buf, nil
}

// writeResult writes one prefetched, verified result line into the artifact.
func (w *artifactWriter) writeResult(ctx context.Context, line []byte) error {
	if err := w.ensure(ctx); err != nil {
		return err
	}
	if _, err := w.Write(line); err != nil {
		return fmt.Errorf("write artifact %s: %w", w.key, err)
	}
	return nil
}

// writeNotExecuted renders a non-terminal entry as a synthetic not-executed
// error line, so a batch terminalized without resuming still produces a
// complete artifact set.
func (w *artifactWriter) writeNotExecuted(ctx context.Context, e *db.ResumableEntry, code batch_types.BatchErrorCode) error {
	if err := w.ensure(ctx); err != nil {
		return err
	}
	lineBytes, err := json.Marshal(&outputLine{
		ID:       e.RequestID,
		CustomID: e.CustomID,
		Error: &outputError{
			Code:    string(code),
			Message: code.Message(),
		},
	})
	if err != nil {
		return fmt.Errorf("marshal not-executed line for %s: %w", e.RequestID, err)
	}
	lineBytes = append(lineBytes, '\n')
	if _, err := w.Write(lineBytes); err != nil {
		return fmt.Errorf("write not-executed line for %s: %w", e.RequestID, err)
	}
	return nil
}

// commit finalizes the artifact and records its file ID; a writer that never
// opened contributes no file. The file record is ensured rather than stored:
// on re-finalization the earlier incarnation may have committed it between
// its artifact commit and its terminal write.
func (w *artifactWriter) commit(ctx context.Context, p *Processor, dbJob *db.BatchItem, jobInfo *batch_types.JobInfo, fileName string) (string, error) {
	if w.existing {
		if err := w.verifyExisting(ctx); err != nil {
			return "", err
		}
		if err := p.ensureFileRecord(ctx, w.fileID, fileName, jobInfo.TenantID, w.count, dbJob.Tags); err != nil {
			return "", err
		}
		return w.fileID, nil
	}
	if w.writer == nil {
		return "", nil
	}
	md, err := w.writer.Commit(ctx)
	if err != nil {
		return "", fmt.Errorf("commit artifact %s: %w", w.key, err)
	}
	w.writer = nil
	if err := p.ensureFileRecord(ctx, w.fileID, fileName, jobInfo.TenantID, md.Size, dbJob.Tags); err != nil {
		return "", err
	}
	return w.fileID, nil
}

// verifyExisting re-reads the committed artifact and checks it matches the
// bytes this incarnation would have written. The artifact key is derived
// from the batch, file type, and ownership epoch, so a match proves the
// stored object holds this batch's artifact rather than a colliding leftover.
func (w *artifactWriter) verifyExisting(ctx context.Context) error {
	reader, _, err := w.storage.Retrieve(ctx, w.key, w.folder)
	if err != nil {
		return fmt.Errorf("retrieve committed artifact %s: %w", w.key, err)
	}
	defer reader.Close()

	hasher := sha256.New()
	size, err := io.Copy(hasher, reader)
	if err != nil {
		return fmt.Errorf("stream committed artifact %s: %w", w.key, err)
	}
	if size != w.count || !bytes.Equal(hasher.Sum(nil), w.hasher.Sum(nil)) {
		return fmt.Errorf("committed artifact %s does not match re-derived content: got %d bytes, want %d", w.key, size, w.count)
	}
	return nil
}

// abort discards the write; a no-op after a successful commit.
func (w *artifactWriter) abort(ctx context.Context) error {
	if w.writer == nil {
		return nil
	}
	err := w.writer.Abort(ctx)
	w.writer = nil
	return err
}

// stableFileID derives an artifact file ID from the batch, file type, and
// ownership epoch, so each ownership generation writes its own artifact
// keys. A re-finalization after a requeue — which always bumps the epoch
// (dequeue, GC reclaim, restart claim) — targets a fresh key and can never
// be blocked by, or clobber, an artifact committed by an earlier generation
// whose terminal write failed; the entry set is frozen within one
// generation once finalization began, so a same-generation retry re-derives
// the same key and bytes (the writer's existing-artifact verification).
// Artifacts committed by losing generations are never referenced by a
// terminal document; their file records expire and the file GC collects
// them.
func stableFileID(batchID string, fileType metrics.FileType, epoch int64) string {
	u := uuid.NewSHA1(uuid.NameSpaceOID, []byte(batchID+"\x00"+string(fileType)+"\x00"+strconv.FormatInt(epoch, 10)))
	return "file_" + u.String()
}
