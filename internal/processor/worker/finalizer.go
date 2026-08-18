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
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/go-logr/logr"
	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/failpoint"
	"go.opentelemetry.io/otel/attribute"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

// uploadFileAndStoreFileRecord uploads a job output or error file to shared storage and creates a file
// record in the database. Returns the assigned file ID, or an empty string if the file is empty.
// fileType distinguishes output files from error files for upload, metrics, and tracing.
func (p *Processor) uploadFileAndStoreFileRecord(
	ctx context.Context,
	jobInfo *batch_types.JobInfo,
	dbJob *db.BatchItem,
	fileType metrics.FileType,
) (string, error) {
	var fileName string
	var fileSize int64
	var err error
	var attrKey string

	fileID := ucom.NewFileID()

	if fileType == metrics.FileTypeOutput {
		fileName = jobOutputStorageName(jobInfo.JobID)
		storageName := ucom.FileStorageName(fileID, fileName)
		fileSize, err = p.uploadOutputFile(ctx, jobInfo, storageName)
		attrKey = uotel.AttrOutputFileID
	} else {
		fileName = jobErrorStorageName(jobInfo.JobID)
		storageName := ucom.FileStorageName(fileID, fileName)
		fileSize, err = p.uploadErrorFile(ctx, jobInfo, storageName)
		attrKey = uotel.AttrErrorFileID
	}
	if err != nil {
		return "", err
	}
	if fileSize == 0 {
		return "", nil
	}

	failpoint.Inject("processor/after-blob-store")

	uotel.SetAttr(ctx, attribute.String(attrKey, fileID))
	if err := p.storeFileRecord(ctx, fileID, fileName, jobInfo.TenantID, fileSize, dbJob.Tags); err != nil {
		return "", err
	}
	return fileID, nil
}

// finalizationTimeout bounds how long finalizeJob waits for file uploads and DB
// writes before giving up. Storage and DB clients have their own timeouts, but
// this provides an explicit application-level bound — same pattern as
// panicRecoveryTimeout. Declared as var (not const) so tests can shorten it.
var finalizationTimeout = 5 * time.Minute

// finalizeJob performs finalization: uploads output and error files to shared storage,
// creates file records in the database, and updates job status to completed.
func (p *Processor) finalizeJob(
	ctx context.Context,
	updater *StatusUpdater,
	dbJob *db.BatchItem,
	jobInfo *batch_types.JobInfo,
	requestCounts *openai.BatchRequestCounts,
) error {
	logger := logr.FromContextOrDiscard(ctx)
	logger.V(logging.INFO).Info("Starting finalization: finalizing job")

	// ioCtx is detached from ctx so that SIGTERM cannot abort file uploads or the
	// final status write. DetachedContext preserves a span link back to the parent
	// process-batch trace while severing cancellation propagation.
	// finalizationTimeout provides an explicit upper bound so that an unreachable
	// storage/DB cannot block the worker token indefinitely.
	ioCtx, ioSpan := uotel.DetachedContext(ctx, "finalize")
	ioCtx, ioCancel := context.WithTimeout(ioCtx, finalizationTimeout)
	defer ioCancel()
	defer ioSpan.End()

	// in_progress → finalizing
	// Written before file uploads so the API server can reject cancel requests once
	// finalization has begun, narrowing the cancel-vs-complete race window.
	if err := updater.UpdatePersistentStatus(ioCtx, dbJob, openai.BatchStatusFinalizing, requestCounts, nil); err != nil {
		return fmt.Errorf("failed to update job status to finalizing: %w", err)
	}

	// Per the OpenAI batch spec, output_file_id and error_file_id are both optional:
	// output_file_id is omitted when all requests failed; error_file_id is omitted when no
	// requests failed. We skip uploading and recording empty files accordingly.
	//
	// The two uploads are fully independent (different local files, different S3 keys,
	// different DB records). A failure in one must not discard the other's file ID —
	// otherwise a transient error-file upload failure would orphan an already-uploaded
	// output file. Both uploads run to completion; if any fails, the job is marked
	// failed with surviving file IDs preserved.
	var outputFileID, errorFileID string
	var outputUploadErr, errorUploadErr error
	var uploadWG sync.WaitGroup
	uploadWG.Add(2)
	go func() {
		defer uploadWG.Done()
		outputFileID, outputUploadErr = p.uploadFileAndStoreFileRecord(ioCtx, jobInfo, dbJob, metrics.FileTypeOutput)
	}()
	go func() {
		defer uploadWG.Done()
		errorFileID, errorUploadErr = p.uploadFileAndStoreFileRecord(ioCtx, jobInfo, dbJob, metrics.FileTypeError)
	}()
	uploadWG.Wait()

	if outputUploadErr != nil {
		logger.Error(outputUploadErr, "Failed to upload output file during finalization")
	}
	if errorUploadErr != nil {
		logger.Error(errorUploadErr, "Failed to upload error file during finalization")
	}

	// If any upload failed, the job cannot be marked completed or cancelled — that would
	// expose a partial-artifact terminal state to the user. Instead, write failed status
	// with whatever file IDs survived so the successfully-uploaded artifact remains
	// reachable via the API.
	if outputUploadErr != nil || errorUploadErr != nil {
		if failErr := updater.UpdateFailedStatus(ioCtx, dbJob, requestCounts, outputFileID, errorFileID); failErr != nil {
			return fmt.Errorf("upload failed and fallback status write also failed: %w", failErr)
		}
		return fmt.Errorf("upload failure during finalization: %w", errFinalizeFailedOver)
	}

	failpoint.Inject("processor/after-file-records")

	// Best-effort: honour a user cancel that arrived during finalization.
	// Only an explicit user cancel routes here — context.Cause is batchctx.ErrCancelled
	// exclusively for user cancellation; SLO expiry and SIGTERM are other causes.
	if errors.Is(context.Cause(ctx), batchctx.ErrCancelled) {
		logger.V(logging.INFO).Info("Cancel requested during finalization; finalizing as cancelled")
		if err := updater.UpdateCancelledStatus(ioCtx, dbJob, requestCounts, outputFileID, errorFileID); err != nil {
			logger.Error(err, "Failed to update cancelled status, falling back to failed with file IDs preserved")
			if failErr := updater.UpdateFailedStatus(ioCtx, dbJob, requestCounts, outputFileID, errorFileID); failErr != nil {
				return fmt.Errorf("failed to update job status to cancelled (%w) and fallback to failed also failed: %w", err, failErr)
			}
			return fmt.Errorf("cancelled status write failed: %w", errFinalizeFailedOver)
		}
		setRequestCountAttrs(ctx, requestCounts)
		return batchctx.ErrCancelled
	}

	// finalizing → completed
	if err := updater.UpdateCompletedStatus(ioCtx, dbJob, requestCounts, outputFileID, errorFileID); err != nil {
		logger.Error(err, "Failed to update job status to completed, falling back to failed with file IDs preserved")
		if failErr := updater.UpdateFailedStatus(ioCtx, dbJob, requestCounts, outputFileID, errorFileID); failErr != nil {
			return fmt.Errorf("failed to update job status to completed (%w) and fallback to failed also failed: %w", err, failErr)
		}
		return fmt.Errorf("completed status write failed: %w", errFinalizeFailedOver)
	}

	setRequestCountAttrs(ctx, requestCounts)

	logger.V(logging.INFO).Info("Finalization completed", "outputFileID", outputFileID, "errorFileID", errorFileID)
	return nil
}

// uploadOutputFile uploads the local output file to shared storage.
// Returns the file size; returns 0 without error if the file is empty (all requests failed).
// Per the OpenAI batch spec, output_file_id is optional and may be omitted when there are no
// successful requests.
func (p *Processor) uploadOutputFile(
	ctx context.Context,
	jobInfo *batch_types.JobInfo,
	fileName string,
) (int64, error) {
	filePath, err := p.jobOutputFilePath(jobInfo.JobID, jobInfo.TenantID)
	if err != nil {
		return 0, err
	}
	return p.uploadJobFile(ctx, filePath, fileName, jobInfo.TenantID)
}

// uploadErrorFile uploads the local error file to shared storage.
// Returns the file size; returns 0 without error if the file is empty (no errors occurred).
// Per the OpenAI batch spec, error_file_id is optional and may be omitted when no requests failed.
func (p *Processor) uploadErrorFile(
	ctx context.Context,
	jobInfo *batch_types.JobInfo,
	fileName string,
) (int64, error) {
	filePath, err := p.jobErrorFilePath(jobInfo.JobID, jobInfo.TenantID)
	if err != nil {
		return 0, err
	}
	return p.uploadJobFile(ctx, filePath, fileName, jobInfo.TenantID)
}

// uploadJobFile uploads a local file to shared storage.
// Returns the file size; returns 0 without error if the file does not exist or is empty.
// Retry logic is handled by the retryclient decorator wrapping the file storage client.
func (p *Processor) uploadJobFile(
	ctx context.Context,
	filePath, fileName, tenantID string,
) (int64, error) {
	stat, err := os.Stat(filePath)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("failed to stat file %s: %w", filePath, err)
	}
	if stat.Size() == 0 {
		return 0, nil
	}

	f, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file %s for upload: %w", filePath, err)
	}
	defer f.Close()

	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		return 0, fmt.Errorf("failed to get folder name: %w", err)
	}

	fileMeta, err := p.files.storage.Store(ctx, fileName, folderName, 0, 0, f)
	if err != nil {
		return 0, fmt.Errorf("failed to upload file %s: %w", fileName, err)
	}

	return fileMeta.Size, nil
}

// storeFileRecord creates a file metadata record in the database.
// Used for both output and error files.
// If the batch has a user-provided output_expires_after_seconds tag, it takes
// precedence over the config default (DefaultOutputExpirationSeconds).
func (p *Processor) storeFileRecord(
	ctx context.Context,
	fileID, fileName, tenantID string,
	size int64,
	batchTags db.Tags,
) error {
	now := time.Now().Unix()

	expiresAt := p.resolveOutputExpiration(now, batchTags)
	var expiresAtPtr *int64
	if expiresAt > 0 {
		expiresAtPtr = &expiresAt
	}

	fileObj := &openai.FileObject{
		ID:        fileID,
		Bytes:     size,
		CreatedAt: now,
		ExpiresAt: expiresAtPtr,
		Filename:  fileName,
		Object:    "file",
		Purpose:   openai.FileObjectPurposeBatchOutput,
		Status:    openai.FileObjectStatusProcessed,
	}
	fileItem, err := converter.FileToDBItem(fileObj, tenantID, db.Tags{})
	if err != nil {
		return fmt.Errorf("failed to convert file to db item: %w", err)
	}

	if err := p.files.db.DBStore(ctx, fileItem); err != nil {
		return fmt.Errorf("failed to store file record: %w", err)
	}
	return nil
}

// resolveOutputExpiration returns the ExpiresAt timestamp for an output or error file.
// Priority: user-provided output_expires_after_seconds tag > config default.
// Returns 0 (no expiration) if neither is set.
func (p *Processor) resolveOutputExpiration(now int64, batchTags db.Tags) int64 {
	if s, ok := batchTags[batch_types.TagOutputExpiresAfterSeconds]; ok {
		if ttl, err := strconv.ParseInt(s, 10, 64); err == nil && ttl > 0 {
			return now + ttl
		}
	}
	if ttl := p.cfg.DefaultOutputExpirationSeconds; ttl > 0 {
		return now + ttl
	}
	return 0
}
