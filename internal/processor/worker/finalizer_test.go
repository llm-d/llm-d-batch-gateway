package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
)

// --- resolveOutputExpiration ---

func TestResolveOutputExpiration_UserTagOverridesConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 7776000 // 90 days
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{batch_types.TagOutputExpiresAfterSeconds: "3600"}

	got := p.resolveOutputExpiration(now, tags)
	want := now + 3600
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (user tag should override config)", got, want)
	}
}

func TestResolveOutputExpiration_FallsBackToConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{}

	got := p.resolveOutputExpiration(now, tags)
	want := now + 86400
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (should fall back to config)", got, want)
	}
}

func TestResolveOutputExpiration_ZeroWhenNeitherSet(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 0
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{}

	got := p.resolveOutputExpiration(now, tags)
	if got != 0 {
		t.Fatalf("resolveOutputExpiration = %d, want 0 (no expiration)", got)
	}
}

func TestResolveOutputExpiration_InvalidTagFallsBackToConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{batch_types.TagOutputExpiresAfterSeconds: "not-a-number"}

	got := p.resolveOutputExpiration(now, tags)
	want := now + 86400
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (invalid tag should fall back to config)", got, want)
	}
}

func TestResolveOutputExpiration_ZeroTagFallsBackToConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{batch_types.TagOutputExpiresAfterSeconds: "0"}

	got := p.resolveOutputExpiration(now, tags)
	want := now + 86400
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (zero tag should fall back to config)", got, want)
	}
}

func TestResolveOutputExpiration_NilTags(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)

	got := p.resolveOutputExpiration(now, nil)
	want := now + 86400
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (nil tags should fall back to config)", got, want)
	}
}

// --- uploadFileAndStoreFileRecord ---

func TestUploadFileAndStoreFileRecord_StorageKeyAndDBFilename(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	cfg.DefaultOutputExpirationSeconds = 86400

	mock := &failNTimesFilesClient{failCount: 0}
	fileDB := newMockFileDBClient()
	batchDB := newMockBatchDBClient()

	clients := &clientset.Clientset{
		File:    mock,
		FileDB:  fileDB,
		BatchDB: batchDB,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-storage-key"
	tenantID := "tenant-1"
	jobInfo := setupJobWithOutputFile(t, cfg, jobID, tenantID)
	ctx := testLoggerCtx(t)
	dbJob := seedDBJob(t, batchDB, jobID)

	fileID, err := p.uploadFileAndStoreFileRecord(ctx, jobInfo, dbJob, metrics.FileTypeOutput)
	if err != nil {
		t.Fatalf("uploadFileAndStoreFileRecord returned error: %v", err)
	}
	if fileID == "" {
		t.Fatal("expected non-empty fileID")
	}

	// Verify storage key uses FileStorageName format: <fileID>.jsonl
	wantStorageKey := fileID + ".jsonl"
	if mock.lastFileName != wantStorageKey {
		t.Errorf("storage key = %q, want %q", mock.lastFileName, wantStorageKey)
	}

	// Verify DB filename preserves the batch output format
	items, _, _, err := fileDB.DBGet(ctx, &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{fileID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	fileObj, err := converter.DBItemToFile(items[0])
	if err != nil {
		t.Fatalf("DBItemToFile: %v", err)
	}
	wantDBFilename := "batch_output_" + jobID + ".jsonl"
	if fileObj.Filename != wantDBFilename {
		t.Errorf("DB filename = %q, want %q", fileObj.Filename, wantDBFilename)
	}
}

// --- storeFileRecord ---

func TestStoreOutputFileRecord_Success(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	fileDB := newMockFileDBClient()
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		FileDB: fileDB,
	})

	ctx := testLoggerCtx(t)
	tags := db.Tags{batch_types.TagOutputExpiresAfterSeconds: "3600"}

	err := p.storeFileRecord(ctx, "file_abc", "output.jsonl", "tenant-1", 1024, tags)
	if err != nil {
		t.Fatalf("storeFileRecord returned error: %v", err)
	}

	items, _, _, err := fileDB.DBGet(ctx, &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{"file_abc"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}

	if items[0].ID != "file_abc" {
		t.Fatalf("stored file ID = %q, want %q", items[0].ID, "file_abc")
	}
	if items[0].Purpose != string(openai.FileObjectPurposeBatchOutput) {
		t.Fatalf("stored purpose = %q, want %q", items[0].Purpose, openai.FileObjectPurposeBatchOutput)
	}
}

func TestStoreFileRecord_NoExpiration_NilExpiresAt(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 0
	fileDB := newMockFileDBClient()
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		FileDB: fileDB,
	})

	ctx := testLoggerCtx(t)
	err := p.storeFileRecord(ctx, "file_no_exp", "output.jsonl", "tenant-1", 1024, nil)
	if err != nil {
		t.Fatalf("storeFileRecord: %v", err)
	}

	items, _, _, err := fileDB.DBGet(ctx, &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{"file_no_exp"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	fileObj, err := converter.DBItemToFile(items[0])
	if err != nil {
		t.Fatalf("DBItemToFile: %v", err)
	}
	if fileObj.ExpiresAt != nil {
		t.Fatalf("ExpiresAt = %v, want nil when resolveOutputExpiration returns 0", fileObj.ExpiresAt)
	}
}

func TestFinalizeJob_CancelRequested_FinalizesCancelled(t *testing.T) {
	ctx := testLoggerCtx(t)
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()

	jobID := "job-late-cancel"
	tenantID := "tenant-1"

	// Simulate a late cancel that was already persisted by the API server,
	// while the worker still has a stale in-memory in_progress job item.
	dbJob := seedDBJob(t, dbClient, jobID)
	// We need to set TenantID and update so DBGet can find it by TenantID
	dbJob.TenantID = tenantID
	dbJob.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusCancelling})
	// DBStore again to ensure it's indexed correctly with TenantID in the mock DB
	if err := dbClient.DBStore(ctx, dbJob); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	// Double check it's actually there
	jobsCheck, _, _, errCheck := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}, TenantID: tenantID}}, true, 0, 1)
	if errCheck != nil || len(jobsCheck) == 0 {
		t.Fatalf("Failed to seed job for test: %v", errCheck)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  newMockFileDBClient(),
		File:    &failNTimesFilesClient{failCount: 0},
		Status:  statusClient,
		Queue:   mockdb.NewMockBatchPriorityQueueClient(),
	}
	p := mustNewProcessor(t, cfg, clients)
	p.poller = NewPoller(clients.Queue, dbClient)

	updater := NewStatusUpdater(dbClient, statusClient, 86400)

	// Setup job dir manually to reuse the pre-seeded DB client
	jobDir, _ := p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte("test output\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	counts := &openai.BatchRequestCounts{Total: 1, Completed: 1, Failed: 0}

	// The worker's in-memory job state is stale and still says in_progress.
	staleJob := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}

	// Simulate that the worker observed the cancel request before writing the final status:
	// the abort context carries the batchctx.ErrCancelled cause.
	cancelledCtx, cancelFn := context.WithCancelCause(ctx)
	cancelFn(batchctx.ErrCancelled)

	err := p.finalizeJob(cancelledCtx, updater, staleJob, jobInfo, counts)
	if !errors.Is(err, batchctx.ErrCancelled) {
		t.Fatalf("expected batchctx.ErrCancelled, got: %v", err)
	}

	// Verify that the final status written to the DB is cancelled, not completed.
	jobsFinal, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(jobsFinal) == 0 {
		t.Fatalf("DBGet failed: err=%v len=%d", err, len(jobsFinal))
	}

	var finalStatus openai.BatchStatusInfo
	if err := json.Unmarshal(jobsFinal[0].Status, &finalStatus); err != nil {
		t.Fatalf("Unmarshal status: %v", err)
	}

	if finalStatus.Status != openai.BatchStatusCancelled {
		t.Fatalf("expected final status to be %s, got %s", openai.BatchStatusCancelled, finalStatus.Status)
	}
}

// TestFinalizeJob_ShutdownDuringFinalization_CompletesNotCancelled verifies that a SIGTERM
// (batchctx.ErrShutdown cause) during finalization does NOT route the job to cancelled.
// Only the batchctx.ErrCancelled cause takes the cancel path; the job must transition to completed.
func TestFinalizeJob_ShutdownDuringFinalization_CompletesNotCancelled(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()

	jobID := "job-shutdown-during-final"
	tenantID := "tenant-1"

	dbJob := seedDBJob(t, dbClient, jobID)
	dbJob.TenantID = tenantID
	dbJob.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	setupCtx := testLoggerCtx(t)
	if err := dbClient.DBStore(setupCtx, dbJob); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  newMockFileDBClient(),
		File:    &failNTimesFilesClient{failCount: 0},
		Status:  statusClient,
		Queue:   mockdb.NewMockBatchPriorityQueueClient(),
	}
	p := mustNewProcessor(t, cfg, clients)
	p.poller = NewPoller(clients.Queue, dbClient)

	updater := NewStatusUpdater(dbClient, statusClient, 86400)

	jobDir, _ := p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte("test output\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	staleJob := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}
	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	counts := &openai.BatchRequestCounts{Total: 1, Completed: 1, Failed: 0}

	// Simulate SIGTERM: the abort context carries the ErrShutdown cause (not ErrCancelled).
	shutdownCtx, shutdownCancel := context.WithCancelCause(testLoggerCtx(t))
	shutdownCancel(batchctx.ErrShutdown) // SIGTERM fires

	err := p.finalizeJob(shutdownCtx, updater, staleJob, jobInfo, counts)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	jobsFinal, _, _, getErr := dbClient.DBGet(testLoggerCtx(t), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if getErr != nil || len(jobsFinal) == 0 {
		t.Fatalf("DBGet failed: err=%v len=%d", getErr, len(jobsFinal))
	}
	var finalStatus openai.BatchStatusInfo
	if err := json.Unmarshal(jobsFinal[0].Status, &finalStatus); err != nil {
		t.Fatalf("Unmarshal status: %v", err)
	}
	if finalStatus.Status != openai.BatchStatusCompleted {
		t.Fatalf("expected completed (SIGTERM must not trigger user-cancel path), got %s", finalStatus.Status)
	}
}

// --- failover file-ID preservation tests ---

// TestFinalizeJob_CompletedWriteFails_FallsBackToFailedWithFileIDs verifies that when
// uploads succeed but UpdateCompletedStatus fails, finalizeJob falls back to
// UpdateFailedStatus with file IDs preserved (not empty). This prevents the orphan-file
// scenario where real output exists in storage but the batch object has no references.
func TestFinalizeJob_CompletedWriteFails_FallsBackToFailedWithFileIDs(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	innerDB := newMockBatchDBClient()
	failDB := &failOnStatusDB{
		inner:      innerDB,
		failStatus: openai.BatchStatusCompleted,
		failErr:    errors.New("injected: completed write failed"),
	}
	statusClient := mockdb.NewMockBatchStatusClient()

	jobID := "job-failover-completed"
	tenantID := "tenant-1"

	dbJob := seedDBJob(t, innerDB, jobID)
	dbJob.TenantID = tenantID

	clients := &clientset.Clientset{
		BatchDB: failDB,
		FileDB:  newMockFileDBClient(),
		File:    &failNTimesFilesClient{failCount: 0},
		Status:  statusClient,
		Queue:   mockdb.NewMockBatchPriorityQueueClient(),
	}
	p := mustNewProcessor(t, cfg, clients)
	p.poller = NewPoller(clients.Queue, failDB)
	updater := NewStatusUpdater(failDB, statusClient, 86400)

	jobDir, _ := p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte(`{"id":"batch_req_1","custom_id":"r1","response":{"status_code":200}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	staleJob := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	counts := &openai.BatchRequestCounts{Total: 1, Completed: 1, Failed: 0}

	ctx := testLoggerCtx(t)
	err := p.finalizeJob(ctx, updater, staleJob, jobInfo, counts)

	// Must return errFinalizeFailedOver (fallback succeeded).
	if !errors.Is(err, errFinalizeFailedOver) {
		t.Fatalf("expected errFinalizeFailedOver, got: %v", err)
	}

	// Verify DB state: status must be "failed" with file IDs preserved.
	items, _, _, getErr := innerDB.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if getErr != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", getErr, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if got.OutputFileID == nil {
		t.Fatal("output_file_id must be preserved in fallback, got nil")
	}
	if got.RequestCounts.Total != 1 || got.RequestCounts.Completed != 1 {
		t.Fatalf("request_counts = %+v, want {1,1,0}", got.RequestCounts)
	}
}

// TestFinalizeJob_CancelledWriteFails_FallsBackToFailedWithFileIDs verifies the cancel
// path of the failover logic: uploads succeed, then user cancel triggers during
// finalization, but UpdateCancelledStatus fails. The fallback must write "failed"
// status with file IDs preserved, exactly like the completed-write-failure variant.
func TestFinalizeJob_CancelledWriteFails_FallsBackToFailedWithFileIDs(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	innerDB := newMockBatchDBClient()
	failDB := &failOnStatusDB{
		inner:      innerDB,
		failStatus: openai.BatchStatusCancelled,
		failErr:    errors.New("injected: cancelled write failed"),
	}
	statusClient := mockdb.NewMockBatchStatusClient()

	jobID := "job-failover-cancelled"
	tenantID := "tenant-1"

	dbJob := seedDBJob(t, innerDB, jobID)
	dbJob.TenantID = tenantID

	clients := &clientset.Clientset{
		BatchDB: failDB,
		FileDB:  newMockFileDBClient(),
		File:    &failNTimesFilesClient{failCount: 0},
		Status:  statusClient,
		Queue:   mockdb.NewMockBatchPriorityQueueClient(),
	}
	p := mustNewProcessor(t, cfg, clients)
	p.poller = NewPoller(clients.Queue, failDB)
	updater := NewStatusUpdater(failDB, statusClient, 86400)

	jobDir, _ := p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte(`{"id":"batch_req_1","custom_id":"r1","response":{"status_code":200}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	staleJob := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	counts := &openai.BatchRequestCounts{Total: 1, Completed: 1, Failed: 0}

	ctx := testLoggerCtx(t)
	// The abort context carries the ErrCancelled cause → finalization takes the cancel path.
	cancelledCtx, cancelFn := context.WithCancelCause(ctx)
	cancelFn(batchctx.ErrCancelled)

	err := p.finalizeJob(cancelledCtx, updater, staleJob, jobInfo, counts)

	if !errors.Is(err, errFinalizeFailedOver) {
		t.Fatalf("expected errFinalizeFailedOver, got: %v", err)
	}

	items, _, _, getErr := innerDB.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if getErr != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", getErr, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if got.OutputFileID == nil {
		t.Fatal("output_file_id must be preserved in cancel-path fallback, got nil")
	}
	if got.RequestCounts.Total != 1 || got.RequestCounts.Completed != 1 {
		t.Fatalf("request_counts = %+v, want {1,1,0}", got.RequestCounts)
	}
}

// TestFinalizeJob_OneUploadFails_FailedWithSurvivingFileID verifies that when one of the
// two concurrent uploads fails, finalizeJob marks the job as failed (not completed) and
// preserves the surviving file ID in the DB. Marking completed with a missing artifact
// would violate the batch contract; marking failed with empty file IDs would orphan the
// successfully-uploaded artifact.
func TestFinalizeJob_OneUploadFails_FailedWithSurvivingFileID(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	innerDB := newMockBatchDBClient()
	statusClient := mockdb.NewMockBatchStatusClient()
	filesClient := &failOnNthCallClient{
		failN:   1,
		failErr: errors.New("injected: one upload fails"),
	}

	jobID := "job-partial-upload"
	tenantID := "tenant-1"

	dbJob := seedDBJob(t, innerDB, jobID)
	dbJob.TenantID = tenantID

	clients := &clientset.Clientset{
		BatchDB: innerDB,
		FileDB:  newMockFileDBClient(),
		File:    filesClient,
		Status:  statusClient,
		Queue:   mockdb.NewMockBatchPriorityQueueClient(),
	}
	p := mustNewProcessor(t, cfg, clients)
	p.poller = NewPoller(clients.Queue, innerDB)
	updater := NewStatusUpdater(innerDB, statusClient, 86400)

	jobDir, _ := p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte(`{"id":"batch_req_1","custom_id":"r1","response":{"status_code":200}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile output: %v", err)
	}
	errorPath, _ := p.jobErrorFilePath(jobID, tenantID)
	if err := os.WriteFile(errorPath, []byte(`{"id":"batch_req_2","custom_id":"r2","error":{"code":"batch_failed"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}

	staleJob := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	counts := &openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}

	ctx := testLoggerCtx(t)
	err := p.finalizeJob(ctx, updater, staleJob, jobInfo, counts)
	if !errors.Is(err, errFinalizeFailedOver) {
		t.Fatalf("expected errFinalizeFailedOver, got: %v", err)
	}

	items, _, _, getErr := innerDB.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if getErr != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", getErr, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed (partial upload must not be marked completed)", got.Status)
	}

	// Exactly one upload failed (1st Store call), so exactly one file ID should survive.
	hasOutput := got.OutputFileID != nil
	hasError := got.ErrorFileID != nil
	if hasOutput == hasError {
		t.Fatalf("expected exactly one surviving file ID, got output=%v error=%v", got.OutputFileID, got.ErrorFileID)
	}
}

// --- parallel upload tests ---

// concurrentFilesClient is a thread-safe mock that records Store calls and
// tracks peak concurrency to verify parallel execution deterministically.
type concurrentFilesClient struct {
	mu        sync.Mutex
	calls     int
	active    int
	maxActive int
	fileNames []string
	delay     time.Duration
}

func (c *concurrentFilesClient) Store(_ context.Context, fileName, _ string, _, _ int64, _ io.Reader) (*filesapi.BatchFileMetadata, error) {
	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.mu.Unlock()

	if c.delay > 0 {
		time.Sleep(c.delay)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.active--
	c.calls++
	c.fileNames = append(c.fileNames, fileName)
	return &filesapi.BatchFileMetadata{Size: 42}, nil
}

func (c *concurrentFilesClient) Retrieve(_ context.Context, _, _ string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	return nil, nil, nil
}
func (c *concurrentFilesClient) List(_ context.Context, _ string) ([]filesapi.BatchFileMetadata, error) {
	return nil, nil
}
func (c *concurrentFilesClient) RetrieveRange(_ context.Context, _, _ string, _ int64, _ int64) (io.ReadCloser, error) {
	return nil, nil
}
func (c *concurrentFilesClient) Delete(_ context.Context, _, _ string) error { return nil }
func (c *concurrentFilesClient) GetContext(p context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.WithCancel(p)
}
func (c *concurrentFilesClient) Close() error { return nil }

func (c *concurrentFilesClient) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *concurrentFilesClient) peakConcurrency() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxActive
}

func TestFinalizeJob_UploadsFilesInParallel(t *testing.T) {
	ctx := testLoggerCtx(t)
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	mock := &concurrentFilesClient{delay: 50 * time.Millisecond}
	dbClient := newMockBatchDBClient()
	fileDB := newMockFileDBClient()
	statusClient := mockdb.NewMockBatchStatusClient()

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDB,
		File:    mock,
		Status:  statusClient,
		Queue:   mockdb.NewMockBatchPriorityQueueClient(),
	}
	p := mustNewProcessor(t, cfg, clients)
	p.poller = NewPoller(clients.Queue, dbClient)

	updater := NewStatusUpdater(dbClient, statusClient, 86400)

	jobID := "job-parallel-upload"
	tenantID := "tenant-1"
	dbJob := seedDBJob(t, dbClient, jobID)

	// Create both output and error files.
	jobDir, _ := p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath := filepath.Join(jobDir, "output.jsonl")
	errorPath := filepath.Join(jobDir, "error.jsonl")
	if err := os.WriteFile(outputPath, []byte(`{"id":"r1","custom_id":"req-1","response":{"status_code":200}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if err := os.WriteFile(errorPath, []byte(`{"id":"r2","custom_id":"req-2","error":{"code":"err","message":"fail"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	counts := &openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}

	err := p.finalizeJob(ctx, updater, dbJob, jobInfo, counts)
	if err != nil {
		t.Fatalf("finalizeJob returned error: %v", err)
	}

	if got := mock.callCount(); got != 2 {
		t.Fatalf("expected 2 Store calls, got %d", got)
	}

	// Assert that both uploads ran concurrently by checking peak active count.
	if peak := mock.peakConcurrency(); peak < 2 {
		t.Errorf("expected peak concurrency >= 2, got %d (uploads ran sequentially)", peak)
	}
}

func TestUploadPartialResults_UploadsFilesInParallel(t *testing.T) {
	ctx := testLoggerCtx(t)
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	mock := &concurrentFilesClient{delay: 50 * time.Millisecond}
	dbClient := newMockBatchDBClient()
	fileDB := newMockFileDBClient()

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDB,
		File:    mock,
		Status:  mockdb.NewMockBatchStatusClient(),
		Queue:   mockdb.NewMockBatchPriorityQueueClient(),
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-partial-parallel"
	tenantID := "tenant-1"
	dbJob := seedDBJob(t, dbClient, jobID)
	createPartialOutputFiles(t, p, jobID, tenantID)

	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}

	outputFileID, errorFileID := p.uploadPartialResults(ctx, jobInfo, dbJob)

	if outputFileID == "" {
		t.Error("expected non-empty outputFileID")
	}
	if errorFileID == "" {
		t.Error("expected non-empty errorFileID")
	}

	if got := mock.callCount(); got != 2 {
		t.Fatalf("expected 2 Store calls, got %d", got)
	}

	if peak := mock.peakConcurrency(); peak < 2 {
		t.Errorf("expected peak concurrency >= 2, got %d (uploads ran sequentially)", peak)
	}
}
