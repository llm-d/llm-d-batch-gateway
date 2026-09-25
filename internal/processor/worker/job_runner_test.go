package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

type errEventClient struct {
	db.BatchEventChannelClient
	err error
}

func (c *errEventClient) ECConsumerGetChannel(ctx context.Context, ID string) (*db.BatchEventsChan, error) {
	return nil, c.err
}

// errPQClient is a BatchPriorityQueueClient whose PQEnqueue always returns err.
type errPQClient struct {
	db.BatchPriorityQueueClient
	err error
}

func (c *errPQClient) PQEnqueue(ctx context.Context, _ *db.BatchJobPriority) error { return c.err }

func TestRunJob_EventWatcherError_ReturnsSafely(t *testing.T) {
	cfg := config.NewConfig()
	cfg.NumWorkers = 1
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		Event: &errEventClient{err: errors.New("event unavailable")},
	})

	if !p.acquire(context.Background()) {
		t.Fatalf("expected token acquire before runJob")
	}
	p.wg.Add(1)

	p.runJob(testLoggerCtx(t), &jobExecutionParams{
		updater: NewStatusUpdater(newMockBatchDBClient()),
		jobItem: &db.BatchItem{BaseIndexes: db.BaseIndexes{ID: "job-1", TenantID: "tenantA"}},
		jobInfo: &batch_types.JobInfo{JobID: "job-1"},
	})
}

func TestRunJob_EventWatcherAndReEnqueueBothFail_MarksJobFailed(t *testing.T) {
	ctx := testLoggerCtx(t)

	dbClient := newMockBatchDBClient()
	pqClient := &errPQClient{err: errors.New("queue unavailable")}

	cfg := config.NewConfig()
	cfg.NumWorkers = 1
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pqClient,
		Event:   &errEventClient{err: errors.New("event unavailable")},
	})

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: "job-stuck", TenantID: "tenantA"},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusValidating}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	if !p.acquire(context.Background()) {
		t.Fatalf("expected token acquire before runJob")
	}
	p.wg.Add(1)

	p.runJob(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: &batch_types.JobInfo{JobID: "job-stuck"},
		task:    &db.BatchJobPriority{ID: "job-stuck"},
	})

	items, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-stuck"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}

	var updated openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &updated); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if updated.Status != openai.BatchStatusFailed {
		t.Fatalf("expected failed status, got %s", updated.Status)
	}
}

func TestRunJob_PreProcessError_HandlesFailedStatus(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.NumWorkers = 1
	cfg.WorkDir = t.TempDir()
	dbClient := newMockBatchDBClient()
	eventClient := mockdb.NewMockBatchEventChannelClient()
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB: dbClient,
		Event:   eventClient,
	})

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: "job-fail", TenantID: "tenantA"},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}

	// Empty InputFileID forces preProcessJob to fail and runJob to handle as failed.
	jobInfo := &batch_types.JobInfo{
		JobID: "job-fail",
		BatchJob: &openai.Batch{
			ID: "job-fail",
			BatchSpec: openai.BatchSpec{
				InputFileID: "",
			},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusInProgress},
		},
		TenantID: "tenantA",
	}

	if !p.acquire(context.Background()) {
		t.Fatalf("expected token acquire before runJob")
	}
	p.wg.Add(1)
	p.runJob(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: jobInfo,
		task: &db.BatchJobPriority{
			ID:  "job-fail",
			SLO: time.Now().Add(1 * time.Hour),
		},
	})

	items, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-fail"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet updated item: err=%v len=%d", err, len(items))
	}

	var updated openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &updated); err != nil {
		t.Fatalf("unmarshal updated status: %v", err)
	}
	if updated.Status != openai.BatchStatusFailed {
		t.Fatalf("expected failed status, got %s", updated.Status)
	}
}

// TestRunJob_ReachesPreProcess verifies that runJob proceeds past event watcher setup
// and into preProcessJob. preProcessJob fails because the input file does not exist on
// disk, so the job transitions to failed — confirming that handleJobError was reached
// rather than a silent panic or early return.
func TestRunJob_ReachesPreProcess(t *testing.T) {
	cfg := config.NewConfig()
	cfg.NumWorkers = 1
	cfg.WorkDir = t.TempDir()

	dbClient := newMockBatchDBClient()
	eventClient := mockdb.NewMockBatchEventChannelClient()
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  newMockFileDBClient(),
		Event:   eventClient,
		File:    mockfiles.NewMockBatchFilesClient(t.TempDir()),
	})

	ctx := testLoggerCtx(t)

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: "job-contract", TenantID: "tenantA"},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	// InputFileID is set so preProcessJob proceeds past the empty-check and attempts
	// to download the input file (which does not exist, causing a handled failure).
	jobInfo := &batch_types.JobInfo{
		JobID: "job-contract",
		BatchJob: &openai.Batch{
			ID: "job-contract",
			BatchSpec: openai.BatchSpec{
				InputFileID: "file-123",
			},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusInProgress},
		},
		TenantID: "tenantA",
	}

	if !p.acquire(context.Background()) {
		t.Fatalf("expected token acquire before runJob")
	}
	p.wg.Add(1)

	p.runJob(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: jobInfo,
		task: &db.BatchJobPriority{
			ID:  "job-contract",
			SLO: time.Now().Add(1 * time.Hour),
		},
	})

	// preProcessJob will fail (file doesn't exist on disk) and handleJobError marks it failed.
	// The key assertion: we reached handleFailed (not a silent panic recovery).
	items, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-contract"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}

	var updated openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &updated); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if updated.Status != openai.BatchStatusFailed {
		t.Fatalf("expected failed status (preprocess error handled), got %s", updated.Status)
	}
}

func TestHandleFailed_DBUpdateError_ReturnsError(t *testing.T) {
	updateErr := errors.New("db update failed")
	dbClient := &dbUpdateErrWrapper{
		inner: newMockBatchDBClient(),
		err:   updateErr,
	}
	updater := NewStatusUpdater(dbClient)

	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{})
	err := p.handleFailed(testLoggerCtx(t), updater, &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: "job-1", TenantID: "tenantA"},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}, nil, nil)
	if !errors.Is(err, updateErr) {
		t.Fatalf("expected update error, got %v", err)
	}
}

// --- handlePanicRecovery tests ---

func TestHandlePanicRecovery_BeforeInProgress_MarksFailed(t *testing.T) {
	ctx := testLoggerCtx(t)
	dbClient := newMockBatchDBClient()

	jobItem := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: "job-panic-pre", TenantID: "tenantA"},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusValidating})},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{BatchDB: dbClient})
	p.handlePanicRecovery(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: &batch_types.JobInfo{JobID: "job-panic-pre"},
	}, false, nil)

	assertJobStatus(t, dbClient, "job-panic-pre", openai.BatchStatusFailed)
}

func TestHandlePanicRecovery_AfterInProgress_WithCounts_MarksFailed(t *testing.T) {
	ctx := testLoggerCtx(t)
	dbClient := newMockBatchDBClient()

	jobItem := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: "job-panic-partial", TenantID: "tenantA"},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	counts := &openai.BatchRequestCounts{Total: 10, Completed: 3, Failed: 0}
	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{BatchDB: dbClient})
	p.handlePanicRecovery(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: &batch_types.JobInfo{JobID: "job-panic-partial"},
	}, true, counts)

	assertJobStatus(t, dbClient, "job-panic-partial", openai.BatchStatusFailed)
}

func TestHandlePanicRecovery_AfterInProgress_NilCounts_MarksFailed(t *testing.T) {
	ctx := testLoggerCtx(t)
	dbClient := newMockBatchDBClient()

	jobItem := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: "job-panic-nocounts", TenantID: "tenantA"},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{BatchDB: dbClient})
	p.handlePanicRecovery(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: &batch_types.JobInfo{JobID: "job-panic-nocounts"},
	}, true, nil)

	assertJobStatus(t, dbClient, "job-panic-nocounts", openai.BatchStatusFailed)
}

func TestHandlePanicRecovery_CancelledContext_StillMarksFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(testLoggerCtx(t))
	cancel()

	dbClient := newMockBatchDBClient()

	jobItem := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: "job-panic-cancelled-ctx", TenantID: "tenantA"},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	if err := dbClient.DBStore(context.Background(), jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{BatchDB: dbClient})
	p.handlePanicRecovery(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: &batch_types.JobInfo{JobID: "job-panic-cancelled-ctx"},
	}, true, nil)

	assertJobStatus(t, dbClient, "job-panic-cancelled-ctx", openai.BatchStatusFailed)
}

func TestHandlePanicRecovery_DBError_DoesNotCrash(t *testing.T) {
	ctx := testLoggerCtx(t)
	dbClient := &dbUpdateFailOnceWrapper{inner: newMockBatchDBClient(), failCount: 1}

	jobItem := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: "job-panic-db-err", TenantID: "tenantA"},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	counts := &openai.BatchRequestCounts{Total: 10, Completed: 3, Failed: 0}
	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{BatchDB: dbClient})
	p.handlePanicRecovery(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: &batch_types.JobInfo{JobID: "job-panic-db-err"},
	}, true, counts)

	assertJobStatus(t, dbClient, "job-panic-db-err", openai.BatchStatusInProgress)
}

func TestHandlePanicRecovery_NilParams_DoesNotPanic(t *testing.T) {
	ctx := testLoggerCtx(t)
	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{})
	p.handlePanicRecovery(ctx, nil, false, nil)
	p.handlePanicRecovery(ctx, &jobExecutionParams{}, false, nil)
}

// dbBlockingUpdateWrapper blocks DBUpdate until its context is cancelled,
// simulating an unreachable database.
type dbBlockingUpdateWrapper struct {
	inner db.BatchProgressDBClient
}

func (d *dbBlockingUpdateWrapper) DBStore(ctx context.Context, item *db.BatchItem) error {
	return d.inner.DBStore(ctx, item)
}
func (d *dbBlockingUpdateWrapper) DBGet(ctx context.Context, query *db.BatchQuery, includeStatic bool, start, limit int) ([]*db.BatchItem, int, bool, error) {
	return d.inner.DBGet(ctx, query, includeStatic, start, limit)
}
func (d *dbBlockingUpdateWrapper) DBUpdate(ctx context.Context, _ *db.BatchItem, _ []byte) error {
	<-ctx.Done()
	return ctx.Err()
}
func (d *dbBlockingUpdateWrapper) DBUpdateProgress(ctx context.Context, id string, epoch int64, counts db.BatchRequestCounts) error {
	return d.inner.DBUpdateProgress(ctx, id, epoch, counts)
}
func (d *dbBlockingUpdateWrapper) DBDelete(ctx context.Context, IDs []string) ([]string, error) {
	return d.inner.DBDelete(ctx, IDs)
}
func (d *dbBlockingUpdateWrapper) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}
func (d *dbBlockingUpdateWrapper) Close() error {
	return d.inner.Close()
}

// TestHandlePanicRecovery_BlockingDB_ReturnsWithinTimeout verifies that
// handlePanicRecovery returns within a bounded time even when the DB is
// unreachable (blocks forever), ensuring wg.Done() and release() are not
// starved.
func TestHandlePanicRecovery_BlockingDB_ReturnsWithinTimeout(t *testing.T) {
	// Use a short timeout so the test completes quickly in CI.
	orig := panicRecoveryTimeout
	panicRecoveryTimeout = 500 * time.Millisecond
	t.Cleanup(func() { panicRecoveryTimeout = orig })

	ctx := testLoggerCtx(t)
	blockingDB := &dbBlockingUpdateWrapper{inner: newMockBatchDBClient()}

	jobItem := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: "job-panic-block", TenantID: "tenantA"},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	if err := blockingDB.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{BatchDB: blockingDB})

	done := make(chan struct{})
	go func() {
		p.handlePanicRecovery(ctx, &jobExecutionParams{
			updater: NewStatusUpdater(blockingDB),
			jobItem: jobItem,
			jobInfo: &batch_types.JobInfo{JobID: "job-panic-block"},
		}, false, nil)
		close(done)
	}()

	select {
	case <-done:
		// handlePanicRecovery returned — the timeout worked.
	case <-time.After(5 * time.Second):
		t.Fatal("handlePanicRecovery blocked beyond panicRecoveryTimeout; timeout not applied")
	}
}

// TestRunJob_Success_CompletesAndCleansArtifacts verifies the full happy-path orchestration:
//
//	preProcessJob → in_progress → executeJob → finalizeJob → completed
//
// It asserts that the final DB status is completed, an output file ID was recorded,
// and the local job directory was removed. It also verifies that runJob returns
// without deadlock (wg and worker token are released).
func TestRunJob_Success_CompletesAndCleansArtifacts(t *testing.T) {
	ctx := testLoggerCtx(t)

	const (
		jobID       = "job-happy-path"
		tenantID    = "tenant-1"
		inputFileID = "file-input-happy"
	)

	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	filesRoot := t.TempDir()

	// Compute where MockBatchFilesClient stores/retrieves files.
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	storageName := ucom.FileStorageName(inputFileID, "input.jsonl") // "<inputFileID>.jsonl"
	storageDir := filepath.Join(filesRoot, folderName)
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatalf("MkdirAll storage dir: %v", err)
	}

	inputContent := `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"test-model","messages":[{"role":"user","content":"hello"}]}}` + "\n"
	if err := os.WriteFile(filepath.Join(storageDir, storageName), []byte(inputContent), 0o644); err != nil {
		t.Fatalf("WriteFile input to mock storage: %v", err)
	}

	// Seed FileDB so openInputFileStream can resolve the input file.
	fileDBClient := newMockFileDBClient()
	fileItem, err := converter.FileToDBItem(&openai.FileObject{
		ID:       inputFileID,
		Filename: "input.jsonl",
		Purpose:  openai.FileObjectPurposeBatch,
		Object:   "file",
	}, tenantID, db.Tags{})
	if err != nil {
		t.Fatalf("FileToDBItem: %v", err)
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	// Seed BatchDB with the job.
	dbClient := newMockBatchDBClient()
	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusValidating}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore batch item: %v", err)
	}

	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    fileDBClient,
		File:      mockfiles.NewMockBatchFilesClient(filesRoot),
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Queue:     mockdb.NewMockBatchPriorityQueueClient(),
		Inference: inference.NewSingleClientResolver(&mockInferenceClient{}),
	})

	jobInfo := &batch_types.JobInfo{
		JobID:    jobID,
		TenantID: tenantID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    "/v1/chat/completions",
			},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusValidating},
		},
	}

	if !p.acquire(context.Background()) {
		t.Fatalf("expected token acquire before runJob")
	}
	p.wg.Add(1)
	p.runJob(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient),
		jobItem: jobItem,
		jobInfo: jobInfo,
		task: &db.BatchJobPriority{
			ID:  jobID,
			SLO: time.Now().Add(1 * time.Hour),
		},
	})

	// Assert DB status is completed.
	items, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var status openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &status); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("expected completed, got %s", status.Status)
	}

	// Assert output file ID was recorded (inference succeeded, output.jsonl was uploaded).
	if status.OutputFileID == nil || *status.OutputFileID == "" {
		t.Fatalf("expected output_file_id to be set, got nil/empty")
	}

	// Assert local job directory was cleaned up.
	jobDir, _ := p.jobRootDir(jobID, tenantID)
	if _, err := os.Stat(jobDir); err == nil {
		t.Fatalf("expected job dir to be removed after completion, still exists: %s", jobDir)
	}
}

func assertJobStatus(t *testing.T, dbClient db.BatchProgressDBClient, jobID string, want openai.BatchStatus) {
	t.Helper()
	items, _, _, err := dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet %s: err=%v len=%d", jobID, err, len(items))
	}
	var status openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &status); err != nil {
		t.Fatalf("unmarshal status for %s: %v", jobID, err)
	}
	if status.Status != want {
		t.Fatalf("job %s: expected status %s, got %s", jobID, want, status.Status)
	}
}

// TestRunJob_FinalizeFailedOver_PreservesFileIDsAndDoesNotCallHandleFailed verifies the
// runJob → finalizeJob → errFinalizeFailedOver integration path. When the completed-status
// DB write fails inside finalizeJob, the fallback writes "failed" status with file IDs
// preserved. runJob must NOT call handleFailed again — that would overwrite the file IDs
// with empty strings, orphaning the already-uploaded files.
func TestRunJob_FinalizeFailedOver_PreservesFileIDsAndDoesNotCallHandleFailed(t *testing.T) {
	ctx := testLoggerCtx(t)

	const (
		jobID       = "job-finalize-failover"
		tenantID    = "tenant-1"
		inputFileID = "file-input-failover"
	)

	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	filesRoot := t.TempDir()

	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	storageName := ucom.FileStorageName(inputFileID, "input.jsonl")
	storageDir := filepath.Join(filesRoot, folderName)
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatalf("MkdirAll storage dir: %v", err)
	}

	inputContent := `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"test-model","messages":[{"role":"user","content":"hello"}]}}` + "\n"
	if err := os.WriteFile(filepath.Join(storageDir, storageName), []byte(inputContent), 0o644); err != nil {
		t.Fatalf("WriteFile input: %v", err)
	}

	fileDBClient := newMockFileDBClient()
	fileItem, err := converter.FileToDBItem(&openai.FileObject{
		ID:       inputFileID,
		Filename: "input.jsonl",
		Purpose:  openai.FileObjectPurposeBatch,
		Object:   "file",
	}, tenantID, db.Tags{})
	if err != nil {
		t.Fatalf("FileToDBItem: %v", err)
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	// failOnStatusDB makes the completed-status write fail while allowing
	// the fallback failed-status write to succeed.
	innerDB := newMockBatchDBClient()
	failDB := &failOnStatusDB{
		inner:      innerDB,
		failStatus: openai.BatchStatusCompleted,
		failErr:    errors.New("injected: completed write failed"),
	}

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusValidating}),
		},
	}
	if err := innerDB.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore batch item: %v", err)
	}

	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB:   failDB,
		FileDB:    fileDBClient,
		File:      mockfiles.NewMockBatchFilesClient(filesRoot),
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Queue:     mockdb.NewMockBatchPriorityQueueClient(),
		Inference: inference.NewSingleClientResolver(&mockInferenceClient{}),
	})
	p.poller = NewPoller(mockdb.NewMockBatchPriorityQueueClient(), failDB)

	jobInfo := &batch_types.JobInfo{
		JobID:    jobID,
		TenantID: tenantID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    "/v1/chat/completions",
			},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusValidating},
		},
	}

	if !p.acquire(context.Background()) {
		t.Fatalf("expected token acquire before runJob")
	}
	p.wg.Add(1)
	p.runJob(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(failDB),
		jobItem: jobItem,
		jobInfo: jobInfo,
		task: &db.BatchJobPriority{
			ID:  jobID,
			SLO: time.Now().Add(1 * time.Hour),
		},
	})

	// DB status must be "failed" (fallback), not "completed" (which was injected to fail).
	items, _, _, getErr := innerDB.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if getErr != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", getErr, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed (errFinalizeFailedOver path)", got.Status)
	}
	// File IDs must be preserved by the failover — handleFailed must NOT have overwritten them.
	if got.OutputFileID == nil {
		t.Fatal("output_file_id must be preserved in failover path, got nil")
	}
}

// TestHandleJobError_Shutdown_LeavesJobInProgress verifies that batchctx.ErrShutdown
// does NOT re-enqueue or call handleFailed. The job stays in_progress for
// the orphan reconciler to detect and transition to a terminal state.
func TestHandleJobError_Shutdown_LeavesJobInProgress(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.NumWorkers = 1
	cfg.WorkDir = t.TempDir()

	dbClient := newMockBatchDBClient()
	pqClient := mockdb.NewMockBatchPriorityQueueClient()

	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     pqClient,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	})
	p.poller = NewPoller(pqClient, dbClient)

	jobID := "job-shutdown-no-reenqueue"
	tenantID := "tenant__tenantA"

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	counts := &openai.BatchRequestCounts{Total: 5, Completed: 3, Failed: 2}

	updater := NewStatusUpdater(dbClient)
	params := &jobExecutionParams{
		updater:       updater,
		jobItem:       jobItem,
		jobInfo:       jobInfo,
		requestCounts: counts,
		task:          &db.BatchJobPriority{ID: jobID},
	}

	p.handleJobError(ctx, params, batchctx.ErrShutdown)

	items, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusInProgress {
		t.Fatalf("status = %s, want in_progress (left for reconciler)", got.Status)
	}
}
