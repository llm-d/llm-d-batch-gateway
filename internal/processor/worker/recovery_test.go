package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

func newRecoveryTestProcessor(t *testing.T, workDir string) (*Processor, db.BatchProgressDBClient, *spyPQ) {
	t.Helper()

	batchDB := newMockBatchDBClient()
	pq := mockdb.NewMockBatchPriorityQueueClient()
	pq.OnClaimOwned = mockClaimOwned(batchDB, "test-processor")
	spyQueue := &spyPQ{inner: pq}

	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   batchDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     spyQueue,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(spyQueue, batchDB)
	p.updater = NewStatusUpdater(batchDB)

	return p, batchDB, spyQueue
}

func seedDBJobWithStatus(t *testing.T, dbClient db.BatchProgressDBClient, jobID, tenantID string, status openai.BatchStatus, counts *openai.BatchRequestCounts) {
	t.Helper()

	slo := time.Now().UTC().Add(24 * time.Hour)
	seedDBJobWithStatusAndSLO(t, dbClient, jobID, tenantID, status, counts, slo)
}

func seedDBJobWithStatusAndSLO(t *testing.T, dbClient db.BatchProgressDBClient, jobID, tenantID string, status openai.BatchStatus, counts *openai.BatchRequestCounts, slo time.Time) {
	t.Helper()

	expiresAt := slo.Unix()
	statusInfo := openai.BatchStatusInfo{
		Status:    status,
		ExpiresAt: &expiresAt,
	}
	if counts != nil {
		statusInfo.RequestCounts = *counts
	}
	statusBytes, _ := json.Marshal(statusInfo)

	specInfo := openai.BatchSpec{
		InputFileID:      "file-input-1",
		Endpoint:         "/v1/chat/completions",
		CompletionWindow: "24h",
	}
	specBytes, _ := json.Marshal(specInfo)

	item := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: tenantID,
		},
		BaseContents: db.BaseContents{
			Status: statusBytes,
			Spec:   specBytes,
		},
		Priority: slo.UTC().UnixMicro(),
	}
	if err := dbClient.DBStore(context.Background(), item); err != nil {
		t.Fatalf("seed DB job: %v", err)
	}
}

func createJobDir(t *testing.T, p *Processor, jobID, tenantID string) string {
	t.Helper()
	jobDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	return jobDir
}

func createOutputFile(t *testing.T, jobDir string, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(jobDir, outputFileName), []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile output: %v", err)
	}
}

func createErrorFile(t *testing.T, jobDir string, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(jobDir, errorFileName), []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}
}

func assertJobDirRemoved(t *testing.T, p *Processor, jobID, tenantID string) {
	t.Helper()
	jobDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if _, err := os.Stat(jobDir); !os.IsNotExist(err) {
		t.Errorf("expected job directory to be removed: %s", jobDir)
	}
}

func getDBJobStatus(t *testing.T, dbClient db.BatchProgressDBClient, jobID string) openai.BatchStatus {
	t.Helper()
	items, _, _, err := dbClient.DBGet(context.Background(),
		&db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}},
		true, 0, 1)
	if err != nil {
		t.Fatalf("DBGet: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("job %s not found in DB", jobID)
	}
	var info openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &info); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	return info.Status
}

// --- Tests ---

// newRecoveryTestProcessorWithQueryFilter is like newRecoveryTestProcessor but
// installs a QueryFilter on the mock BatchDB that respects ProcessorID and
// HasProcessorID query fields — matching the real Postgres behavior.
func newRecoveryTestProcessorWithQueryFilter(t *testing.T, workDir string) (*Processor, *mockdb.MockDBClient[db.BatchItem, db.BatchQuery], *spyPQ) {
	t.Helper()

	batchDB := mockdb.NewMockDBClient[db.BatchItem, db.BatchQuery](
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)
	batchDB.QueryFilter = func(item *db.BatchItem, query *db.BatchQuery) bool {
		if query.ProcessorID != "" && item.ProcessorID != query.ProcessorID {
			return false
		}
		if query.HasProcessorID && item.ProcessorID == "" {
			return false
		}
		return true
	}

	pq := mockdb.NewMockBatchPriorityQueueClient()
	pq.OnClaimOwned = mockClaimOwned(batchDB, "test-processor")
	spyQueue := &spyPQ{inner: pq}

	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   batchDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     spyQueue,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(spyQueue, batchDB)
	p.updater = NewStatusUpdater(batchDB)

	return p, batchDB, spyQueue
}

func TestRecoverOwnedJobs(t *testing.T) {
	t.Run("recovers jobs owned by this processor", func(t *testing.T) {
		workDir := t.TempDir()
		p, batchDB, spyQueue := newRecoveryTestProcessorWithQueryFilter(t, workDir)

		// Seed two jobs owned by this processor (in_progress) and one owned by another.
		seedDBJobWithStatus(t, batchDB, "owned-1", "tenant-1", openai.BatchStatusInProgress, nil)
		seedDBJobWithStatus(t, batchDB, "owned-2", "tenant-1", openai.BatchStatusFinalizing, nil)
		seedDBJobWithStatus(t, batchDB, "other-1", "tenant-1", openai.BatchStatusInProgress, nil)

		// Set processor_id on owned items.
		setProcessorID(t, batchDB, "owned-1", p.processorID)
		setProcessorID(t, batchDB, "owned-2", p.processorID)
		setProcessorID(t, batchDB, "other-1", "other-processor")

		// Create job dirs so recovery can find artifacts.
		createJobDir(t, p, "owned-1", "tenant-1")
		createJobDir(t, p, "owned-2", "tenant-1")

		ctx := testLoggerCtx(t)
		p.recoverOwnedJobs(ctx)

		// owned-1 was in_progress with no local artifacts → re-enqueued.
		// owned-2 was finalizing with no output files → transitions to failed.
		// other-1 should not be touched.
		if spyQueue.EnqueueCalls() < 1 {
			t.Errorf("expected at least 1 re-enqueue for owned jobs, got %d", spyQueue.EnqueueCalls())
		}

		// Verify other-1 is untouched (still in_progress).
		otherStatus := getDBJobStatus(t, batchDB, "other-1")
		if otherStatus != openai.BatchStatusInProgress {
			t.Errorf("expected other-1 to remain in_progress, got %s", otherStatus)
		}
	})

	t.Run("no owned jobs is a no-op", func(t *testing.T) {
		workDir := t.TempDir()
		p, batchDB, spyQueue := newRecoveryTestProcessorWithQueryFilter(t, workDir)

		// Seed a job owned by a different processor.
		seedDBJobWithStatus(t, batchDB, "other-1", "tenant-1", openai.BatchStatusInProgress, nil)
		setProcessorID(t, batchDB, "other-1", "other-processor")

		ctx := testLoggerCtx(t)
		p.recoverOwnedJobs(ctx)

		if spyQueue.EnqueueCalls() != 0 {
			t.Errorf("expected 0 enqueue calls, got %d", spyQueue.EnqueueCalls())
		}
	})

	t.Run("DB error is handled gracefully", func(t *testing.T) {
		workDir := t.TempDir()
		p, _, _ := newRecoveryTestProcessorWithQueryFilter(t, workDir)

		// Replace batchDB with one that always fails on DBGet.
		p.batchDB = &failOnGetDB{err: fmt.Errorf("connection refused")}

		ctx := testLoggerCtx(t)
		// Should not panic — just logs the error and returns.
		p.recoverOwnedJobs(ctx)
	})

	t.Run("claims every owned job in one call", func(t *testing.T) {
		workDir := t.TempDir()
		p, batchDB, spyQueue := newRecoveryTestProcessorWithQueryFilter(t, workDir)

		for i := 1; i <= 5; i++ {
			id := fmt.Sprintf("owned-%d", i)
			seedDBJobWithStatus(t, batchDB, id, "tenant-1", openai.BatchStatusInProgress, nil)
			setProcessorID(t, batchDB, id, p.processorID)
			createJobDir(t, p, id, "tenant-1")
		}

		ctx := testLoggerCtx(t)
		p.recoverOwnedJobs(ctx)

		if spyQueue.EnqueueCalls() != 5 {
			t.Errorf("expected 5 re-enqueue calls, got %d", spyQueue.EnqueueCalls())
		}
	})

	t.Run("fails job past its recovery budget", func(t *testing.T) {
		workDir := t.TempDir()
		p, batchDB, _ := newRecoveryTestProcessorWithQueryFilter(t, workDir)

		id := "poison-1"
		seedDBJobWithStatus(t, batchDB, id, "tenant-1", openai.BatchStatusFinalizing, &openai.BatchRequestCounts{Total: 10, Completed: 10})
		setProcessorID(t, batchDB, id, p.processorID)

		// Set attempts to the budget: PQClaimOwned bumps it past.
		items, _, _, err := batchDB.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{id}}}, true, 0, 1)
		if err != nil || len(items) != 1 {
			t.Fatalf("DBGet: %v", err)
		}
		items[0].RecoveryAttempts = maxRecoveryAttempts
		if err := batchDB.DBUpdate(context.Background(), items[0], nil); err != nil {
			t.Fatalf("DBUpdate: %v", err)
		}
		createJobDir(t, p, id, "tenant-1")

		ctx := testLoggerCtx(t)
		p.recoverOwnedJobs(ctx)

		items, _, _, err = batchDB.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{id}}}, true, 0, 1)
		if err != nil || len(items) != 1 {
			t.Fatalf("DBGet: %v", err)
		}
		var info openai.BatchStatusInfo
		if err := json.Unmarshal(items[0].Status, &info); err != nil {
			t.Fatalf("unmarshal status: %v", err)
		}
		if info.Status != openai.BatchStatusFailed {
			t.Errorf("job past its recovery budget should be failed, got %s", info.Status)
		}
	})
}

// setProcessorID updates a stored BatchItem's ProcessorID in the mock DB.
func setProcessorID(t *testing.T, dbClient *mockdb.MockDBClient[db.BatchItem, db.BatchQuery], jobID, processorID string) {
	t.Helper()
	items, _, _, err := dbClient.DBGet(context.Background(),
		&db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}},
		true, 0, 1)
	if err != nil || len(items) == 0 {
		t.Fatalf("setProcessorID: failed to get job %s: %v", jobID, err)
	}
	items[0].ProcessorID = processorID
	if err := dbClient.DBUpdate(context.Background(), items[0], nil); err != nil {
		t.Fatalf("setProcessorID: failed to update job %s: %v", jobID, err)
	}
}

// failOnGetDB is a minimal mock that returns an error on DBGet.
type failOnGetDB struct {
	err error
}

func (f *failOnGetDB) DBStore(_ context.Context, _ *db.BatchItem) error { return nil }
func (f *failOnGetDB) DBGet(_ context.Context, _ *db.BatchQuery, _ bool, _, _ int) ([]*db.BatchItem, int, bool, error) {
	return nil, 0, false, f.err
}
func (f *failOnGetDB) DBUpdate(_ context.Context, _ *db.BatchItem, _ []byte) error { return nil }
func (f *failOnGetDB) DBUpdateProgress(_ context.Context, _ string, _ int64, _ db.BatchRequestCounts) error {
	return nil
}
func (f *failOnGetDB) DBDelete(_ context.Context, _ []string) ([]string, error) { return nil, nil }
func (f *failOnGetDB) Close() error                                             { return nil }
func (f *failOnGetDB) GetContext(_ context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.Background(), func() {}
}

func TestRecoverJob_Finalizing(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, _ := newRecoveryTestProcessor(t, workDir)

	jobID := "job-finalizing"
	tenantID := "tenant-1"
	counts := &openai.BatchRequestCounts{Total: 10, Completed: 8, Failed: 2}

	seedDBJobWithStatus(t, dbClient, jobID, tenantID, openai.BatchStatusFinalizing, counts)

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, `{"id":"batch_req_1","custom_id":"req-1","response":{"status_code":200}}`+"\n")
	createErrorFile(t, jobDir, `{"id":"batch_req_2","custom_id":"req-2","error":{"code":"server_error","message":"fail"}}`+"\n")

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	status := getDBJobStatus(t, dbClient, jobID)
	if status != openai.BatchStatusCompleted {
		t.Errorf("expected completed, got %s", status)
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_Cancelling(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, _ := newRecoveryTestProcessor(t, workDir)

	jobID := "job-cancelling"
	tenantID := "tenant-1"
	counts := &openai.BatchRequestCounts{Total: 10, Completed: 5, Failed: 5}

	seedDBJobWithStatus(t, dbClient, jobID, tenantID, openai.BatchStatusCancelling, counts)

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, `{"id":"batch_req_1","custom_id":"req-1","response":{"status_code":200}}`+"\n")

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	status := getDBJobStatus(t, dbClient, jobID)
	if status != openai.BatchStatusCancelled {
		t.Errorf("expected cancelled, got %s", status)
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_InProgress_WithPartialOutput(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, _ := newRecoveryTestProcessor(t, workDir)

	jobID := "job-in-progress-partial"
	tenantID := "tenant-1"
	counts := &openai.BatchRequestCounts{Total: 100, Completed: 50, Failed: 0}

	seedDBJobWithStatus(t, dbClient, jobID, tenantID, openai.BatchStatusInProgress, counts)

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, `{"id":"batch_req_1","custom_id":"req-1","response":{"status_code":200}}`+"\n")

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	status := getDBJobStatus(t, dbClient, jobID)
	if status != openai.BatchStatusFailed {
		t.Errorf("expected failed, got %s", status)
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_InProgress_EmptyOutput_ReEnqueues(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-in-progress-empty"
	tenantID := "tenant-1"

	seedDBJobWithStatus(t, dbClient, jobID, tenantID, openai.BatchStatusInProgress, nil)

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, "")

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	status := getDBJobStatus(t, dbClient, jobID)
	if status != openai.BatchStatusValidating {
		t.Errorf("expected validating, got %s", status)
	}

	if spyQueue.EnqueueCalls() != 1 {
		t.Errorf("expected 1 enqueue call, got %d", spyQueue.EnqueueCalls())
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_InProgress_NoOutputFile_ReEnqueues(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-in-progress-nofile"
	tenantID := "tenant-1"

	seedDBJobWithStatus(t, dbClient, jobID, tenantID, openai.BatchStatusInProgress, nil)

	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	status := getDBJobStatus(t, dbClient, jobID)
	if status != openai.BatchStatusValidating {
		t.Errorf("expected validating, got %s", status)
	}

	if spyQueue.EnqueueCalls() != 1 {
		t.Errorf("expected 1 enqueue call, got %d", spyQueue.EnqueueCalls())
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_InProgress_EmptyOutput_ExpiredSLO_MarksExpiredWithoutEnqueue(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-in-progress-empty-expired"
	tenantID := "tenant-1"
	expiredSLO := time.Now().UTC().Add(-1 * time.Minute)

	seedDBJobWithStatusAndSLO(t, dbClient, jobID, tenantID, openai.BatchStatusInProgress, nil, expiredSLO)

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, "")

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	status := getDBJobStatus(t, dbClient, jobID)
	if status != openai.BatchStatusExpired {
		t.Fatalf("expected expired, got %s", status)
	}

	if spyQueue.EnqueueCalls() != 0 {
		t.Fatalf("expected 0 enqueue calls, got %d", spyQueue.EnqueueCalls())
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_Validating_ReEnqueues(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-validating"
	tenantID := "tenant-1"

	seedDBJobWithStatus(t, dbClient, jobID, tenantID, openai.BatchStatusValidating, nil)

	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	if spyQueue.EnqueueCalls() != 1 {
		t.Errorf("expected 1 enqueue call, got %d", spyQueue.EnqueueCalls())
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_Validating_ReEnqueuesWithExactTaggedSLO(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-validating-precise-slo"
	tenantID := "tenant-1"
	wantSLO := time.UnixMicro(time.Now().UTC().Add(24*time.Hour).UnixMicro() + 321).UTC()

	seedDBJobWithStatusAndSLO(t, dbClient, jobID, tenantID, openai.BatchStatusValidating, nil, wantSLO)
	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	tasks, err := spyQueue.PQDequeue(ctx, 0, 1)
	if err != nil {
		t.Fatalf("PQDequeue: %v", err)
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 re-enqueued task, got %d", len(tasks))
	}
	if !tasks[0].SLO.Equal(wantSLO) {
		t.Fatalf("re-enqueued SLO = %v, want %v", tasks[0].SLO, wantSLO)
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_Validating_ExpiredSLO_MarksExpiredWithoutEnqueue(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-validating-expired"
	tenantID := "tenant-1"
	expiredSLO := time.Now().UTC().Add(-1 * time.Minute)

	seedDBJobWithStatusAndSLO(t, dbClient, jobID, tenantID, openai.BatchStatusValidating, nil, expiredSLO)
	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	status := getDBJobStatus(t, dbClient, jobID)
	if status != openai.BatchStatusExpired {
		t.Fatalf("expected expired, got %s", status)
	}

	if spyQueue.EnqueueCalls() != 0 {
		t.Fatalf("expected 0 enqueue calls, got %d", spyQueue.EnqueueCalls())
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_Validating_MissingSLO_FallsBackToFailed(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-validating-missing-slo"
	tenantID := "tenant-1"

	statusInfo := openai.BatchStatusInfo{Status: openai.BatchStatusValidating}
	statusBytes, _ := json.Marshal(statusInfo)
	specInfo := openai.BatchSpec{
		InputFileID:      "file-input-1",
		Endpoint:         "/v1/chat/completions",
		CompletionWindow: "24h",
	}
	specBytes, _ := json.Marshal(specInfo)

	item := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: tenantID,
			Tags:     db.Tags{},
		},
		BaseContents: db.BaseContents{
			Status: statusBytes,
			Spec:   specBytes,
		},
	}
	if err := dbClient.DBStore(context.Background(), item); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	status := getDBJobStatus(t, dbClient, jobID)
	if status != openai.BatchStatusFailed {
		t.Fatalf("expected failed, got %s", status)
	}

	if spyQueue.EnqueueCalls() != 0 {
		t.Fatalf("expected 0 enqueue calls, got %d", spyQueue.EnqueueCalls())
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_TerminalStatus_CleansUp(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-completed"
	tenantID := "tenant-1"

	seedDBJobWithStatus(t, dbClient, jobID, tenantID, openai.BatchStatusCompleted, nil)

	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	if spyQueue.EnqueueCalls() != 0 {
		t.Errorf("expected 0 enqueue calls for terminal job, got %d", spyQueue.EnqueueCalls())
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_NotInDB_CleansUp(t *testing.T) {
	workDir := t.TempDir()
	p, _, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-not-in-db"
	tenantDir := filepath.Join(workDir, "t-somehash", jobsDirName, jobID)
	if err := os.MkdirAll(tenantDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	if spyQueue.EnqueueCalls() != 0 {
		t.Errorf("expected 0 enqueue calls, got %d", spyQueue.EnqueueCalls())
	}

	if _, err := os.Stat(tenantDir); !os.IsNotExist(err) {
		t.Errorf("expected stale directory to be removed: %s", tenantDir)
	}
}

// ---------------------------------------------------------------------------
// Fallback path tests — verify recoverWithFailed is invoked on failures
// ---------------------------------------------------------------------------

// failOnNthBatchDB wraps a BatchDBClient and fails DBUpdate on the nth call only.
// All other calls are delegated to the inner client.
type failOnNthBatchDB struct {
	db.BatchProgressDBClient
	mu      sync.Mutex
	callN   int
	failOn  int // 1-based: which call number to fail
	failErr error
}

func (f *failOnNthBatchDB) DBUpdate(ctx context.Context, item *db.BatchItem, expectedStatus []byte) error {
	f.mu.Lock()
	f.callN++
	n := f.callN
	f.mu.Unlock()
	if n == f.failOn {
		return f.failErr
	}
	return f.BatchProgressDBClient.DBUpdate(ctx, item, expectedStatus)
}

// failPQ wraps a BatchPriorityQueueClient and always fails PQEnqueue.
type failPQ struct {
	db.BatchPriorityQueueClient
	err error
}

func (f *failPQ) PQEnqueue(_ context.Context, _ *db.BatchJobPriority) error {
	return f.err
}

func newRecoveryTestProcessorWithFailDB(t *testing.T, workDir string, failOn int) (*Processor, db.BatchProgressDBClient) {
	t.Helper()

	innerDB := newMockBatchDBClient()
	failDB := &failOnNthBatchDB{
		BatchProgressDBClient: innerDB,
		failOn:                failOn,
		failErr:               errors.New("db update failed"),
	}
	pq := mockdb.NewMockBatchPriorityQueueClient()

	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   failDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     pq,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(pq, failDB)
	p.updater = NewStatusUpdater(failDB)

	return p, innerDB
}

// alwaysFailUpdateDB wraps a BatchDBClient where every DBUpdate fails.
type alwaysFailUpdateDB struct {
	db.BatchProgressDBClient
	err error
}

func (f *alwaysFailUpdateDB) DBUpdate(_ context.Context, _ *db.BatchItem, _ []byte) error {
	return f.err
}

func TestRecoverJob_Cancelling_AllUpdatesFail_ReturnsError(t *testing.T) {
	workDir := t.TempDir()

	innerDB := newMockBatchDBClient()
	failDB := &alwaysFailUpdateDB{BatchProgressDBClient: innerDB, err: errors.New("db update failed")}
	pq := mockdb.NewMockBatchPriorityQueueClient()

	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   failDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     pq,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(pq, failDB)
	p.updater = NewStatusUpdater(failDB)

	jobID := "job-cancel-all-fail"
	tenantID := "tenant-1"

	seedDBJobWithStatus(t, innerDB, jobID, tenantID, openai.BatchStatusCancelling,
		&openai.BatchRequestCounts{Total: 10, Completed: 5, Failed: 5})

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, `{"id":"batch_req_1"}`+"\n")

	ctx := testLoggerCtx(t)
	recoverErr := p.recoverJob(ctx, jobID)
	if recoverErr == nil {
		t.Fatal("expected error when all DB updates fail")
	}
}

func TestRecoverJob_Validating_EnqueueFails_FallsBackToFailed(t *testing.T) {
	workDir := t.TempDir()

	batchDB := newMockBatchDBClient()
	pq := &failPQ{
		BatchPriorityQueueClient: mockdb.NewMockBatchPriorityQueueClient(),
		err:                      errors.New("enqueue failed"),
	}

	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   batchDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     pq,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(pq, batchDB)
	p.updater = NewStatusUpdater(batchDB)

	jobID := "job-validating-enq-fail"
	tenantID := "tenant-1"

	seedDBJobWithStatus(t, batchDB, jobID, tenantID, openai.BatchStatusValidating, nil)
	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob should succeed via fallback, got: %v", err)
	}

	// enqueue failed → recoverWithFailed → should be failed now
	status := getDBJobStatus(t, batchDB, jobID)
	if status != openai.BatchStatusFailed {
		t.Errorf("expected failed after enqueue failure fallback, got %s", status)
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_InProgressReEnqueue_EnqueueFails_FallsBackToFailed(t *testing.T) {
	workDir := t.TempDir()

	batchDB := newMockBatchDBClient()
	pq := &failPQ{
		BatchPriorityQueueClient: mockdb.NewMockBatchPriorityQueueClient(),
		err:                      errors.New("enqueue failed"),
	}

	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   batchDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     pq,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(pq, batchDB)
	p.updater = NewStatusUpdater(batchDB)

	jobID := "job-inprog-enq-fail"
	tenantID := "tenant-1"

	seedDBJobWithStatus(t, batchDB, jobID, tenantID, openai.BatchStatusInProgress, nil)
	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob should succeed via fallback, got: %v", err)
	}

	status := getDBJobStatus(t, batchDB, jobID)
	if status != openai.BatchStatusFailed {
		t.Errorf("expected failed after enqueue failure fallback, got %s", status)
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func getDBJobStatusInfo(t *testing.T, dbClient db.BatchProgressDBClient, jobID string) openai.BatchStatusInfo {
	t.Helper()
	items, _, _, err := dbClient.DBGet(context.Background(),
		&db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}},
		true, 0, 1)
	if err != nil {
		t.Fatalf("DBGet: %v", err)
	}
	if len(items) == 0 {
		t.Fatalf("job %s not found in DB", jobID)
	}
	var info openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &info); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	return info
}

func TestRecoverJob_Cancelling_UpdateFails_FallbackSucceeds_PreservesCounts(t *testing.T) {
	workDir := t.TempDir()
	// failOn=1: first DBUpdate (UpdateCancelledStatus) fails, second (fallback UpdateFailedStatus) succeeds
	p, innerDB := newRecoveryTestProcessorWithFailDB(t, workDir, 1)

	jobID := "job-cancel-fallback-ok"
	tenantID := "tenant-1"
	counts := &openai.BatchRequestCounts{Total: 10, Completed: 5, Failed: 5}

	seedDBJobWithStatus(t, innerDB, jobID, tenantID, openai.BatchStatusCancelling, counts)

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, `{"id":"batch_req_1"}`+"\n")

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob should succeed via fallback, got: %v", err)
	}

	info := getDBJobStatusInfo(t, innerDB, jobID)
	if info.Status != openai.BatchStatusFailed {
		t.Errorf("expected failed, got %s", info.Status)
	}
	if info.RequestCounts.Total != counts.Total {
		t.Errorf("expected counts.Total=%d preserved, got %d", counts.Total, info.RequestCounts.Total)
	}
	if info.RequestCounts.Completed != counts.Completed {
		t.Errorf("expected counts.Completed=%d preserved, got %d", counts.Completed, info.RequestCounts.Completed)
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

// -- Concurrency tests --

// slowBatchDBClient wraps a BatchDBClient and adds a per-DBGet delay
// while tracking peak concurrency to verify parallel execution.
type slowBatchDBClient struct {
	db.BatchProgressDBClient
	delay     time.Duration
	mu        sync.Mutex
	active    int
	maxActive int
}

func (s *slowBatchDBClient) DBGet(ctx context.Context, query *db.BatchQuery, includeStatic bool, start, limit int) ([]*db.BatchItem, int, bool, error) {
	s.mu.Lock()
	s.active++
	if s.active > s.maxActive {
		s.maxActive = s.active
	}
	s.mu.Unlock()

	time.Sleep(s.delay)

	s.mu.Lock()
	s.active--
	s.mu.Unlock()

	return s.BatchProgressDBClient.DBGet(ctx, query, includeStatic, start, limit)
}

func (s *slowBatchDBClient) peakConcurrency() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxActive
}

func TestRecoverOwnedJobs_RunsConcurrently(t *testing.T) {
	workDir := t.TempDir()

	innerDB := newMockBatchDBClient()
	slowDB := &slowBatchDBClient{
		BatchProgressDBClient: innerDB,
		delay:                 50 * time.Millisecond,
	}
	pq := mockdb.NewMockBatchPriorityQueueClient()
	pq.OnClaimOwned = mockClaimOwned(innerDB, "test-processor")

	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	cfg.Concurrency.Recovery = 5

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   slowDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     pq,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(pq, slowDB)
	p.updater = NewStatusUpdater(slowDB)

	// Create 5 owned cancelling jobs; each recovery does one DB lookup.
	numJobs := 5
	tenantID := "tenant-conc"
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("job-conc-%d", i)
		seedDBJobWithStatus(t, innerDB, jobID, tenantID, openai.BatchStatusCancelling, nil)
		// Set processor_id so recoverOwnedJobs finds them.
		items, _, _, _ := innerDB.DBGet(context.Background(),
			&db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
		items[0].ProcessorID = p.processorID
		_ = innerDB.DBUpdate(context.Background(), items[0], nil)
		createJobDir(t, p, jobID, tenantID)
	}

	ctx := testLoggerCtx(t)
	p.recoverOwnedJobs(ctx)

	// With concurrency=5 and 5 jobs at 50ms delay each, parallel should
	// complete in ~50ms. Sequential would take ~250ms.
	// Assert peak concurrency to verify parallel execution deterministically.
	if peak := slowDB.peakConcurrency(); peak < 2 {
		t.Errorf("expected peak concurrency >= 2, got %d (recovery ran sequentially)", peak)
	}
	t.Logf("peak recovery concurrency: %d", slowDB.peakConcurrency())

	// All directories should have been cleaned up.
	for i := 0; i < numJobs; i++ {
		jobID := fmt.Sprintf("job-conc-%d", i)
		assertJobDirRemoved(t, p, jobID, tenantID)
	}
}

func TestOutputFileHasContent(t *testing.T) {
	workDir := t.TempDir()
	p, _, _ := newRecoveryTestProcessor(t, workDir)

	jobID := "job-check"
	tenantID := "tenant-1"

	has, err := p.outputFileHasContent(jobID, tenantID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if has {
		t.Error("expected false for non-existent file")
	}

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, "")

	has, err = p.outputFileHasContent(jobID, tenantID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if has {
		t.Error("expected false for empty file")
	}

	createOutputFile(t, jobDir, "some content\n")

	has, err = p.outputFileHasContent(jobID, tenantID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !has {
		t.Error("expected true for file with content")
	}
}

// ---------------------------------------------------------------------------
// recoverExpired success path — verify partial file IDs are recorded in DB
// ---------------------------------------------------------------------------

func TestRecoverJob_InProgress_EmptyOutput_ExpiredSLO_PreservesErrorFileID(t *testing.T) {
	workDir := t.TempDir()
	p, dbClient, spyQueue := newRecoveryTestProcessor(t, workDir)

	jobID := "job-inprog-expired-with-errors"
	tenantID := "tenant-1"
	counts := &openai.BatchRequestCounts{Total: 100, Completed: 50, Failed: 0}
	expiredSLO := time.Now().UTC().Add(-1 * time.Minute)

	seedDBJobWithStatusAndSLO(t, dbClient, jobID, tenantID,
		openai.BatchStatusInProgress, counts, expiredSLO)

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, "")
	createErrorFile(t, jobDir, `{"id":"batch_req_2","custom_id":"req-2","error":{"code":"server_error","message":"fail"}}`+"\n")

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob: %v", err)
	}

	info := getDBJobStatusInfo(t, dbClient, jobID)
	if info.Status != openai.BatchStatusExpired {
		t.Fatalf("expected expired, got %s", info.Status)
	}
	if info.OutputFileID != nil {
		t.Fatalf("output_file_id should be nil for empty output file, got %s", *info.OutputFileID)
	}
	if info.ErrorFileID == nil {
		t.Fatal("error_file_id must be preserved for expired job with partial errors on disk")
	}

	if spyQueue.EnqueueCalls() != 0 {
		t.Fatalf("expected 0 enqueue calls, got %d", spyQueue.EnqueueCalls())
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

// ---------------------------------------------------------------------------
// Router fallback tests — verify recoverJob routes errors through
// recoverWithFailed and preserves fileIDs from the recoveryResult.
// ---------------------------------------------------------------------------

func TestRecoverJob_Finalizing_UpdateFails_FallbackPreservesFileIDs(t *testing.T) {
	workDir := t.TempDir()
	p, innerDB := newRecoveryTestProcessorWithFailDB(t, workDir, 1)

	jobID := "job-finalizing-fallback"
	tenantID := "tenant-1"
	counts := &openai.BatchRequestCounts{Total: 10, Completed: 8, Failed: 2}

	seedDBJobWithStatus(t, innerDB, jobID, tenantID, openai.BatchStatusFinalizing, counts)

	jobDir := createJobDir(t, p, jobID, tenantID)
	createOutputFile(t, jobDir, `{"id":"batch_req_1","custom_id":"req-1","response":{"status_code":200}}`+"\n")
	createErrorFile(t, jobDir, `{"id":"batch_req_2","custom_id":"req-2","error":{"code":"server_error","message":"fail"}}`+"\n")

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob should succeed via fallback, got: %v", err)
	}

	info := getDBJobStatusInfo(t, innerDB, jobID)
	if info.Status != openai.BatchStatusFailed {
		t.Fatalf("expected failed, got %s", info.Status)
	}
	if info.OutputFileID == nil {
		t.Fatal("output_file_id must be preserved in fallback")
	}
	if info.ErrorFileID == nil {
		t.Fatal("error_file_id must be preserved in fallback")
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}

func TestRecoverJob_ExpiredWriteFails_FallbackToFailed(t *testing.T) {
	workDir := t.TempDir()

	innerDB := newMockBatchDBClient()
	failDB := &failOnNthBatchDB{
		BatchProgressDBClient: innerDB,
		failOn:                1,
		failErr:               errors.New("expired write failed"),
	}
	pq := mockdb.NewMockBatchPriorityQueueClient()

	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   failDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     pq,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(pq, failDB)
	p.updater = NewStatusUpdater(failDB)

	jobID := "job-expired-fallback"
	tenantID := "tenant-1"
	expiredSLO := time.Now().UTC().Add(-1 * time.Minute)

	seedDBJobWithStatusAndSLO(t, innerDB, jobID, tenantID, openai.BatchStatusValidating, nil, expiredSLO)
	createJobDir(t, p, jobID, tenantID)

	ctx := testLoggerCtx(t)
	if err := p.recoverJob(ctx, jobID); err != nil {
		t.Fatalf("recoverJob should succeed via fallback, got: %v", err)
	}

	status := getDBJobStatus(t, innerDB, jobID)
	if status != openai.BatchStatusFailed {
		t.Fatalf("expected failed after expired write failure fallback, got %s", status)
	}

	assertJobDirRemoved(t, p, jobID, tenantID)
}
