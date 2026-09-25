package worker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

type cancelBeforeEnqueueQueue struct {
	api.BatchPriorityQueueClient
	cancel func(context.Context) error
	once   sync.Once
}

func (q *cancelBeforeEnqueueQueue) PQEnqueue(ctx context.Context, task *api.BatchJobPriority) error {
	var cancelErr error
	q.once.Do(func() { cancelErr = q.cancel(ctx) })
	if cancelErr != nil {
		return cancelErr
	}
	return q.BatchPriorityQueueClient.PQEnqueue(ctx, task)
}

func TestRecoverOwnedJobsFencesPreviousEpoch(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := testLoggerCtx(t)
	const processorID = "processor-0"

	cfg := &postgresql.PostgreSQLConfig{Url: url}
	batchDB, err := postgresql.NewPostgresBatchDBClient(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })
	queue, err := postgresql.NewPostgresBatchQueueClient(ctx, cfg, processorID)
	if err != nil {
		t.Fatalf("NewPostgresBatchQueueClient: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "TRUNCATE batch_items"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	statusClient := mockdb.NewMockBatchStatusClient()
	pcfg := config.NewConfig()
	pcfg.WorkDir = t.TempDir()
	p, err := NewProcessor(pcfg, &clientset.Clientset{
		BatchDB:   batchDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     queue,
		Status:    statusClient,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, processorID, testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.poller = NewPoller(queue, batchDB)
	p.updater = NewStatusUpdater(batchDB, statusClient, 86400)

	const jobID = "job-zombie-fence"
	const oldEpoch = int64(4)
	expiresAt := time.Now().Add(24 * time.Hour).Unix()
	statusBytes, _ := json.Marshal(openai.BatchStatusInfo{
		Status:        openai.BatchStatusCancelling,
		ExpiresAt:     &expiresAt,
		RequestCounts: openai.BatchRequestCounts{Total: 10, Completed: 5},
	})
	specBytes, _ := json.Marshal(openai.BatchSpec{InputFileID: "file-1", Endpoint: "/v1/chat/completions", CompletionWindow: "24h"})
	if err := batchDB.DBStore(ctx, &api.BatchItem{
		BaseIndexes:  api.BaseIndexes{ID: jobID, TenantID: "tenant-1"},
		BaseContents: api.BaseContents{Status: statusBytes, Spec: specBytes},
		ProcessorID:  processorID,
		Priority:     time.Now().Add(24 * time.Hour).UnixMicro(),
		Epoch:        oldEpoch,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	if err := p.recoverOwnedJobs(ctx); err != nil {
		t.Fatalf("recoverOwnedJobs: %v", err)
	}

	if got := getDBJobStatus(t, batchDB, jobID); got != openai.BatchStatusCancelled {
		t.Fatalf("recovery did not finish the cancel: status %s", got)
	}

	// A zombie of the previous incarnation still holds the pre-recovery epoch.
	zombieStatus, _ := json.Marshal(openai.BatchStatusInfo{Status: openai.BatchStatusInProgress, ExpiresAt: &expiresAt})
	err = batchDB.DBUpdate(ctx, &api.BatchItem{
		BaseIndexes:  api.BaseIndexes{ID: jobID},
		BaseContents: api.BaseContents{Status: zombieStatus},
		Epoch:        oldEpoch,
	}, nil)
	if !errors.Is(err, api.ErrConflict) {
		t.Errorf("zombie write at epoch %d was accepted after recovery (err=%v); status now %s",
			oldEpoch, err, getDBJobStatus(t, batchDB, jobID))
	}

	// A job past its recovery budget is failed instead of recovered again.
	const poisonID = "job-poison"
	poisonStatus, _ := json.Marshal(openai.BatchStatusInfo{
		Status:        openai.BatchStatusFinalizing,
		ExpiresAt:     &expiresAt,
		RequestCounts: openai.BatchRequestCounts{Total: 10, Completed: 10},
	})
	if err := batchDB.DBStore(ctx, &api.BatchItem{
		BaseIndexes:      api.BaseIndexes{ID: poisonID, TenantID: "tenant-1"},
		BaseContents:     api.BaseContents{Status: poisonStatus, Spec: specBytes},
		ProcessorID:      processorID,
		Priority:         time.Now().Add(24 * time.Hour).UnixMicro(),
		Epoch:            1,
		RecoveryAttempts: maxRecoveryAttempts,
	}); err != nil {
		t.Fatalf("seed poison: %v", err)
	}
	if err := p.recoverOwnedJobs(ctx); err != nil {
		t.Fatalf("recoverOwnedJobs: %v", err)
	}
	if got := getDBJobStatus(t, batchDB, poisonID); got != openai.BatchStatusFailed {
		t.Errorf("job past its recovery budget should be failed, got %s", got)
	}
}

func TestRecoverOwnedJobsFinalizesCancellationAfterEnqueueConflict(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := testLoggerCtx(t)
	const processorID = "processor-cancellation-race"
	const jobID = "job-cancellation-race"

	cfg := &postgresql.PostgreSQLConfig{Url: url}
	batchDB, err := postgresql.NewPostgresBatchDBClient(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })
	queue, err := postgresql.NewPostgresBatchQueueClient(ctx, cfg, processorID)
	if err != nil {
		t.Fatalf("NewPostgresBatchQueueClient: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "TRUNCATE batch_items"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	statusClient := mockdb.NewMockBatchStatusClient()
	pcfg := config.NewConfig()
	pcfg.WorkDir = t.TempDir()
	p, err := NewProcessor(pcfg, &clientset.Clientset{
		BatchDB:   batchDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     queue,
		Status:    statusClient,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}, processorID, testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.updater = NewStatusUpdater(batchDB, statusClient, 86400)

	expiresAt := time.Now().Add(24 * time.Hour).Unix()
	statusBytes, _ := json.Marshal(openai.BatchStatusInfo{
		Status:    openai.BatchStatusValidating,
		ExpiresAt: &expiresAt,
	})
	specBytes, _ := json.Marshal(openai.BatchSpec{InputFileID: "file-1", Endpoint: "/v1/chat/completions", CompletionWindow: "24h"})
	if err := batchDB.DBStore(ctx, &api.BatchItem{
		BaseIndexes:  api.BaseIndexes{ID: jobID, TenantID: "tenant-1"},
		BaseContents: api.BaseContents{Status: statusBytes, Spec: specBytes},
		ProcessorID:  processorID,
		Priority:     time.Now().Add(24 * time.Hour).UnixMicro(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	p.poller = NewPoller(&cancelBeforeEnqueueQueue{
		BatchPriorityQueueClient: queue,
		cancel: func(ctx context.Context) error {
			item, err := p.poller.fetchJobItemByID(ctx, jobID)
			if err != nil {
				return err
			}
			return p.updater.UpdatePersistentStatus(ctx, item, openai.BatchStatusCancelling, nil, nil)
		},
	}, batchDB)

	if err := p.recoverOwnedJobs(ctx); err != nil {
		t.Fatalf("recoverOwnedJobs: %v", err)
	}
	if got := getDBJobStatus(t, batchDB, jobID); got != openai.BatchStatusCancelled {
		t.Fatalf("recovery did not finish the cancellation: status %s", got)
	}

	var processor string
	var status openai.BatchStatus
	if err := pool.QueryRow(ctx, "SELECT COALESCE(processor_id, ''), status->>'status' FROM batch_items WHERE id = $1", jobID).Scan(&processor, &status); err != nil {
		t.Fatalf("read recovered job: %v", err)
	}
	if processor != "" && !status.IsTerminal() {
		t.Fatalf("owned non-terminal row remains: processor_id=%q status=%s", processor, status)
	}
}
