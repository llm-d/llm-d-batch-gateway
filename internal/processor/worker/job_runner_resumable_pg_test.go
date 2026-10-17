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
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batch_utils"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// TestRunJob_Resumable_RequeueClaimResume is the PG-backed kill simulation:
// a first incarnation activates a batch and checkpoints one of two results;
// the reconciler requeues the orphan; a second processor's unified dequeue
// re-claims the row and resumes it through the real PG stores. The
// checkpointed entry must not be re-submitted, and both incarnations' result
// objects must land in one artifact in input order. The requeue must also
// fence the dead incarnation out of every subsequent write.
func TestRunJob_Resumable_RequeueClaimResume(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := testLoggerCtx(t)
	const processorID = "processor-resume-pg"
	const jobID = "job-resumable-pg-resume"
	const tenantID = "tenant-1"
	inputFileID := "file-input-" + jobID

	filesRoot, fileDBClient := seedDurableInput(t, tenantID, inputFileID, []string{"test-model", "test-model"})

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
	resumable, err := postgresql.NewPostgresResumableStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPostgresResumableStore: %v", err)
	}
	t.Cleanup(func() { _ = resumable.Close() })
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "TRUNCATE batch_items CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// The first incarnation's dequeued claim: a validating row owned at epoch 1.
	if err := batchDB.DBStore(ctx, &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusValidating}),
			Spec:   mustJSON(t, openai.BatchSpec{InputFileID: inputFileID, Endpoint: openai.EndpointChatCompletions}),
		},
		ProcessorID: processorID,
		Priority:    time.Now().Add(1 * time.Hour).UnixMicro(),
		Epoch:       1,
	}); err != nil {
		t.Fatalf("seed batch item: %v", err)
	}

	// Activation through the real CTE: the row flips to in_progress/resumable,
	// ownership and lease are taken, and both entries are inserted as pending.
	if err := resumable.ActivateResumableBatch(ctx, &db.ResumableActivation{
		BatchID:     jobID,
		ProcessorID: processorID,
		Epoch:       1,
		LeaseTTL:    time.Minute,
		Entries: []db.ResumableActivationEntry{
			pendingLineEntry(jobID, "req-1", 0),
			pendingLineEntry(jobID, "req-2", 1),
		},
	}); err != nil {
		t.Fatalf("ActivateResumableBatch: %v", err)
	}

	// The first incarnation checkpoints req-1, then dies.
	filesClient := mockfiles.NewMockBatchFilesClient(filesRoot)
	key, digest, size := storeCheckpointedResult(t, filesClient, tenantID, jobID, "req-1")
	if _, err := resumable.CheckpointEntryResult(ctx, jobID, stableBatchRequestID(jobID, "req-1"), processorID, 1, &db.ResumableResult{
		Status:       db.ResumableEntryCompleted,
		ResultKey:    key,
		ResultBytes:  size,
		ResultSHA256: digest,
	}); err != nil {
		t.Fatalf("CheckpointEntryResult: %v", err)
	}

	// The reconciler requeues the orphan, as the GC reconciler would.
	if err := resumable.RequeueResumableOrphan(ctx, jobID, processorID, 1, false, true); err != nil {
		t.Fatalf("RequeueResumableOrphan: %v", err)
	}

	// The requeue fences the dead incarnation out of every write: its pending
	// checkpoint at the old epoch must conflict, not silently apply.
	_, err = resumable.CheckpointEntryResult(ctx, jobID, stableBatchRequestID(jobID, "req-2"), processorID, 1, &db.ResumableResult{
		Status: db.ResumableEntryCompleted,
	})
	if !errors.Is(err, db.ErrConflict) {
		t.Fatalf("zombie checkpoint after requeue: err=%v, want ErrConflict", err)
	}

	// A second processor's unified dequeue re-claims the requeued row with a
	// fresh lease: the epoch was bumped to 2 by the requeue and to 3 by the
	// claim, and the recovery budget is preserved.
	pcfg := config.NewConfig()
	pcfg.WorkDir = t.TempDir()
	tasks, err := queue.PQDequeue(ctx, pcfg.Resumable.LeaseDuration, 1)
	if err != nil {
		t.Fatalf("PQDequeue: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != jobID || !tasks[0].Resumable || tasks[0].Epoch != 3 {
		t.Fatalf("dequeued tasks = %+v, want the requeued %s at epoch 3 with a resumable lease", tasks, jobID)
	}

	// The second incarnation resumes through the real PG stores; only the
	// file and async-inference layers stay fake.
	asyncClient := &fakeAsyncInferenceClient{
		submittedCh: make(chan *inference.GenerateRequest),
		results:     make(chan *inference.GenerateResponse, 64),
	}
	resolver := inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{
		"test-model": func() inference.AsyncInferenceClient { return asyncClient },
	})
	p := mustNewProcessorWithID(t, pcfg, &clientset.Clientset{
		BatchDB:         batchDB,
		BatchProgressDB: batchDB,
		FileDB:          fileDBClient,
		File:            filesClient,
		Event:           mockdb.NewMockBatchEventChannelClient(),
		Queue:           queue,
		AsyncInference:  resolver,
		Resumable:       resumable,
	}, processorID)

	bctx, bcancel := context.WithCancel(context.Background())
	residueSafe := residueGate(resumable, residueCheckTimeout)
	p.broadcasters = newBroadcasterRegistry(resolver, testLogger(t), residueSafe)
	p.broadcasters.Run(bctx)
	t.Cleanup(func() {
		bcancel()
		p.broadcasters.Wait()
	})

	jobItem, err := p.poller.fetchJobItemByID(ctx, jobID)
	if err != nil {
		t.Fatalf("fetchJobItemByID: %v", err)
	}
	jobInfo, err := batch_utils.FromDBItemToJobInfoObject(jobItem)
	if err != nil {
		t.Fatalf("FromDBItemToJobInfoObject: %v", err)
	}

	env := &durableJobEnv{
		p:           p,
		dbClient:    batchDB,
		fileDB:      fileDBClient,
		filesClient: filesClient,
		asyncClient: asyncClient,
		jobItem:     jobItem,
		jobInfo:     jobInfo,
	}
	done := env.runResumedJobAsync(t, tasks[0])

	// Answer every submitted request — the resumed run must submit only the
	// unfinished entry — until the run returns.
	go func() {
		for {
			select {
			case req := <-env.asyncClient.submittedCh:
				env.asyncClient.deliver(req.RequestID, map[string]any{"choices": []any{map[string]any{"text": "ok"}}})
			case <-done:
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("runJob did not return after the resumed execution")
	}

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 2, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}
	// The terminal document must carry the activation's in_progress_at —
	// building it from a stale in-memory status drops the timestamp.
	if status.InProgressAt == nil || *status.InProgressAt == 0 {
		t.Fatalf("in_progress_at = %v, want the activation timestamp", status.InProgressAt)
	}

	// The checkpointed entry was not re-submitted.
	if got := env.asyncClient.submitCount(); got != 1 {
		t.Fatalf("submitted requests = %d, want 1", got)
	}
	state, err := resumable.EntryResumeState(ctx, jobID)
	if err != nil {
		t.Fatalf("EntryResumeState: %v", err)
	}
	if state.Completed != 2 || state.Pending+state.InFlight+state.Failed != 0 {
		t.Fatalf("entry state = %+v, want 2 completed", state)
	}

	// Output artifact: both incarnations' objects, in input order.
	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 1, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	if len(outLines) != 2 {
		t.Fatalf("output artifact lines = %d, want 2", len(outLines))
	}
	for i, want := range []string{stableBatchRequestID(jobID, "req-1"), stableBatchRequestID(jobID, "req-2")} {
		if outLines[i].ID != want {
			t.Fatalf("output line %d id = %s, want %s", i, outLines[i].ID, want)
		}
	}
	// Line 0 carries the object the earlier incarnation checkpointed; line 1
	// this run's result.
	for i, wantText := range []string{"earlier-incarnation", "ok"} {
		choices, ok := outLines[i].Response.Body["choices"].([]any)
		if !ok || len(choices) != 1 {
			t.Fatalf("output line %d body = %+v, want one choice", i, outLines[i].Response.Body)
		}
		if text, ok := choices[0].(map[string]any)["text"]; !ok || text != wantText {
			t.Fatalf("output line %d choice text = %v, want %q", i, text, wantText)
		}
	}
}

// TestRunJob_Resumable_FreshActivationKeepsInProgressAt verifies the fresh
// activation path against real Postgres: the activation CTE writes
// in_progress (and its timestamp) in the row, and the terminal document must
// carry that timestamp — building it from the stale in-memory validating
// status silently drops it.
func TestRunJob_Resumable_FreshActivationKeepsInProgressAt(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := testLoggerCtx(t)
	const processorID = "processor-fresh-pg"
	const jobID = "job-resumable-pg-fresh"
	const tenantID = "tenant-1"
	inputFileID := "file-input-" + jobID

	filesRoot, fileDBClient := seedDurableInput(t, tenantID, inputFileID, []string{"test-model"})

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
	resumable, err := postgresql.NewPostgresResumableStore(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPostgresResumableStore: %v", err)
	}
	t.Cleanup(func() { _ = resumable.Close() })
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, "TRUNCATE batch_items CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}

	// A fresh validating row, as the unified dequeue delivers it — the
	// in-memory status the run starts from.
	if err := batchDB.DBStore(ctx, &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusValidating}),
			Spec:   mustJSON(t, openai.BatchSpec{InputFileID: inputFileID, Endpoint: openai.EndpointChatCompletions}),
		},
		ProcessorID: processorID,
		Priority:    time.Now().Add(1 * time.Hour).UnixMicro(),
		Epoch:       1,
	}); err != nil {
		t.Fatalf("seed batch item: %v", err)
	}

	filesClient := mockfiles.NewMockBatchFilesClient(filesRoot)
	asyncClient := &fakeAsyncInferenceClient{
		submittedCh: make(chan *inference.GenerateRequest),
		results:     make(chan *inference.GenerateResponse, 64),
	}
	resolver := inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{
		"test-model": func() inference.AsyncInferenceClient { return asyncClient },
	})
	pcfg := config.NewConfig()
	pcfg.WorkDir = t.TempDir()
	p := mustNewProcessorWithID(t, pcfg, &clientset.Clientset{
		BatchDB:         batchDB,
		BatchProgressDB: batchDB,
		FileDB:          fileDBClient,
		File:            filesClient,
		Event:           mockdb.NewMockBatchEventChannelClient(),
		Queue:           queue,
		AsyncInference:  resolver,
		Resumable:       resumable,
	}, processorID)

	bctx, bcancel := context.WithCancel(context.Background())
	residueSafe := residueGate(resumable, residueCheckTimeout)
	p.broadcasters = newBroadcasterRegistry(resolver, testLogger(t), residueSafe)
	p.broadcasters.Run(bctx)
	t.Cleanup(func() {
		bcancel()
		p.broadcasters.Wait()
	})

	jobItem, err := p.poller.fetchJobItemByID(ctx, jobID)
	if err != nil {
		t.Fatalf("fetchJobItemByID: %v", err)
	}
	jobInfo, err := batch_utils.FromDBItemToJobInfoObject(jobItem)
	if err != nil {
		t.Fatalf("FromDBItemToJobInfoObject: %v", err)
	}
	env := &durableJobEnv{
		p:           p,
		dbClient:    batchDB,
		fileDB:      fileDBClient,
		filesClient: filesClient,
		asyncClient: asyncClient,
		jobItem:     jobItem,
		jobInfo:     jobInfo,
	}

	// A fresh (non-requeued) task: runJob activates through the real CTE.
	done := env.runResumedJobAsync(t, &db.BatchJobPriority{
		ID:    jobID,
		SLO:   time.Now().Add(1 * time.Hour),
		Epoch: 1,
	})
	go func() {
		for {
			select {
			case req := <-env.asyncClient.submittedCh:
				env.asyncClient.deliver(req.RequestID, map[string]any{"choices": []any{map[string]any{"text": "ok"}}})
			case <-done:
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("runJob did not return after the fresh execution")
	}

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want completed", status.Status)
	}
	if status.InProgressAt == nil || *status.InProgressAt == 0 {
		t.Fatalf("in_progress_at = %v, want the activation timestamp", status.InProgressAt)
	}
	if status.CompletedAt != nil && *status.CompletedAt < *status.InProgressAt {
		t.Fatalf("completed_at (%d) < in_progress_at (%d): the activation must not stamp into the future", status.CompletedAt, status.InProgressAt)
	}
}
