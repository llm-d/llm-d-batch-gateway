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
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
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

	// Output artifact: both incarnations' objects, in input order. The
	// finalization ran at the epoch the requeue/claim chain left behind:
	// epoch 3 (1 seeded, 2 from the requeue, 3 from the claim).
	outLines := readArtifactLines(t, env, env.artifactKey(metrics.FileTypeOutput, 3))
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

// waitForLapsedLease polls the database clock until the batch's ownership
// lease has lapsed. Every PG fence compares owner_lease_expires_at against
// the database's NOW(), so polling that clock is the deterministic PG-tier
// equivalent of the unit tier's injectable clock: the short claim lease
// staged by a test is lived out against the clock that owns the fence.
func waitForLapsedLease(t *testing.T, pool *pgxpool.Pool, jobID string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		var lapsed bool
		if err := pool.QueryRow(context.Background(),
			`SELECT owner_lease_expires_at IS NOT NULL AND owner_lease_expires_at <= NOW()
			 FROM batch_items WHERE id = $1`, jobID,
		).Scan(&lapsed); err != nil {
			t.Fatalf("poll lease expiry of %s: %v", jobID, err)
		}
		if lapsed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ownership lease of %s never lapsed", jobID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestRunJob_Resumable_InterruptedFinalizationCompletesAfterRequeue is the
// PG-backed guard for the interrupted-finalization → requeue → replacement
// completion scenario of issue #744, driven through the real SQL fencing,
// result store, and file storage. A first incarnation's aborted-run
// finalization commits partial artifacts under its epoch's keys but loses
// its fenced terminal write on a lapsed lease; the reconciler requeue and a
// fresh dequeue claim hand the batch to a replacement generation at the
// bumped epoch, which executes the remaining request and re-finalizes with
// different artifact bytes. The replacement must complete the batch with
// the fresh epoch's artifact holding every result in input order, while the
// interrupted generation's committed artifacts survive untouched (never
// clobbers).
func TestRunJob_Resumable_InterruptedFinalizationCompletesAfterRequeue(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := testLoggerCtx(t)
	const processorID = "processor-refinalize-pg"
	const jobID = "job-resumable-pg-refinalize"
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

	// Stage 1 — the interrupted finalization. The first incarnation owns the
	// row at epoch 1 with req-1 checkpointed and req-2 never submitted; its
	// claim lease is staged with a one-second TTL so the database clock —
	// which owns every fence — lapses it promptly.
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

	// Activation through the real CTE, with req-1's checkpointed result in
	// the entry set: the row flips to in_progress/resumable under a
	// one-second lease.
	req1Key, req1Digest, req1Size := storeCheckpointedResult(t, filesClient, tenantID, jobID, "req-1")
	if err := resumable.ActivateResumableBatch(ctx, &db.ResumableActivation{
		BatchID:     jobID,
		ProcessorID: processorID,
		Epoch:       1,
		LeaseTTL:    time.Second,
		Entries: []db.ResumableActivationEntry{
			{
				RequestID:    stableBatchRequestID(jobID, "req-1"),
				Ordinal:      0,
				CustomID:     "req-1",
				Status:       string(db.ResumableEntryCompleted),
				ResultKey:    req1Key,
				ResultBytes:  req1Size,
				ResultSHA256: req1Digest,
			},
			pendingLineEntry(jobID, "req-2", 1),
		},
	}); err != nil {
		t.Fatalf("ActivateResumableBatch: %v", err)
	}

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

	// The aborted run's finalization, run once its lease has lapsed: the
	// artifact assembly is unfenced, so the partial artifacts — req-1's
	// result in the output artifact, req-2's not-executed line in the error
	// artifact — are committed under the epoch-1 keys, but the fenced
	// terminal write is refused by the lapsed lease and the row stays
	// non-terminal with no terminal document referencing them.
	waitForLapsedLease(t, pool, jobID)
	err = p.finalizeResumableAs(ctx, NewStatusUpdater(batchDB), jobItem, jobInfo,
		openai.BatchStatusFailed, batch_types.ErrCodeBatchFailed, openai.BatchStatusInProgress)
	if !errors.Is(err, db.ErrConflict) {
		t.Fatalf("interrupted finalization error = %v, want the fenced-out terminal write", err)
	}
	assertJobStatus(t, batchDB, jobID, openai.BatchStatusInProgress)

	// assertInterruptedArtifacts verifies the interrupted generation's
	// committed artifacts — called once right after the interrupted
	// finalization (proving the staging) and once after the replacement
	// completes (proving it never clobbered them).
	assertInterruptedArtifacts := func() {
		t.Helper()
		staleOutLines := readArtifactLines(t, env, env.artifactKey(metrics.FileTypeOutput, 1))
		if len(staleOutLines) != 1 || staleOutLines[0].ID != stableBatchRequestID(jobID, "req-1") {
			t.Fatalf("interrupted generation's output artifact = %+v, want its original single-result contents", staleOutLines)
		}
		staleErrLines := readArtifactLines(t, env, env.artifactKey(metrics.FileTypeError, 1))
		if len(staleErrLines) != 1 || staleErrLines[0].ID != stableBatchRequestID(jobID, "req-2") {
			t.Fatalf("interrupted generation's error artifact = %+v, want the not-executed req-2 line", staleErrLines)
		}
		if staleErrLines[0].Error == nil || staleErrLines[0].Error.Code != string(batch_types.ErrCodeBatchFailed) {
			t.Fatalf("interrupted generation's error line = %+v, want code %s", staleErrLines[0].Error, batch_types.ErrCodeBatchFailed)
		}
	}
	assertInterruptedArtifacts()

	// Stage 2 — the requeue and the replacement. The reconciler requeues the
	// orphaned row — the lapsed lease is exactly its trigger — bumping the
	// epoch to 2, and a fresh dequeue claim takes it to epoch 3 with a
	// current lease.
	if err := resumable.RequeueResumableOrphan(ctx, jobID, processorID, 1, true, true); err != nil {
		t.Fatalf("RequeueResumableOrphan: %v", err)
	}
	tasks, err := queue.PQDequeue(ctx, pcfg.Resumable.LeaseDuration, 1)
	if err != nil {
		t.Fatalf("PQDequeue: %v", err)
	}
	if len(tasks) != 1 || tasks[0].ID != jobID || !tasks[0].Resumable || tasks[0].Epoch != 3 {
		t.Fatalf("dequeued tasks = %+v, want the requeued %s at epoch 3 with a resumable lease", tasks, jobID)
	}

	// The replacement generation resumes through the real PG stores: only
	// the unfinished entry is dispatched, its result is checkpointed, and the
	// finalization is re-derived at the fresh epoch.
	jobItem, err = p.poller.fetchJobItemByID(ctx, jobID)
	if err != nil {
		t.Fatalf("fetchJobItemByID after claim: %v", err)
	}
	jobInfo, err = batch_utils.FromDBItemToJobInfoObject(jobItem)
	if err != nil {
		t.Fatalf("FromDBItemToJobInfoObject after claim: %v", err)
	}
	env.jobItem = jobItem
	env.jobInfo = jobInfo

	done := env.runResumedJobAsync(t, tasks[0])
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

	// The replacement completed the batch with complete artifacts.
	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 2, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}
	if got := env.asyncClient.submitCount(); got != 1 {
		t.Fatalf("submitted requests = %d, want 1 (only the unfinished entry)", got)
	}
	state, err := resumable.EntryResumeState(ctx, jobID)
	if err != nil {
		t.Fatalf("EntryResumeState: %v", err)
	}
	if state.Completed != 2 || state.Pending+state.InFlight+state.Failed != 0 {
		t.Fatalf("entry state = %+v, want 2 completed", state)
	}

	// The terminal document references the replacement's fresh epoch-3
	// artifact and no error artifact: the winning generation produced no
	// error lines, so the interrupted generation's error artifact must not
	// leak into the winner's file IDs.
	if status.OutputFileID == nil || *status.OutputFileID != env.artifactFileID(metrics.FileTypeOutput, 3) {
		t.Fatalf("output_file_id = %v, want the fresh epoch-3 artifact %s", status.OutputFileID, env.artifactFileID(metrics.FileTypeOutput, 3))
	}
	if status.ErrorFileID != nil {
		t.Fatalf("error_file_id = %v, want nil (the winning generation produced no error lines)", *status.ErrorFileID)
	}

	// The winning artifact holds every result in input order: the earlier
	// generation's checkpointed req-1 object plus the replacement's req-2
	// result — not the stale single-result artifact the interrupted
	// finalization committed.
	outLines := readArtifactLines(t, env, env.artifactKey(metrics.FileTypeOutput, 3))
	if len(outLines) != 2 {
		t.Fatalf("output artifact lines = %d, want 2", len(outLines))
	}
	for i, want := range []string{stableBatchRequestID(jobID, "req-1"), stableBatchRequestID(jobID, "req-2")} {
		if outLines[i].ID != want {
			t.Fatalf("output line %d id = %s, want %s", i, outLines[i].ID, want)
		}
	}
	for i, wantText := range []string{"earlier-incarnation", "ok"} {
		choices, ok := outLines[i].Response.Body["choices"].([]any)
		if !ok || len(choices) != 1 {
			t.Fatalf("output line %d body = %+v, want one choice", i, outLines[i].Response.Body)
		}
		if text, ok := choices[0].(map[string]any)["text"]; !ok || text != wantText {
			t.Fatalf("output line %d choice text = %v, want %q", i, text, wantText)
		}
	}

	// Never clobbers: the replacement wrote fresh epoch-3 keys, so the
	// interrupted generation's committed artifacts survive its finalization
	// untouched, as unreferenced losers.
	assertInterruptedArtifacts()
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
