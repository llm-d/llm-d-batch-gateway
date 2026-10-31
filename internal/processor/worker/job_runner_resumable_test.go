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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// durableJobEnv bundles the processor and mock stores of a durable (async
// dispatch) runJob test.
type durableJobEnv struct {
	p           *Processor
	dbClient    db.BatchProgressDBClient
	fileDB      db.FileDBClient
	filesClient *mockfiles.MockBatchFilesClient
	resumable   *mockdb.MockResumableStore
	events      *mockdb.MockBatchEventChannelClient
	pqClient    *mockdb.MockBatchPriorityQueueClient
	asyncClient *fakeAsyncInferenceClient
	altClient   *fakeAsyncInferenceClient
	jobItem     *db.BatchItem
	jobInfo     *batch_types.JobInfo
}

// rowSyncedResumable bridges the split-brain mock stores: production commits
// the fenced terminal status and the resumable-flag clear in one UPDATE on
// the batch row, but the mock row client and the mock resumable store are
// two maps. The decorator mirrors a successful FinalizeResumableBatch into
// the row mock so row-level assertions see what production's single
// statement would have committed.
type rowSyncedResumable struct {
	*mockdb.MockResumableStore
	rows db.BatchProgressDBClient
}

var _ db.ResumableStore = (*rowSyncedResumable)(nil)

func (r *rowSyncedResumable) FinalizeResumableBatch(ctx context.Context, fin *db.ResumableFinalization) error {
	if err := r.MockResumableStore.FinalizeResumableBatch(ctx, fin); err != nil {
		return err
	}
	items, _, _, err := r.rows.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{fin.BatchID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		return fmt.Errorf("mirror finalize of %s into row mock: err=%v len=%d", fin.BatchID, err, len(items))
	}
	item := *items[0]
	item.Status = fin.Status
	item.Resumable = false
	item.ExpectedResumable = nil
	item.OwnerLeaseExpiresAt = time.Time{}
	if err := r.rows.DBStore(ctx, &item); err != nil {
		return fmt.Errorf("mirror finalize of %s into row mock: %w", fin.BatchID, err)
	}
	return nil
}

// failOnceResumable wraps a ResumableStore and makes the first
// FinalizeResumableBatch call fail with db.ErrConflict (simulating a
// terminal write failure), then delegates to the inner store.
type failOnceResumable struct {
	db.ResumableStore
	failed atomic.Bool
}

func (f *failOnceResumable) FinalizeResumableBatch(ctx context.Context, fin *db.ResumableFinalization) error {
	if !f.failed.Swap(true) {
		return db.ErrConflict
	}
	return f.ResumableStore.FinalizeResumableBatch(ctx, fin)
}

// durableInputLine builds one input line for the given model and custom ID.
func durableInputLine(model, customID string) string {
	return fmt.Sprintf(`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"hi"}]}}`+"\n", customID, model)
}

// seedDurableInput writes the batch input file for the given models into a
// fresh mock file store and records its file DB item, returning the store's
// root and the file DB client.
func seedDurableInput(t *testing.T, tenantID, inputFileID string, models []string) (string, db.FileDBClient) {
	t.Helper()

	filesRoot := t.TempDir()
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	var input bytes.Buffer
	for i, model := range models {
		input.WriteString(durableInputLine(model, fmt.Sprintf("req-%d", i+1)))
	}
	storageDir := filepath.Join(filesRoot, folderName)
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		t.Fatalf("MkdirAll storage dir: %v", err)
	}
	storageName := ucom.FileStorageName(inputFileID, "input.jsonl")
	if err := os.WriteFile(filepath.Join(storageDir, storageName), input.Bytes(), 0o644); err != nil {
		t.Fatalf("write input to mock storage: %v", err)
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
	if err := fileDBClient.DBStore(testLoggerCtx(t), fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}
	return filesRoot, fileDBClient
}

// setupDurableJob seeds a validating batch and wires a processor for the
// durable pipeline: an async resolver with "test-model" and "alt-model"
// registered, each on its own client (in production a pool maps to a single
// model, so two broadcasters never share a result stream; every other model
// is rejected at ingestion), a mock resumable store seeded with the
// processor's claim, and manually-wired result broadcasters (Run wires them
// in production).
func setupDurableJob(t *testing.T, jobID string, models []string) *durableJobEnv {
	t.Helper()
	ctx := testLoggerCtx(t)

	const tenantID = "tenant-1"
	inputFileID := "file-input-" + jobID

	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	filesRoot, fileDBClient := seedDurableInput(t, tenantID, inputFileID, models)

	resumable := mockdb.NewMockResumableStore()
	resumable.SeedBatch(jobID, "test-processor", 1)

	dbClient := newSpyBatchDB(newMockBatchDBClient())
	jobItem := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: tenantID, Tags: db.Tags{}},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusValidating})},
		ProcessorID:  "test-processor",
		Epoch:        1,
		// The activation CTE flips the persisted resumable flag in production;
		// the mock stores cannot mirror that cross-store write, so seed the
		// post-activation value.
		Resumable: true,
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore batch item: %v", err)
	}
	mirrorResumableStatusWrites(dbClient, resumable)

	asyncClient := &fakeAsyncInferenceClient{
		submittedCh: make(chan *inference.GenerateRequest),
		results:     make(chan *inference.GenerateResponse, 64),
	}
	altClient := &fakeAsyncInferenceClient{
		submittedCh: make(chan *inference.GenerateRequest),
		results:     make(chan *inference.GenerateResponse, 64),
	}
	resolver := inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{
		"test-model": func() inference.AsyncInferenceClient { return asyncClient },
		"alt-model":  func() inference.AsyncInferenceClient { return altClient },
	})

	events := mockdb.NewMockBatchEventChannelClient()
	pqClient := mockdb.NewMockBatchPriorityQueueClient()
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB:         dbClient,
		BatchProgressDB: dbClient,
		FileDB:          fileDBClient,
		File:            mockfiles.NewMockBatchFilesClient(filesRoot),
		Event:           events,
		Queue:           pqClient,
		AsyncInference:  resolver,
		Resumable:       &rowSyncedResumable{MockResumableStore: resumable, rows: dbClient},
	})
	p.poller = NewPoller(pqClient, dbClient)

	bctx, bcancel := context.WithCancel(context.Background())
	p.broadcasters = newBroadcasterRegistry(resolver, testLogger(t), nil)
	p.broadcasters.Run(bctx)
	t.Cleanup(func() {
		bcancel()
		p.broadcasters.Wait()
	})

	jobInfo := &batch_types.JobInfo{
		JobID:    jobID,
		TenantID: tenantID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusValidating},
		},
	}

	return &durableJobEnv{
		p:           p,
		dbClient:    dbClient,
		fileDB:      fileDBClient,
		filesClient: mockfiles.NewMockBatchFilesClient(filesRoot),
		resumable:   resumable,
		events:      events,
		pqClient:    pqClient,
		asyncClient: asyncClient,
		altClient:   altClient,
		jobItem:     jobItem,
		jobInfo:     jobInfo,
	}
}

func (e *durableJobEnv) runJob(t *testing.T) {
	t.Helper()
	if !e.p.acquire(context.Background()) {
		t.Fatalf("expected token acquire before runJob")
	}
	e.p.wg.Add(1)
	e.p.runJob(testLoggerCtx(t), &jobExecutionParams{
		updater: NewStatusUpdater(e.dbClient),
		jobItem: e.jobItem,
		jobInfo: e.jobInfo,
		task:    &db.BatchJobPriority{ID: e.jobItem.ID, SLO: time.Now().Add(1 * time.Hour)},
	})
}

// runResumedJobAsync launches runJob for a requeued resumable task and returns
// a channel closed when it returns. The token and logger context are taken on
// the caller's goroutine so a staging failure fails the test, not the spawned
// one.
func (e *durableJobEnv) runResumedJobAsync(t *testing.T, task *db.BatchJobPriority) <-chan struct{} {
	t.Helper()
	if !e.p.acquire(context.Background()) {
		t.Fatalf("expected token acquire before runJob")
	}
	ctx := testLoggerCtx(t)
	done := make(chan struct{})
	e.p.wg.Add(1)
	go func() {
		defer close(done)
		e.p.runJob(ctx, &jobExecutionParams{
			updater: NewStatusUpdater(e.dbClient),
			jobItem: e.jobItem,
			jobInfo: e.jobInfo,
			task:    task,
		})
	}()
	return done
}

// runResumedJob runs runJob synchronously with a requeued resumable task —
// the delivery shape of the unified dequeue.
func (e *durableJobEnv) runResumedJob(t *testing.T, task *db.BatchJobPriority) {
	t.Helper()
	<-e.runResumedJobAsync(t, task)
}

// pendingLineEntry builds the activation entry of one unfinished input line.
func pendingLineEntry(jobID, customID string, ordinal int64) db.ResumableActivationEntry {
	return db.ResumableActivationEntry{
		RequestID: stableBatchRequestID(jobID, customID),
		Ordinal:   ordinal,
		CustomID:  customID,
		Status:    string(db.ResumableEntryPending),
	}
}

// storeCheckpointedResult stores a previous incarnation's result object for
// one input line at its canonical key, returning its key, SHA-256, and size.
func storeCheckpointedResult(t *testing.T, filesClient *mockfiles.MockBatchFilesClient, tenantID, jobID, customID string) (string, string, int64) {
	t.Helper()
	reqID := stableBatchRequestID(jobID, customID)
	line, digest, size, err := pipeline.MarshalResultLine(&pipeline.ResultItem{
		RequestID: reqID,
		CustomID:  customID,
		Response: &batch_types.ResponseData{
			StatusCode: 200,
			RequestID:  reqID,
			Body:       map[string]any{"choices": []any{map[string]any{"text": "earlier-incarnation"}}},
		},
	})
	if err != nil {
		t.Fatalf("marshal seeded result for %s: %v", customID, err)
	}
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	key := resumableResultKey(jobID, reqID)
	if _, err := filesClient.Store(testLoggerCtx(t), key, folder, 0, 0, bytes.NewReader(line)); err != nil {
		t.Fatalf("store seeded result object %s: %v", key, err)
	}
	return key, digest, size
}

// existingLineEntry builds an activation entry for a result object that is
// already stored, without re-storing it (the file store is create-only).
// It duplicates storeCheckpointedResult's marshalling but tolerates
// ErrFileExists, since the object may already be present from a prior
// incarnation's checkpoint.
func existingLineEntry(t *testing.T, env *durableJobEnv, jobID, customID string, ordinal int64) db.ResumableActivationEntry {
	t.Helper()
	reqID := stableBatchRequestID(jobID, customID)
	line, digest, size, err := pipeline.MarshalResultLine(&pipeline.ResultItem{
		RequestID: reqID,
		CustomID:  customID,
		Response: &batch_types.ResponseData{
			StatusCode: 200,
			RequestID:  reqID,
			Body:       map[string]any{"choices": []any{map[string]any{"text": "earlier-incarnation"}}},
		},
	})
	if err != nil {
		t.Fatalf("marshal existing result for %s: %v", customID, err)
	}
	folder, err := ucom.GetFolderNameByTenantID(env.jobInfo.TenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	key := resumableResultKey(jobID, reqID)
	if _, err := env.filesClient.Store(testLoggerCtx(t), key, folder, 0, 0, bytes.NewReader(line)); err != nil &&
		!errors.Is(err, filesapi.ErrFileExists) {
		t.Fatalf("store existing result object %s: %v", key, err)
	}
	return db.ResumableActivationEntry{
		RequestID:    reqID,
		Ordinal:      ordinal,
		CustomID:     customID,
		Status:       string(db.ResumableEntryCompleted),
		ResultKey:    key,
		ResultBytes:  size,
		ResultSHA256: digest,
	}
}

// checkpointedLineEntry stores a previous incarnation's result object for one
// input line at its canonical key and returns the activation entry that
// checkpoints it.
func checkpointedLineEntry(t *testing.T, env *durableJobEnv, jobID, customID string, ordinal int64) db.ResumableActivationEntry {
	t.Helper()
	key, digest, size := storeCheckpointedResult(t, env.filesClient, env.jobInfo.TenantID, jobID, customID)
	return db.ResumableActivationEntry{
		RequestID:    stableBatchRequestID(jobID, customID),
		Ordinal:      ordinal,
		CustomID:     customID,
		Status:       string(db.ResumableEntryCompleted),
		ResultKey:    key,
		ResultBytes:  size,
		ResultSHA256: digest,
	}
}

// stageResumedClaim stages the mock resumable store at the state the unified
// dequeue leaves behind after re-claiming a requeued row: the processor owns
// the batch at the given epoch with the given (partially checkpointed) entry
// set. The requeue/claim bookkeeping itself is PG-tested — the mock cannot
// express a claim over an already-activated batch.
func (e *durableJobEnv) stageResumedClaim(t *testing.T, epoch int64, entries ...db.ResumableActivationEntry) {
	e.stageResumedClaimWithLease(t, epoch, time.Minute, entries...)
}

// stageResumedClaimWithLease is stageResumedClaim with an explicit claim
// lease TTL, for tests that need the lease to lapse on a controlled clock.
func (e *durableJobEnv) stageResumedClaimWithLease(t *testing.T, epoch int64, ttl time.Duration, entries ...db.ResumableActivationEntry) {
	t.Helper()
	e.resumable.SeedBatch(e.jobItem.ID, e.jobItem.ProcessorID, epoch)
	err := e.resumable.ActivateResumableBatch(testLoggerCtx(t), &db.ResumableActivation{
		BatchID:     e.jobItem.ID,
		ProcessorID: e.jobItem.ProcessorID,
		Epoch:       epoch,
		LeaseTTL:    ttl,
		Entries:     entries,
	})
	if err != nil {
		t.Fatalf("stage resumed claim: %v", err)
	}
}

// durableJobStatus fetches the batch's persisted status info.
func durableJobStatus(t *testing.T, env *durableJobEnv) openai.BatchStatusInfo {
	t.Helper()
	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{env.jobItem.ID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet %s: err=%v len=%d", env.jobItem.ID, err, len(items))
	}
	var status openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &status); err != nil {
		t.Fatalf("unmarshal status for %s: %v", env.jobItem.ID, err)
	}
	return status
}

// readArtifactLines retrieves and parses one finalization artifact.
func readArtifactLines(t *testing.T, env *durableJobEnv, key string) []outputLine {
	t.Helper()
	folderName, err := ucom.GetFolderNameByTenantID(env.jobInfo.TenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	reader, _, err := env.filesClient.Retrieve(context.Background(), key, folderName)
	if err != nil {
		t.Fatalf("retrieve artifact %s: %v", key, err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read artifact %s: %v", key, err)
	}
	var lines []outputLine
	for _, line := range bytes.Split(bytes.TrimSpace(data), []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var ol outputLine
		if err := json.Unmarshal(line, &ol); err != nil {
			t.Fatalf("parse artifact line %s: %v", line, err)
		}
		lines = append(lines, ol)
	}
	return lines
}

// TestRunJob_Resumable_CompletesDurablePipeline verifies the durable
// orchestration end to end:
//
//	preProcessJob (ingestion rejection of the unregistered model)
//	→ activateResumable (rejected object PUT + fenced entry insert)
//	→ async dispatch with beforeSubmit fencing → result checkpointing
//	  (a 403 response is preserved as an output line, like the sync pipeline)
//	→ finalizeResumableJob (artifacts streamed from entry objects,
//	  deterministic file IDs, completed)
func TestRunJob_Resumable_CompletesDurablePipeline(t *testing.T) {
	const jobID = "job-resumable-happy"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model", "test-model", "missing-model"})

	// Answer the submitted requests: two successes around one preserved 403.
	go func() {
		for i := range 3 {
			req := <-env.asyncClient.submittedCh
			if i == 1 {
				env.asyncClient.deliverStatus(req.RequestID, http.StatusForbidden)
				continue
			}
			env.asyncClient.deliver(req.RequestID, map[string]any{"choices": []any{map[string]any{"text": "ok"}}})
		}
	}()

	env.runJob(t)

	reqID := func(customID string) string { return stableBatchRequestID(jobID, customID) }

	// Terminal state: completed, counts derived from the entry set, and
	// artifact file IDs derived from the batch.
	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var status openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &status); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 4, Completed: 2, Failed: 2}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}
	outFileID := stableFileID(jobID, 1, metrics.FileTypeOutput)
	errFileID := stableFileID(jobID, 1, metrics.FileTypeError)
	if status.OutputFileID == nil || *status.OutputFileID != outFileID {
		t.Fatalf("output_file_id = %v, want %s", status.OutputFileID, outFileID)
	}
	if status.ErrorFileID == nil || *status.ErrorFileID != errFileID {
		t.Fatalf("error_file_id = %v, want %s", status.ErrorFileID, errFileID)
	}

	// Only the registered-model requests were dispatched; the rejected line
	// never reached the queue.
	if got := env.asyncClient.submitCount(); got != 3 {
		t.Fatalf("submitted requests = %d, want 3", got)
	}

	// Entry set: two completed, two failed (the 403 and the rejected line).
	state, err := env.resumable.EntryResumeState(context.Background(), jobID)
	if err != nil {
		t.Fatalf("EntryResumeState: %v", err)
	}
	if state.Completed != 2 || state.Failed != 2 || state.Pending != 0 || state.InFlight != 0 {
		t.Fatalf("entry state = %+v, want 2 completed / 2 failed", state)
	}

	// Output artifact: the completed entries' objects plus the preserved 403,
	// in input order.
	outKey := ucom.FileStorageName(outFileID, jobOutputStorageName(jobID))
	outLines := readArtifactLines(t, env, outKey)
	if len(outLines) != 3 {
		t.Fatalf("output artifact lines = %d, want 3", len(outLines))
	}
	wantStatus := []int{http.StatusOK, http.StatusForbidden, http.StatusOK}
	for i, want := range []string{reqID("req-1"), reqID("req-2"), reqID("req-3")} {
		if outLines[i].ID != want {
			t.Fatalf("output line %d id = %s, want %s", i, outLines[i].ID, want)
		}
		if outLines[i].Error != nil || outLines[i].Response == nil || outLines[i].Response.StatusCode != wantStatus[i] {
			t.Fatalf("output line %d is not a %d response line: %+v", i, wantStatus[i], outLines[i])
		}
	}

	// Error artifact: the rejected entry's model_not_found object.
	errKey := ucom.FileStorageName(errFileID, jobErrorStorageName(jobID))
	errLines := readArtifactLines(t, env, errKey)
	if len(errLines) != 1 {
		t.Fatalf("error artifact lines = %d, want 1", len(errLines))
	}
	if errLines[0].ID != reqID("req-4") {
		t.Fatalf("error line id = %s, want %s", errLines[0].ID, reqID("req-4"))
	}
	if errLines[0].Error == nil || errLines[0].Error.Code != inference.ErrCodeModelNotFound {
		t.Fatalf("error line is not model_not_found: %+v", errLines[0])
	}

	// File records exist for both artifacts.
	for _, fileID := range []string{outFileID, errFileID} {
		files, _, _, err := env.fileDB.DBGet(context.Background(), &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{fileID}}}, true, 0, 1)
		if err != nil || len(files) != 1 {
			t.Fatalf("file record %s: err=%v len=%d", fileID, err, len(files))
		}
	}
}

// TestRunJob_Resumable_ActivationConflict_LeavesJobValidating verifies that a
// fenced-out activation (another incarnation owns the batch) aborts without a
// terminal write and without inserting entries — the new owner owns the
// batch's terminal state.
func TestRunJob_Resumable_ActivationConflict_LeavesJobValidating(t *testing.T) {
	const jobID = "job-resumable-conflict"

	env := setupDurableJob(t, jobID, []string{"test-model"})
	// A foreign owner already claimed the batch at the same epoch.
	env.resumable.SeedBatch(jobID, "other-processor", 1)

	env.runJob(t)

	assertJobStatus(t, env.dbClient, jobID, openai.BatchStatusValidating)

	state, err := env.resumable.EntryResumeState(context.Background(), jobID)
	if err != nil {
		t.Fatalf("EntryResumeState: %v", err)
	}
	if state.Pending+state.InFlight+state.Completed+state.Failed != 0 {
		t.Fatalf("entries inserted despite activation conflict: %+v", state)
	}
}

// TestRunPollingLoop_ResumableRouting covers the polling loop's handling of a
// claimed requeued resumable task (task.Resumable): the loop hands it to
// runJob whose pickup routing terminalizes terminal-bound rows with partial
// artifacts, a processor without the durable pipeline skips it fail-closed,
// and the shutdown guard requeues it through the resumable store instead of
// the legacy enqueue.
func TestRunPollingLoop_ResumableRouting(t *testing.T) {
	t.Run("sync processor skips the task without a terminal write", func(t *testing.T) {
		ctx := testLoggerCtx(t)

		cfg := config.NewConfig()
		cfg.PollInterval = 5 * time.Millisecond
		cfg.NumWorkers = 1

		pq := &spyPQ{inner: mockdb.NewMockBatchPriorityQueueClient()}
		dbClient := newSpyBatchDB(newMockBatchDBClient())
		jobID := "job-resumable-sync-skip"

		jobItem := &db.BatchItem{
			BaseIndexes: db.BaseIndexes{
				ID:       jobID,
				TenantID: "tenantA",
				Tags:     db.Tags{"tenant": "tenantA"},
			},
			BaseContents: db.BaseContents{
				Spec:   mustJSON(t, openai.BatchSpec{InputFileID: "unused"}),
				Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
			},
		}
		if err := dbClient.DBStore(ctx, jobItem); err != nil {
			t.Fatalf("DBStore job item: %v", err)
		}
		if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
			ID:        jobID,
			SLO:       time.Now().Add(-1 * time.Second),
			Resumable: true,
		}); err != nil {
			t.Fatalf("PQEnqueue task: %v", err)
		}

		p := mustNewProcessor(t, cfg, &clientset.Clientset{BatchDB: dbClient, Queue: pq})

		runCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
		defer cancel()
		if err := p.runPollingLoop(runCtx, runCtx); err != nil {
			t.Fatalf("runPollingLoop: %v", err)
		}

		// Fail-closed: the lapsed SLO must not expire the batch and the
		// in-memory task is dropped, not re-enqueued.
		if calls := dbClient.StatusCalls(openai.BatchStatusExpired); calls != 0 {
			t.Fatalf("sync processor wrote expired status %d times for a resumable task", calls)
		}
		assertJobStatus(t, dbClient, jobID, openai.BatchStatusInProgress)
		if calls := pq.EnqueueCalls(); calls != 1 {
			t.Fatalf("enqueue calls = %d, want only the seed", calls)
		}
	})

	t.Run("durable pickup terminalizes a cancelling row with partial artifacts", func(t *testing.T) {
		const jobID = "job-resumable-poll-route"
		ctx := testLoggerCtx(t)

		env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

		// A requeued cancelling row whose previous incarnation checkpointed
		// one of two results. The poll loop must not terminalize it itself —
		// runJob's pickup routing owns the durable finalize: the completed
		// entry's object lands in the output artifact, the pending entry
		// becomes a not-executed cancelled line, and nothing is dispatched.
		env.jobItem.Spec = mustJSON(t, openai.BatchSpec{
			InputFileID: "file-input-" + jobID,
			Endpoint:    openai.EndpointChatCompletions,
		})
		env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusCancelling})
		env.jobItem.Epoch = 2
		if err := env.dbClient.DBStore(ctx, env.jobItem); err != nil {
			t.Fatalf("DBStore job item: %v", err)
		}
		env.stageResumedClaim(t, 2,
			checkpointedLineEntry(t, env, jobID, "req-1", 0),
			pendingLineEntry(jobID, "req-2", 1),
		)
		// markCancelling flipped the row to cancelling before the requeue;
		// mirror it into the resumable store's fenced status.
		env.resumable.SetBatchStatus(jobID, string(openai.BatchStatusCancelling))

		if err := env.pqClient.PQEnqueue(ctx, &db.BatchJobPriority{
			ID:        jobID,
			SLO:       time.Now().Add(1 * time.Hour),
			Epoch:     2,
			Resumable: true,
		}); err != nil {
			t.Fatalf("PQEnqueue task: %v", err)
		}

		runCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		if err := env.p.runPollingLoop(runCtx, runCtx); err != nil {
			t.Fatalf("runPollingLoop: %v", err)
		}
		env.p.wg.Wait()

		status := durableJobStatus(t, env)
		if status.Status != openai.BatchStatusCancelled {
			t.Fatalf("status = %s, want cancelled", status.Status)
		}
		wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
		if status.RequestCounts != wantCounts {
			t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
		}

		// Output artifact: the checkpointed object of the completed entry.
		outLines := readArtifactLines(t, env,
			ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
		if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
			t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
		}

		// Error artifact: the pending entry as a not-executed cancelled line.
		errLines := readArtifactLines(t, env,
			ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeError), jobErrorStorageName(jobID)))
		if len(errLines) != 1 || errLines[0].ID != stableBatchRequestID(jobID, "req-2") {
			t.Fatalf("error artifact = %+v, want the not-executed req-2 line", errLines)
		}
		if errLines[0].Error == nil || errLines[0].Error.Code != string(batch_types.ErrCodeBatchCancelled) {
			t.Fatalf("error line is not batch_cancelled: %+v", errLines[0])
		}

		if got := env.asyncClient.submitCount(); got != 0 {
			t.Fatalf("submitted requests = %d, want 0 (a terminal-bound pickup dispatches nothing)", got)
		}
	})

	t.Run("shutdown guard requeues through the resumable store", func(t *testing.T) {
		const jobID = "job-resumable-poll-shutdown"
		ctx := testLoggerCtx(t)

		env := setupDurableJob(t, jobID, []string{"test-model"})

		// Put the mock batch in the activated state a requeued claim implies.
		// The Spec only needs to parse: runJob is never launched.
		env.jobItem.Spec = mustJSON(t, openai.BatchSpec{InputFileID: "unused"})
		env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
		if err := env.dbClient.DBStore(ctx, env.jobItem); err != nil {
			t.Fatalf("DBStore job item: %v", err)
		}
		err := env.resumable.ActivateResumableBatch(ctx, &db.ResumableActivation{
			BatchID:     jobID,
			ProcessorID: "test-processor",
			Epoch:       1,
			LeaseTTL:    time.Minute,
			Entries: []db.ResumableActivationEntry{
				{RequestID: "r-1", Ordinal: 0, CustomID: "c-1", Status: string(db.ResumableEntryPending)},
			},
		})
		if err != nil {
			t.Fatalf("ActivateResumableBatch: %v", err)
		}

		// Cancel the polling context right after the claim: the pre-launch
		// guard fires with the task in hand.
		runCtx, cancel := context.WithCancel(ctx)
		pq := &spyPQ{inner: env.pqClient, afterDequeueFn: cancel}
		env.p.poller = NewPoller(pq, env.dbClient)

		if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
			ID:        jobID,
			SLO:       time.Now().Add(1 * time.Hour),
			Epoch:     1,
			Resumable: true,
		}); err != nil {
			t.Fatalf("PQEnqueue task: %v", err)
		}

		if err := env.p.runPollingLoop(runCtx, runCtx); err != nil {
			t.Fatalf("runPollingLoop: %v", err)
		}

		// The batch is back in the queue via the fenced requeue: ownership
		// released, epoch bumped, one recovery attempt — and no terminal
		// write or legacy re-enqueue.
		owner, epoch, attempts := env.resumable.BatchOwnership(jobID)
		if owner != "" || epoch != 2 || attempts != 1 {
			t.Fatalf("ownership after shutdown requeue = (%q, %d, %d), want released at epoch 2 with 1 attempt", owner, epoch, attempts)
		}
		assertJobStatus(t, env.dbClient, jobID, openai.BatchStatusInProgress)
		if calls := pq.EnqueueCalls(); calls != 1 {
			t.Fatalf("enqueue calls = %d, want only the seed (legacy re-enqueue must be skipped)", calls)
		}
	})
}

// TestRunJob_Resumable_PickupRouting covers the terminal-bound pickup matrix
// of a requeued resumable task: every branch finalizes with partial artifacts
// (non-terminal entries become not-executed error lines) in its terminal
// status, without dispatching anything.
func TestRunJob_Resumable_PickupRouting(t *testing.T) {
	tests := []struct {
		name           string
		stage          func(env *durableJobEnv, task *db.BatchJobPriority)
		want           openai.BatchStatus
		wantNotExecVer batch_types.BatchErrorCode
	}{
		{
			name: "SLO-expired pickup finalizes as expired",
			stage: func(env *durableJobEnv, task *db.BatchJobPriority) {
				task.SLO = time.Now().Add(-1 * time.Second)
			},
			want:           openai.BatchStatusExpired,
			wantNotExecVer: batch_types.ErrCodeBatchExpired,
		},
		{
			name: "exhausted recovery budget finalizes as failed",
			stage: func(env *durableJobEnv, task *db.BatchJobPriority) {
				env.jobItem.RecoveryAttempts = maxRecoveryAttempts + 1
			},
			want:           openai.BatchStatusFailed,
			wantNotExecVer: batch_types.ErrCodeBatchFailed,
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobID := fmt.Sprintf("job-resumable-route-%d", i)

			env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})
			env.jobItem.Spec = mustJSON(t, openai.BatchSpec{
				InputFileID: "file-input-" + jobID,
				Endpoint:    openai.EndpointChatCompletions,
			})
			env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
			env.jobItem.Epoch = 2

			task := &db.BatchJobPriority{
				ID:        jobID,
				SLO:       time.Now().Add(1 * time.Hour),
				Epoch:     2,
				Resumable: true,
			}
			tt.stage(env, task)

			if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
				t.Fatalf("DBStore job item: %v", err)
			}
			env.stageResumedClaim(t, 2,
				checkpointedLineEntry(t, env, jobID, "req-1", 0),
				pendingLineEntry(jobID, "req-2", 1),
			)

			env.runResumedJob(t, task)

			status := durableJobStatus(t, env)
			if status.Status != tt.want {
				t.Fatalf("status = %s, want %s", status.Status, tt.want)
			}
			wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
			if status.RequestCounts != wantCounts {
				t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
			}

			outLines := readArtifactLines(t, env,
				ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
			if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
				t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
			}

			errLines := readArtifactLines(t, env,
				ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeError), jobErrorStorageName(jobID)))
			if len(errLines) != 1 || errLines[0].ID != stableBatchRequestID(jobID, "req-2") {
				t.Fatalf("error artifact = %+v, want the not-executed req-2 line", errLines)
			}
			if errLines[0].Error == nil || errLines[0].Error.Code != string(tt.wantNotExecVer) {
				t.Fatalf("error line code = %+v, want %s", errLines[0].Error, tt.wantNotExecVer)
			}

			if got := env.asyncClient.submitCount(); got != 0 {
				t.Fatalf("submitted requests = %d, want 0 (a terminal-bound pickup dispatches nothing)", got)
			}
		})
	}
}

// slowRetrieveFilesClient delays every retrieval, stretching artifact
// assembly past a staged claim's lease TTL.
type slowRetrieveFilesClient struct {
	filesapi.BatchFilesClient
	delay time.Duration
}

func (c *slowRetrieveFilesClient) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	time.Sleep(c.delay)
	return c.BatchFilesClient.Retrieve(ctx, fileName, folderName)
}

// TestRunJob_Resumable_PickupRenewsLeaseThroughTerminalization verifies the
// pickup-phase lease renewal: a terminal-bound requeued batch finalizes
// before any execution-phase setup, and its artifact assembly can run far
// past the lease TTL claimed at dequeue (finalizeTimeout defaults to 30m
// against a 5m lease). Without a renewal running from the pickup, the lease
// lapses mid-assembly, the fenced terminal write is refused, and the
// finalization loops through requeues.
func TestRunJob_Resumable_PickupRenewsLeaseThroughTerminalization(t *testing.T) {
	const jobID = "job-resumable-pickup-lease"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	// A short renewal interval so several ticks fire during the assembly,
	// and a staged claim lease the delayed assembly outlives unless the
	// renewal extends it: the fenced terminal write at the end of the
	// assembly then requires the renewed lease.
	env.p.cfg.Resumable.LeaseDuration = 100 * time.Millisecond
	env.p.files.storage = &slowRetrieveFilesClient{
		BatchFilesClient: env.p.files.storage.(*mockfiles.MockBatchFilesClient),
		delay:            600 * time.Millisecond,
	}

	env.jobItem.Spec = mustJSON(t, openai.BatchSpec{
		InputFileID: "file-input-" + jobID,
		Endpoint:    openai.EndpointChatCompletions,
	})
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusCancelling})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaimWithLease(t, 2, 200*time.Millisecond,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		pendingLineEntry(jobID, "req-2", 1),
	)
	// markCancelling flipped the row to cancelling before the requeue;
	// mirror it into the resumable store's fenced status.
	env.resumable.SetBatchStatus(jobID, string(openai.BatchStatusCancelling))

	env.runResumedJob(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Hour),
		Epoch:     2,
		Resumable: true,
	})

	// The renewal ran during the pickup's terminalization — before any
	// execution-phase setup registers one.
	if renewals := env.resumable.LeaseRenewals(jobID); renewals < 1 {
		t.Fatalf("lease renewals during the pickup = %d, want >= 1", renewals)
	}

	// The lease was alive at the fenced terminal write: the batch
	// terminalized as cancelled instead of being left for a requeue.
	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCancelled {
		t.Fatalf("status = %s, want cancelled (the lease lapsed before the fenced terminal write)", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}
	if got := env.asyncClient.submitCount(); got != 0 {
		t.Fatalf("submitted requests = %d, want 0 (a terminal-bound pickup dispatches nothing)", got)
	}
}

// TestRunJob_Resumable_ResumeSkipsCheckpointedEntries verifies the resumed
// incarnation of a requeued batch end to end: the persisted entry set passes
// the manifest integrity check, checkpointed entries are skipped at the source
// (never re-dispatched), and finalization streams both incarnations' result
// objects into one artifact in input order.
func TestRunJob_Resumable_ResumeSkipsCheckpointedEntries(t *testing.T) {
	const jobID = "job-resumable-resume"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	// The previous incarnation checkpointed req-1 before dying; the requeued
	// row was re-claimed at epoch 2 (the requeue/claim bookkeeping is
	// PG-tested — the mock stages the claimed state directly).
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		pendingLineEntry(jobID, "req-2", 1),
	)

	done := env.runResumedJobAsync(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Hour),
		Epoch:     2,
		Resumable: true,
	})

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
	<-done

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 2, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}

	// Only the unfinished entry was re-dispatched.
	if got := env.asyncClient.submitCount(); got != 1 {
		t.Fatalf("submitted requests = %d, want 1", got)
	}

	// Output artifact: both incarnations' objects, in input order.
	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	if len(outLines) != 2 {
		t.Fatalf("output artifact lines = %d, want 2", len(outLines))
	}
	wantIDs := []string{stableBatchRequestID(jobID, "req-1"), stableBatchRequestID(jobID, "req-2")}
	for i, want := range wantIDs {
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

// TestRunJob_Resumable_IntegrityMismatchFailsTheJob verifies that a persisted
// entry set that no longer matches the manifest re-derived from the input file
// fails the job instead of resuming a diverged batch. Because recoverResumable
// failed with the entry set already checkpointed, the failure must route
// through finalizeResumableAs rather than the legacy handleFailed: the terminal
// row carries the checkpointed entry's counts and output artifact, and the
// fenced terminal write clears the resumable flag.
func TestRunJob_Resumable_IntegrityMismatchFailsTheJob(t *testing.T) {
	const jobID = "job-resumable-integrity"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	// The input has two lines but only one entry survived: allowlist drift
	// between incarnations must be caught, not resumed.
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2, checkpointedLineEntry(t, env, jobID, "req-1", 0))

	env.runResumedJob(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Hour),
		Epoch:     2,
		Resumable: true,
	})

	if got := env.asyncClient.submitCount(); got != 0 {
		t.Fatalf("submitted requests = %d, want 0 (a diverged batch must not resume)", got)
	}

	// The recover failure finalizes through the entry set: the terminal row
	// carries the checkpointed entry's counts and output artifact, which the
	// legacy handleFailed would have dropped for a 0/0/0 plain failed write.
	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want %s", status.Status, openai.BatchStatusFailed)
	}
	wantCounts := openai.BatchRequestCounts{Total: 1, Completed: 1, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v (the checkpointed entry's counts)", status.RequestCounts, wantCounts)
	}
	outFileID := stableFileID(jobID, 2, metrics.FileTypeOutput)
	if status.OutputFileID == nil || *status.OutputFileID != outFileID {
		t.Fatalf("output_file_id = %v, want the committed artifact %s", status.OutputFileID, outFileID)
	}
	outLines := readArtifactLines(t, env, ucom.FileStorageName(outFileID, jobOutputStorageName(jobID)))
	if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
		t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
	}

	// The fenced terminal write cleared the resumable flag; the legacy path
	// would have left it set on the terminal row.
	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet %s: err=%v len=%d", jobID, err, len(items))
	}
	if items[0].Resumable {
		t.Fatal("terminal row is still flagged resumable")
	}
}

// TestRunJob_Resumable_MidRunOwnershipLoss verifies the zombie-abort contract
// when ownership is taken away mid-run: the fenced checkpoint surfaces
// db.ErrConflict, runJob aborts via handleOwnershipLost, and no terminal write
// or entry mutation happens — the new owner owns the batch's terminal state.
func TestRunJob_Resumable_MidRunOwnershipLoss(t *testing.T) {
	const jobID = "job-resumable-midrun-fence"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		pendingLineEntry(jobID, "req-1", 0),
		pendingLineEntry(jobID, "req-2", 1),
	)

	done := env.runResumedJobAsync(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Hour),
		Epoch:     2,
		Resumable: true,
	})

	// Let the run dispatch both entries, then take ownership away the way the
	// GC reconciler does, and answer the results: their checkpoints must
	// fence out and abort the run.
	var submitted []string
	for range 2 {
		req := <-env.asyncClient.submittedCh
		submitted = append(submitted, req.RequestID)
	}
	if err := env.resumable.RequeueResumableOrphan(context.Background(), jobID, env.jobItem.ProcessorID, 2, false, true); err != nil {
		t.Fatalf("RequeueResumableOrphan: %v", err)
	}
	for _, id := range submitted {
		env.asyncClient.deliver(id, map[string]any{"choices": []any{map[string]any{"text": "late"}}})
	}

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runJob did not return after the fenced checkpoint")
	}

	// No terminal write: the row stays in_progress for the new owner.
	assertJobStatus(t, env.dbClient, jobID, openai.BatchStatusInProgress)

	// Ownership was released to the requeue state; nothing was checkpointed
	// after the takeover.
	owner, epoch, attempts := env.resumable.BatchOwnership(jobID)
	if owner != "" || epoch != 3 || attempts != 1 {
		t.Fatalf("ownership = (%q, %d, %d), want released at epoch 3 with 1 attempt", owner, epoch, attempts)
	}
	state, err := env.resumable.EntryResumeState(context.Background(), jobID)
	if err != nil {
		t.Fatalf("EntryResumeState: %v", err)
	}
	if state.Completed != 0 || state.Failed != 0 || state.InFlight != 2 {
		t.Fatalf("entry state = %+v, want 2 in-flight and nothing checkpointed", state)
	}
}

// stageFinalizingClaim stages an in_progress row at epoch 2 with both entries
// checkpointed — the state a crashed incarnation leaves behind after
// checkpointing both results but before its terminal write.
func stageFinalizingClaim(t *testing.T, env *durableJobEnv, jobID string) {
	t.Helper()
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		checkpointedLineEntry(t, env, jobID, "req-2", 1),
	)
}

// TestFinalizeResumableJob_RefinalizesIdempotently covers the crash window
// between artifact commit and terminal write: the next incarnation re-derives
// the same artifact keys, verifies the committed bytes instead of rewriting
// them, completes the fenced terminal write, and purges the checkpointed
// result objects.
func TestFinalizeResumableJob_RefinalizesIdempotently(t *testing.T) {
	const jobID = "job-resumable-refinalize"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})
	stageFinalizingClaim(t, env, jobID)

	// The earlier incarnation's crash point: artifacts committed, terminal
	// write never happened.
	_, outFileID, _, err := env.p.assembleResumableArtifacts(testLoggerCtx(t), env.jobItem, env.jobInfo, "")
	if err != nil {
		t.Fatalf("first artifact assembly: %v", err)
	}
	outKey := ucom.FileStorageName(outFileID, jobOutputStorageName(jobID))
	wantLines := readArtifactLines(t, env, outKey)

	if err := env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo); err != nil {
		t.Fatalf("finalizeResumableJob: %v", err)
	}

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 2, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}
	if status.ErrorFileID != nil {
		t.Fatalf("error_file_id = %v, want nil", status.ErrorFileID)
	}

	// The re-derived artifact is byte-identical: same lines in the same order.
	gotLines := readArtifactLines(t, env, outKey)
	if len(gotLines) != len(wantLines) {
		t.Fatalf("re-finalized artifact lines = %d, want %d", len(gotLines), len(wantLines))
	}
	for i := range wantLines {
		if gotLines[i].ID != wantLines[i].ID {
			t.Fatalf("re-finalized line %d id = %s, want %s", i, gotLines[i].ID, wantLines[i].ID)
		}
	}

	// The checkpointed result objects are purged once the row is terminal.
	folder, err := ucom.GetFolderNameByTenantID(env.jobInfo.TenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	if _, _, err := env.filesClient.Retrieve(context.Background(), resumableResultKey(jobID, stableBatchRequestID(jobID, "req-1")), folder); err == nil {
		t.Fatal("result object survived the terminal write")
	}
}

// TestFinalizeResumableJob_PartialArtifactRetryWithNewEntries (issue #744)
// verifies that a finalization attempt that commits an artifact but fails the
// fenced terminal write does not block a later finalization after additional
// entries become terminal. The first attempt (epoch 2) commits an artifact at
// its epoch-scoped key; the terminal write fails (db.ErrConflict). The requeue
// bumps the epoch to 3, new entries are checkpointed, and the second attempt
// writes a fresh artifact key — the different (more complete) bytes cannot
// collide with the orphaned artifact, so the terminal write succeeds.
func TestFinalizeResumableJob_PartialArtifactRetryWithNewEntries(t *testing.T) {
	const jobID = "job-resumable-partial-retry"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model", "test-model", "test-model"})
	stageFinalizingClaim(t, env, jobID)

	// epoch 2 — first finalization attempt: the terminal write fails
	// (db.ErrConflict), but the artifact was already committed to S3.
	failingStore := &failOnceResumable{ResumableStore: env.p.resumable}
	env.p.resumable = failingStore
	err := env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo)
	if !errors.Is(err, db.ErrConflict) {
		t.Fatalf("first attempt error = %v, want db.ErrConflict", err)
	}

	// The epoch-2 artifact was committed before the terminal write failed.
	firstOutKey := ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID))
	firstLines := readArtifactLines(t, env, firstOutKey)
	if len(firstLines) != 2 {
		t.Fatalf("first attempt artifact lines = %d, want 2", len(firstLines))
	}

	// Requeue: restore the real store, epoch bumps to 3, req-3 + req-4 terminal.
	env.p.resumable = failingStore.ResumableStore
	env.jobItem.Epoch = 3
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore after requeue: %v", err)
	}
	env.resumable.SeedBatch(jobID, "test-processor", 3)

	// req-1/req-2 objects already exist (create-only) — build entries without
	// re-storing. req-3/req-4 are new.
	req1 := existingLineEntry(t, env, jobID, "req-1", 0)
	req2 := existingLineEntry(t, env, jobID, "req-2", 1)
	req3 := checkpointedLineEntry(t, env, jobID, "req-3", 2)
	req4 := checkpointedLineEntry(t, env, jobID, "req-4", 3)
	if err := env.resumable.ActivateResumableBatch(testLoggerCtx(t), &db.ResumableActivation{
		BatchID:     jobID,
		ProcessorID:  env.jobItem.ProcessorID,
		Epoch:        3,
		LeaseTTL:     time.Minute,
		Entries:      []db.ResumableActivationEntry{req1, req2, req3, req4},
	}); err != nil {
		t.Fatalf("re-activate after requeue: %v", err)
	}

	// epoch 3 — second attempt: 4 terminal entries, different artifact bytes.
	// The real store's FinalizeResumableBatch succeeds this time.
	if err := env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo); err != nil {
		t.Fatalf("retry finalizeResumableJob: %v", err)
	}

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status after retry = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 4, Completed: 4, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts after retry = %+v, want %+v", status.RequestCounts, wantCounts)
	}

	// The winning artifact (epoch 3) has all 4 results at a different key.
	secondOutKey := ucom.FileStorageName(stableFileID(jobID, 3, metrics.FileTypeOutput), jobOutputStorageName(jobID))
	secondLines := readArtifactLines(t, env, secondOutKey)
	if len(secondLines) != 4 {
		t.Fatalf("second attempt artifact lines = %d, want 4", len(secondLines))
	}

	// The epoch-scoped keys must not collide: the orphaned epoch-2 artifact
	// does not block the winning epoch-3 artifact (issue #744).
	if firstOutKey == secondOutKey {
		t.Fatalf("epoch-scoped keys collided: first=%s second=%s", firstOutKey, secondOutKey)
	}

	// The losing epoch-2 artifact was cleaned up by cleanupLosingArtifacts.
	folder, err := ucom.GetFolderNameByTenantID(env.jobInfo.TenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	if _, _, err := env.filesClient.Retrieve(context.Background(), firstOutKey, folder); err == nil {
		t.Fatal("losing epoch-2 artifact survived cleanup")
	}
}

// TestFinalizeResumableJob_IntegrityFailureLeavesRowRetryable verifies that
// a result object that no longer matches its checkpointed digest aborts the
// artifact write (no partial object is committed) and returns the error
// without a terminal write: the row stays non-terminal and resumable, so the
// reconciler requeue retries the finalization once the divergence is gone.
func TestFinalizeResumableJob_IntegrityFailureLeavesRowRetryable(t *testing.T) {
	const jobID = "job-resumable-integrity-fin"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})
	stageFinalizingClaim(t, env, jobID)

	// Corrupt req-2's stored object: the entry's checkpointed digest must
	// catch the divergence while streaming.
	folder, err := ucom.GetFolderNameByTenantID(env.jobInfo.TenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	corruptKey := resumableResultKey(jobID, stableBatchRequestID(jobID, "req-2"))
	if err := env.filesClient.Delete(testLoggerCtx(t), corruptKey, folder); err != nil {
		t.Fatalf("delete result object: %v", err)
	}
	if _, err := env.filesClient.Store(testLoggerCtx(t), corruptKey, folder, 0, 0, strings.NewReader("corrupted bytes\n")); err != nil {
		t.Fatalf("store corrupted object: %v", err)
	}

	err = env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo)
	if err == nil || errors.Is(err, errFinalizeFailedOver) {
		t.Fatalf("error = %v, want the surfaced assembly failure", err)
	}

	// No terminal write and no unfenced failed fallback: the row stays in the
	// finalizing status the finalizer wrote before assembly, still resumable,
	// so the reconciler requeue can retry the finalization.
	assertJobStatus(t, env.dbClient, jobID, openai.BatchStatusFinalizing)
	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet %s: err=%v len=%d", jobID, err, len(items))
	}
	if !items[0].Resumable {
		t.Fatal("row lost the resumable flag; the requeue cannot retry the finalization")
	}

	// The aborted output artifact was never committed.
	outKey := ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID))
	if _, _, err := env.filesClient.Retrieve(context.Background(), outKey, folder); err == nil {
		t.Fatal("aborted output artifact is visible in storage")
	}

	// The retry a replacement incarnation runs after the requeue completes
	// the batch: re-finalization is idempotent, so the artifacts are rebuilt
	// (or verified) under a current lease.
	if err := env.filesClient.Delete(testLoggerCtx(t), corruptKey, folder); err != nil {
		t.Fatalf("delete corrupted object for retry: %v", err)
	}
	storeCheckpointedResult(t, env.filesClient, env.jobInfo.TenantID, jobID, "req-2")
	if err := env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo); err != nil {
		t.Fatalf("retry finalizeResumableJob: %v", err)
	}
	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status after retry = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 2, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts after retry = %+v, want %+v", status.RequestCounts, wantCounts)
	}
}

// failingCommitFilesClient wraps the mock files store and fails Commit for
// one artifact key — the crash window between the output artifact's commit
// and the error artifact's.
type failingCommitFilesClient struct {
	*mockfiles.MockBatchFilesClient
	failKey string
}

func (c *failingCommitFilesClient) OpenStream(ctx context.Context, fileName, folderName string) (filesapi.StreamWriter, error) {
	w, err := c.MockBatchFilesClient.OpenStream(ctx, fileName, folderName)
	if err != nil || fileName != c.failKey {
		return w, err
	}
	return &failingCommitWriter{StreamWriter: w}, nil
}

// failingCommitWriter accepts writes but never commits.
type failingCommitWriter struct {
	filesapi.StreamWriter
}

func (w *failingCommitWriter) Commit(ctx context.Context) (*filesapi.BatchFileMetadata, error) {
	return nil, errors.New("injected error-artifact commit failure")
}

// TestFinalizeResumableJob_ErrorArtifactCommitFailureKeepsOutput covers the
// window between the output artifact's commit and the error artifact's:
// the failure returns the error without a terminal write, so the row stays
// non-terminal for the reconciler requeue while the committed output
// artifact survives in storage — and the retry completes both artifacts.
func TestFinalizeResumableJob_ErrorArtifactCommitFailureKeepsOutput(t *testing.T) {
	const jobID = "job-resumable-partial-artifacts"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	// One completed entry (output artifact) and one failed entry with an
	// error object (error artifact), so both writers open during assembly.
	env.stageResumedClaim(t, 2,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		rejectedLineEntry(t, env, jobID, "req-2", 1),
	)

	// Fail only the error artifact's commit; the output artifact commits.
	errKey := ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeError), jobErrorStorageName(jobID))
	env.p.files.storage = &failingCommitFilesClient{
		MockBatchFilesClient: env.p.files.storage.(*mockfiles.MockBatchFilesClient),
		failKey:              errKey,
	}

	err := env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo)
	if err == nil || errors.Is(err, errFinalizeFailedOver) {
		t.Fatalf("finalizeResumableJob error = %v, want the surfaced assembly failure", err)
	}

	// No terminal write and no unfenced failed fallback: the row stays in the
	// finalizing status, so the reconciler requeue retries the finalization.
	assertJobStatus(t, env.dbClient, jobID, openai.BatchStatusFinalizing)

	// The committed output artifact survives in storage with its file record,
	// instead of being orphaned by a degraded failed write.
	outFileID := stableFileID(jobID, 2, metrics.FileTypeOutput)
	outLines := readArtifactLines(t, env, ucom.FileStorageName(outFileID, jobOutputStorageName(jobID)))
	if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
		t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
	}
	files, _, _, err := env.fileDB.DBGet(context.Background(), &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{outFileID}}}, true, 0, 1)
	if err != nil || len(files) != 1 {
		t.Fatalf("file record %s: err=%v len=%d", outFileID, err, len(files))
	}

	// The retry a replacement incarnation runs after the requeue completes
	// the batch: the committed output artifact is re-derived and verified
	// (existing mode), the error artifact commits, and the fenced terminal
	// write records both.
	env.p.files.storage = env.filesClient
	if err := env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo); err != nil {
		t.Fatalf("retry finalizeResumableJob: %v", err)
	}
	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status after retry = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts after retry = %+v, want %+v", status.RequestCounts, wantCounts)
	}
	if status.OutputFileID == nil || *status.OutputFileID != outFileID {
		t.Fatalf("output_file_id = %v, want the committed artifact %s", status.OutputFileID, outFileID)
	}
	if status.ErrorFileID == nil || *status.ErrorFileID != stableFileID(jobID, 2, metrics.FileTypeError) {
		t.Fatalf("error_file_id = %v, want the error artifact", status.ErrorFileID)
	}
}

// TestFinalizeResumableJob_FencedOutZombieCannotComplete verifies the lease
// fence from the other side: a zombie incarnation whose lease expired (but
// whose row was not requeued) cannot write completed — the fenced write
// refuses, and the row stays non-terminal and resumable for the
// fence-passing owner instead of being degraded by an unfenced failed write.
func TestFinalizeResumableJob_FencedOutZombieCannotComplete(t *testing.T) {
	const jobID = "job-resumable-zombie"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})
	stageFinalizingClaim(t, env, jobID)

	// The claim's lease (staged with a one-minute TTL) has expired.
	env.resumable.Now = func() time.Time { return time.Now().Add(2 * time.Minute) }

	err := env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo)
	if !errors.Is(err, db.ErrConflict) {
		t.Fatalf("error = %v, want a fenced-out conflict", err)
	}

	// No terminal write and no unfenced failed fallback: the row stays in the
	// finalizing status the finalizer wrote before assembly, still resumable,
	// so the fence-passing owner (or a requeue) can complete it.
	assertJobStatus(t, env.dbClient, jobID, openai.BatchStatusFinalizing)
	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet %s: err=%v len=%d", jobID, err, len(items))
	}
	if !items[0].Resumable {
		t.Fatal("row lost the resumable flag; the requeue cannot retry the finalization")
	}
}

// TestFinalizeResumableJob_CancelDuringFinalization verifies that a user
// cancel landing after artifact assembly but before the terminal write is
// honoured: the fenced write targets cancelled, and both checkpointed results
// land in the output artifact.
func TestFinalizeResumableJob_CancelDuringFinalization(t *testing.T) {
	const jobID = "job-resumable-cancel-fin"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})
	stageFinalizingClaim(t, env, jobID)

	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(batchctx.ErrCancelled)

	err := env.p.finalizeResumableJob(ctx, NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo)
	if !errors.Is(err, batchctx.ErrCancelled) {
		t.Fatalf("error = %v, want batchctx.ErrCancelled", err)
	}

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCancelled {
		t.Fatalf("status = %s, want cancelled", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 2, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}
	if status.ErrorFileID != nil {
		t.Fatalf("error_file_id = %v, want nil", status.ErrorFileID)
	}

	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	wantIDs := []string{stableBatchRequestID(jobID, "req-1"), stableBatchRequestID(jobID, "req-2")}
	if len(outLines) != len(wantIDs) {
		t.Fatalf("output artifact lines = %d, want %d", len(outLines), len(wantIDs))
	}
	for i, want := range wantIDs {
		if outLines[i].ID != want {
			t.Fatalf("output line %d id = %s, want %s", i, outLines[i].ID, want)
		}
	}
}

// TestRunJob_Resumable_ArtifactFollowsInputOrderAcrossModels verifies that
// the final artifact is ordered by input line, not by completion order, when
// a batch spans several models whose requests resolve out of order.
func TestRunJob_Resumable_ArtifactFollowsInputOrderAcrossModels(t *testing.T) {
	const jobID = "job-resumable-order"

	env := setupDurableJob(t, jobID, []string{"alt-model", "test-model", "alt-model"})

	// Collect all three submits across both models' clients, then deliver
	// their results in reverse through the submitting client.
	go func() {
		type submission struct {
			client *fakeAsyncInferenceClient
			req    *inference.GenerateRequest
		}
		var subs []submission
		for range 3 {
			select {
			case req := <-env.asyncClient.submittedCh:
				subs = append(subs, submission{env.asyncClient, req})
			case req := <-env.altClient.submittedCh:
				subs = append(subs, submission{env.altClient, req})
			}
		}
		for i := len(subs) - 1; i >= 0; i-- {
			subs[i].client.deliver(subs[i].req.RequestID, map[string]any{"choices": []any{map[string]any{"text": "ok"}}})
		}
	}()

	env.runJob(t)

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 3, Completed: 3, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}

	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 1, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	wantIDs := []string{
		stableBatchRequestID(jobID, "req-1"),
		stableBatchRequestID(jobID, "req-2"),
		stableBatchRequestID(jobID, "req-3"),
	}
	if len(outLines) != len(wantIDs) {
		t.Fatalf("output artifact lines = %d, want %d", len(outLines), len(wantIDs))
	}
	for i, want := range wantIDs {
		if outLines[i].ID != want {
			t.Fatalf("output line %d id = %s, want %s (input order, not completion order)", i, outLines[i].ID, want)
		}
	}
}

// TestRunJob_Resumable_MidRunCancelFinalizesThroughTheFence covers a user
// cancel arriving mid-execution: the API server has already flipped the row
// to cancelling, the in-flight request is drained as a batch_cancelled error
// and checkpointed, and the terminal write is the fenced cancelling →
// cancelled transition with partial artifacts.
func TestRunJob_Resumable_MidRunCancelFinalizesThroughTheFence(t *testing.T) {
	const jobID = "job-resumable-midrun-cancel"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	env.jobItem.Spec = mustJSON(t, openai.BatchSpec{
		InputFileID: "file-input-" + jobID,
		Endpoint:    openai.EndpointChatCompletions,
	})
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		pendingLineEntry(jobID, "req-2", 1),
	)

	done := env.runResumedJobAsync(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Hour),
		Epoch:     2,
		Resumable: true,
	})

	// Only the unfinished entry is dispatched.
	if req := <-env.asyncClient.submittedCh; req.RequestID != stableBatchRequestID(jobID, "req-2") {
		t.Fatalf("submitted %s, want the unfinished req-2", req.RequestID)
	}

	// markCancelling flipped the row before the cancel event was sent; mirror
	// it into both mock stores the way production's single UPDATE would.
	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet %s: err=%v len=%d", jobID, err, len(items))
	}
	row := *items[0]
	row.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusCancelling})
	if err := env.dbClient.DBStore(testLoggerCtx(t), &row); err != nil {
		t.Fatalf("DBStore cancelling row: %v", err)
	}
	env.resumable.SetBatchStatus(jobID, string(openai.BatchStatusCancelling))

	if _, err := env.events.ECProducerSendEvents(testLoggerCtx(t), []db.BatchEvent{{ID: jobID, Type: db.BatchEventCancel, TTL: 60}}); err != nil {
		t.Fatalf("send cancel event: %v", err)
	}

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runJob did not return after the cancel event")
	}

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCancelled {
		t.Fatalf("status = %s, want cancelled", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}

	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
		t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
	}

	errLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeError), jobErrorStorageName(jobID)))
	if len(errLines) != 1 || errLines[0].ID != stableBatchRequestID(jobID, "req-2") {
		t.Fatalf("error artifact = %+v, want the req-2 line", errLines)
	}
	if errLines[0].Error == nil || errLines[0].Error.Code != string(batch_types.ErrCodeBatchCancelled) {
		t.Fatalf("error line = %+v, want code %s", errLines[0], batch_types.ErrCodeBatchCancelled)
	}
}

// TestRunJob_Resumable_MidRunExpiryFinalizesThroughTheFence covers the SLO
// deadline firing mid-execution: the in-flight request is drained as a
// batch_expired error and checkpointed, and the in_progress → expired
// transition goes through the fence.
func TestRunJob_Resumable_MidRunExpiryFinalizesThroughTheFence(t *testing.T) {
	const jobID = "job-resumable-midrun-expiry"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	env.jobItem.Spec = mustJSON(t, openai.BatchSpec{
		InputFileID: "file-input-" + jobID,
		Endpoint:    openai.EndpointChatCompletions,
	})
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		pendingLineEntry(jobID, "req-2", 1),
	)

	// The SLO is in the future at pickup (so the run resumes) but fires while
	// req-2 is in flight; the request is never answered.
	env.runResumedJob(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Second),
		Epoch:     2,
		Resumable: true,
	})

	if got := env.asyncClient.submitCount(); got != 1 {
		t.Fatalf("submitted requests = %d, want 1", got)
	}

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusExpired {
		t.Fatalf("status = %s, want expired", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}

	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
		t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
	}

	errLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeError), jobErrorStorageName(jobID)))
	if len(errLines) != 1 || errLines[0].ID != stableBatchRequestID(jobID, "req-2") {
		t.Fatalf("error artifact = %+v, want the req-2 line", errLines)
	}
	if errLines[0].Error == nil || errLines[0].Error.Code != string(batch_types.ErrCodeBatchExpired) {
		t.Fatalf("error line = %+v, want code %s", errLines[0], batch_types.ErrCodeBatchExpired)
	}
}

// inputGateFilesClient gates the input file's retrieval so a test can land
// an abort during preprocessing — before recoverResumable constructs the
// result sink — and release (or abort) it deterministically.
type inputGateFilesClient struct {
	filesapi.BatchFilesClient
	gateKey string
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *inputGateFilesClient) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	if fileName != c.gateKey {
		return c.BatchFilesClient.Retrieve(ctx, fileName, folderName)
	}
	c.once.Do(func() { close(c.reached) })
	select {
	case <-c.release:
		return c.BatchFilesClient.Retrieve(ctx, fileName, folderName)
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}
}

// TestRunJob_Resumable_CancelDuringPreprocessingFinalizesThroughTheFence
// covers the ingestion window of a resumed pickup: the row already carries
// its checkpointed entry set, but recoverResumable has not constructed the
// result sink yet. A user cancel landing in that window must finalize
// through the entry set — the fenced cancelling → cancelled transition with
// partial artifacts — not through the legacy write, which would drop the
// artifact IDs and strand the checkpointed output.
func TestRunJob_Resumable_CancelDuringPreprocessingFinalizesThroughTheFence(t *testing.T) {
	const jobID = "job-resumable-cancel-ingest"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	env.jobItem.Spec = mustJSON(t, openai.BatchSpec{
		InputFileID: "file-input-" + jobID,
		Endpoint:    openai.EndpointChatCompletions,
	})
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		pendingLineEntry(jobID, "req-2", 1),
	)

	// Hold the run in preprocessing at the input-file download.
	gated := &inputGateFilesClient{
		BatchFilesClient: env.p.files.storage.(*mockfiles.MockBatchFilesClient),
		gateKey:          ucom.FileStorageName("file-input-"+jobID, "input.jsonl"),
		reached:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	env.p.files.storage = gated

	done := env.runResumedJobAsync(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Hour),
		Epoch:     2,
		Resumable: true,
	})

	// Preprocessing reached the input download: the sink does not exist yet.
	select {
	case <-gated.reached:
	case <-time.After(30 * time.Second):
		t.Fatal("preprocessing never reached the input download")
	}

	// The user cancels: the API flips the row to cancelling before the
	// event is delivered; mirror the flip into both mock stores the way
	// production's single UPDATE would.
	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet %s: err=%v len=%d", jobID, err, len(items))
	}
	row := *items[0]
	row.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusCancelling})
	if err := env.dbClient.DBStore(testLoggerCtx(t), &row); err != nil {
		t.Fatalf("DBStore cancelling row: %v", err)
	}
	env.resumable.SetBatchStatus(jobID, string(openai.BatchStatusCancelling))
	if _, err := env.events.ECProducerSendEvents(testLoggerCtx(t), []db.BatchEvent{{ID: jobID, Type: db.BatchEventCancel, TTL: 60}}); err != nil {
		t.Fatalf("send cancel event: %v", err)
	}

	// The abort cancels the gated download; the run finalizes through the
	// entry set. The gate is never released — only the abort may unblock it.
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("runJob did not return after the cancel during preprocessing")
	}

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCancelled {
		t.Fatalf("status = %s, want cancelled", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}

	// The checkpointed entry's object is the output artifact; the pending
	// entry is a not-executed cancelled line.
	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
		t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
	}
	errLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeError), jobErrorStorageName(jobID)))
	if len(errLines) != 1 || errLines[0].ID != stableBatchRequestID(jobID, "req-2") {
		t.Fatalf("error artifact = %+v, want the not-executed req-2 line", errLines)
	}
	if errLines[0].Error == nil || errLines[0].Error.Code != string(batch_types.ErrCodeBatchCancelled) {
		t.Fatalf("error line = %+v, want code %s", errLines[0].Error, batch_types.ErrCodeBatchCancelled)
	}

	if got := env.asyncClient.submitCount(); got != 0 {
		t.Fatalf("submitted requests = %d, want 0 (nothing dispatched past preprocessing)", got)
	}
}

// TestRunJob_Resumable_ExpiryDuringPreprocessingFinalizesThroughTheFence
// covers the SLO deadline firing during the ingestion window of a resumed
// pickup: like the cancel, it must finalize through the entry set with
// partial artifacts rather than the legacy expired write.
func TestRunJob_Resumable_ExpiryDuringPreprocessingFinalizesThroughTheFence(t *testing.T) {
	const jobID = "job-resumable-expiry-ingest"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	env.jobItem.Spec = mustJSON(t, openai.BatchSpec{
		InputFileID: "file-input-" + jobID,
		Endpoint:    openai.EndpointChatCompletions,
	})
	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		pendingLineEntry(jobID, "req-2", 1),
	)

	// Hold the run in preprocessing at the input-file download; the gate is
	// never released, so only the SLO deadline can unblock it.
	gated := &inputGateFilesClient{
		BatchFilesClient: env.p.files.storage.(*mockfiles.MockBatchFilesClient),
		gateKey:          ucom.FileStorageName("file-input-"+jobID, "input.jsonl"),
		reached:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	env.p.files.storage = gated

	// The SLO is in the future at pickup (so the run resumes) but fires
	// while preprocessing is held at the gate.
	env.runResumedJob(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(2 * time.Second),
		Epoch:     2,
		Resumable: true,
	})

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusExpired {
		t.Fatalf("status = %s, want expired", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}

	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
		t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
	}
	errLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeError), jobErrorStorageName(jobID)))
	if len(errLines) != 1 || errLines[0].ID != stableBatchRequestID(jobID, "req-2") {
		t.Fatalf("error artifact = %+v, want the not-executed req-2 line", errLines)
	}
	if errLines[0].Error == nil || errLines[0].Error.Code != string(batch_types.ErrCodeBatchExpired) {
		t.Fatalf("error line = %+v, want code %s", errLines[0].Error, batch_types.ErrCodeBatchExpired)
	}

	if got := env.asyncClient.submitCount(); got != 0 {
		t.Fatalf("submitted requests = %d, want 0 (nothing dispatched past preprocessing)", got)
	}
}

// TestHandleJobError_Resumable_MidRunFailureFinalizesThroughTheFence covers
// the system-error branch of the durable mid-run routing: a backend failure
// after activation terminalizes through the fence with the unfinished entry
// rendered as a not-executed batch_failed line.
func TestHandleJobError_Resumable_MidRunFailureFinalizesThroughTheFence(t *testing.T) {
	const jobID = "job-resumable-midrun-failure"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})

	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		checkpointedLineEntry(t, env, jobID, "req-1", 0),
		pendingLineEntry(jobID, "req-2", 1),
	)

	sink, err := newBatchResultStore(env.filesClient, env.p.resumable, jobID, env.jobInfo.TenantID, env.p.processorID, 2)
	if err != nil {
		t.Fatalf("newBatchResultStore: %v", err)
	}

	env.p.handleJobError(testLoggerCtx(t), &jobExecutionParams{
		updater:    NewStatusUpdater(env.dbClient),
		jobItem:    env.jobItem,
		jobInfo:    env.jobInfo,
		task:       &db.BatchJobPriority{ID: jobID, SLO: time.Now().Add(1 * time.Hour), Epoch: 2, Resumable: true},
		resultSink: sink,
	}, fmt.Errorf("inference backend unavailable"))

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}

	outLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID)))
	if len(outLines) != 1 || outLines[0].ID != stableBatchRequestID(jobID, "req-1") {
		t.Fatalf("output artifact = %+v, want the checkpointed req-1 line", outLines)
	}

	errLines := readArtifactLines(t, env,
		ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeError), jobErrorStorageName(jobID)))
	if len(errLines) != 1 || errLines[0].ID != stableBatchRequestID(jobID, "req-2") {
		t.Fatalf("error artifact = %+v, want the not-executed req-2 line", errLines)
	}
	if errLines[0].Error == nil || errLines[0].Error.Code != string(batch_types.ErrCodeBatchFailed) {
		t.Fatalf("error line = %+v, want code %s", errLines[0], batch_types.ErrCodeBatchFailed)
	}
}

// panickingOpenStreamClient returns stream writers that panic on their
// first write — a crash after the checkpoints are durably committed but
// before the artifacts complete, on the runJob goroutine.
type panickingOpenStreamClient struct {
	filesapi.BatchFilesClient
}

func (c *panickingOpenStreamClient) OpenStream(ctx context.Context, fileName, folderName string) (filesapi.StreamWriter, error) {
	w, err := c.BatchFilesClient.OpenStream(ctx, fileName, folderName)
	if err != nil {
		return nil, err
	}
	return panickingStreamWriter{StreamWriter: w}, nil
}

// panickingStreamWriter panics on Write.
type panickingStreamWriter struct {
	filesapi.StreamWriter
}

func (w panickingStreamWriter) Write(p []byte) (int, error) {
	panic("injected artifact-write panic")
}

// TestRunJob_Resumable_PanicAfterCheckpointLeavesRowRetryable verifies the
// panic path of a durable batch: a crash after results are checkpointed but
// before the artifacts complete must NOT terminalize the row with the
// unfenced legacy failed write (which would supply no artifact IDs and make
// the row ineligible for recovery). The row stays non-terminal and resumable,
// and a replacement incarnation's finalization completes it.
func TestRunJob_Resumable_PanicAfterCheckpointLeavesRowRetryable(t *testing.T) {
	const jobID = "job-resumable-panic-after-checkpoint"

	env := setupDurableJob(t, jobID, []string{"test-model", "test-model"})
	// Both entries are checkpointed: the panic lands during artifact
	// assembly, after the durable work.
	stageFinalizingClaim(t, env, jobID)

	// The artifact write panics on the runJob goroutine.
	env.p.files.storage = &panickingOpenStreamClient{
		BatchFilesClient: env.p.files.storage.(*mockfiles.MockBatchFilesClient),
	}

	// runJob must return normally: the panic is contained.
	env.runResumedJob(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Hour),
		Epoch:     2,
		Resumable: true,
	})

	// No unfenced failed write: the row stays in the finalizing status the
	// finalizer wrote before assembly, still resumable, so the reconciler
	// requeue can retry the finalization.
	assertJobStatus(t, env.dbClient, jobID, openai.BatchStatusFinalizing)
	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet %s: err=%v len=%d", jobID, err, len(items))
	}
	if !items[0].Resumable {
		t.Fatal("row lost the resumable flag; the requeue cannot retry the finalization")
	}

	// The aborted artifact was never committed.
	folder, err := ucom.GetFolderNameByTenantID(env.jobInfo.TenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	outKey := ucom.FileStorageName(stableFileID(jobID, 2, metrics.FileTypeOutput), jobOutputStorageName(jobID))
	if _, _, err := env.filesClient.Retrieve(context.Background(), outKey, folder); err == nil {
		t.Fatal("aborted output artifact is visible in storage")
	}

	// The replacement incarnation a requeue delivers finalizes from the
	// checkpointed entries and completes the batch.
	env.p.files.storage = env.filesClient
	if err := env.p.finalizeResumableJob(testLoggerCtx(t), NewStatusUpdater(env.dbClient), env.jobItem, env.jobInfo); err != nil {
		t.Fatalf("retry finalizeResumableJob: %v", err)
	}
	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status after retry = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 2, Failed: 0}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts after retry = %+v, want %+v", status.RequestCounts, wantCounts)
	}
}

func TestRenewBatchLease(t *testing.T) {
	const jobID = "job-lease"

	seedActivated := func(t *testing.T) *mockdb.MockResumableStore {
		t.Helper()
		store := mockdb.NewMockResumableStore()
		store.SeedBatch(jobID, "test-processor", 1)
		err := store.ActivateResumableBatch(context.Background(), &db.ResumableActivation{
			BatchID:     jobID,
			ProcessorID: "test-processor",
			Epoch:       1,
			LeaseTTL:    time.Minute,
			Entries: []db.ResumableActivationEntry{
				{RequestID: "r-1", Ordinal: 0, CustomID: "c-1", Status: string(db.ResumableEntryPending)},
			},
		})
		if err != nil {
			t.Fatalf("ActivateResumableBatch: %v", err)
		}
		return store
	}

	newLeaseProcessor := func(t *testing.T, store *mockdb.MockResumableStore) *Processor {
		t.Helper()
		cfg := config.NewConfig()
		cfg.Resumable.LeaseDuration = 10 * time.Millisecond
		return mustNewProcessor(t, cfg, &clientset.Clientset{Resumable: store})
	}

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID},
		Epoch:       1,
	}

	t.Run("fenced renewal trips onOwnershipLost", func(t *testing.T) {
		store := seedActivated(t)
		store.MarkTerminal(jobID) // a terminal transition fences the owner out

		p := newLeaseProcessor(t, store)
		lost := make(chan struct{})
		go p.renewBatchLease(context.Background(), jobItem, func() { close(lost) }, make(chan struct{}))

		select {
		case <-lost:
		case <-time.After(5 * time.Second):
			t.Fatal("fenced renewal did not trip onOwnershipLost")
		}
	})

	t.Run("successful renewals continue until done closes", func(t *testing.T) {
		store := seedActivated(t)
		// Freeze the store's clock: the 10ms lease leaves a wall-clock-thin
		// renewal window, and under load a scheduler hiccup lets it lapse and
		// fences the owner out. Renewals succeeding — not expiry — is what
		// this subtest exercises.
		clock := time.Now()
		store.Now = func() time.Time { return clock }
		p := newLeaseProcessor(t, store)

		lost := make(chan struct{})
		exited := make(chan struct{})
		done := make(chan struct{})
		go func() {
			defer close(exited)
			p.renewBatchLease(context.Background(), jobItem, func() { close(lost) }, done)
		}()

		// Several renewal ticks pass without an ownership loss…
		time.Sleep(50 * time.Millisecond)
		select {
		case <-lost:
			t.Fatal("successful renewals must not trip onOwnershipLost")
		default:
		}

		close(done)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			t.Fatal("renewBatchLease did not stop after done closed")
		}
	})
}

// gateFilesClient wraps a files client whose Retrieve blocks until gate
// retrievals are in flight simultaneously — a fetch pool that runs entries
// one at a time would deadlock on it.
type gateFilesClient struct {
	filesapi.BatchFilesClient
	inFlight atomic.Int32
	peak     atomic.Int32
	gate     int
	open     sync.Once
	released chan struct{}
}

func (c *gateFilesClient) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	n := c.inFlight.Add(1)
	for {
		p := c.peak.Load()
		if n <= p || c.peak.CompareAndSwap(p, n) {
			break
		}
	}
	if int(n) >= c.gate {
		c.open.Do(func() { close(c.released) })
	}
	select {
	case <-c.released:
	case <-ctx.Done():
		c.inFlight.Add(-1)
		return nil, nil, ctx.Err()
	}
	defer c.inFlight.Add(-1)
	return c.BatchFilesClient.Retrieve(ctx, fileName, folderName)
}

// TestAssembleResumableArtifacts_PrefetchesConcurrently proves the finalizer
// fetches result objects with bounded concurrency (the gate below only opens
// when two retrievals overlap) while the artifact lines keep ordinal order.
func TestAssembleResumableArtifacts_PrefetchesConcurrently(t *testing.T) {
	const jobID = "job-assemble-concurrency"
	const tenantID = "tenant-1"

	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	cfg.Resumable.FinalizeConcurrency = 4

	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())
	gated := &gateFilesClient{BatchFilesClient: filesClient, gate: 2, released: make(chan struct{})}

	resumable := mockdb.NewMockResumableStore()
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB:   newMockBatchDBClient(),
		FileDB:    newMockFileDBClient(),
		File:      gated,
		Queue:     mockdb.NewMockBatchPriorityQueueClient(),
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
		Resumable: resumable,
	})

	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	const entries = 4
	activation := make([]db.ResumableActivationEntry, entries)
	for i := range entries {
		line := fmt.Sprintf("line-%d\n", i)
		sum := sha256.Sum256([]byte(line))
		requestID := stableBatchRequestID(jobID, fmt.Sprintf("c-%d", i))
		key := resumableResultKey(jobID, requestID)
		if _, err := filesClient.Store(testLoggerCtx(t), key, folder, 0, 0, strings.NewReader(line)); err != nil {
			t.Fatalf("store result object: %v", err)
		}
		activation[i] = db.ResumableActivationEntry{
			RequestID:    requestID,
			Ordinal:      int64(i),
			CustomID:     fmt.Sprintf("c-%d", i),
			Status:       string(db.ResumableEntryCompleted),
			ResultKey:    key,
			ResultBytes:  int64(len(line)),
			ResultSHA256: hex.EncodeToString(sum[:]),
		}
	}
	resumable.SeedBatch(jobID, "test-processor", 1)
	if err := resumable.ActivateResumableBatch(testLoggerCtx(t), &db.ResumableActivation{
		BatchID:     jobID,
		ProcessorID: "test-processor",
		Epoch:       1,
		LeaseTTL:    time.Minute,
		Entries:     activation,
	}); err != nil {
		t.Fatalf("activate: %v", err)
	}

	ctx, cancel := context.WithTimeout(testLoggerCtx(t), 10*time.Second)
	defer cancel()
	counts, outputFileID, _, err := p.assembleResumableArtifacts(ctx,
		&db.BatchItem{BaseIndexes: db.BaseIndexes{ID: jobID}},
		&batch_types.JobInfo{JobID: jobID, TenantID: tenantID}, "")
	if err != nil {
		t.Fatalf("assembleResumableArtifacts: %v", err)
	}

	if peak := gated.peak.Load(); peak < 2 {
		t.Fatalf("peak concurrent fetches = %d, want >= 2 (fetches are serialized)", peak)
	}
	if counts.Total != entries || counts.Completed != entries {
		t.Fatalf("counts = %+v, want total/completed %d", counts, entries)
	}

	// The artifact holds every line in ordinal order.
	artifact, _, err := filesClient.Retrieve(ctx,
		ucom.FileStorageName(outputFileID, jobOutputStorageName(jobID)), folder)
	if err != nil {
		t.Fatalf("retrieve artifact: %v", err)
	}
	defer artifact.Close()
	data, err := io.ReadAll(artifact)
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	want := "line-0\nline-1\nline-2\nline-3\n"
	if string(data) != want {
		t.Fatalf("artifact = %q, want %q (ordinal order)", data, want)
	}
}

// rejectedLineEntry stores the error object of an ingestion-rejected line
// (unregistered model) at its canonical key — activation does this in the
// real flow — and returns the activation entry that commits it. The entry
// has zero attempts: a rejection was never dispatched.
func rejectedLineEntry(t *testing.T, env *durableJobEnv, jobID, customID string, ordinal int64) db.ResumableActivationEntry {
	t.Helper()
	reqID := stableBatchRequestID(jobID, customID)
	errLine := fmt.Sprintf(`{"id":%q,"custom_id":%q,"error":{"code":"model_not_found","message":"model missing-model is not configured in any gateway"}}`+"\n",
		reqID, customID)
	folder, err := ucom.GetFolderNameByTenantID(env.jobInfo.TenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	key := resumableResultKey(jobID, reqID)
	if _, err := env.filesClient.Store(testLoggerCtx(t), key, folder, 0, 0, bytes.NewReader([]byte(errLine))); err != nil {
		t.Fatalf("store rejected error object %s: %v", key, err)
	}
	sum := sha256.Sum256([]byte(errLine))
	return db.ResumableActivationEntry{
		RequestID:    reqID,
		Ordinal:      ordinal,
		CustomID:     customID,
		Status:       string(db.ResumableEntryFailed),
		ErrorCode:    "model_not_found",
		ResultKey:    key,
		ResultBytes:  int64(len(errLine)),
		ResultSHA256: hex.EncodeToString(sum[:]),
	}
}

// TestRunJob_Resumable_ResumeWithRejectedEntries guards the manifest
// integrity contract for ingestion-rejected lines: a rejection is a failed
// entry with zero attempts (never dispatched), so a requeued batch holding
// rejected lines must resume instead of failing the integrity check.
func TestRunJob_Resumable_ResumeWithRejectedEntries(t *testing.T) {
	const jobID = "job-resumable-rejected-resume"

	env := setupDurableJob(t, jobID, []string{"test-model", "missing-model"})

	env.jobItem.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
	env.jobItem.Epoch = 2
	if err := env.dbClient.DBStore(testLoggerCtx(t), env.jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	env.stageResumedClaim(t, 2,
		pendingLineEntry(jobID, "req-1", 0),
		rejectedLineEntry(t, env, jobID, "req-2", 1),
	)

	done := env.runResumedJobAsync(t, &db.BatchJobPriority{
		ID:        jobID,
		SLO:       time.Now().Add(1 * time.Hour),
		Epoch:     2,
		Resumable: true,
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
	<-done

	status := durableJobStatus(t, env)
	if status.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want completed", status.Status)
	}
	wantCounts := openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}
	if status.RequestCounts != wantCounts {
		t.Fatalf("counts = %+v, want %+v", status.RequestCounts, wantCounts)
	}
	// Only the pending entry is (re-)dispatched; the rejected one is
	// already terminal.
	if got := env.asyncClient.submitCount(); got != 1 {
		t.Fatalf("submitted requests = %d, want 1", got)
	}
}
