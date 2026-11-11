package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/semaphore"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// ---------------------------------------------------------------------------
// Common helpers
// ---------------------------------------------------------------------------

func testLogger(t testing.TB) logr.Logger {
	return testr.NewWithInterface(t, testr.Options{})
}

func testLoggerCtx(t *testing.T) context.Context {
	return logr.NewContext(context.Background(), testLogger(t))
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

// ---------------------------------------------------------------------------
// Mock DB constructors
// ---------------------------------------------------------------------------

func newMockBatchDBClient() db.BatchProgressDBClient {
	return mockdb.NewMockDBClient[db.BatchItem, db.BatchQuery](
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)
}

func newMockFileDBClient() db.FileDBClient {
	return mockdb.NewMockDBClient[db.FileItem, db.FileQuery](
		func(f *db.FileItem) string { return f.ID },
		func(q *db.FileQuery) *db.BaseQuery { return &q.BaseQuery },
	)
}

// ---------------------------------------------------------------------------
// Mock inference clients
// ---------------------------------------------------------------------------

type fakeInferenceClient struct{}

func (f *fakeInferenceClient) Generate(_ context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return nil, nil
}

type mockInferenceClient struct {
	generateFn func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError)
}

func (m *mockInferenceClient) Generate(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	if m.generateFn != nil {
		return m.generateFn(ctx, req)
	}
	return &inference.GenerateResponse{
		RequestID: "server-req-1",
		Response:  []byte(`{"choices":[{"message":{"content":"hello"}}]}`),
	}, nil
}

// fakeAsyncInferenceClient is the worker-test stand-in for a shared async
// client: Submit records the request and hands it to submittedCh; GetResult
// serves responses pushed onto results.
type fakeAsyncInferenceClient struct {
	mu          sync.Mutex
	submitted   []*inference.GenerateRequest
	submittedCh chan *inference.GenerateRequest
	results     chan *inference.GenerateResponse
}

var _ inference.AsyncInferenceClient = (*fakeAsyncInferenceClient)(nil)

func (c *fakeAsyncInferenceClient) Submit(ctx context.Context, req *inference.GenerateRequest) *inference.ClientError {
	c.mu.Lock()
	c.submitted = append(c.submitted, req)
	c.mu.Unlock()
	if c.submittedCh != nil {
		select {
		case c.submittedCh <- req:
		case <-ctx.Done():
		}
	}
	return nil
}

func (c *fakeAsyncInferenceClient) GetResult(ctx context.Context) (*inference.GenerateResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-c.results:
		return r, nil
	}
}

func (c *fakeAsyncInferenceClient) Cancel(context.Context, []string) error { return nil }
func (c *fakeAsyncInferenceClient) Close() error                           { return nil }

func (c *fakeAsyncInferenceClient) submitCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.submitted)
}

func (c *fakeAsyncInferenceClient) deliver(requestID string, body map[string]any) {
	resp, _ := json.Marshal(body)
	c.results <- &inference.GenerateResponse{
		RequestID:  requestID,
		Response:   resp,
		StatusCode: 200,
	}
}

// deliverStatus delivers a bare HTTP status with no body — e.g. a 403 from
// auth — which the pipeline preserves in the output line.
func (c *fakeAsyncInferenceClient) deliverStatus(requestID string, statusCode int) {
	c.results <- &inference.GenerateResponse{
		RequestID:  requestID,
		StatusCode: statusCode,
	}
}

// ---------------------------------------------------------------------------
// Mock DB error wrappers
// ---------------------------------------------------------------------------

type dbStoreErrFileClient struct {
	db.FileDBClient
	err error
}

func (d *dbStoreErrFileClient) DBStore(_ context.Context, _ *db.FileItem) error {
	return d.err
}

// ---------------------------------------------------------------------------
// Spy wrappers
// ---------------------------------------------------------------------------

// mockClaimOwned mirrors PostgresBatchQueueClient.PQClaimOwned on the mock DB:
// every non-terminal job owned by processorID gets its epoch and recovery
// counter bumped and is returned as a task.
func mockClaimOwned(batchDB db.BatchProgressDBClient, processorID string) func(context.Context) ([]*db.BatchJobPriority, error) {
	return func(ctx context.Context) ([]*db.BatchJobPriority, error) {
		items, _, _, err := batchDB.DBGet(ctx, &db.BatchQuery{ProcessorID: processorID, NonTerminal: true}, true, 0, 1000)
		if err != nil {
			return nil, err
		}
		var tasks []*db.BatchJobPriority
		for _, item := range items {
			if item.ProcessorID != processorID {
				continue
			}
			var info openai.BatchStatusInfo
			if err := json.Unmarshal(item.Status, &info); err != nil || info.Status.IsTerminal() {
				continue
			}
			item.Epoch++
			item.RecoveryAttempts++
			if err := batchDB.DBUpdate(ctx, item, nil); err != nil {
				return nil, err
			}
			tasks = append(tasks, &db.BatchJobPriority{ID: item.ID, SLO: time.UnixMicro(item.Priority), Epoch: item.Epoch, RecoveryAttempts: item.RecoveryAttempts, Resumable: item.Resumable})
		}
		return tasks, nil
	}
}

type spyPQ struct {
	inner          db.BatchPriorityQueueClient
	mu             sync.Mutex
	enqN           int
	delN           int
	afterDequeueFn func() // called after a successful dequeue (non-empty result)
	enqueueErr     error  // if non-nil, PQEnqueue returns this error (after incrementing counter)
}

func (s *spyPQ) PQEnqueue(ctx context.Context, jobPriority *db.BatchJobPriority) error {
	s.mu.Lock()
	s.enqN++
	injectedErr := s.enqueueErr
	s.mu.Unlock()
	if injectedErr != nil {
		return injectedErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return s.inner.PQEnqueue(ctx, jobPriority)
}
func (s *spyPQ) PQDequeue(ctx context.Context, timeout time.Duration, maxObjs int) ([]*db.BatchJobPriority, error) {
	items, err := s.inner.PQDequeue(ctx, timeout, maxObjs)
	if err == nil && len(items) > 0 && s.afterDequeueFn != nil {
		s.afterDequeueFn()
	}
	return items, err
}
func (s *spyPQ) PQDelete(ctx context.Context, jobPriority *db.BatchJobPriority) (int, error) {
	s.mu.Lock()
	s.delN++
	s.mu.Unlock()
	return s.inner.PQDelete(ctx, jobPriority)
}
func (s *spyPQ) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}
func (s *spyPQ) PQClaimOwned(ctx context.Context) ([]*db.BatchJobPriority, error) {
	return s.inner.PQClaimOwned(ctx)
}
func (s *spyPQ) PQGetIDs(ctx context.Context) (map[string]bool, error) {
	return s.inner.PQGetIDs(ctx)
}
func (s *spyPQ) Close() error { return s.inner.Close() }

func (s *spyPQ) DeleteCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.delN
}

func (s *spyPQ) EnqueueCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.enqN
}

type spyBatchDB struct {
	inner db.BatchProgressDBClient
	mu    sync.Mutex
	calls map[openai.BatchStatus]int
	// onUpdate, when set, fires after a successful inner DBUpdate with the
	// replacement item.
	onUpdate func(*db.BatchItem)
}

func newSpyBatchDB(inner db.BatchProgressDBClient) *spyBatchDB {
	return &spyBatchDB{
		inner: inner,
		calls: make(map[openai.BatchStatus]int),
	}
}

func (s *spyBatchDB) DBStore(ctx context.Context, item *db.BatchItem) error {
	return s.inner.DBStore(ctx, item)
}

func (s *spyBatchDB) DBGet(ctx context.Context, query *db.BatchQuery, includeStatic bool, start, limit int) ([]*db.BatchItem, int, bool, error) {
	return s.inner.DBGet(ctx, query, includeStatic, start, limit)
}

func (s *spyBatchDB) DBUpdate(ctx context.Context, item *db.BatchItem, expectedStatus []byte) error {
	if len(item.Status) > 0 {
		var st openai.BatchStatusInfo
		if err := json.Unmarshal(item.Status, &st); err == nil {
			s.mu.Lock()
			s.calls[st.Status]++
			s.mu.Unlock()
		}
	}
	if err := s.inner.DBUpdate(ctx, item, expectedStatus); err != nil {
		return err
	}
	if s.onUpdate != nil {
		s.onUpdate(item)
	}
	return nil
}

func (s *spyBatchDB) DBDelete(ctx context.Context, IDs []string) ([]string, error) {
	return s.inner.DBDelete(ctx, IDs)
}

func (s *spyBatchDB) DBUpdateProgress(ctx context.Context, id string, epoch int64, countsJSON []byte) error {
	return s.inner.DBUpdateProgress(ctx, id, epoch, countsJSON)
}

func (s *spyBatchDB) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}

func (s *spyBatchDB) Close() error {
	return s.inner.Close()
}

func (s *spyBatchDB) StatusCalls(status openai.BatchStatus) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[status]
}

// failOnStatusDB wraps a BatchDBClient and injects an error when DBUpdate tries
// to write a specific status. All other operations pass through.
type failOnStatusDB struct {
	inner      db.BatchProgressDBClient
	failStatus openai.BatchStatus
	failErr    error
}

func (f *failOnStatusDB) DBStore(ctx context.Context, item *db.BatchItem) error {
	return f.inner.DBStore(ctx, item)
}
func (f *failOnStatusDB) DBGet(ctx context.Context, query *db.BatchQuery, includeStatic bool, start, limit int) ([]*db.BatchItem, int, bool, error) {
	return f.inner.DBGet(ctx, query, includeStatic, start, limit)
}
func (f *failOnStatusDB) DBUpdate(ctx context.Context, item *db.BatchItem, expectedStatus []byte) error {
	if len(item.Status) > 0 {
		var st openai.BatchStatusInfo
		if err := json.Unmarshal(item.Status, &st); err == nil && st.Status == f.failStatus {
			return f.failErr
		}
	}
	return f.inner.DBUpdate(ctx, item, expectedStatus)
}
func (f *failOnStatusDB) DBDelete(ctx context.Context, IDs []string) ([]string, error) {
	return f.inner.DBDelete(ctx, IDs)
}
func (f *failOnStatusDB) DBUpdateProgress(ctx context.Context, id string, epoch int64, countsJSON []byte) error {
	return f.inner.DBUpdateProgress(ctx, id, epoch, countsJSON)
}
func (f *failOnStatusDB) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}
func (f *failOnStatusDB) Close() error { return f.inner.Close() }

// ---------------------------------------------------------------------------
// Processor construction helpers
// ---------------------------------------------------------------------------

func mustNewProcessor(t *testing.T, cfg *config.ProcessorConfig, clients *clientset.Clientset) *Processor {
	return mustNewProcessorWithID(t, cfg, clients, "test-processor")
}

func mustNewProcessorWithID(t *testing.T, cfg *config.ProcessorConfig, clients *clientset.Clientset, processorID string) *Processor {
	t.Helper()
	if clients.BatchProgressDB == nil {
		if batchDB, ok := clients.BatchDB.(db.BatchProgressDBClient); ok {
			clients.BatchProgressDB = batchDB
		}
	}
	// The resumable pipeline is the only pipeline: every test processor needs
	// an entry store and a files store, even when the test never activates
	// a batch.
	if clients.Resumable == nil {
		clients.Resumable = mockdb.NewMockResumableStore()
	}
	if clients.File == nil {
		clients.File = mockfiles.NewMockBatchFilesClient(t.TempDir())
	}
	p, err := NewProcessor(cfg, clients, processorID, testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	// Tests that call executeJob/processModel directly (without Run) need
	// semaphores initialized. Run() normally does this with guard callbacks,
	// but tests use nil callbacks since they don't need graceful shutdown.
	p.tokens, err = semaphore.New(cfg.NumWorkers, nil)
	if err != nil {
		t.Fatalf("worker semaphore: %v", err)
	}
	p.globalSem, err = semaphore.New(cfg.Concurrency.Global, nil)
	if err != nil {
		t.Fatalf("global semaphore: %v", err)
	}
	initTestEndpointLimits(t, p, cfg)
	return p
}

func validProcessorClients(t testing.TB) *clientset.Clientset {
	t.Helper()
	batchDB := newMockBatchDBClient()
	return &clientset.Clientset{
		BatchDB:         batchDB,
		BatchProgressDB: batchDB,
		FileDB:          newMockFileDBClient(),
		File:            mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:           mockdb.NewMockBatchPriorityQueueClient(),
		Event:           mockdb.NewMockBatchEventChannelClient(),
		Inference:       inference.NewSingleClientResolver(&fakeInferenceClient{}),
		Resumable:       mockdb.NewMockResumableStore(),
	}
}

// testProcessorEnv holds the processor and its mock clients for test inspection.
type testProcessorEnv struct {
	p        *Processor
	dbClient db.BatchProgressDBClient
	pqClient db.BatchPriorityQueueClient
	updater  *StatusUpdater
	// jobItem and sink are set by setupExecutionJob: the activated batch's
	// DB item and its result sink, for tests that drive executeJob directly.
	jobItem *db.BatchItem
	sink    *batchResultStore
}

// newTestProcessorEnv creates a Processor wired with mock clients.
// The returned env exposes the shared dbClient and pqClient for seeding and verification.
func newTestProcessorEnv(t *testing.T, cfg *config.ProcessorConfig, inferClient inference.InferenceClient) *testProcessorEnv {
	t.Helper()

	dbClient := newMockBatchDBClient()
	pqClient := mockdb.NewMockBatchPriorityQueueClient()

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:         dbClient,
		BatchProgressDB: dbClient,
		FileDB:          newMockFileDBClient(),
		File:            mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:           pqClient,
		Event:           mockdb.NewMockBatchEventChannelClient(),
		Inference:       inference.NewSingleClientResolver(inferClient),
		Resumable:       mockdb.NewMockResumableStore(),
	}, "test-processor", testLogger(t))
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	p.tokens, err = semaphore.New(cfg.NumWorkers, nil)
	if err != nil {
		t.Fatalf("worker semaphore: %v", err)
	}
	p.globalSem, err = semaphore.New(cfg.Concurrency.Global, nil)
	if err != nil {
		t.Fatalf("global semaphore: %v", err)
	}
	initTestEndpointLimits(t, p, cfg)
	p.poller = NewPoller(pqClient, dbClient)

	return &testProcessorEnv{
		p:        p,
		dbClient: dbClient,
		pqClient: pqClient,
		updater:  NewStatusUpdater(dbClient),
	}
}

// seedDBJob stores a BatchItem in the DB so the updater can find and update it.
func seedDBJob(t *testing.T, dbClient db.BatchProgressDBClient, jobID string) *db.BatchItem {
	t.Helper()
	return seedDBJobWithEpoch(t, dbClient, jobID, 0)
}

// seedDBJobWithEpoch is seedDBJob for rows fenced to a nonzero epoch.
// DBUpdateProgress mirrors the Postgres epoch fence, so progress writes
// carrying an activation's epoch need a row seeded with that epoch — a
// stale-epoch row makes every write conflict.
func seedDBJobWithEpoch(t *testing.T, dbClient db.BatchProgressDBClient, jobID string, epoch int64) *db.BatchItem {
	t.Helper()
	statusInfo := openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}
	statusBytes, _ := json.Marshal(statusInfo)
	item := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: "tenant-1", Tags: db.Tags{}},
		BaseContents: db.BaseContents{Status: statusBytes},
		Epoch:        epoch,
	}
	if err := dbClient.DBStore(context.Background(), item); err != nil {
		t.Fatalf("seed DB job: %v", err)
	}
	return item
}

// ---------------------------------------------------------------------------
// Job setup helpers
// ---------------------------------------------------------------------------

// activateJobEntries seeds the processor's claim on the batch and activates
// one pending entry per custom ID, returning the result sink and DB item.
// It mirrors runJob's activation for tests that drive executeJob directly.
func activateJobEntries(t *testing.T, p *Processor, jobID, tenantID string, customIDs []string) (*batchResultStore, *db.BatchItem) {
	t.Helper()
	store, ok := p.resumable.(*mockdb.MockResumableStore)
	if !ok {
		t.Fatalf("processor resumable store is %T, want *mockdb.MockResumableStore", p.resumable)
	}
	store.SeedBatch(jobID, "test-processor", 1)
	entries := make([]db.ResumableActivationEntry, len(customIDs))
	for i, customID := range customIDs {
		entries[i] = db.ResumableActivationEntry{
			RequestID: stableBatchRequestID(jobID, customID),
			Ordinal:   int64(i),
			CustomID:  customID,
			Status:    string(db.ResumableEntryPending),
		}
	}
	if err := store.ActivateResumableBatch(testLoggerCtx(t), &db.ResumableActivation{
		BatchID:     jobID,
		ProcessorID: "test-processor",
		Epoch:       1,
		LeaseTTL:    time.Minute,
		Entries:     entries,
	}); err != nil {
		t.Fatalf("ActivateResumableBatch: %v", err)
	}
	sink, err := newBatchResultStore(p.files.storage, store, jobID, tenantID, "test-processor", 1)
	if err != nil {
		t.Fatalf("newBatchResultStore: %v", err)
	}
	return sink, &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID},
		Epoch:       1,
	}
}

// mirrorResumableStatusWrites wires the mock batch DB to mirror resumable-
// fenced row writes into the mock resumable store's status: production
// commits the fenced status and the row in one statement, the mocks are
// split.
func mirrorResumableStatusWrites(dbClient db.BatchProgressDBClient, resumable *mockdb.MockResumableStore) {
	if s, ok := dbClient.(*spyBatchDB); ok {
		s.onUpdate = func(item *db.BatchItem) {
			if item.ExpectedResumable == nil || !*item.ExpectedResumable {
				return
			}
			var st openai.BatchStatusInfo
			if err := json.Unmarshal(item.Status, &st); err == nil {
				resumable.SetBatchStatus(item.ID, string(st.Status))
			}
		}
	}
}

// listEntriesByCustomID returns the batch's committed entries keyed by
// custom ID, for tests asserting how results were classified.
func listEntriesByCustomID(t *testing.T, p *Processor, jobID string) map[string]*db.ResumableEntry {
	t.Helper()
	// Pagination is exclusive of afterOrdinal, so -1 includes ordinal 0.
	entries, err := p.resumable.ListBatchEntries(context.Background(), jobID, -1, 1000)
	if err != nil {
		t.Fatalf("ListBatchEntries: %v", err)
	}
	m := make(map[string]*db.ResumableEntry, len(entries))
	for _, e := range entries {
		m[e.CustomID] = e
	}
	return m
}

// setupExecutionJob creates a complete job directory with input file, plan
// files, and model map, then activates the batch in the processor's entry
// store — mirroring runJob's activation — so tests that drive executeJob
// directly checkpoint through a real sink.
func setupExecutionJob(
	t *testing.T,
	cfg *config.ProcessorConfig,
	inferClient inference.InferenceClient,
	requests []batch_types.Request,
	modelToSafe map[string]string,
) (*testProcessorEnv, *batch_types.JobInfo) {
	t.Helper()

	env := newTestProcessorEnv(t, cfg, inferClient)

	jobID := "test-job"
	tenantID := "tenant-1"

	jobRootDir, err := env.p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if err := os.MkdirAll(jobRootDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	inputPath := filepath.Join(jobRootDir, "input.jsonl")
	rawInput := writeInputJSONL(t, inputPath, requests)

	allEntries := planEntriesFromLines(rawInput)

	safeToModel := make(map[string]string, len(modelToSafe))
	modelEntries := make(map[string][]planEntry)
	for model, safe := range modelToSafe {
		safeToModel[safe] = model
	}

	for i, req := range requests {
		safe := modelToSafe[req.Body["model"].(string)]
		modelEntries[safe] = append(modelEntries[safe], allEntries[i])
	}

	plansDir := filepath.Join(jobRootDir, "plans")
	for safe, entries := range modelEntries {
		writePlanFile(t, plansDir, safe, entries)
	}

	writeModelMap(t, jobRootDir, modelMapFile{
		ModelToSafe: modelToSafe,
		SafeToModel: safeToModel,
		LineCount:   int64(len(requests)),
	})

	jobInfo := &batch_types.JobInfo{
		JobID:    jobID,
		TenantID: tenantID,
	}

	customIDs := make([]string, len(requests))
	for i, req := range requests {
		customIDs[i] = req.CustomID
	}
	env.sink, env.jobItem = activateJobEntries(t, env.p, jobID, tenantID, customIDs)

	return env, jobInfo
}

// ---------------------------------------------------------------------------
// File / plan helpers
// ---------------------------------------------------------------------------

func writePlanFile(t *testing.T, dir, safeModelID string, entries []planEntry) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll plans dir: %v", err)
	}
	path := filepath.Join(dir, safeModelID+".plan")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("create plan file: %v", err)
	}
	defer f.Close()
	for _, e := range entries {
		buf := e.marshalBinary()
		if _, err := f.Write(buf[:]); err != nil {
			t.Fatalf("write plan entry: %v", err)
		}
	}
}

func writeModelMap(t *testing.T, jobRootDir string, mm modelMapFile) {
	t.Helper()
	data, err := json.Marshal(mm)
	if err != nil {
		t.Fatalf("marshal model map: %v", err)
	}
	if err := os.WriteFile(filepath.Join(jobRootDir, modelMapFileName), data, 0o644); err != nil {
		t.Fatalf("write model map: %v", err)
	}
}

// writeInputJSONL writes request lines and returns the bytes (including trailing newlines)
// so the caller can compute plan entry offsets.
func writeInputJSONL(t *testing.T, path string, requests []batch_types.Request) []byte {
	t.Helper()
	var buf bytes.Buffer
	for _, r := range requests {
		line, err := json.Marshal(r)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}
	return buf.Bytes()
}

// planEntriesFromLines computes plan entries from the raw input bytes (one entry per line).
func planEntriesFromLines(raw []byte) []planEntry {
	var entries []planEntry
	offset := int64(0)
	for _, line := range bytes.Split(raw, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		length := uint32(len(line) + 1) // include trailing '\n'
		entries = append(entries, planEntry{Offset: offset, Length: length})
		offset += int64(length)
	}
	return entries
}

func makeInputLines(models []string) [][]byte {
	lines := make([][]byte, 0, len(models))
	for i, m := range models {
		req := map[string]any{
			"custom_id": fmt.Sprintf("req-%d", i),
			"method":    "POST",
			"url":       "/v1/chat/completions",
			"body": map[string]any{
				"model": m,
			},
			"meta": map[string]any{
				"i": i,
			},
		}
		b, _ := json.Marshal(req)
		b = append(b, '\n')
		lines = append(lines, b)
	}
	return lines
}

type inputLineSpec struct {
	Model        string
	SystemPrompt string // empty means no system prompt
}

func makeInputLinesWithSystemPrompts(specs []inputLineSpec) [][]byte {
	lines := make([][]byte, 0, len(specs))
	for i, s := range specs {
		body := map[string]any{"model": s.Model}
		if s.SystemPrompt != "" {
			body["messages"] = []map[string]string{
				{"role": "system", "content": s.SystemPrompt},
				{"role": "user", "content": fmt.Sprintf("question %d", i)},
			}
		}
		req := map[string]any{
			"custom_id": fmt.Sprintf("req-%d", i),
			"method":    "POST",
			"url":       "/v1/chat/completions",
			"body":      body,
			"meta":      map[string]any{"i": i},
		}
		b, _ := json.Marshal(req)
		lines = append(lines, append(b, '\n'))
	}
	return lines
}

// testReadPlanEntries reads plan entries from a single plan file (test helper).
func testReadPlanEntries(t *testing.T, planPath string) []planEntry {
	t.Helper()
	b, err := os.ReadFile(planPath)
	if err != nil {
		t.Fatalf("read plan file: %v", err)
	}
	if len(b)%planEntrySize != 0 {
		t.Fatalf("plan file size not multiple of %d: %d", planEntrySize, len(b))
	}

	n := len(b) / planEntrySize
	out := make([]planEntry, 0, n)
	for i := 0; i < n; i++ {
		var buf [planEntrySize]byte
		copy(buf[:], b[i*planEntrySize:(i+1)*planEntrySize])
		out = append(out, unmarshalPlanEntry(buf))
	}
	return out
}

func readAtExact(t *testing.T, f *os.File, off int64, n uint32) []byte {
	t.Helper()
	buf := make([]byte, n)
	readN, err := f.ReadAt(buf, off)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadAt(off=%d,n=%d): %v", off, n, err)
	}
	if uint32(readN) != n {
		t.Fatalf("ReadAt short: got=%d want=%d", readN, n)
	}
	return buf
}

// initTestEndpointLimits creates per-endpoint AIMD+AdaptiveSemaphore pairs for
// each unique client in the processor's resolver.
func initTestEndpointLimits(t *testing.T, p *Processor, cfg *config.ProcessorConfig) {
	t.Helper()
	if p.inference == nil {
		p.endpointLimits = make(map[inference.InferenceClient]*endpointLimit)
		return
	}
	cc := &cfg.Concurrency
	clients := p.inference.Clients()
	p.endpointLimits = make(map[inference.InferenceClient]*endpointLimit, len(clients))
	for _, client := range clients {
		epLabel := p.inference.ClientLabel(client)
		epSem, err := semaphore.NewAdaptive(cc.PerEndpoint, nil)
		if err != nil {
			t.Fatalf("endpoint semaphore: %v", err)
		}
		var epAIMD *semaphore.AIMDController
		if cc.AIMD.Enabled {
			epAIMD = semaphore.NewAIMDController(
				semaphore.AIMDConfig{
					MinLimit:         cc.AIMD.Min,
					MaxLimit:         cc.PerEndpoint,
					BackoffFactor:    cc.AIMD.BackoffFactor,
					AdditiveIncrease: cc.AIMD.AdditiveIncrease,
				},
				cc.PerEndpoint,
				func(limit int) { epSem.SetLimit(limit) },
				logr.Discard(),
			)
		}
		p.endpointLimits[client] = &endpointLimit{sem: epSem, aimd: epAIMD, label: epLabel}
	}
}

func uniqueTestFolder(t *testing.T, base string) string {
	t.Helper()
	testName := strings.ReplaceAll(t.Name(), "/", "_")
	return filepath.Join(base, testName, fmt.Sprintf("%d", time.Now().UnixNano()))
}
