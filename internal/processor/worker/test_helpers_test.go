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
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
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

func newMockBatchDBClient() db.BatchDBClient {
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

// ---------------------------------------------------------------------------
// Mock files client (upload retry testing)
// ---------------------------------------------------------------------------

type failNTimesFilesClient struct {
	failCount    int
	calls        int
	lastMeta     *filesapi.BatchFileMetadata
	lastFileName string // records the fileName passed to the most recent successful Store call
}

func (f *failNTimesFilesClient) Store(_ context.Context, fileName, _ string, _, _ int64, _ io.Reader) (*filesapi.BatchFileMetadata, error) {
	f.calls++
	if f.calls <= f.failCount {
		return nil, errors.New("transient upload error")
	}
	f.lastFileName = fileName
	f.lastMeta = &filesapi.BatchFileMetadata{Size: 42}
	return f.lastMeta, nil
}

func (f *failNTimesFilesClient) Retrieve(_ context.Context, _, _ string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	return nil, nil, nil
}
func (f *failNTimesFilesClient) List(_ context.Context, _ string) ([]filesapi.BatchFileMetadata, error) {
	return nil, nil
}
func (f *failNTimesFilesClient) RetrieveRange(_ context.Context, _, _ string, _ int64, _ int64) (io.ReadCloser, error) {
	return nil, nil
}
func (f *failNTimesFilesClient) Delete(_ context.Context, _, _ string) error { return nil }
func (f *failNTimesFilesClient) GetContext(p context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.WithCancel(p)
}
func (f *failNTimesFilesClient) Close() error { return nil }

// failOnNthCallClient fails the Nth Store call (1-based). All other calls succeed.
// Thread-safe for concurrent callers.
type failOnNthCallClient struct {
	failN   int32
	calls   atomic.Int32
	failErr error
}

func (f *failOnNthCallClient) Store(_ context.Context, _ string, _ string, _, _ int64, _ io.Reader) (*filesapi.BatchFileMetadata, error) {
	n := f.calls.Add(1)
	if n == f.failN {
		return nil, f.failErr
	}
	return &filesapi.BatchFileMetadata{Size: 42}, nil
}
func (f *failOnNthCallClient) Retrieve(_ context.Context, _, _ string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	return nil, nil, nil
}
func (f *failOnNthCallClient) List(_ context.Context, _ string) ([]filesapi.BatchFileMetadata, error) {
	return nil, nil
}
func (f *failOnNthCallClient) RetrieveRange(_ context.Context, _, _ string, _ int64, _ int64) (io.ReadCloser, error) {
	return nil, nil
}
func (f *failOnNthCallClient) Delete(_ context.Context, _, _ string) error { return nil }
func (f *failOnNthCallClient) GetContext(p context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.WithCancel(p)
}
func (f *failOnNthCallClient) Close() error { return nil }

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
func mockClaimOwned(batchDB db.BatchDBClient, processorID string) func(context.Context) ([]*db.BatchJobPriority, error) {
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
			tasks = append(tasks, &db.BatchJobPriority{ID: item.ID, SLO: time.UnixMicro(item.Priority), Epoch: item.Epoch, RecoveryAttempts: item.RecoveryAttempts})
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
	inner db.BatchDBClient
	mu    sync.Mutex
	calls map[openai.BatchStatus]int
}

func newSpyBatchDB(inner db.BatchDBClient) *spyBatchDB {
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
	return s.inner.DBUpdate(ctx, item, expectedStatus)
}

func (s *spyBatchDB) DBDelete(ctx context.Context, IDs []string) ([]string, error) {
	return s.inner.DBDelete(ctx, IDs)
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
	inner      db.BatchDBClient
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
func (f *failOnStatusDB) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}
func (f *failOnStatusDB) Close() error { return f.inner.Close() }

// ---------------------------------------------------------------------------
// Processor construction helpers
// ---------------------------------------------------------------------------

func mustNewProcessor(t *testing.T, cfg *config.ProcessorConfig, clients *clientset.Clientset) *Processor {
	t.Helper()
	p, err := NewProcessor(cfg, clients, "test-processor", testLogger(t))
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
	return &clientset.Clientset{
		BatchDB:   newMockBatchDBClient(),
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     mockdb.NewMockBatchPriorityQueueClient(),
		Status:    mockdb.NewMockBatchStatusClient(),
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}
}

// testProcessorEnv holds the processor and its mock clients for test inspection.
type testProcessorEnv struct {
	p        *Processor
	dbClient db.BatchDBClient
	pqClient db.BatchPriorityQueueClient
	updater  *StatusUpdater
}

// newTestProcessorEnv creates a Processor wired with mock clients.
// The returned env exposes the shared dbClient and pqClient for seeding and verification.
func newTestProcessorEnv(t *testing.T, cfg *config.ProcessorConfig, inferClient inference.InferenceClient) *testProcessorEnv {
	t.Helper()

	dbClient := newMockBatchDBClient()
	pqClient := mockdb.NewMockBatchPriorityQueueClient()
	statusClient := mockdb.NewMockBatchStatusClient()

	p, err := NewProcessor(cfg, &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     pqClient,
		Status:    statusClient,
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(inferClient),
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
		updater:  NewStatusUpdater(dbClient, statusClient, 86400),
	}
}

// seedDBJob stores a BatchItem in the DB so the updater can find and update it.
func seedDBJob(t *testing.T, dbClient db.BatchDBClient, jobID string) *db.BatchItem {
	t.Helper()
	statusInfo := openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}
	statusBytes, _ := json.Marshal(statusInfo)
	item := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: "tenant-1", Tags: db.Tags{}},
		BaseContents: db.BaseContents{Status: statusBytes},
	}
	if err := dbClient.DBStore(context.Background(), item); err != nil {
		t.Fatalf("seed DB job: %v", err)
	}
	return item
}

// ---------------------------------------------------------------------------
// Job setup helpers
// ---------------------------------------------------------------------------

// setupJobWithOutputFile creates a job directory with a non-empty output.jsonl
// so that uploadFileAndStoreFileRecord can find and upload it.
func setupJobWithOutputFile(t *testing.T, cfg *config.ProcessorConfig, jobID, tenantID string) *batch_types.JobInfo {
	t.Helper()
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	jobDir := filepath.Join(cfg.WorkDir, folderName, jobsDirName, jobID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath := filepath.Join(jobDir, outputFileName)
	if err := os.WriteFile(outputPath, []byte(`{"id":"batch_req_1","custom_id":"req-1","response":{"status_code":200}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	return &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
}

// setupExecutionJob creates a complete job directory with input file, plan files, and model map.
// It also stores the input data in the mock files client and seeds a FileDB record so that
// resolveInputFileCoords works for executeJob tests. The returned jobInfo has BatchJob.InputFileID set.
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
	inputFileID := "file-test-input"

	jobRootDir, err := env.p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if err := os.MkdirAll(jobRootDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	// Write input locally for plan entry computation.
	inputPath := filepath.Join(jobRootDir, "input.jsonl")
	rawInput := writeInputJSONL(t, inputPath, requests)

	// Store input data in mock files client for ranged reads.
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	storageName := ucom.FileStorageName(inputFileID, "input.jsonl")
	if _, err := env.p.files.storage.Store(
		context.Background(), storageName, folderName, 0, 0, bytes.NewReader(rawInput),
	); err != nil {
		t.Fatalf("Store input in mock files client: %v", err)
	}

	// Seed FileDB record so resolveInputFileCoords can look it up.
	fileSpec := &openai.FileObject{Filename: "input.jsonl"}
	if err := env.p.files.db.DBStore(context.Background(), &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

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
		BatchJob: &openai.Batch{
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
			},
		},
	}

	return env, jobInfo
}

// createPartialOutputFiles creates dummy output.jsonl and error.jsonl under the job dir
// so uploadPartialResults can find and upload them.
func createPartialOutputFiles(t *testing.T, p *Processor, jobID, tenantID string) {
	t.Helper()
	jobDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath := filepath.Join(jobDir, "output.jsonl")
	errorPath := filepath.Join(jobDir, "error.jsonl")
	if err := os.WriteFile(outputPath, []byte(`{"id":"batch_req_1","custom_id":"req-1","response":{"status_code":200}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile output: %v", err)
	}
	if err := os.WriteFile(errorPath, []byte(`{"id":"batch_req_2","custom_id":"req-2","error":{"code":"batch_cancelled","message":"cancelled"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}
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

// readNonEmptyJSONLLines reads a JSONL file and returns non-empty lines as byte slices.
func readNonEmptyJSONLLines(t *testing.T, path string) [][]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	var lines [][]byte
	for _, line := range bytes.Split(data, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) > 0 {
			lines = append(lines, line)
		}
	}
	return lines
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
