package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// -------------------------
// Test 1: Ingestion
// - local input.jsonl exact copy (line-by-line)
// - plan offsets/lengths are correct (ReadAt matches original line bytes)
// - model_map.json consistency
// -------------------------

func TestPreProcess_BuildsPlansAndModelMap_OffsetsCorrect(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	// Build remote input in mock files store
	tenantID := uniqueTestFolder(t, "tenantA/job-inputs")
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	filename := "input.jsonl"
	models := []string{
		"m1", "m2", "m1", "m3",
		"m2", "m2", "m1",
		// include characters requiring sanitization (safe name logic)
		"org/model-A:1",
		"org/model-A?1", // collision candidate with above depending on sanitization
	}

	lines := makeInputLines(models)
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	// Create DB item for "input file metadata"
	inputFileID := "file-123"

	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes: db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Spec: mustJSON(t, fileSpec),
		},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDBClient,
		File:    filesClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	// Build JobInfo (only BatchSpec.InputFileID is used in preProcessJob)
	jobID := "job-abc"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	if err := p.preProcessJob(ctx, jobInfo); err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}

	// 1) local input exists and equals remoteBuf
	localInput, err := p.jobInputFilePath(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobInputFilePath: %v", err)
	}
	gotLocal, err := os.ReadFile(localInput)
	if err != nil {
		t.Fatalf("read local input: %v", err)
	}
	if !bytes.Equal(gotLocal, remoteBuf.Bytes()) {
		t.Fatalf("local input != remote input (bytes differ)")
	}

	// 2) model_map.json exists and is consistent
	jobRootDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	mapPath := filepath.Join(jobRootDir, modelMapFileName)
	mapBytes, err := os.ReadFile(mapPath)
	if err != nil {
		t.Fatalf("read model_map.json: %v", err)
	}
	var mm modelMapFile
	if err := json.Unmarshal(mapBytes, &mm); err != nil {
		t.Fatalf("unmarshal model_map.json: %v", err)
	}

	// quick sanity:
	if mm.LineCount != int64(len(lines)) {
		t.Fatalf("LineCount mismatch: got=%d want=%d", mm.LineCount, len(lines))
	}
	for model, safe := range mm.ModelToSafe {
		back, ok := mm.SafeToModel[safe]
		if !ok || back != model {
			t.Fatalf("model_map not bijective: model=%q safe=%q back=%q ok=%v", model, safe, back, ok)
		}
	}

	// 3) plan files exist and offsets/length map back to exact original line bytes (ReadAt)
	f, err := os.Open(localInput)
	if err != nil {
		t.Fatalf("open local input for ReadAt: %v", err)
	}
	defer f.Close()

	plansDir := filepath.Join(jobRootDir, "plans")
	for safeID := range mm.SafeToModel {
		planPath := filepath.Join(plansDir, safeID+".plan")
		if _, err := os.Stat(planPath); err != nil {
			t.Fatalf("missing plan file for safeID=%q: %v", safeID, err)
		}
		entries := testReadPlanEntries(t, planPath)

		// For each entry, read input.jsonl slice and ensure it is a valid JSON line ending with '\n'
		for _, e := range entries {
			chunk := readAtExact(t, f, e.Offset, e.Length)
			if len(chunk) == 0 || chunk[len(chunk)-1] != '\n' {
				t.Fatalf("entry does not end with newline: safeID=%q off=%d len=%d", safeID, e.Offset, e.Length)
			}
			trimmed := bytes.TrimSuffix(chunk, []byte{'\n'})
			var req planRequestLine
			if err := json.Unmarshal(trimmed, &req); err != nil {
				t.Fatalf("entry not valid json: safeID=%q off=%d len=%d err=%v", safeID, e.Offset, e.Length, err)
			}
			model := req.Body.Model
			if model == "" {
				t.Fatalf("entry missing body.model: safeID=%q off=%d", safeID, e.Offset)
			}

			// And ensure that this model maps to this safeID in model_map.json
			expectedSafe := mm.ModelToSafe[model]
			if expectedSafe != safeID {
				t.Fatalf("plan safeID mismatch: model=%q expectedSafe=%q gotSafe=%q", model, expectedSafe, safeID)
			}
		}
	}
}

func TestPreProcess_SystemPrompts_PrefixHashAndSortOrder(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := uniqueTestFolder(t, "tenantA/job-sys-prompt")
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	specs := []inputLineSpec{
		{Model: "m1", SystemPrompt: "You are a helpful assistant."},
		{Model: "m1", SystemPrompt: "You are a code reviewer."},
		{Model: "m1", SystemPrompt: "You are a helpful assistant."}, // same as [0]
		{Model: "m1", SystemPrompt: ""},                             // no system prompt
	}

	lines := makeInputLinesWithSystemPrompts(specs)
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	filename := "input.jsonl"
	inputFileID := "file-sys-prompt"

	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	cs := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDBClient,
		File:    filesClient,
	}
	p := mustNewProcessor(t, cfg, cs)

	jobID := "job-sys-prompt"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	if err := p.preProcessJob(ctx, jobInfo); err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}

	jobRootDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}

	mm, err := readModelMap(jobRootDir)
	if err != nil {
		t.Fatalf("readModelMap: %v", err)
	}

	safeID := mm.ModelToSafe["m1"]
	planPath := filepath.Join(jobRootDir, "plans", safeID+".plan")
	entries := testReadPlanEntries(t, planPath)
	if len(entries) != len(specs) {
		t.Fatalf("expected %d entries, got %d", len(specs), len(entries))
	}

	// Collect hashes to verify properties
	hashBySpec := make([]uint32, len(specs))
	for i, e := range entries {
		// Read actual line from local input to identify which spec it corresponds to
		localInput, err := p.jobInputFilePath(jobID, tenantID)
		if err != nil {
			t.Fatalf("jobInputFilePath: %v", err)
		}
		f, err := os.Open(localInput)
		if err != nil {
			t.Fatalf("open local input: %v", err)
		}
		chunk := readAtExact(t, f, e.Offset, e.Length)
		f.Close()
		_ = chunk
		hashBySpec[i] = e.PrefixHash
	}

	// Entries must be sorted by PrefixHash (ascending)
	for i := 1; i < len(entries); i++ {
		if entries[i].PrefixHash < entries[i-1].PrefixHash {
			t.Fatalf("entries not sorted by PrefixHash: [%d]=%d > [%d]=%d",
				i-1, entries[i-1].PrefixHash, i, entries[i].PrefixHash)
		}
	}

	// The entry with no system prompt should have PrefixHash == NoPrefixHash
	foundZero := false
	for _, e := range entries {
		if e.PrefixHash == NoPrefixHash {
			foundZero = true
			break
		}
	}
	if !foundZero {
		t.Fatalf("expected at least one entry with NoPrefixHash (no system prompt)")
	}

	// Entries with system prompts should have PrefixHash != NoPrefixHash
	nonZeroCount := 0
	for _, e := range entries {
		if e.PrefixHash != NoPrefixHash {
			nonZeroCount++
		}
	}
	if nonZeroCount != 3 {
		t.Fatalf("expected 3 entries with non-zero PrefixHash, got %d", nonZeroCount)
	}

	// Two entries with identical system prompt ("You are a helpful assistant.") must share the same hash
	hashCounts := map[uint32]int{}
	for _, e := range entries {
		hashCounts[e.PrefixHash]++
	}
	foundDuplicate := false
	for h, c := range hashCounts {
		if h != NoPrefixHash && c >= 2 {
			foundDuplicate = true
			break
		}
	}
	if !foundDuplicate {
		t.Fatalf("expected at least one non-zero PrefixHash shared by 2+ entries, got counts: %v", hashCounts)
	}
}

func TestWatchCancel_SetsFlag_CancelsInferContext(t *testing.T) {
	ctx := testLoggerCtx(t)

	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()
	eventClient := mockdb.NewMockBatchEventChannelClient()

	jobID := "job-cancel-1"
	initialStatus := openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}
	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID: jobID,
			Tags: db.Tags{
				"tenant": "tenantA",
			},
		},
		BaseContents: db.BaseContents{
			Spec:   mustJSON(t, openai.BatchSpec{InputFileID: "unused-for-watch-cancel"}),
			Status: mustJSON(t, initialStatus),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}

	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{})
	updater := NewStatusUpdater(dbClient, statusClient, 86400)

	evCh, err := eventClient.ECConsumerGetChannel(ctx, jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer evCh.CloseFn()

	abortCtx, cause := context.WithCancelCause(ctx)

	params := &jobExecutionParams{
		eventWatcher: evCh,
		updater:      updater,
		jobItem:      jobItem,
		cancelUser:   func() { cause(batchctx.ErrCancelled) },
	}
	go p.watchCancel(ctx, params)

	_, _ = eventClient.ECProducerSendEvents(ctx, []db.BatchEvent{
		{ID: jobID, Type: db.BatchEventCancel, TTL: 60},
	})

	// Verify watchCancel tripped the user-cancel layer with the ErrCancelled cause.
	select {
	case <-abortCtx.Done():
		if !errors.Is(batchctx.Cause(abortCtx), batchctx.ErrCancelled) {
			t.Fatalf("expected batchctx.ErrCancelled cause, got: %v", batchctx.Cause(abortCtx))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("abort context was not cancelled within 2s after cancel event")
	}

	// Verify that watchCancel does NOT update status to cancelling
	// (API server already did that before sending the event)
	time.Sleep(100 * time.Millisecond)
	if dbClient.StatusCalls(openai.BatchStatusCancelling) > 0 {
		t.Fatalf("watchCancel should not update status to cancelling, got=%d calls", dbClient.StatusCalls(openai.BatchStatusCancelling))
	}
}

func TestPreProcess_CancelFlag_ReturnsErrCancelled(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDBClient,
		File:    filesClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-preprocess-cancel"
	inputFileID := "file-preprocess-cancel"
	tenantID := uniqueTestFolder(t, "tenantA/preprocess-cancel")
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	models := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		switch i % 3 {
		case 0:
			models = append(models, "mA")
		case 1:
			models = append(models, "mB")
		default:
			models = append(models, "mC")
		}
	}
	lines := makeInputLines(models)
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, "input.jsonl"), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: "input.jsonl"}
	if err := fileDBClient.DBStore(ctx, &db.FileItem{
		BaseIndexes: db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Spec: mustJSON(t, fileSpec),
		},
	}); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	cancelCtx, cancel := context.WithCancelCause(ctx)
	cancel(batchctx.ErrCancelled)
	err = p.preProcessJob(cancelCtx, jobInfo)
	if !errors.Is(err, batchctx.ErrCancelled) {
		t.Fatalf("expected batchctx.ErrCancelled, got: %v", err)
	}
}

// TestPreProcess_CancelBeforeSIGTERM_ReturnsErrCancelled verifies first-cause-wins:
// TestPreProcess_GenuineErrorNotMaskedByCancel verifies that a real preprocessing
// failure which coincides with a cancelled context surfaces as itself, not
// reclassified as a terminal cancel/expiry/shutdown. The empty-InputFileID check
// returns before the loop's cause-check, so only the deferred wrapper could mask it.
func TestPreProcess_GenuineErrorNotMaskedByCancel(t *testing.T) {
	p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{})

	// Context is cancelled with a user-cancel cause...
	ctx, cancel := context.WithCancelCause(testLoggerCtx(t))
	cancel(batchctx.ErrCancelled)

	// ...but the failure is a genuine config error, not a cancellation.
	jobInfo := &batch_types.JobInfo{
		JobID:    "job-genuine-err",
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{InputFileID: ""}},
	}

	err := p.preProcessJob(ctx, jobInfo)
	if err == nil {
		t.Fatal("expected an error for empty input file ID")
	}
	if batchctx.IsTerminal(err) {
		t.Fatalf("genuine error was masked as a terminal state: %v", err)
	}
	if !strings.Contains(err.Error(), "input file ID is empty") {
		t.Fatalf("expected 'input file ID is empty', got: %v", err)
	}
}

// when a user cancel is recorded before a later SIGTERM on the same abort context,
// preProcessJob returns batchctx.ErrCancelled — the shutdown is a no-op on the
// already-cancelled context, so the job is cancelled rather than left for the reconciler.
func TestPreProcess_CancelBeforeSIGTERM_ReturnsErrCancelled(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDBClient,
		File:    filesClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-preprocess-cancel-sigterm"
	inputFileID := "file-preprocess-cancel-sigterm"
	tenantID := uniqueTestFolder(t, "tenantA/preprocess-cancel-sigterm")
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	models := make([]string, 0, 2000)
	for i := 0; i < 2000; i++ {
		models = append(models, "mA")
	}
	lines := makeInputLines(models)
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, "input.jsonl"), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: "input.jsonl"}
	if err := fileDBClient.DBStore(ctx, &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	// Both fire, but the user cancel is recorded first. The shutdown layer is the
	// parent; the user-cancel layer is nested below it (as in runJob). Tripping the
	// inner cancel first sets the cause; the later shutdown is a no-op on it.
	shutdownBase, tripShutdown := context.WithCancelCause(ctx)
	cancelCtx, cancelUser := context.WithCancelCause(shutdownBase)
	cancelUser(batchctx.ErrCancelled)  // user cancel first
	tripShutdown(batchctx.ErrShutdown) // SIGTERM second — no-op on the already-cancelled inner layer

	err = p.preProcessJob(cancelCtx, jobInfo)
	if !errors.Is(err, batchctx.ErrCancelled) {
		t.Fatalf("expected batchctx.ErrCancelled when user cancel is recorded before SIGTERM, got: %v", err)
	}
}

func TestPreProcess_SLOExpiredDuringIngestion_ReturnsErrExpired(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDBClient,
		File:    filesClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-preprocess-slo-expired"
	inputFileID := "file-preprocess-slo-expired"
	tenantID := uniqueTestFolder(t, "tenantA/preprocess-slo-expired")
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	// A single request is enough: sloCtx is already expired, so the ingestion loop
	// returns batchctx.ErrExpired on the first iteration before reading any lines.
	lines := makeInputLines([]string{"any-model"}) // content irrelevant; loop exits before reading
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, "input.jsonl"), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: "input.jsonl"}
	if err := fileDBClient.DBStore(ctx, &db.FileItem{
		BaseIndexes: db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{
			Spec: mustJSON(t, fileSpec),
		},
	}); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	// Use a context with a deadline in the past so context.Cause == DeadlineExceeded immediately.
	sloCtx, sloCancel := context.WithDeadline(ctx, time.Now().Add(-1*time.Second))
	defer sloCancel()

	err = p.preProcessJob(sloCtx, jobInfo)
	if !errors.Is(err, batchctx.ErrExpired) {
		t.Fatalf("expected batchctx.ErrExpired, got: %v", err)
	}
}

func TestHandleCancelled_CleansDir_UpdatesCancelled(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	dbClient := newMockBatchDBClient()
	statusClient := mockdb.NewMockBatchStatusClient()
	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-handle-cancelled"
	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: "tenantA",
			Tags: db.Tags{
				"tenant": "tenantA",
			},
		},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{
				Status: openai.BatchStatusCancelling,
			}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}

	jobDir, err := p.jobRootDir(jobID, jobItem.TenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll jobDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "dummy.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile dummy: %v", err)
	}

	updater := NewStatusUpdater(dbClient, statusClient, 86400)

	if err := p.handleCancelled(ctx, &jobExecutionParams{
		updater: updater,
		jobItem: jobItem,
	}); err != nil {
		t.Fatalf("handleCancelled: %v", err)
	}

	if _, err := os.Stat(jobDir); err == nil {
		t.Fatalf("expected job dir removed, still exists: %s", jobDir)
	}

	jobs, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("DBGet job after cancel: err=%v len=%d", err, len(jobs))
	}

	var status openai.BatchStatusInfo
	if err := json.Unmarshal(jobs[0].Status, &status); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if status.Status != openai.BatchStatusCancelled {
		t.Fatalf("status = %s, want %s", status.Status, openai.BatchStatusCancelled)
	}
	// handleCancelled was called before executeJob (requestCounts nil) so counts remain zero.
	if status.RequestCounts.Total != 0 {
		t.Fatalf("expected zero request_counts for pre-execution cancel, got %+v", status.RequestCounts)
	}
}

func TestRunPollingLoop_ExpiredJob_UpdatesExpiredStatus(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.NumWorkers = 1

	pq := &spyPQ{inner: mockdb.NewMockBatchPriorityQueueClient()}
	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()
	jobID := "job-expired-1"

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: "tenantA",
			Tags: db.Tags{
				"tenant": "tenantA",
			},
		},
		BaseContents: db.BaseContents{
			Spec: mustJSON(t, openai.BatchSpec{
				InputFileID: "unused",
			}),
			Status: mustJSON(t, openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
		ID:  jobID,
		SLO: time.Now().Add(-1 * time.Second),
	}); err != nil {
		t.Fatalf("PQEnqueue task: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pq,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	runCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	if err := p.runPollingLoop(runCtx, runCtx); err != nil {
		t.Fatalf("runPollingLoop: %v", err)
	}

	if dbClient.StatusCalls(openai.BatchStatusExpired) < 1 {
		t.Fatalf("expected expired status update at least once")
	}
}

func TestRunPollingLoop_DBTransient_ReEnqueuesTask(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.NumWorkers = 1

	pq := &spyPQ{inner: mockdb.NewMockBatchPriorityQueueClient()}
	innerDB := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)
	dbClient := &dbGetErrWrapper{
		inner: innerDB,
		err:   errors.New("db transient"),
	}
	statusClient := mockdb.NewMockBatchStatusClient()
	jobID := "job-db-transient-1"

	if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
		ID:  jobID,
		SLO: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatalf("PQEnqueue task: %v", err)
	}
	initialEnqueueCalls := pq.EnqueueCalls()

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pq,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	runCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	if err := p.runPollingLoop(runCtx, runCtx); err != nil {
		t.Fatalf("runPollingLoop: %v", err)
	}

	if pq.EnqueueCalls() <= initialEnqueueCalls {
		t.Fatalf("expected task re-enqueue on transient DB error")
	}
}

func TestRunPollingLoop_MalformedJobItem_MarksFailed(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.NumWorkers = 1

	pq := &spyPQ{inner: mockdb.NewMockBatchPriorityQueueClient()}
	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()
	jobID := "job-malformed-1"

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: "tenantA",
			Tags: db.Tags{
				"tenant": "tenantA",
			},
		},
		BaseContents: db.BaseContents{
			// Invalid JSON in Spec forces DBItemToBatch -> FromDBItemToJobInfoObject to fail,
			// while keeping Status valid so handleFailed can still write a terminal status.
			Spec: []byte(`{"input_file_id":`),
			Status: mustJSON(t, openai.BatchStatusInfo{
				Status: openai.BatchStatusValidating,
			}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
		ID:  jobID,
		SLO: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatalf("PQEnqueue task: %v", err)
	}
	initialEnqueueCalls := pq.EnqueueCalls()

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pq,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	runCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	if err := p.runPollingLoop(runCtx, runCtx); err != nil {
		t.Fatalf("runPollingLoop: %v", err)
	}

	if dbClient.StatusCalls(openai.BatchStatusFailed) < 1 {
		t.Fatalf("expected malformed dequeued job to be marked failed")
	}
	if pq.EnqueueCalls() != initialEnqueueCalls {
		t.Fatalf("expected malformed dequeued job not to be re-enqueued")
	}
}

func TestRunPollingLoop_NotRunnableJob_SkipsWithoutStatusUpdate(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.NumWorkers = 1

	pq := &spyPQ{inner: mockdb.NewMockBatchPriorityQueueClient()}
	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()
	jobID := "job-not-runnable-1"

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: "tenantA",
			Tags: db.Tags{
				"tenant": "tenantA",
			},
		},
		BaseContents: db.BaseContents{
			Spec: mustJSON(t, openai.BatchSpec{
				InputFileID: "unused",
			}),
			// completed is terminal and not runnable
			Status: mustJSON(t, openai.BatchStatusInfo{
				Status: openai.BatchStatusCompleted,
			}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
		ID:  jobID,
		SLO: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatalf("PQEnqueue task: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pq,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	runCtx, cancel := context.WithTimeout(ctx, 40*time.Millisecond)
	defer cancel()
	if err := p.runPollingLoop(runCtx, runCtx); err != nil {
		t.Fatalf("runPollingLoop: %v", err)
	}

	// no persistent status transition should be attempted for not-runnable jobs.
	if dbClient.StatusCalls(openai.BatchStatusCompleted) > 0 || dbClient.StatusCalls(openai.BatchStatusFailed) > 0 || dbClient.StatusCalls(openai.BatchStatusExpired) > 0 {
		t.Fatalf("expected no status updates for not-runnable job")
	}
}

func TestRunPollingLoop_GuardCancelAfterDequeue_ReEnqueuesBeforeLaunch(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.NumWorkers = 1

	pollingCtx, pollingCancel := context.WithCancel(ctx)
	defer pollingCancel()

	pq := &spyPQ{
		inner: mockdb.NewMockBatchPriorityQueueClient(),
		afterDequeueFn: func() {
			pollingCancel()
		},
	}
	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()
	jobID := "job-guard-requeue-1"

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: "tenantA",
			Tags:     db.Tags{"tenant": "tenantA"},
		},
		BaseContents: db.BaseContents{
			Spec: mustJSON(t, openai.BatchSpec{
				InputFileID: "unused",
			}),
			Status: mustJSON(t, openai.BatchStatusInfo{
				Status: openai.BatchStatusValidating,
			}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
		ID:  jobID,
		SLO: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatalf("PQEnqueue task: %v", err)
	}
	initialEnqueueCalls := pq.EnqueueCalls()

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pq,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	if err := p.runPollingLoop(pollingCtx, ctx); err != nil {
		t.Fatalf("runPollingLoop: %v", err)
	}

	if pq.EnqueueCalls() <= initialEnqueueCalls {
		t.Fatalf("expected task re-enqueue when guard cancels pollingCtx after dequeue")
	}
}

func TestRunPollingLoop_SIGTERMAfterDequeue_ReEnqueuesViaDetachedCtx(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(testLoggerCtx(t))
	defer parentCancel()

	cfg := config.NewConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.NumWorkers = 1

	pollingCtx, pollingCancel := context.WithCancel(parentCtx)
	defer pollingCancel()

	pq := &spyPQ{
		inner: mockdb.NewMockBatchPriorityQueueClient(),
		afterDequeueFn: func() {
			parentCancel()
		},
	}
	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()
	jobID := "job-sigterm-requeue-1"

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: "tenantA",
			Tags:     db.Tags{"tenant": "tenantA"},
		},
		BaseContents: db.BaseContents{
			Spec: mustJSON(t, openai.BatchSpec{
				InputFileID: "unused",
			}),
			Status: mustJSON(t, openai.BatchStatusInfo{
				Status: openai.BatchStatusValidating,
			}),
		},
	}
	if err := dbClient.DBStore(parentCtx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	if err := pq.PQEnqueue(parentCtx, &db.BatchJobPriority{
		ID:  jobID,
		SLO: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatalf("PQEnqueue task: %v", err)
	}
	initialEnqueueCalls := pq.EnqueueCalls()

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pq,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	if err := p.runPollingLoop(pollingCtx, parentCtx); err != nil {
		t.Fatalf("runPollingLoop: %v", err)
	}

	if pq.EnqueueCalls() <= initialEnqueueCalls {
		t.Fatalf("expected task re-enqueue even when parent ctx (SIGTERM) is cancelled")
	}
}

func TestRunPollingLoop_FetchFailsWithCancelledCtx_ReEnqueuesViaDetachedCtx(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.NumWorkers = 1

	pollingCtx, pollingCancel := context.WithCancel(ctx)
	defer pollingCancel()

	pq := &spyPQ{
		inner: mockdb.NewMockBatchPriorityQueueClient(),
		afterDequeueFn: func() {
			pollingCancel()
		},
	}

	innerDB := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)
	dbClient := &dbGetErrWrapper{
		inner: innerDB,
		err:   context.Canceled,
	}
	statusClient := mockdb.NewMockBatchStatusClient()
	jobID := "job-fetch-cancel-requeue-1"

	if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
		ID:  jobID,
		SLO: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatalf("PQEnqueue task: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pq,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	if err := p.runPollingLoop(pollingCtx, ctx); err != nil {
		t.Fatalf("runPollingLoop: %v", err)
	}

	tasks, err := pq.PQDequeue(ctx, 0, 10)
	if err != nil {
		t.Fatalf("PQDequeue: %v", err)
	}
	if len(tasks) == 0 {
		t.Fatalf("expected task to be back in queue after re-enqueue via detached context")
	}
}

func TestRunPollingLoop_GuardReEnqueueFails_FallsBackToHandleFailed(t *testing.T) {
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.PollInterval = 5 * time.Millisecond
	cfg.NumWorkers = 1

	pollingCtx, pollingCancel := context.WithCancel(ctx)
	defer pollingCancel()

	pq := &spyPQ{
		inner: mockdb.NewMockBatchPriorityQueueClient(),
		afterDequeueFn: func() {
			pollingCancel()
		},
		enqueueErr: fmt.Errorf("redis unavailable"),
	}
	dbClient := newSpyBatchDB(newMockBatchDBClient())
	statusClient := mockdb.NewMockBatchStatusClient()
	jobID := "job-guard-fail-fallback-1"

	jobItem := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       jobID,
			TenantID: "tenantA",
			Tags:     db.Tags{"tenant": "tenantA"},
		},
		BaseContents: db.BaseContents{
			Spec: mustJSON(t, openai.BatchSpec{
				InputFileID: "unused",
			}),
			Status: mustJSON(t, openai.BatchStatusInfo{
				Status: openai.BatchStatusValidating,
			}),
		},
	}
	if err := dbClient.DBStore(ctx, jobItem); err != nil {
		t.Fatalf("DBStore job item: %v", err)
	}
	// Seed the queue via the inner client directly (bypasses enqueueErr).
	if err := pq.inner.PQEnqueue(ctx, &db.BatchJobPriority{
		ID:  jobID,
		SLO: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatalf("PQEnqueue task: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		Queue:   pq,
		Status:  statusClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	if err := p.runPollingLoop(pollingCtx, ctx); err != nil {
		t.Fatalf("runPollingLoop: %v", err)
	}

	if dbClient.StatusCalls(openai.BatchStatusFailed) < 1 {
		t.Fatalf("expected handleFailed to mark job as failed when re-enqueue fails, got %d failed status calls",
			dbClient.StatusCalls(openai.BatchStatusFailed))
	}
}

// ---------------------------------------------------------------------------
// stream: true rejection
// ---------------------------------------------------------------------------

func TestExtractAndValidateLine_StreamTrue_ReturnsError(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","stream":true,"messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for stream: true, got nil")
	}
	if !strings.Contains(err.Error(), "streaming is not supported") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_StreamFalse_OK(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","stream":false,"messages":[{"role":"user","content":"hi"}]}}` + "\n")
	meta, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err != nil {
		t.Fatalf("unexpected error for stream: false: %v", err)
	}
	if meta.ModelID != "gpt-4" {
		t.Fatalf("expected model gpt-4, got %s", meta.ModelID)
	}
	if meta.CustomID != "r1" {
		t.Fatalf("expected custom_id r1, got %s", meta.CustomID)
	}
}

func TestExtractAndValidateLine_StreamOmitted_OK(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	meta, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err != nil {
		t.Fatalf("unexpected error when stream is omitted: %v", err)
	}
	if meta.ModelID != "gpt-4" {
		t.Fatalf("expected model gpt-4, got %s", meta.ModelID)
	}
}

func TestExtractAndValidateLine_EmptyCustomID_ReturnsError(t *testing.T) {
	line := []byte(`{"custom_id":"","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for empty custom_id, got nil")
	}
	if !strings.Contains(err.Error(), "custom_id is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_MissingCustomID_ReturnsError(t *testing.T) {
	line := []byte(`{"method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for missing custom_id, got nil")
	}
	if !strings.Contains(err.Error(), "custom_id is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_MissingMethod_ReturnsError(t *testing.T) {
	line := []byte(`{"custom_id":"r1","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for missing method, got nil")
	}
	if !strings.Contains(err.Error(), "method is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_InvalidMethod_ReturnsError(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"DELETE","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for invalid method, got nil")
	}
	if !strings.Contains(err.Error(), "invalid method") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_MissingURL_ReturnsError(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for missing url, got nil")
	}
	if !strings.Contains(err.Error(), "url is required") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_AbsoluteURL_ReturnsError(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"http://evil.com","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for absolute url, got nil")
	}
	if !strings.Contains(err.Error(), "relative path") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_DoubleSlashURL_ReturnsError(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"//evil.com","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for protocol-relative url, got nil")
	}
	if !strings.Contains(err.Error(), "relative path") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_NotAllowedEndpoint_ReturnsError(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/not-allowed","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error for invalid endpoint, got nil")
	}
	if !strings.Contains(err.Error(), "invalid endpoint") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_BatchEndpointMismatch(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/embeddings","body":{"model":"embedding-model"}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err == nil {
		t.Fatal("expected error when request endpoint differs from batch endpoint")
	}
	if !strings.Contains(err.Error(), "does not match batch endpoint") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_EmptyBatchEndpoint(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4"}}` + "\n")
	_, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "")
	if err == nil {
		t.Fatal("expected error when stored batch endpoint is empty")
	}
	if !strings.Contains(err.Error(), "does not match batch endpoint") {
		t.Fatalf("unexpected error message: %v", err)
	}
}

func TestExtractAndValidateLine_BatchEndpointMatch(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/embeddings","body":{"model":"embedding-model"}}` + "\n")
	if _, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/embeddings"); err != nil {
		t.Fatalf("unexpected error when endpoints match: %v", err)
	}
}

func TestExtractAndValidateLine_ConfiguredEndpoint_OK(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/classify","body":{"model":"classifier"}}` + "\n")
	allowlist, err := openai.NewEndpointAllowlist([]string{"/v1/classify"})
	if err != nil {
		t.Fatalf("NewEndpointAllowlist() unexpected error: %v", err)
	}
	meta, err := extractAndValidateLine(line, allowlist, "/v1/classify")
	if err != nil {
		t.Fatalf("unexpected error for configured endpoint: %v", err)
	}
	if meta.ModelID != "classifier" {
		t.Fatalf("expected model classifier, got %s", meta.ModelID)
	}
}

func TestExtractAndValidateLine_AllowedEndpoint_OK(t *testing.T) {
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"hi"}]}}` + "\n")
	meta, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions")
	if err != nil {
		t.Fatalf("unexpected error for allowed endpoint: %v", err)
	}
	if meta.CustomID != "r1" {
		t.Fatalf("expected custom_id r1, got %s", meta.CustomID)
	}
	if meta.ModelID != "gpt-4" {
		t.Fatalf("expected model gpt-4, got %s", meta.ModelID)
	}
}

func TestPreProcess_StreamTrue_FailsJob(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := uniqueTestFolder(t, "tenantA/stream-reject")
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	var remoteBuf bytes.Buffer
	remoteBuf.WriteString(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"ok"}]}}` + "\n")
	remoteBuf.WriteString(`{"custom_id":"r2","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","stream":true,"messages":[{"role":"user","content":"bad"}]}}` + "\n")
	remoteBuf.WriteString(`{"custom_id":"r3","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"ok2"}]}}` + "\n")

	filename := "input.jsonl"
	inputFileID := "file-stream-reject"

	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDBClient,
		File:    filesClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-stream-reject"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	err = p.preProcessJob(ctx, jobInfo)
	if err == nil {
		t.Fatal("expected preProcessJob to fail for input with stream: true")
	}
	if !strings.Contains(err.Error(), "streaming is not supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestPreProcess_DuplicateCustomID_FailsJob(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := uniqueTestFolder(t, "tenantA/dup-custom-id")
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	var remoteBuf bytes.Buffer
	remoteBuf.WriteString(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"a"}]}}` + "\n")
	remoteBuf.WriteString(`{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"b"}]}}` + "\n")
	remoteBuf.WriteString(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"c"}]}}` + "\n")

	filename := "input.jsonl"
	inputFileID := "file-dup-custom-id"

	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDBClient,
		File:    filesClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-dup-custom-id"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	err = p.preProcessJob(ctx, jobInfo)
	if err == nil {
		t.Fatal("expected preProcessJob to fail for duplicate custom_id")
	}
	if !strings.Contains(err.Error(), "duplicate custom_id") {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(err.Error(), "req-1") {
		t.Fatalf("error should mention the duplicated custom_id value: %v", err)
	}
}

func TestPreProcess_UniqueCustomIDs_Succeeds(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir
	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := uniqueTestFolder(t, "tenantA/unique-custom-id")
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}

	var remoteBuf bytes.Buffer
	remoteBuf.WriteString(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"a"}]}}` + "\n")
	remoteBuf.WriteString(`{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"b"}]}}` + "\n")
	remoteBuf.WriteString(`{"custom_id":"req-3","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"c"}]}}` + "\n")

	filename := "input.jsonl"
	inputFileID := "file-unique-custom-id"

	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB: dbClient,
		FileDB:  fileDBClient,
		File:    filesClient,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-unique-custom-id"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	if err := p.preProcessJob(ctx, jobInfo); err != nil {
		t.Fatalf("expected preProcessJob to succeed with unique custom_ids, got: %v", err)
	}
}

func TestPreProcess_UnregisteredModel_RejectedToErrorFile(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := "tenant__tenantA"
	folder, _ := ucom.GetFolderNameByTenantID(tenantID)
	filename := "input.jsonl"

	// 3 requests: model-a (registered), model-b (NOT registered), model-a again
	lines := [][]byte{
		[]byte(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"model-a","messages":[{"role":"user","content":"hi"}]}}` + "\n"),
		[]byte(`{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"model-b","messages":[{"role":"user","content":"hi"}]}}` + "\n"),
		[]byte(`{"custom_id":"req-3","method":"POST","url":"/v1/chat/completions","body":{"model":"model-a","messages":[{"role":"user","content":"bye"}]}}` + "\n"),
	}
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	inputFileID := "file-unregistered-model"
	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	// Per-model resolver with only "model-a" registered.
	resolver, err := inference.NewPerModelResolver(
		map[string]inference.GatewayClientConfig{
			"model-a": {URL: "http://fake:8000"},
		},
		testLogger(t),
	)
	if err != nil {
		t.Fatalf("NewPerModelResolver: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    fileDBClient,
		File:      filesClient,
		Inference: resolver,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-unregistered"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	if err := p.preProcessJob(ctx, jobInfo); err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}

	// Error file should contain exactly 1 line for model-b
	errorPath, _ := p.jobErrorFilePath(jobID, tenantID)
	errorBytes, err := os.ReadFile(errorPath)
	if err != nil {
		t.Fatalf("read error file: %v", err)
	}
	errorLines := bytes.Split(bytes.TrimSpace(errorBytes), []byte{'\n'})
	if len(errorLines) != 1 {
		t.Fatalf("error file lines = %d, want 1", len(errorLines))
	}
	if !bytes.Contains(errorLines[0], []byte(`"model_not_found"`)) {
		t.Fatalf("error line missing model_not_found code: %s", errorLines[0])
	}
	if !bytes.Contains(errorLines[0], []byte(`"req-2"`)) {
		t.Fatalf("error line missing custom_id req-2: %s", errorLines[0])
	}

	// model_map.json should record 1 rejected request
	jobRootDir, _ := p.jobRootDir(jobID, tenantID)
	mm, err := readModelMap(jobRootDir)
	if err != nil {
		t.Fatalf("readModelMap: %v", err)
	}
	if mm.RejectedCount != 1 {
		t.Fatalf("RejectedCount = %d, want 1", mm.RejectedCount)
	}
	if _, ok := mm.ModelToSafe["model-b"]; ok {
		t.Fatal("model-b should not appear in model map (rejected during ingestion)")
	}
	safeA, ok := mm.ModelToSafe["model-a"]
	if !ok {
		t.Fatal("model-a missing from model map")
	}
	planPath := filepath.Join(jobRootDir, "plans", safeA+".plan")
	planEntries := testReadPlanEntries(t, planPath)
	if len(planEntries) != 2 {
		t.Fatalf("plan entries for model-a = %d, want 2", len(planEntries))
	}
}

// TestPreProcess_AllRequestsUnregistered_ExecuteJobCounts covers the edge case where every
// line targets an unregistered model: modelToSafe is empty, no plan files are written,
// executeJob launches zero processModel goroutines, progress is seeded from RejectedCount,
// and counts reflect Total=N, Failed=N, Completed=0.
func TestPreProcess_AllRequestsUnregistered_ExecuteJobCounts(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := "tenant__tenantA"
	folder, _ := ucom.GetFolderNameByTenantID(tenantID)
	filename := "input.jsonl"

	lines := [][]byte{
		[]byte(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"model-x","messages":[{"role":"user","content":"a"}]}}` + "\n"),
		[]byte(`{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"model-y","messages":[{"role":"user","content":"b"}]}}` + "\n"),
		[]byte(`{"custom_id":"req-3","method":"POST","url":"/v1/chat/completions","body":{"model":"model-z","messages":[{"role":"user","content":"c"}]}}` + "\n"),
	}
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	inputFileID := "file-all-unregistered"
	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	resolver, err := inference.NewPerModelResolver(
		map[string]inference.GatewayClientConfig{
			"model-a": {URL: "http://fake:8000"},
		},
		testLogger(t),
	)
	if err != nil {
		t.Fatalf("NewPerModelResolver: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    fileDBClient,
		File:      filesClient,
		Inference: resolver,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-all-unregistered"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusInProgress,
			},
		},
		TenantID: tenantID,
	}

	if err := p.preProcessJob(ctx, jobInfo); err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}

	errorPath, _ := p.jobErrorFilePath(jobID, tenantID)
	errorBytes, err := os.ReadFile(errorPath)
	if err != nil {
		t.Fatalf("read error file: %v", err)
	}
	errorLines := bytes.Split(bytes.TrimSpace(errorBytes), []byte{'\n'})
	if len(errorLines) != 3 {
		t.Fatalf("error file lines = %d, want 3", len(errorLines))
	}

	jobRootDir, _ := p.jobRootDir(jobID, tenantID)
	mm, err := readModelMap(jobRootDir)
	if err != nil {
		t.Fatalf("readModelMap: %v", err)
	}
	if mm.RejectedCount != 3 {
		t.Fatalf("RejectedCount = %d, want 3", mm.RejectedCount)
	}
	if len(mm.ModelToSafe) != 0 {
		t.Fatalf("ModelToSafe = %v, want empty (no plans)", mm.ModelToSafe)
	}
	if len(mm.SafeToModel) != 0 {
		t.Fatalf("SafeToModel = %v, want empty", mm.SafeToModel)
	}
	plansDir := filepath.Join(jobRootDir, "plans")
	entries, err := os.ReadDir(plansDir)
	if err != nil {
		t.Fatalf("ReadDir plans: %v", err)
	}
	var planFiles int
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".plan") {
			planFiles++
		}
	}
	if planFiles != 0 {
		t.Fatalf("plan files = %d, want 0", planFiles)
	}

	counts, execErr := p.executeJob(ctx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient, mockdb.NewMockBatchStatusClient(), 86400),
		jobInfo: jobInfo,
	})
	if execErr != nil {
		t.Fatalf("executeJob: %v", execErr)
	}
	if counts.Total != 3 || counts.Failed != 3 || counts.Completed != 0 {
		t.Fatalf("counts = %+v, want Total=3 Failed=3 Completed=0", counts)
	}
}

// TestPreProcess_ReEnqueue_TruncatesStaleErrorFile verifies that when a job is
// re-enqueued (e.g. after pod shutdown), the next ingestion run truncates any
// stale error.jsonl left by the previous attempt. Without truncation, execution's
// O_APPEND would mix stale entries with fresh ones.
func TestPreProcess_ReEnqueue_TruncatesStaleErrorFile(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := "tenant__tenantA"
	folder, _ := ucom.GetFolderNameByTenantID(tenantID)
	filename := "input.jsonl"

	// All models are registered — no rejections expected.
	lines := [][]byte{
		[]byte(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"model-a","messages":[{"role":"user","content":"hi"}]}}` + "\n"),
	}
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	inputFileID := "file-reenqueue"
	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	resolver, err := inference.NewPerModelResolver(
		map[string]inference.GatewayClientConfig{
			"model-a": {URL: "http://fake:8000"},
		},
		testLogger(t),
	)
	if err != nil {
		t.Fatalf("NewPerModelResolver: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    fileDBClient,
		File:      filesClient,
		Inference: resolver,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-reenqueue"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID:              jobID,
			BatchSpec:       openai.BatchSpec{InputFileID: inputFileID, Endpoint: openai.EndpointChatCompletions},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusInProgress},
		},
		TenantID: tenantID,
	}

	// Simulate a stale error.jsonl from a previous attempt.
	errorPath, _ := p.jobErrorFilePath(jobID, tenantID)
	jobRootDir, _ := p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobRootDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(errorPath, []byte(`{"id":"stale","custom_id":"old","error":{"code":"stale_error","message":"from previous attempt"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile stale error: %v", err)
	}

	if err := p.preProcessJob(ctx, jobInfo); err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}

	// error.jsonl should be empty (truncated by ingestion, no rejections this time).
	errorBytes, err := os.ReadFile(errorPath)
	if err != nil {
		t.Fatalf("read error file: %v", err)
	}
	if len(bytes.TrimSpace(errorBytes)) != 0 {
		t.Fatalf("expected empty error.jsonl after re-enqueue with no rejections, got:\n%s", errorBytes)
	}
}

// TestPreProcess_ModelNotFound_ThenEarlySLO_PreservesErrorFile verifies that
// when ingestion writes model_not_found entries and execution hits early SLO
// expiration, the error file is preserved (not truncated by execution).
func TestPreProcess_ModelNotFound_ThenEarlySLO_PreservesErrorFile(t *testing.T) {
	ctx := testLoggerCtx(t)

	workDir := t.TempDir()
	cfg := config.NewConfig()
	cfg.WorkDir = workDir

	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := "tenant__tenantA"
	folder, _ := ucom.GetFolderNameByTenantID(tenantID)
	filename := "input.jsonl"

	// 2 requests: model-a (registered), model-b (NOT registered)
	lines := [][]byte{
		[]byte(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"model-a","messages":[{"role":"user","content":"hi"}]}}` + "\n"),
		[]byte(`{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"model-b","messages":[{"role":"user","content":"hi"}]}}` + "\n"),
	}
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	inputFileID := "file-slo-preserve"
	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileSpec := &openai.FileObject{Filename: filename}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, fileSpec)},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	resolver, err := inference.NewPerModelResolver(
		map[string]inference.GatewayClientConfig{
			"model-a": {URL: "http://fake:8000"},
		},
		testLogger(t),
	)
	if err != nil {
		t.Fatalf("NewPerModelResolver: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    fileDBClient,
		File:      filesClient,
		Inference: resolver,
	}
	p := mustNewProcessor(t, cfg, clients)

	jobID := "job-slo-preserve"
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID:              jobID,
			BatchSpec:       openai.BatchSpec{InputFileID: inputFileID, Endpoint: openai.EndpointChatCompletions},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusInProgress},
		},
		TenantID: tenantID,
	}

	// Run ingestion — should reject model-b and write to error.jsonl.
	if err := p.preProcessJob(ctx, jobInfo); err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}

	// Verify error.jsonl has 1 model_not_found entry before execution.
	errorPath, _ := p.jobErrorFilePath(jobID, tenantID)
	errorBytes, err := os.ReadFile(errorPath)
	if err != nil {
		t.Fatalf("read error file: %v", err)
	}
	errorLines := bytes.Split(bytes.TrimSpace(errorBytes), []byte{'\n'})
	if len(errorLines) != 1 {
		t.Fatalf("error file lines before execution = %d, want 1", len(errorLines))
	}

	// Simulate early SLO expiration: create an already-expired context.
	sloCtx, sloCancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer sloCancel()

	counts, execErr := p.executeJob(sloCtx, &jobExecutionParams{
		updater: NewStatusUpdater(dbClient, mockdb.NewMockBatchStatusClient(), 86400),
		jobInfo: jobInfo,
	})
	if !errors.Is(execErr, batchctx.ErrExpired) {
		t.Fatalf("expected batchctx.ErrExpired, got: %v", execErr)
	}
	// Failed = 2: 1 model_not_found (ingestion) + 1 batch_expired (pipeline drain for model-a).
	if counts.Failed != 2 {
		t.Fatalf("Failed = %d, want 2 (1 model_not_found + 1 batch_expired)", counts.Failed)
	}

	// error.jsonl should have both the model_not_found and batch_expired entries.
	errorBytesAfter, err := os.ReadFile(errorPath)
	if err != nil {
		t.Fatalf("read error file after execution: %v", err)
	}
	if !bytes.Contains(errorBytesAfter, []byte(`"model_not_found"`)) {
		t.Fatalf("error file lost model_not_found entry after early SLO:\n%s", errorBytesAfter)
	}
	if !bytes.Contains(errorBytesAfter, []byte(`"batch_expired"`)) {
		t.Fatalf("error file missing batch_expired entry for undispatched request:\n%s", errorBytesAfter)
	}
}
