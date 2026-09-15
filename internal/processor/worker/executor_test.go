package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"

	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

func TestClassifyOutcome(t *testing.T) {
	persistErr := errors.New("output file flush failed")
	allSucceeded := &openai.BatchRequestCounts{Total: 2, Completed: 2, Failed: 0}
	partial := &openai.BatchRequestCounts{Total: 2, Completed: 1, Failed: 1}

	tests := []struct {
		name    string
		cause   error
		counts  *openai.BatchRequestCounts
		execErr error
		want    error
	}{
		{name: "expired wins over progress", cause: batchctx.ErrExpired, counts: allSucceeded, want: batchctx.ErrExpired},
		{name: "cancelled wins over progress", cause: batchctx.ErrCancelled, counts: allSucceeded, want: batchctx.ErrCancelled},
		{name: "shutdown after all succeeded finalizes", cause: batchctx.ErrShutdown, counts: allSucceeded, want: nil},
		{name: "shutdown with work left is terminal", cause: batchctx.ErrShutdown, counts: partial, want: batchctx.ErrShutdown},
		{name: "happy path", cause: nil, counts: allSucceeded, want: nil},
		{name: "executor error surfaces", cause: nil, counts: partial, execErr: persistErr, want: persistErr},
		// Regression: a persistence/flush error can arrive after every request was
		// recorded as completed (AllSucceeded), with no cancel/expiry/shutdown cause.
		// The outcome must be the executor error, not a masked success.
		{name: "persistence error not masked by all-succeeded", cause: nil, counts: allSucceeded, execErr: persistErr, want: persistErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyOutcome(tt.cause, tt.counts, tt.execErr); !errors.Is(got, tt.want) {
				t.Fatalf("classifyOutcome(%v, counts, %v) = %v, want %v", tt.cause, tt.execErr, got, tt.want)
			}
		})
	}
}

func TestExecuteJob_SingleModel(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	mock := &mockInferenceClient{}
	requests := []batch_types.Request{
		{CustomID: "r1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	ctx := testLoggerCtx(t)
	counts, err := env.p.executeJob(ctx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if err != nil {
		t.Fatalf("executeJob error: %v", err)
	}
	if counts.Total != 2 {
		t.Fatalf("Total = %d, want 2", counts.Total)
	}
	if counts.Completed+counts.Failed != 2 {
		t.Fatalf("Completed+Failed = %d, want 2", counts.Completed+counts.Failed)
	}

	outputPath, _ := env.p.jobOutputFilePath(jobInfo.JobID, jobInfo.TenantID)
	outBytes, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	outputLines := bytes.Split(bytes.TrimSpace(outBytes), []byte{'\n'})
	if len(outputLines) != 2 {
		t.Fatalf("output lines = %d, want 2", len(outputLines))
	}

	// Verify each custom_id appears exactly once — guards against duplicates.
	seenIDs := make(map[string]int)
	for _, line := range outputLines {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal output line: %v", err)
		}
		seenIDs[entry.CustomID]++
	}
	for _, wantID := range []string{"r1", "r2"} {
		if seenIDs[wantID] != 1 {
			t.Errorf("custom_id %q appeared %d times, want 1", wantID, seenIDs[wantID])
		}
	}

	// Error file should be empty (all requests succeeded).
	errorPath, _ := env.p.jobErrorFilePath(jobInfo.JobID, jobInfo.TenantID)
	errBytes, err := os.ReadFile(errorPath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("read error file: %v", err)
	}
	if len(bytes.TrimSpace(errBytes)) > 0 {
		t.Fatalf("expected empty error file, got: %s", errBytes)
	}
}

func TestExecuteJob_MultipleModels(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	var callCount atomic.Int32
	mock := &mockInferenceClient{
		generateFn: func(_ context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			callCount.Add(1)
			return &inference.GenerateResponse{RequestID: "srv", Response: []byte(`{"ok":true}`)}, nil
		},
	}

	requests := []batch_types.Request{
		{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "b", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m2"}},
		{CustomID: "c", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "d", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m2"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1", "m2": "m2"})

	ctx := testLoggerCtx(t)
	counts, err := env.p.executeJob(ctx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if err != nil {
		t.Fatalf("executeJob error: %v", err)
	}
	if counts.Total != 4 {
		t.Fatalf("Total = %d, want 4", counts.Total)
	}
	if int(callCount.Load()) != 4 {
		t.Fatalf("inference calls = %d, want 4", callCount.Load())
	}

	// Verify each custom_id appears exactly once and all 4 are present.
	outputPath, _ := env.p.jobOutputFilePath(jobInfo.JobID, jobInfo.TenantID)
	outBytes, err := os.ReadFile(outputPath)
	if err != nil {
		t.Fatalf("read output file: %v", err)
	}
	outputLines := bytes.Split(bytes.TrimSpace(outBytes), []byte{'\n'})
	if len(outputLines) != 4 {
		t.Fatalf("output lines = %d, want 4", len(outputLines))
	}
	seenIDs := make(map[string]int)
	for _, line := range outputLines {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal output line: %v", err)
		}
		seenIDs[entry.CustomID]++
	}
	for _, wantID := range []string{"a", "b", "c", "d"} {
		if seenIDs[wantID] != 1 {
			t.Errorf("custom_id %q appeared %d times, want 1", wantID, seenIDs[wantID])
		}
	}
}

func TestExecuteJob_ContextCancelled(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	mock := &mockInferenceClient{
		generateFn: func(ctx context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			<-ctx.Done()
			return nil, &inference.ClientError{Category: httpclient.ErrCategoryServer, Message: "cancelled"}
		},
	}

	requests := []batch_types.Request{
		{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	// Cancel with the batchctx.ErrCancelled cause so batchctx.Cause classifies this as a
	// user cancellation (a bare cancel is the neutral cause and routes to execErr).
	ctx, cancel := context.WithCancelCause(testLoggerCtx(t))
	cancel(batchctx.ErrCancelled)

	_, err := env.p.executeJob(ctx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if err == nil {
		t.Fatalf("expected error on cancelled context")
	}
	if !errors.Is(err, batchctx.ErrCancelled) {
		t.Fatalf("expected batchctx.ErrCancelled, got: %v", err)
	}
}

// TestExecuteJob_UserCancelFlag verifies that a user cancel (batchctx.ErrCancelled cause)
// which arrives after the request has already completed still routes to batchctx.ErrCancelled,
// so the job is not finalized as completed. The mock returns a normal response and
// trips the cause, exercising the batchctx.ErrCancelled branch winning over AllSucceeded.
func TestExecuteJob_UserCancelFlag(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	ctx, cancelFn := context.WithCancelCause(testLoggerCtx(t))

	mock := &mockInferenceClient{
		generateFn: func(_ context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			cancelFn(batchctx.ErrCancelled)
			return &inference.GenerateResponse{RequestID: "srv", Response: []byte(`{"ok":true}`)}, nil
		},
	}

	requests := []batch_types.Request{
		{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	counts, err := env.p.executeJob(ctx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if !errors.Is(err, batchctx.ErrCancelled) {
		t.Fatalf("expected batchctx.ErrCancelled, got: %v", err)
	}
	if counts == nil || counts.Total != 1 {
		t.Fatalf("expected counts with Total=1, got %+v", counts)
	}
	if counts.Completed != 1 {
		t.Fatalf("expected Completed=1 (request finished before cancel), got %+v", counts)
	}
}

// TestExecuteJob_CancelAfterAllRequestsComplete verifies that if userCancelCtx is cancelled
// after all requests have already been dispatched and completed successfully (i.e. context
// cancellation never interrupted dispatch), executeJob still returns batchctx.ErrCancelled rather than
// nil, preventing the job from being finalized as "completed".
func TestExecuteJob_CancelAfterAllRequestsComplete(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	ctx, abortFn := context.WithCancelCause(testLoggerCtx(t))

	// The mock trips the ErrCancelled cause after the inference call returns, simulating
	// the race where the cancel event arrives while (or just after) the last request completes.
	mock := &mockInferenceClient{
		generateFn: func(_ context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			abortFn(batchctx.ErrCancelled)
			return &inference.GenerateResponse{RequestID: "srv", Response: []byte(`{"ok":true}`)}, nil
		},
	}

	requests := []batch_types.Request{
		{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	_, err := env.p.executeJob(ctx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if !errors.Is(err, batchctx.ErrCancelled) {
		t.Fatalf("expected batchctx.ErrCancelled when cancel arrives after all requests complete, got: %v", err)
	}
}

// TestExecuteJob_SIGTERMAfterAllComplete verifies that when all requests finish successfully
// and SIGTERM arrives before executeJob returns, the function returns nil (not batchctx.ErrShutdown).
// This ensures the caller proceeds to finalizeJob (which uses a detached context) rather than
// taking the batchctx.ErrShutdown path (which leaves the job for the orphan reconciler).
func TestExecuteJob_SIGTERMAfterAllComplete(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	// ctx is tripped with the ErrShutdown cause after the inference call returns,
	// simulating SIGTERM arriving just after the last request completes.
	mainCtx, mainCancel := context.WithCancelCause(testLoggerCtx(t))
	mock := &mockInferenceClient{
		generateFn: func(_ context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			mainCancel(batchctx.ErrShutdown)
			return &inference.GenerateResponse{RequestID: "srv", Response: []byte(`{"ok":true}`)}, nil
		},
	}

	requests := []batch_types.Request{
		{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	counts, err := env.p.executeJob(mainCtx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if err != nil {
		t.Fatalf("expected nil error when SIGTERM arrives after all requests complete, got: %v", err)
	}
	if counts == nil {
		t.Fatal("expected non-nil counts")
		return
	}
	if counts.Total != 1 || counts.Completed != 1 {
		t.Fatalf("counts = {Total:%d, Completed:%d, Failed:%d}, want {1,1,0}",
			counts.Total, counts.Completed, counts.Failed)
	}
}

// TestExecuteJob_AbortCtxCancel_AbortsInflightRequests verifies that a user cancel aborts
// in-flight inference requests. The test trips params.cancelUser(), mirroring
// watchCancel's production behavior. The mock blocks until the abort context propagates to
// its ctx argument. executeJob must return batchctx.ErrCancelled with the in-flight request counted
// as failed.
func TestExecuteJob_AbortCtxCancel_AbortsInflightRequests(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	inferStarted := make(chan struct{})
	mock := &mockInferenceClient{
		generateFn: func(ctx context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			close(inferStarted)
			// Block until context is cancelled (simulates slow inference)
			<-ctx.Done()
			return nil, &inference.ClientError{
				Category: httpclient.ErrCategoryServer,
				Message:  "context cancelled",
				RawError: ctx.Err(),
			}
		},
	}

	requests := []batch_types.Request{
		{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	// cancelUser is pre-set in params before the goroutine starts, matching the production
	// flow where runJob sets params.cancelUser (a closure baking the ErrCancelled cause)
	// before starting watchCancel.
	ctx, cause := context.WithCancelCause(testLoggerCtx(t))
	cancelUser := func() { cause(batchctx.ErrCancelled) }

	params := &jobExecutionParams{
		updater:    env.updater,
		jobInfo:    jobInfo,
		cancelUser: cancelUser,
	}
	type result struct {
		counts *openai.BatchRequestCounts
		err    error
	}
	resCh := make(chan result, 1)
	go func() {
		counts, err := env.p.executeJob(ctx, params)
		resCh <- result{counts, err}
	}()

	<-inferStarted
	// Simulate watchCancel tripping the user-cancel layer, which aborts dispatch.
	cancelUser()

	select {
	case res := <-resCh:
		if !errors.Is(res.err, batchctx.ErrCancelled) {
			t.Fatalf("expected batchctx.ErrCancelled, got: %v", res.err)
		}
		if res.counts == nil {
			t.Fatal("expected non-nil counts")
		}
		if res.counts.Total != 1 {
			t.Errorf("Total = %d, want 1", res.counts.Total)
		}
		if res.counts.Completed != 0 {
			t.Errorf("Completed = %d, want 0 (request was aborted)", res.counts.Completed)
		}
		if res.counts.Failed != 1 {
			t.Errorf("Failed = %d, want 1 (aborted request counted as failed)", res.counts.Failed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executeJob did not return within 5s after abort cause was set")
	}
}

// TestExecuteJob_SLOExpiredBeforeDispatch verifies that when the SLO deadline has already
// passed before execution begins, the pipeline still runs and drains all requests as
// batch_expired errors. This ensures that handleExpired can upload a non-empty error file
// and the DB reflects Failed == Total.
func TestExecuteJob_SLOExpiredBeforeDispatch(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	requests := []batch_types.Request{
		{CustomID: "r1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r3", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, &mockInferenceClient{}, requests, map[string]string{"m1": "m1"})

	// A past deadline makes context.Cause report DeadlineExceeded, which both drains
	// undispatched requests as batch_expired and classifies the job as batchctx.ErrExpired.
	ctx, cancel := context.WithDeadline(testLoggerCtx(t), time.Now().Add(-1*time.Second))
	defer cancel()

	counts, err := env.p.executeJob(ctx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if !errors.Is(err, batchctx.ErrExpired) {
		t.Fatalf("expected batchctx.ErrExpired, got: %v", err)
	}
	if counts == nil {
		t.Fatal("expected non-nil counts")
		return
	}
	if counts.Total != 3 {
		t.Fatalf("Total = %d, want 3", counts.Total)
	}
	if counts.Completed != 0 {
		t.Fatalf("Completed = %d, want 0", counts.Completed)
	}
	if counts.Failed != 3 {
		t.Fatalf("Failed = %d, want 3 (all requests drained as batch_expired)", counts.Failed)
	}

	errorPath, _ := env.p.jobErrorFilePath(jobInfo.JobID, jobInfo.TenantID)
	errorData, readErr := os.ReadFile(errorPath)
	if readErr != nil {
		t.Fatalf("error.jsonl should exist: %v", readErr)
	}
	errorLines := bytes.Count(bytes.TrimSpace(errorData), []byte("\n")) + 1
	if errorLines != 3 {
		t.Fatalf("error.jsonl lines = %d, want 3", errorLines)
	}

	outputPath, _ := env.p.jobOutputFilePath(jobInfo.JobID, jobInfo.TenantID)
	outputData, readErr := os.ReadFile(outputPath)
	if readErr != nil {
		t.Fatalf("output.jsonl should exist: %v", readErr)
	}
	if len(outputData) != 0 {
		t.Fatalf("output.jsonl should be empty, got %d bytes", len(outputData))
	}
}

// TestExecuteJob_SLOExpiredDuringDispatch verifies that when the SLO deadline fires while
// requests are being dispatched, completed requests are preserved in the output file,
// undispatched requests are drained to the error file as batch_expired, and executeJob
// returns batchctx.ErrExpired with accurate partial counts.
//
// The single abort context carries the deadline, so context.Cause reports
// DeadlineExceeded: dispatch stops, the drain path selects batch_expired, and the
// post-execution switch classifies the job as batchctx.ErrExpired.
func TestExecuteJob_SLOExpiredDuringDispatch(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	cfg.Concurrency.Global = 1
	cfg.Concurrency.PerEndpoint = 1

	// The mock blocks until the context is cancelled (SLO deadline fires).
	// Concurrency = 1, so the first request holds the semaphore while blocking,
	// preventing the second request from being dispatched. When the deadline fires,
	// semaphore.Acquire returns an error and the dispatch loop exits.
	mock := &mockInferenceClient{
		generateFn: func(ctx context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			<-ctx.Done()
			return &inference.GenerateResponse{RequestID: "srv", Response: []byte(`{"ok":true}`)}, nil
		},
	}

	requests := []batch_types.Request{
		{CustomID: "r1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r3", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	// Use context.WithDeadline so context.Cause returns DeadlineExceeded (matching real code).
	ctx, sloCancel := context.WithDeadline(testLoggerCtx(t), time.Now().Add(100*time.Millisecond))
	defer sloCancel()

	type result struct {
		counts *openai.BatchRequestCounts
		err    error
	}
	resCh := make(chan result, 1)
	go func() {
		counts, err := env.p.executeJob(ctx, &jobExecutionParams{
			updater: env.updater,
			jobInfo: jobInfo,
		})
		resCh <- result{counts, err}
	}()

	select {
	case res := <-resCh:
		if !errors.Is(res.err, batchctx.ErrExpired) {
			t.Fatalf("expected batchctx.ErrExpired, got: %v", res.err)
		}
		if res.counts == nil {
			t.Fatal("expected non-nil counts")
		}
		if res.counts.Total != 3 {
			t.Errorf("Total = %d, want 3", res.counts.Total)
		}
		// r1 was dispatched and completed (mock returns success after ctx cancellation);
		// r2, r3 were never dispatched and drained as batch_expired.
		if res.counts.Completed != 1 {
			t.Errorf("Completed = %d, want 1", res.counts.Completed)
		}
		if res.counts.Failed != 2 {
			t.Errorf("Failed = %d, want 2 (undispatched drained as expired)", res.counts.Failed)
		}

		// Verify the error file contains batch_expired entries for undispatched requests.
		errorPath, _ := env.p.jobErrorFilePath(jobInfo.JobID, jobInfo.TenantID)
		errLines := readNonEmptyJSONLLines(t, errorPath)
		if len(errLines) != 2 {
			t.Fatalf("error.jsonl lines = %d, want 2", len(errLines))
		}
		for i, line := range errLines {
			var entry outputLine
			if err := json.Unmarshal(line, &entry); err != nil {
				t.Fatalf("unmarshal error line %d: %v", i, err)
			}
			if entry.Error == nil || entry.Error.Code != string(batch_types.ErrCodeBatchExpired) {
				t.Errorf("error line %d: expected code %s, got %+v", i, batch_types.ErrCodeBatchExpired, entry.Error)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executeJob did not return within 5s")
	}
}

// =====================================================================
// Tests: finalizeJob
// =====================================================================

func TestFinalizeJob_Success(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	cfg.DefaultOutputExpirationSeconds = 86400

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "finalize-job"
	tenantID := "tenant-1"
	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}

	jobDir, _ := env.p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := env.p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte(`{"id":"batch_req_1","custom_id":"r1","response":{"status_code":200}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	dbJob := seedDBJob(t, env.dbClient, jobID)
	counts := &openai.BatchRequestCounts{Total: 1, Completed: 1, Failed: 0}

	ctx := testLoggerCtx(t)
	err := env.p.finalizeJob(ctx, env.updater, dbJob, jobInfo, counts)
	if err != nil {
		t.Fatalf("finalizeJob error: %v", err)
	}

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var statusInfo openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &statusInfo); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if statusInfo.Status != openai.BatchStatusCompleted {
		t.Fatalf("status = %s, want %s", statusInfo.Status, openai.BatchStatusCompleted)
	}
	if statusInfo.OutputFileID == nil {
		t.Fatalf("expected OutputFileID to be set")
	}
}

// TestFinalizeJob_UploadFailure verifies that when all uploads fail, finalizeJob marks the
// job as failed (not completed) and returns errFinalizeFailedOver. The job must not be
// marked completed with missing artifacts — that would violate the batch contract.
func TestFinalizeJob_UploadFailure(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})
	env.p.files.storage = &failNTimesFilesClient{failCount: 100}

	jobID := "finalize-fail"
	tenantID := "tenant-1"
	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}

	jobDir, _ := env.p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := env.p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte("output\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	dbJob := seedDBJob(t, env.dbClient, jobID)
	counts := &openai.BatchRequestCounts{Total: 1, Completed: 1}

	ctx := testLoggerCtx(t)
	err := env.p.finalizeJob(ctx, env.updater, dbJob, jobInfo, counts)
	if !errors.Is(err, errFinalizeFailedOver) {
		t.Fatalf("expected errFinalizeFailedOver, got: %v", err)
	}

	items, _, _, getErr := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if getErr != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", getErr, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed (upload failure must not produce completed)", got.Status)
	}
}

// =====================================================================
// Tests: error file separation
// =====================================================================

// TestExecuteJob_SeparatesSuccessAndErrors verifies that successful responses
// are written to output.jsonl and failed responses are written to error.jsonl.
func TestExecuteJob_SeparatesSuccessAndErrors(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	var callCount atomic.Int32
	mock := &mockInferenceClient{
		generateFn: func(_ context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			if callCount.Add(1)%2 == 1 {
				return &inference.GenerateResponse{RequestID: "srv", Response: []byte(`{"ok":true}`)}, nil
			}
			return nil, &inference.ClientError{Category: httpclient.ErrCategoryServer, Message: "mock error"}
		},
	}

	requests := []batch_types.Request{
		{CustomID: "r1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	ctx := testLoggerCtx(t)
	counts, err := env.p.executeJob(ctx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if err != nil {
		t.Fatalf("executeJob error: %v", err)
	}
	if counts.Completed != 1 || counts.Failed != 1 {
		t.Fatalf("counts: completed=%d failed=%d, want completed=1 failed=1", counts.Completed, counts.Failed)
	}

	outputPath, _ := env.p.jobOutputFilePath(jobInfo.JobID, jobInfo.TenantID)
	outputLines := readNonEmptyJSONLLines(t, outputPath)
	if len(outputLines) != 1 {
		t.Fatalf("output.jsonl lines = %d, want 1", len(outputLines))
	}
	var outLine outputLine
	if err := json.Unmarshal(outputLines[0], &outLine); err != nil {
		t.Fatalf("unmarshal output line: %v", err)
	}
	if outLine.Response == nil || outLine.Error != nil {
		t.Fatalf("output line: want response set and error nil, got response=%v error=%v", outLine.Response, outLine.Error)
	}

	errorPath, _ := env.p.jobErrorFilePath(jobInfo.JobID, jobInfo.TenantID)
	errorLines := readNonEmptyJSONLLines(t, errorPath)
	if len(errorLines) != 1 {
		t.Fatalf("error.jsonl lines = %d, want 1", len(errorLines))
	}
	var errLine outputLine
	if err := json.Unmarshal(errorLines[0], &errLine); err != nil {
		t.Fatalf("unmarshal error line: %v", err)
	}
	if errLine.Error == nil || errLine.Response != nil {
		t.Fatalf("error line: want error set and response nil, got response=%v error=%v", errLine.Response, errLine.Error)
	}
}

// TestExecuteJob_HTTPErrorGoesToOutputFile verifies that HTTP error responses (4xx/5xx)
// are written to output.jsonl (not error.jsonl) with the response field populated,
// while non-HTTP errors go to error.jsonl with the error field populated.
// This matches the OpenAI batch spec: error file is for non-HTTP failures only.
func TestExecuteJob_HTTPErrorGoesToOutputFile(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	var callCount atomic.Int32
	mock := &mockInferenceClient{
		generateFn: func(_ context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			n := callCount.Add(1)
			switch n {
			case 1:
				// success
				return &inference.GenerateResponse{RequestID: "srv-1", Response: []byte(`{"ok":true}`)}, nil
			case 2:
				// HTTP 422 error — should go to output file
				return nil, &inference.ClientError{
					Category:     httpclient.ErrCategoryInvalidReq,
					Message:      "HTTP 422: Invalid model",
					StatusCode:   422,
					ResponseBody: []byte(`{"error":{"message":"Invalid model","type":"invalid_request_error","code":"model_not_found"}}`),
				}
			default:
				// non-HTTP error — should go to error file
				return nil, &inference.ClientError{
					Category: httpclient.ErrCategoryServer,
					Message:  "connection refused",
				}
			}
		},
	}

	requests := []batch_types.Request{
		{CustomID: "r1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r3", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

	ctx := testLoggerCtx(t)
	counts, err := env.p.executeJob(ctx, &jobExecutionParams{
		updater: env.updater,
		jobInfo: jobInfo,
	})
	if err != nil {
		t.Fatalf("executeJob error: %v", err)
	}
	// Only the 200 response counts as completed; HTTP 422 and non-HTTP error are both failures.
	if counts.Completed != 1 {
		t.Fatalf("Completed = %d, want 1", counts.Completed)
	}
	if counts.Failed != 2 {
		t.Fatalf("Failed = %d, want 2", counts.Failed)
	}

	// output.jsonl should contain 2 lines: the 200 success AND the HTTP 422 error.
	outputPath, _ := env.p.jobOutputFilePath(jobInfo.JobID, jobInfo.TenantID)
	outputLines := readNonEmptyJSONLLines(t, outputPath)
	if len(outputLines) != 2 {
		t.Fatalf("output.jsonl lines = %d, want 2", len(outputLines))
	}

	// Verify both output lines: one success (200) and one HTTP error (422).
	var found200, found422 bool
	for _, line := range outputLines {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal output line: %v", err)
		}
		if entry.Error != nil {
			t.Fatalf("output line should not have error field, got: %+v", entry.Error)
		}
		if entry.Response == nil {
			t.Fatalf("output line should have response field")
		}
		switch entry.Response.StatusCode {
		case 200:
			found200 = true
		case 422:
			found422 = true
			errObj, ok := entry.Response.Body["error"].(map[string]interface{})
			if !ok {
				t.Fatalf("HTTP error response body should contain error object, got: %v", entry.Response.Body)
			}
			if errObj["code"] != "model_not_found" {
				t.Fatalf("expected error code 'model_not_found', got: %v", errObj["code"])
			}
		default:
			t.Fatalf("unexpected status code %d in output file", entry.Response.StatusCode)
		}
	}
	if !found200 || !found422 {
		t.Fatalf("expected both 200 and 422 in output file, found200=%v found422=%v", found200, found422)
	}

	// error.jsonl should contain 1 line: the non-HTTP error only.
	errorPath, _ := env.p.jobErrorFilePath(jobInfo.JobID, jobInfo.TenantID)
	errorLines := readNonEmptyJSONLLines(t, errorPath)
	if len(errorLines) != 1 {
		t.Fatalf("error.jsonl lines = %d, want 1", len(errorLines))
	}
	var errEntry outputLine
	if err := json.Unmarshal(errorLines[0], &errEntry); err != nil {
		t.Fatalf("unmarshal error line: %v", err)
	}
	if errEntry.Error == nil {
		t.Fatalf("error file line should have error field")
	}
	if errEntry.Response != nil {
		t.Fatalf("error file line should not have response field")
	}
	if errEntry.Error.Code != string(httpclient.ErrCategoryServer) {
		t.Fatalf("error code = %q, want %q", errEntry.Error.Code, httpclient.ErrCategoryServer)
	}
}

// TestFinalizeJob_EmptyOutputFile_OutputFileIDOmitted verifies that when the output
// file is empty (all requests failed), output_file_id is omitted per the OpenAI spec.
func TestFinalizeJob_EmptyOutputFile_OutputFileIDOmitted(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	cfg.DefaultOutputExpirationSeconds = 86400

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "finalize-empty-output"
	tenantID := "tenant-1"
	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}

	jobDir, _ := env.p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := env.p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte{}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	errorPath, _ := env.p.jobErrorFilePath(jobID, tenantID)
	if err := os.WriteFile(errorPath, []byte(`{"id":"batch_req_1","custom_id":"r1","error":{"code":"server_error","message":"fail"}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	dbJob := seedDBJob(t, env.dbClient, jobID)
	counts := &openai.BatchRequestCounts{Total: 1, Completed: 0, Failed: 1}

	ctx := testLoggerCtx(t)
	if err := env.p.finalizeJob(ctx, env.updater, dbJob, jobInfo, counts); err != nil {
		t.Fatalf("finalizeJob error: %v", err)
	}

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var statusInfo openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &statusInfo); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if statusInfo.OutputFileID != nil {
		t.Errorf("OutputFileID = %q, want nil (output file was empty)", *statusInfo.OutputFileID)
	}
	if statusInfo.ErrorFileID == nil {
		t.Errorf("ErrorFileID should be set when error file has content")
	}
}

// TestFinalizeJob_EmptyErrorFile_ErrorFileIDOmitted verifies that when the error
// file is empty (no requests failed), error_file_id is omitted per the OpenAI spec.
func TestFinalizeJob_EmptyErrorFile_ErrorFileIDOmitted(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	cfg.DefaultOutputExpirationSeconds = 86400

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "finalize-empty-error"
	tenantID := "tenant-1"
	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}

	jobDir, _ := env.p.jobRootDir(jobID, tenantID)
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := env.p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte(`{"id":"batch_req_1","custom_id":"r1","response":{"status_code":200}}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	errorPath, _ := env.p.jobErrorFilePath(jobID, tenantID)
	if err := os.WriteFile(errorPath, []byte{}, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	dbJob := seedDBJob(t, env.dbClient, jobID)
	counts := &openai.BatchRequestCounts{Total: 1, Completed: 1, Failed: 0}

	ctx := testLoggerCtx(t)
	if err := env.p.finalizeJob(ctx, env.updater, dbJob, jobInfo, counts); err != nil {
		t.Fatalf("finalizeJob error: %v", err)
	}

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var statusInfo openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &statusInfo); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if statusInfo.OutputFileID == nil {
		t.Errorf("OutputFileID should be set when output file has content")
	}
	if statusInfo.ErrorFileID != nil {
		t.Errorf("ErrorFileID = %q, want nil (error file was empty)", *statusInfo.ErrorFileID)
	}
}

// =====================================================================
// Tests: handleJobError (routing branches)
// =====================================================================

func TestHandleJobError_errCancelled(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	dbJob := seedDBJob(t, env.dbClient, "job-cancel")
	ji := &batch_types.JobInfo{
		JobID:    "job-cancel",
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-10 * time.Second).Unix()}},
	}

	before := gatherHistogramSampleCount(t, "batch_job_e2e_latency_seconds", map[string]string{"status": "cancelled"})

	ctx := testLoggerCtx(t)
	env.p.handleJobError(ctx, &jobExecutionParams{
		updater: env.updater,
		jobItem: dbJob,
		jobInfo: ji,
	}, batchctx.ErrCancelled)

	after := gatherHistogramSampleCount(t, "batch_job_e2e_latency_seconds", map[string]string{"status": "cancelled"})
	if delta := after - before; delta != 1 {
		t.Fatalf("E2E latency cancelled: delta=%d, want 1", delta)
	}

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-cancel"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if got.Status != openai.BatchStatusCancelled {
		t.Fatalf("status = %s, want %s", got.Status, openai.BatchStatusCancelled)
	}
}

func TestHandleJobError_Shutdown_LeavesJobForReconciler(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	dbJob := seedDBJob(t, env.dbClient, "job-ctx")
	task := &db.BatchJobPriority{ID: "job-ctx"}
	ji := &batch_types.JobInfo{
		JobID:    "job-ctx",
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-10 * time.Second).Unix()}},
	}

	ctx := testLoggerCtx(t)
	env.p.handleJobError(ctx, &jobExecutionParams{
		updater: env.updater,
		jobItem: dbJob,
		task:    task,
		jobInfo: ji,
	}, batchctx.ErrShutdown)

	// Job must NOT be re-enqueued — reconciler handles recovery.
	tasks, err := env.pqClient.PQDequeue(ctx, 0, 10)
	if err != nil {
		t.Fatalf("PQDequeue: %v", err)
	}
	if len(tasks) != 0 {
		t.Fatalf("expected no re-enqueued tasks, got %d", len(tasks))
	}

	// Job status must remain in_progress (not transitioned by the processor).
	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-ctx"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if got.Status != openai.BatchStatusInProgress {
		t.Fatalf("status = %s, want %s (unchanged, left for reconciler)", got.Status, openai.BatchStatusInProgress)
	}
}

func TestHandleJobError_Shutdown_NilTask(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	dbJob := seedDBJob(t, env.dbClient, "job-ctx-nil")

	ctx := testLoggerCtx(t)
	// task is nil — should not panic, and job status should remain unchanged
	env.p.handleJobError(ctx, &jobExecutionParams{
		updater: env.updater,
		jobItem: dbJob,
	}, batchctx.ErrShutdown)

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-ctx-nil"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if got.Status != openai.BatchStatusInProgress {
		t.Fatalf("status = %s, want %s (unchanged)", got.Status, openai.BatchStatusInProgress)
	}
}

func TestHandleJobError_Default_MarksFailed(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	dbJob := seedDBJob(t, env.dbClient, "job-fail")
	ji := &batch_types.JobInfo{
		JobID:    "job-fail",
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-10 * time.Second).Unix()}},
	}

	before := gatherHistogramSampleCount(t, "batch_job_e2e_latency_seconds", map[string]string{"status": "failed"})

	ctx := testLoggerCtx(t)
	env.p.handleJobError(ctx, &jobExecutionParams{
		updater: env.updater,
		jobItem: dbJob,
		jobInfo: ji,
	}, errors.New("some error"))

	after := gatherHistogramSampleCount(t, "batch_job_e2e_latency_seconds", map[string]string{"status": "failed"})
	if delta := after - before; delta != 1 {
		t.Fatalf("E2E latency failed: delta=%d, want 1", delta)
	}

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-fail"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want %s", got.Status, openai.BatchStatusFailed)
	}
}

// TestHandleJobError_ExpiredWithCancelledCtx_StillTransitionsExpired verifies that
// handleExpired completes the DB status write even when the parent context is already
// cancelled (e.g. SIGTERM arrived concurrently with SLO expiry). handleExpired must use
// a detached context for the UpdateExpiredStatus call so that SIGTERM does not abort
// the DB write after file uploads succeed.
func TestHandleJobError_ExpiredWithCancelledCtx_StillTransitionsExpired(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "job-expired-sigterm"
	dbJob := seedDBJob(t, env.dbClient, jobID)
	ji := &batch_types.JobInfo{
		JobID:    jobID,
		TenantID: dbJob.TenantID,
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-10 * time.Second).Unix()}},
	}
	counts := &openai.BatchRequestCounts{Total: 10, Completed: 5, Failed: 5}

	// Simulate SIGTERM: parent ctx is already cancelled.
	cancelledCtx, cancel := context.WithCancel(testLoggerCtx(t))
	cancel()

	env.p.handleJobError(cancelledCtx, &jobExecutionParams{
		updater:       env.updater,
		jobItem:       dbJob,
		jobInfo:       ji,
		requestCounts: counts,
	}, batchctx.ErrExpired)

	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if got.Status != openai.BatchStatusExpired {
		t.Fatalf("status = %s, want expired (detached context must survive SIGTERM)", got.Status)
	}
	if got.RequestCounts.Total != 10 || got.RequestCounts.Completed != 5 {
		t.Fatalf("request_counts = %+v, want {10,5,5}", got.RequestCounts)
	}
}

// TestHandleJobError_ExpiredDuringIngestion_NilCountsTransitionsExpired verifies that
// handleJobError routes batchctx.ErrExpired with nil requestCounts (SLO expired during preprocessing,
// before executeJob ran) to handleExpired, which tolerates nil counts and transitions the
// job to expired status.
func TestHandleJobError_ExpiredDuringIngestion_NilCountsTransitionsExpired(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	dbJob := seedDBJob(t, env.dbClient, "job-expired-ingestion")
	ji := &batch_types.JobInfo{
		JobID:    "job-expired-ingestion",
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-10 * time.Second).Unix()}},
	}

	ctx := testLoggerCtx(t)
	env.p.handleJobError(ctx, &jobExecutionParams{
		updater:       env.updater,
		jobItem:       dbJob,
		jobInfo:       ji,
		requestCounts: nil, // nil: SLO expired before executeJob ran
	}, batchctx.ErrExpired)

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-expired-ingestion"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if got.Status != openai.BatchStatusExpired {
		t.Fatalf("status = %s, want %s", got.Status, openai.BatchStatusExpired)
	}
}

// =====================================================================
// Tests: handleCancelled / handleFailed
// with partial output
// =====================================================================

func TestHandleCancelled_Execution_UploadsPartialOutput(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "job-cancel-partial"
	tenantID := "tenant__tenantA"
	dbJob := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: tenantID, Tags: db.Tags{}},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusCancelling})},
	}
	if err := env.dbClient.DBStore(context.Background(), dbJob); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	createPartialOutputFiles(t, env.p, jobID, tenantID)

	jobInfo := &batch_types.JobInfo{
		JobID:    jobID,
		TenantID: tenantID,
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-10 * time.Second).Unix()}},
	}
	counts := &openai.BatchRequestCounts{Total: 5, Completed: 3, Failed: 2}

	before := gatherHistogramSampleCount(t, "batch_job_e2e_latency_seconds", map[string]string{"status": "cancelled"})

	ctx := testLoggerCtx(t)
	if err := env.p.handleCancelled(ctx, &jobExecutionParams{
		updater:       env.updater,
		jobItem:       dbJob,
		jobInfo:       jobInfo,
		requestCounts: counts,
	}); err != nil {
		t.Fatalf("handleCancelled: %v", err)
	}

	after := gatherHistogramSampleCount(t, "batch_job_e2e_latency_seconds", map[string]string{"status": "cancelled"})
	if delta := after - before; delta != 1 {
		t.Fatalf("E2E latency cancelled: delta=%d, want 1", delta)
	}

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusCancelled {
		t.Fatalf("status = %s, want cancelled", got.Status)
	}
	if got.RequestCounts.Total != 5 || got.RequestCounts.Completed != 3 || got.RequestCounts.Failed != 2 {
		t.Fatalf("request_counts = %+v, want {5,3,2}", got.RequestCounts)
	}
	if got.OutputFileID == nil {
		t.Fatal("expected output_file_id to be set")
	}
	if got.ErrorFileID == nil {
		t.Fatal("expected error_file_id to be set")
	}
}

// TestHandleCancelled_CancelledWriteFails_FallsBackToFailed verifies that when
// UpdateCancelledStatus fails inside handleCancelled, the fallback writes "failed"
// status with file IDs preserved. This mirrors the failover pattern in finalizeJob.
func TestHandleCancelled_CancelledWriteFails_FallsBackToFailed(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	innerDB := newMockBatchDBClient()
	failDB := &failOnStatusDB{
		inner:      innerDB,
		failStatus: openai.BatchStatusCancelled,
		failErr:    errors.New("injected: cancelled write failed"),
	}
	jobID := "job-cancel-failover"
	tenantID := "tenant__tenantA"

	dbJob := seedDBJob(t, innerDB, jobID)
	dbJob.TenantID = tenantID

	clients := &clientset.Clientset{
		BatchDB:   failDB,
		FileDB:    newMockFileDBClient(),
		File:      mockfiles.NewMockBatchFilesClient(t.TempDir()),
		Queue:     mockdb.NewMockBatchPriorityQueueClient(),
		Event:     mockdb.NewMockBatchEventChannelClient(),
		Inference: inference.NewSingleClientResolver(&mockInferenceClient{}),
	}
	p := mustNewProcessor(t, cfg, clients)
	p.poller = NewPoller(clients.Queue, failDB)
	updater := NewStatusUpdater(failDB)

	createPartialOutputFiles(t, p, jobID, tenantID)

	jobInfo := &batch_types.JobInfo{
		JobID:    jobID,
		TenantID: tenantID,
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-10 * time.Second).Unix()}},
	}
	counts := &openai.BatchRequestCounts{Total: 5, Completed: 3, Failed: 2}

	ctx := testLoggerCtx(t)
	err := p.handleCancelled(ctx, &jobExecutionParams{
		updater:       updater,
		jobItem:       dbJob,
		jobInfo:       jobInfo,
		requestCounts: counts,
	})

	if !errors.Is(err, errFinalizeFailedOver) {
		t.Fatalf("expected errFinalizeFailedOver, got: %v", err)
	}

	items, _, _, getErr := innerDB.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if getErr != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", getErr, len(items))
	}
	var got openai.BatchStatusInfo
	if unmarshalErr := json.Unmarshal(items[0].Status, &got); unmarshalErr != nil {
		t.Fatalf("unmarshal: %v", unmarshalErr)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if got.OutputFileID == nil {
		t.Fatal("output_file_id must be preserved in fallback, got nil")
	}
	if got.RequestCounts.Total != 5 || got.RequestCounts.Completed != 3 || got.RequestCounts.Failed != 2 {
		t.Fatalf("request_counts = %+v, want {5,3,2}", got.RequestCounts)
	}
}

func TestHandleFailed_Execution_UploadsPartialOutput(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "job-fail-partial"
	tenantID := "tenant__tenantA"
	dbJob := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: tenantID, Tags: db.Tags{}},
		BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
	}
	if err := env.dbClient.DBStore(context.Background(), dbJob); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	createPartialOutputFiles(t, env.p, jobID, tenantID)

	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	counts := &openai.BatchRequestCounts{Total: 10, Completed: 7, Failed: 3}

	ctx := testLoggerCtx(t)
	if err := env.p.handleFailed(ctx, env.updater, dbJob, counts, jobInfo); err != nil {
		t.Fatalf("handleFailed: %v", err)
	}

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if got.RequestCounts.Total != 10 || got.RequestCounts.Completed != 7 || got.RequestCounts.Failed != 3 {
		t.Fatalf("request_counts = %+v, want {10,7,3}", got.RequestCounts)
	}
	if got.OutputFileID == nil {
		t.Fatal("expected output_file_id to be set")
	}
	if got.ErrorFileID == nil {
		t.Fatal("expected error_file_id to be set")
	}
}

func TestHandleFailed_Finalization_RecordsCountsOnly(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "job-fail-finalization"
	dbJob := seedDBJob(t, env.dbClient, jobID)
	ji := &batch_types.JobInfo{
		JobID:    jobID,
		BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-10 * time.Second).Unix()}},
	}

	counts := &openai.BatchRequestCounts{Total: 8, Completed: 8, Failed: 0}

	before := gatherHistogramSampleCount(t, "batch_job_e2e_latency_seconds", map[string]string{"status": "failed"})

	ctx := testLoggerCtx(t)
	if err := env.p.handleFailed(ctx, env.updater, dbJob, counts, ji); err != nil {
		t.Fatalf("handleFailed: %v", err)
	}

	after := gatherHistogramSampleCount(t, "batch_job_e2e_latency_seconds", map[string]string{"status": "failed"})
	if delta := after - before; delta != 1 {
		t.Fatalf("E2E latency failed: delta=%d, want 1", delta)
	}

	items, _, _, err := env.dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed", got.Status)
	}
	if got.RequestCounts.Total != 8 || got.RequestCounts.Completed != 8 || got.RequestCounts.Failed != 0 {
		t.Fatalf("request_counts = %+v, want {8,8,0}", got.RequestCounts)
	}
	if got.OutputFileID != nil {
		t.Fatalf("expected nil output_file_id, got %s", *got.OutputFileID)
	}
	if got.ErrorFileID != nil {
		t.Fatalf("expected nil error_file_id, got %s", *got.ErrorFileID)
	}
}

// TestHandleFailed_CancelledCtx_StillWritesDB verifies that handleFailed completes
// the DB status write even when the parent context is already cancelled.
func TestHandleFailed_CancelledCtx_StillWritesDB(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "job-failed-sigterm"
	dbJob := seedDBJob(t, env.dbClient, jobID)
	counts := &openai.BatchRequestCounts{Total: 3, Completed: 1, Failed: 2}

	cancelledCtx, cancel := context.WithCancel(testLoggerCtx(t))
	cancel()

	if err := env.p.handleFailed(cancelledCtx, env.updater, dbJob, counts, nil); err != nil {
		t.Fatalf("handleFailed with cancelled ctx: %v", err)
	}

	items, _, _, err := env.dbClient.DBGet(context.Background(), &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("status = %s, want failed (detached context must survive cancelled parent)", got.Status)
	}
}

// =====================================================================
// Tests: uploadPartialResults — empty / missing files
// =====================================================================

// TestUploadPartialResults_EmptyFiles verifies that when both output and error files
// exist but are empty (0 bytes), uploadPartialResults returns empty file IDs and does
// not create any file records in the database.
func TestUploadPartialResults_EmptyFiles(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "partial-empty"
	tenantID := "tenant__tenantA"

	jobDir, err := env.p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := env.p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte{}, 0o644); err != nil {
		t.Fatalf("WriteFile output: %v", err)
	}
	errorPath, _ := env.p.jobErrorFilePath(jobID, tenantID)
	if err := os.WriteFile(errorPath, []byte{}, 0o644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}

	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	dbJob := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID, Tags: db.Tags{}},
	}

	ctx := testLoggerCtx(t)
	outputFileID, errorFileID := env.p.uploadPartialResults(ctx, jobInfo, dbJob)

	if outputFileID != "" {
		t.Fatalf("outputFileID = %q, want empty (output file was 0 bytes)", outputFileID)
	}
	if errorFileID != "" {
		t.Fatalf("errorFileID = %q, want empty (error file was 0 bytes)", errorFileID)
	}
}

// TestUploadPartialResults_MissingFiles verifies that when neither output nor error
// files exist on disk, uploadPartialResults returns empty file IDs without error.
func TestUploadPartialResults_MissingFiles(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	env := newTestProcessorEnv(t, cfg, &mockInferenceClient{})

	jobID := "partial-missing"
	tenantID := "tenant__tenantA"

	jobDir, err := env.p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	dbJob := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID, TenantID: tenantID, Tags: db.Tags{}},
	}

	ctx := testLoggerCtx(t)
	outputFileID, errorFileID := env.p.uploadPartialResults(ctx, jobInfo, dbJob)

	if outputFileID != "" {
		t.Fatalf("outputFileID = %q, want empty (output file does not exist)", outputFileID)
	}
	if errorFileID != "" {
		t.Fatalf("errorFileID = %q, want empty (error file does not exist)", errorFileID)
	}
}

// TestUploadPartialResults_OneUploadFails_OtherSurvives verifies that when one of the two
// parallel uploads fails, the other file ID is still returned. uploadPartialResults is
// best-effort: one-side failure must not lose the other side's reference.
func TestUploadPartialResults_OneUploadFails_OtherSurvives(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	dbClient := newMockBatchDBClient()
	filesClient := &failOnNthCallClient{
		failN:   1,
		failErr: errors.New("injected: one-side upload failure"),
	}

	clients := &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    newMockFileDBClient(),
		File:      filesClient,
		Queue:     mockdb.NewMockBatchPriorityQueueClient(),
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}
	p := mustNewProcessor(t, cfg, clients)
	p.poller = NewPoller(clients.Queue, dbClient)

	jobID := "partial-one-side"
	tenantID := "tenant__tenantA"

	jobDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	if err := os.MkdirAll(jobDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	outputPath, _ := p.jobOutputFilePath(jobID, tenantID)
	if err := os.WriteFile(outputPath, []byte(`{"custom_id":"r1"}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile output: %v", err)
	}
	errorPath, _ := p.jobErrorFilePath(jobID, tenantID)
	if err := os.WriteFile(errorPath, []byte(`{"custom_id":"e1"}`+"\n"), 0o644); err != nil {
		t.Fatalf("WriteFile error: %v", err)
	}

	jobInfo := &batch_types.JobInfo{JobID: jobID, TenantID: tenantID}
	dbJob := seedDBJob(t, dbClient, jobID)

	ctx := testLoggerCtx(t)
	outputFileID, errorFileID := p.uploadPartialResults(ctx, jobInfo, dbJob)

	// Exactly one of the two uploads failed (the first call to Store).
	// The other must have succeeded and returned a non-empty file ID.
	nonEmpty := 0
	if outputFileID != "" {
		nonEmpty++
	}
	if errorFileID != "" {
		nonEmpty++
	}
	if nonEmpty != 1 {
		t.Fatalf("expected exactly 1 surviving file ID (one upload failed), got output=%q error=%q",
			outputFileID, errorFileID)
	}
}

// =====================================================================
// Tests: cleanupJobArtifacts
// =====================================================================

func TestCleanupJobArtifacts_RemovesDirectory(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	jobDir, _ := p.jobRootDir("cleanup-job", "tenant-1")
	if err := os.MkdirAll(filepath.Join(jobDir, "plans"), 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(jobDir, "input.jsonl"), []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	ctx := testLoggerCtx(t)
	p.cleanupJobArtifacts(ctx, "cleanup-job", "tenant-1")

	if _, err := os.Stat(jobDir); !os.IsNotExist(err) {
		t.Fatalf("expected job directory to be removed, stat err: %v", err)
	}
}

// =====================================================================
// Tests: storeFileRecord error path
// =====================================================================

func TestStoreOutputFileRecord_DBError(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400

	failDB := &dbStoreErrFileClient{err: errors.New("db write failed")}
	p := mustNewProcessor(t, cfg, &clientset.Clientset{FileDB: failDB})

	ctx := testLoggerCtx(t)
	err := p.storeFileRecord(ctx, "file_x", "output.jsonl", "tenant-1", 100, db.Tags{})
	if err == nil {
		t.Fatalf("expected error from DB failure")
	}
}

func TestRecordE2ELatency(t *testing.T) {
	t.Run("nil jobInfo", func(t *testing.T) {
		recordE2ELatency(nil, metrics.E2EStatusCompleted)
	})

	t.Run("nil BatchJob", func(t *testing.T) {
		ji := &batch_types.JobInfo{JobID: "j1"}
		recordE2ELatency(ji, metrics.E2EStatusCompleted)
	})

	t.Run("zero CreatedAt", func(t *testing.T) {
		ji := &batch_types.JobInfo{
			JobID:    "j1",
			BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: 0}},
		}
		recordE2ELatency(ji, metrics.E2EStatusCompleted)
	})

	t.Run("valid CreatedAt", func(t *testing.T) {
		ji := &batch_types.JobInfo{
			JobID:    "j1",
			BatchJob: &openai.Batch{BatchSpec: openai.BatchSpec{CreatedAt: time.Now().Add(-30 * time.Second).Unix()}},
		}
		recordE2ELatency(ji, metrics.E2EStatusCompleted)
	})
}

// =====================================================================
// Tests: mergeInferenceHeaders
// =====================================================================

// =====================================================================

// =====================================================================
// Test helpers: metric gathering
// =====================================================================

func findMetric(t *testing.T, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
	outer:
		for _, m := range mf.Metric {
			for k, v := range labels {
				var got string
				for _, lp := range m.Label {
					if lp.GetName() == k {
						got = lp.GetValue()
						break
					}
				}
				if got != v {
					continue outer
				}
			}
			return m
		}
	}
	return nil
}

func gatherHistogramSampleCount(t *testing.T, name string, labels map[string]string) uint64 {
	t.Helper()
	m := findMetric(t, name, labels)
	if m == nil {
		return 0
	}
	return m.GetHistogram().GetSampleCount()
}
