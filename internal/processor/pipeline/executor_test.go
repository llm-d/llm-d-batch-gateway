package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

type mockInferenceClient struct {
	response []byte
}

func (m *mockInferenceClient) Generate(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return &inference.GenerateResponse{
		RequestID: req.RequestID,
		Response:  m.response,
	}, nil
}

type sliceSource struct {
	items []RequestItem
}

func (s *sliceSource) Produce(_ context.Context, out chan<- RequestItem) error {
	defer close(out)
	for _, item := range s.items {
		out <- item
	}
	return nil
}

func TestJobExecutorEndToEnd(t *testing.T) {
	body := map[string]any{"choices": []any{}, "usage": map[string]any{"prompt_tokens": 10.0, "completion_tokens": 5.0}}
	respBytes, _ := json.Marshal(body)

	client := &mockInferenceClient{response: respBytes}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "c-1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-2", CustomID: "c-2", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-3", CustomID: "c-3", ModelID: "m1", Endpoint: "/v1/chat/completions"},
	}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if counts.Total != 3 {
		t.Errorf("Total = %d, want 3", counts.Total)
	}
	if counts.Completed != 3 {
		t.Errorf("Completed = %d, want 3", counts.Completed)
	}
	if counts.Failed != 0 {
		t.Errorf("Failed = %d, want 0", counts.Failed)
	}

	outputData := sinkOutputBytes(t, sink)
	lines := bytes.Split(bytes.TrimSpace(outputData), []byte("\n"))
	if len(lines) != 3 {
		t.Fatalf("output lines = %d, want 3", len(lines))
	}

	for i, line := range lines {
		var out outputLine
		if err := json.Unmarshal(line, &out); err != nil {
			t.Fatalf("line %d: unmarshal error: %v", i, err)
		}
		if out.Response == nil {
			t.Errorf("line %d: response is nil", i)
		} else if out.Response.StatusCode != 200 {
			t.Errorf("line %d: status = %d, want 200", i, out.Response.StatusCode)
		}
	}

	errorData := sinkErrorBytes(t, sink)
	if len(bytes.TrimSpace(errorData)) != 0 {
		t.Errorf("error output = %q, want empty", errorData)
	}
}

func TestJobExecutorWithErrors(t *testing.T) {
	resolver := inference.NewPerModelClientResolver(map[string]inference.InferenceClient{
		"m1": &mockInferenceClient{response: []byte(`{}`)},
	})
	defer func() { _ = resolver.Close() }()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "c-1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-bad", CustomID: "c-bad", ModelID: "no-such-model", Endpoint: "/v1/chat/completions"},
	}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if counts.Completed+counts.Failed != int64(len(items)) {
		t.Errorf("Completed(%d) + Failed(%d) != Total(%d)", counts.Completed, counts.Failed, counts.Total)
	}

	errorData := sinkErrorBytes(t, sink)
	if len(bytes.TrimSpace(errorData)) == 0 {
		t.Error("expected error output for unknown model")
	}

	var errLine outputLine
	if err := json.Unmarshal(bytes.TrimSpace(errorData), &errLine); err != nil {
		t.Fatalf("unmarshal error line: %v", err)
	}
	if errLine.Error == nil {
		t.Fatal("expected error field in error output")
	}
	if errLine.Error.Code != inference.ErrCodeModelNotFound {
		t.Errorf("error code = %q, want %q", errLine.Error.Code, inference.ErrCodeModelNotFound)
	}
}

func TestJobExecutorCancellation(t *testing.T) {
	resolver := inference.NewSingleClientResolver(&mockInferenceClient{response: []byte(`{}`)})
	defer func() { _ = resolver.Close() }()

	ctx, cancel := context.WithCancel(context.Background())

	source := &cancellingSource{
		items:    []RequestItem{{RequestID: "req-1", CustomID: "c-1", ModelID: "m1"}},
		cancelFn: cancel,
	}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(10, nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	if err != nil && err != context.Canceled {
		t.Fatalf("Execute() error: %v", err)
	}
}

// cancellingSource sends one item then cancels the context.
type cancellingSource struct {
	items    []RequestItem
	cancelFn context.CancelFunc
}

func (s *cancellingSource) Produce(_ context.Context, out chan<- RequestItem) error {
	defer close(out)
	for _, item := range s.items {
		out <- item
	}
	s.cancelFn()
	return nil
}

func TestJobExecutorMultipleModels(t *testing.T) {
	body := map[string]any{"ok": true}
	respBytes, _ := json.Marshal(body)

	resolver := inference.NewPerModelClientResolver(map[string]inference.InferenceClient{
		"m1": &mockInferenceClient{response: respBytes},
		"m2": &mockInferenceClient{response: respBytes},
	})
	defer func() { _ = resolver.Close() }()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "a", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-2", CustomID: "b", ModelID: "m2", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-3", CustomID: "c", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-4", CustomID: "d", ModelID: "m2", Endpoint: "/v1/chat/completions"},
	}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if counts.Total != 4 {
		t.Fatalf("Total = %d, want 4", counts.Total)
	}
	if counts.Completed != 4 {
		t.Fatalf("Completed = %d, want 4", counts.Completed)
	}

	outputData := sinkOutputBytes(t, sink)
	lines := bytes.Split(bytes.TrimSpace(outputData), []byte("\n"))
	if len(lines) != 4 {
		t.Fatalf("output lines = %d, want 4", len(lines))
	}

	seenIDs := make(map[string]int)
	for _, line := range lines {
		var out outputLine
		if err := json.Unmarshal(line, &out); err != nil {
			t.Fatalf("unmarshal error: %v", err)
		}
		seenIDs[out.CustomID]++
	}
	for _, wantID := range []string{"a", "b", "c", "d"} {
		if seenIDs[wantID] != 1 {
			t.Errorf("custom_id %q appeared %d times, want 1", wantID, seenIDs[wantID])
		}
	}

	errorData := sinkErrorBytes(t, sink)
	if len(bytes.TrimSpace(errorData)) != 0 {
		t.Errorf("expected empty error file, got %q", errorData)
	}
}

func TestJobExecutorMultipleModels_ModelNotFound(t *testing.T) {
	resolver := inference.NewPerModelClientResolver(map[string]inference.InferenceClient{
		"m1": &mockInferenceClient{response: []byte(`{"ok":true}`)},
	})
	defer func() { _ = resolver.Close() }()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "a", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-2", CustomID: "b", ModelID: "no-such-model", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-3", CustomID: "c", ModelID: "m1", Endpoint: "/v1/chat/completions"},
	}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if counts.Total != 3 {
		t.Fatalf("Total = %d, want 3", counts.Total)
	}
	if counts.Completed != 2 {
		t.Fatalf("Completed = %d, want 2 (model-not-found for one shouldn't affect others)", counts.Completed)
	}
	if counts.Failed != 1 {
		t.Fatalf("Failed = %d, want 1", counts.Failed)
	}

	outputData := sinkOutputBytes(t, sink)
	outputLines := splitLines(outputData)
	if len(outputLines) != 2 {
		t.Fatalf("output lines = %d, want 2", len(outputLines))
	}

	errorData := sinkErrorBytes(t, sink)
	errorLines := splitLines(errorData)
	if len(errorLines) != 1 {
		t.Fatalf("error lines = %d, want 1", len(errorLines))
	}

	var errLine outputLine
	if err := json.Unmarshal(errorLines[0], &errLine); err != nil {
		t.Fatalf("unmarshal error line: %v", err)
	}
	if errLine.Error == nil || errLine.Error.Code != inference.ErrCodeModelNotFound {
		t.Fatalf("expected model_not_found error, got %+v", errLine.Error)
	}
}

func TestJobExecutorSeparatesSuccessAndErrors(t *testing.T) {
	var callCount atomic.Int32
	client := &mockInferenceClientForE2E{
		generateFn: func(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			n := callCount.Add(1)
			if n%2 == 1 {
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
			return nil, &inference.ClientError{Category: httpclient.ErrCategoryServer, Message: "mock error"}
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "r1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-2", CustomID: "r2", ModelID: "m1", Endpoint: "/v1/chat/completions"},
	}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if counts.Completed != 1 || counts.Failed != 1 {
		t.Fatalf("counts: completed=%d failed=%d, want completed=1 failed=1", counts.Completed, counts.Failed)
	}

	outputData := sinkOutputBytes(t, sink)
	outputLines := splitLines(outputData)
	if len(outputLines) != 1 {
		t.Fatalf("output lines = %d, want 1", len(outputLines))
	}
	var outLine outputLine
	if err := json.Unmarshal(outputLines[0], &outLine); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if outLine.Response == nil || outLine.Error != nil {
		t.Fatalf("output: want response set and error nil, got response=%v error=%v", outLine.Response, outLine.Error)
	}

	errorData := sinkErrorBytes(t, sink)
	errorLines := splitLines(errorData)
	if len(errorLines) != 1 {
		t.Fatalf("error lines = %d, want 1", len(errorLines))
	}
	var errLine outputLine
	if err := json.Unmarshal(errorLines[0], &errLine); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if errLine.Error == nil || errLine.Response != nil {
		t.Fatalf("error: want error set and response nil, got response=%v error=%v", errLine.Response, errLine.Error)
	}
}

func TestJobExecutorHTTPErrorGoesToOutputFile(t *testing.T) {
	var callCount atomic.Int32
	client := &mockInferenceClientForE2E{
		generateFn: func(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			n := callCount.Add(1)
			switch n {
			case 1:
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			case 2:
				return nil, &inference.ClientError{
					Category:     httpclient.ErrCategoryInvalidReq,
					Message:      "HTTP 422: Invalid model",
					StatusCode:   422,
					ResponseBody: []byte(`{"error":{"message":"Invalid model","type":"invalid_request_error","code":"model_not_found"}}`),
				}
			default:
				return nil, &inference.ClientError{
					Category: httpclient.ErrCategoryServer,
					Message:  "connection refused",
				}
			}
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "r1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-2", CustomID: "r2", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-3", CustomID: "r3", ModelID: "m1", Endpoint: "/v1/chat/completions"},
	}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}
	if counts.Completed != 1 {
		t.Fatalf("Completed = %d, want 1", counts.Completed)
	}
	if counts.Failed != 2 {
		t.Fatalf("Failed = %d, want 2", counts.Failed)
	}

	// output.jsonl: 200 success + HTTP 422 (HTTP errors go to output per OpenAI spec)
	outputData := sinkOutputBytes(t, sink)
	outputLines := splitLines(outputData)
	if len(outputLines) != 2 {
		t.Fatalf("output lines = %d, want 2 (200 + 422)", len(outputLines))
	}
	var found200, found422 bool
	for _, line := range outputLines {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal output: %v", err)
		}
		if entry.Error != nil {
			t.Fatalf("output line should not have error field, got %+v", entry.Error)
		}
		if entry.Response == nil {
			t.Fatal("output line should have response field")
		}
		switch entry.Response.StatusCode {
		case 200:
			found200 = true
		case 422:
			found422 = true
		}
	}
	if !found200 || !found422 {
		t.Fatalf("expected 200 and 422 in output, found200=%v found422=%v", found200, found422)
	}

	// error.jsonl: only non-HTTP error (connection refused)
	errorData := sinkErrorBytes(t, sink)
	errorLines := splitLines(errorData)
	if len(errorLines) != 1 {
		t.Fatalf("error lines = %d, want 1 (non-HTTP error only)", len(errorLines))
	}
	var errEntry outputLine
	if err := json.Unmarshal(errorLines[0], &errEntry); err != nil {
		t.Fatalf("unmarshal error: %v", err)
	}
	if errEntry.Error == nil {
		t.Fatal("error file line should have error field")
	}
}

type mockInferenceClientForE2E struct {
	generateFn func(context.Context, *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError)
}

func (m *mockInferenceClientForE2E) Generate(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return m.generateFn(ctx, req)
}

// TestJobExecutorCancellation_AllRequestsAccountedFor verifies that when the
// context is cancelled mid-execution, completed + failed == total. Every
// request must appear in either the output or error file.
func TestJobExecutorCancellation_AllRequestsAccountedFor(t *testing.T) {
	const total = 10
	body, _ := json.Marshal(map[string]any{"ok": true})
	client := &mockInferenceClient{response: body}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := makeItems(total, "m1")

	// Source sends one item then cancels ctx, but keeps producing
	// remaining items (matching fixed PlanFileSource behavior).
	ctx, cancel := context.WithCancel(context.Background())
	source := &cancelAfterNSource{items: items, cancelAt: 1, cancelFn: cancel}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(total), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(ctx)
	if err != nil && err != context.Canceled {
		t.Fatalf("Execute() error: %v", err)
	}

	counts := tracker.Counts()
	accounted := counts.Completed + counts.Failed
	if accounted != int64(total) {
		t.Fatalf("Completed(%d) + Failed(%d) = %d, want %d: %d requests were silently dropped",
			counts.Completed, counts.Failed, accounted, total, int64(total)-accounted)
	}
}

// TestJobExecutorCancelAfterComplete verifies that when a request completes
// successfully and the context is then cancelled, the request stays as
// Completed (not retroactively failed as batch_cancelled).
func TestJobExecutorCancelAfterComplete(t *testing.T) {
	body, _ := json.Marshal(map[string]any{"ok": true})
	resolver := inference.NewSingleClientResolver(&mockInferenceClient{response: body})
	defer func() { _ = resolver.Close() }()

	items := makeItems(3, "m1")

	ctx, cancel := context.WithCancel(context.Background())
	source := &cancelAfterNSource{items: items, cancelAt: 2, cancelFn: cancel}

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, _ = executor.Execute(ctx)

	counts := tracker.Counts()
	if counts.Completed < 2 {
		t.Fatalf("Completed = %d, want >= 2 (requests that finished before cancel must not be retroactively failed)",
			counts.Completed)
	}

	outputData := sinkOutputBytes(t, sink)
	outputLines := splitLines(outputData)
	for _, line := range outputLines {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if entry.Response == nil || entry.Response.StatusCode != 200 {
			t.Errorf("completed request should have 200 response, got %+v", entry.Response)
		}
		if entry.Error != nil {
			t.Errorf("completed request should not have error, got %+v", entry.Error)
		}
	}
}

func TestCancelCode(t *testing.T) {
	t.Run("SLO deadline", func(t *testing.T) {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		<-ctx.Done()

		code, message := cancelCode(ctx)
		want := batch_types.ErrCodeBatchExpired
		if code != string(want) || message != want.Message() {
			t.Fatalf("cancelCode() = (%q, %q), want (%q, %q)", code, message, want, want.Message())
		}
	})

	tests := []struct {
		name  string
		cause error
		want  batch_types.BatchErrorCode
	}{
		{name: "user cancellation", cause: batchctx.ErrCancelled, want: batch_types.ErrCodeBatchCancelled},
		{name: "batch expiry", cause: batchctx.ErrExpired, want: batch_types.ErrCodeBatchExpired},
		{name: "shutdown", cause: batchctx.ErrShutdown, want: batch_types.ErrCodeBatchFailed},
		{name: "neutral cancellation", cause: context.Canceled, want: batch_types.ErrCodeBatchFailed},
		{name: "source failure", cause: errors.New("source read failed"), want: batch_types.ErrCodeBatchFailed},
		{name: "no cancellation", want: batch_types.ErrCodeBatchFailed},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(context.Canceled)
			if tt.cause != nil {
				cancel(tt.cause)
			}

			code, message := cancelCode(ctx)
			if code != string(tt.want) || message != tt.want.Message() {
				t.Fatalf("cancelCode() = (%q, %q), want (%q, %q)", code, message, tt.want, tt.want.Message())
			}
		})
	}
}

// TestDirectDispatcher_MetricsNotDoubleCounted verifies that RecordRequestError
// and RecordTokenUsage are each called exactly once per request, not twice
// (once in buildResult/handleSuccess and again in the collector).
func TestDirectDispatcher_MetricsNotDoubleCounted(t *testing.T) {
	respBody, _ := json.Marshal(map[string]any{
		"ok": true,
		"usage": map[string]any{
			"prompt_tokens":     float64(10),
			"completion_tokens": float64(5),
		},
	})

	var callCount atomic.Int32
	client := &mockInferenceClientForE2E{
		generateFn: func(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			n := callCount.Add(1)
			if n == 1 {
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: respBody}, nil
			}
			return nil, &inference.ClientError{Category: httpclient.ErrCategoryServer, Message: "fail"}
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := []RequestItem{
		{RequestID: "req-ok", CustomID: "ok", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-err", CustomID: "err", ModelID: "m1", Endpoint: "/v1/chat/completions"},
	}

	errorsBefore := getCounterValue(t, "request_errors_by_model_total", "m1")
	promptBefore := getCounterValue(t, "batch_request_prompt_tokens_total", "m1")
	genBefore := getCounterValue(t, "batch_request_generation_tokens_total", "m1")

	sink := &fakeSink{}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: NewDirectDispatcher(resolver, logr.Discard()),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	if _, err := executor.Execute(context.Background()); err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	errorsAfter := getCounterValue(t, "request_errors_by_model_total", "m1")
	promptAfter := getCounterValue(t, "batch_request_prompt_tokens_total", "m1")
	genAfter := getCounterValue(t, "batch_request_generation_tokens_total", "m1")

	errorsDelta := errorsAfter - errorsBefore
	if errorsDelta != 1 {
		t.Errorf("request_errors delta = %.0f, want 1 (must not double-count)", errorsDelta)
	}

	promptDelta := promptAfter - promptBefore
	if promptDelta != 10 {
		t.Errorf("prompt_tokens delta = %.0f, want 10 (must not double-count)", promptDelta)
	}

	genDelta := genAfter - genBefore
	if genDelta != 5 {
		t.Errorf("generation_tokens delta = %.0f, want 5 (must not double-count)", genDelta)
	}
}

func getCounterValue(t *testing.T, name, model string) float64 {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.Metric {
			for _, lp := range m.Label {
				if lp.GetName() == "model" && lp.GetValue() == model {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// TestJobExecutor_PersistenceFailureCancelsDispatch verifies that when the
// collector encounters a write failure, dispatch is cancelled early: the slow
// source should NOT deliver all requests because dispatchCtx is cancelled
// after the first persistence error.
func TestJobExecutor_PersistenceFailureCancelsDispatch(t *testing.T) {
	const total = 20
	var dispatched atomic.Int32
	client := &mockInferenceClientForE2E{
		generateFn: func(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			dispatched.Add(1)
			body, _ := json.Marshal(map[string]any{"ok": true})
			return &inference.GenerateResponse{RequestID: req.RequestID, Response: body}, nil
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := makeItems(total, "m1")

	sink := &fakeSink{}
	var checkpointed atomic.Int32
	sink.fn = func(_ *ResultItem) (bool, error) {
		if checkpointed.Add(1) > 1 {
			return false, errors.New("write failed")
		}
		return true, nil
	}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(total), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	source := &throttledSource{items: items, delay: 10 * time.Millisecond}

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: NewPreDispatcher(NewDirectDispatcher(resolver, logr.Discard())),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(context.Background())
	if err == nil {
		t.Fatal("expected persistence error from Execute")
	}

	d := dispatched.Load()
	if d >= int32(total) {
		t.Fatalf("dispatched %d requests (all %d), want fewer: persistence failure should cancel dispatch early", d, total)
	}
	t.Logf("dispatched %d/%d requests before cancellation", d, total)
}

// throttledSource emits items with a configurable delay between sends,
// respecting context cancellation.
type throttledSource struct {
	items []RequestItem
	delay time.Duration
}

func (s *throttledSource) Produce(ctx context.Context, out chan<- RequestItem) error {
	defer close(out)
	for _, item := range s.items {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- item:
		}
		if s.delay > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.delay):
			}
		}
	}
	return nil
}

// TestJobExecutor_PersistenceFailureReturnedNotCanceled verifies that Execute()
// returns the collector's write error, not context.Canceled from the source.
// When onPersistenceFailure cancels dispatchCtx, the source exits early with
// context.Canceled. Without the fix, errgroup's sync.Once records that as the
// first error, masking the persistence failure.
func TestJobExecutor_PersistenceFailureReturnedNotCanceled(t *testing.T) {
	const total = 10
	client := &mockInferenceClientForE2E{
		generateFn: func(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			body, _ := json.Marshal(map[string]any{"ok": true})
			return &inference.GenerateResponse{RequestID: req.RequestID, Response: body}, nil
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := makeItems(total, "m1")

	sink := &fakeSink{}
	var checkpointed atomic.Int32
	sink.fn = func(_ *ResultItem) (bool, error) {
		if checkpointed.Add(1) > 1 {
			return false, errors.New("write failed")
		}
		return true, nil
	}
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(int64(total), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	source := &throttledSource{items: items, delay: 10 * time.Millisecond}

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: NewPreDispatcher(NewDirectDispatcher(resolver, logr.Discard())),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(context.Background())
	if err == nil {
		t.Fatal("expected error from Execute")
	}
	if err == context.Canceled {
		t.Fatalf("Execute() returned context.Canceled; want the persistence write error")
	}
	if !strings.Contains(err.Error(), "write") {
		t.Errorf("Execute() error = %q; want an error containing 'write' (the persistence failure)", err)
	}
	t.Logf("Execute() correctly returned persistence error: %v", err)
}

var _ inference.InferenceClient = (*mockInferenceClient)(nil)
var _ inference.InferenceClient = (*mockInferenceClientForE2E)(nil)
var _ RequestSource = (*sliceSource)(nil)
