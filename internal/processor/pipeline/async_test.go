package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

type fakeAsyncClient struct {
	mu           sync.Mutex
	submitted    []*inference.GenerateRequest
	submittedCh  chan struct{}
	results      chan *inference.GenerateResponse
	cancelledIDs []string
}

var _ inference.AsyncInferenceClient = (*fakeAsyncClient)(nil)

func newFakeAsyncClient() *fakeAsyncClient {
	return &fakeAsyncClient{
		results: make(chan *inference.GenerateResponse, 64),
	}
}

func (c *fakeAsyncClient) Submit(ctx context.Context, req *inference.GenerateRequest) *inference.ClientError {
	c.mu.Lock()
	c.submitted = append(c.submitted, req)
	c.mu.Unlock()
	if c.submittedCh != nil {
		select {
		case c.submittedCh <- struct{}{}:
		case <-ctx.Done():
		}
	}
	return nil
}

func (c *fakeAsyncClient) GetResult(ctx context.Context) (*inference.GenerateResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-c.results:
		return r, nil
	}
}

func (c *fakeAsyncClient) Cancel(_ context.Context, ids []string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancelledIDs = append(c.cancelledIDs, ids...)
	return nil
}
func (c *fakeAsyncClient) Close() error { return nil }

func (c *fakeAsyncClient) deliver(requestID string, body map[string]any) {
	resp, _ := json.Marshal(body)
	c.results <- &inference.GenerateResponse{
		RequestID:  requestID,
		Response:   resp,
		StatusCode: 200,
	}
}

func TestAsyncEndToEnd(t *testing.T) {
	// Shared client — same instance for submit (via ClientFor) and
	// collect (via SharedClientFor → broadcaster)
	client := newFakeAsyncClient()
	client.submittedCh = make(chan struct{})
	resolver := inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{
		"m1": func() inference.AsyncInferenceClient { return client },
	})
	defer func() { _ = resolver.Close() }()

	// Broadcaster backed by the shared client
	broadcaster := NewResultBroadcaster(client, logr.Discard())
	broadcasters := NewBroadcasterGroup([]*ResultBroadcaster{broadcaster})
	broadcasterCtx, broadcasterCancel := context.WithCancel(context.Background())
	broadcasterDone := make(chan struct{})
	defer func() {
		broadcasterCancel()
		<-broadcasterDone
	}()
	go func() {
		defer close(broadcasterDone)
		broadcaster.Run(broadcasterCtx)
	}()

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "c-1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-2", CustomID: "c-2", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-3", CustomID: "c-3", ModelID: "m1", Endpoint: "/v1/chat/completions"},
	}

	pending := NewPendingRequests(0)
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	dispatcher := NewAsyncDispatcher(resolver,
		broadcasters,
		pending, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	// Do not make any result available until every request has been submitted.
	// This verifies queue submission is independent from inference completion.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	deliveryDone := make(chan struct{})
	defer func() {
		cancel()
		<-deliveryDone
	}()
	go func() {
		defer close(deliveryDone)
		for range items {
			select {
			case <-client.submittedCh:
			case <-ctx.Done():
				return
			}
		}
		for i := len(items) - 1; i >= 0; i-- {
			client.deliver(items[i].RequestID, map[string]any{"ok": true})
		}
	}()

	counts, err := executor.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if counts.Completed != 3 {
		t.Errorf("Completed = %d, want 3", counts.Completed)
	}
	if counts.Failed != 0 {
		t.Errorf("Failed = %d, want 0", counts.Failed)
	}
	client.mu.Lock()
	submitted := len(client.submitted)
	client.mu.Unlock()
	if submitted != len(items) {
		t.Errorf("submitted requests = %d, want %d before results", submitted, len(items))
	}

	outputData := readFile(t, outputFile)
	lines := countLines(outputData)
	if lines != 3 {
		t.Errorf("output lines = %d, want 3", lines)
	}
}

func TestAsyncResult(t *testing.T) {
	tests := []struct {
		name           string
		resp           *inference.GenerateResponse
		wantStatusCode int
		wantErrCode    string
		wantResponse   bool
	}{
		{
			name: "nil body treated as empty 2xx",
			resp: &inference.GenerateResponse{
				RequestID: "req-nil-body",
				Response:  nil,
			},
			wantErrCode: "server_error",
		},
		{
			name: "bad JSON on 2xx is parse_error",
			resp: &inference.GenerateResponse{
				RequestID:  "req-bad-json",
				Response:   []byte(`{not valid json`),
				StatusCode: 200,
			},
			wantErrCode: "parse_error",
		},
		{
			name: "legacy success without StatusCode defaults to 200",
			resp: &inference.GenerateResponse{
				RequestID: "req-ok",
				Response:  []byte(`{"choices":[]}`),
			},
			wantStatusCode: 200,
			wantResponse:   true,
		},
		{
			name: "success with StatusCode 200",
			resp: &inference.GenerateResponse{
				RequestID:  "req-ok-200",
				Response:   []byte(`{"choices":[]}`),
				StatusCode: 200,
			},
			wantStatusCode: 200,
			wantResponse:   true,
		},
		{
			name: "HTTP 403 with empty body preserves status",
			resp: &inference.GenerateResponse{
				RequestID:  "req-403",
				Response:   []byte{},
				StatusCode: 403,
			},
			wantStatusCode: 403,
			wantResponse:   true,
		},
		{
			name: "HTTP 403 with JSON error body preserves status",
			resp: &inference.GenerateResponse{
				RequestID:  "req-403-json",
				Response:   []byte(`{"error":{"message":"unauthorized"}}`),
				StatusCode: 403,
			},
			wantStatusCode: 403,
			wantResponse:   true,
		},
		{
			name: "HTTP 422 with unparsable body preserves status",
			resp: &inference.GenerateResponse{
				RequestID:  "req-422",
				Response:   []byte(`not-json`),
				StatusCode: 422,
			},
			wantStatusCode: 422,
			wantResponse:   true,
		},
		{
			name: "non-HTTP failure uses ErrorCode",
			resp: &inference.GenerateResponse{
				RequestID:    "req-deadline",
				StatusCode:   0,
				ErrorCode:    "DEADLINE_EXCEEDED",
				ErrorMessage: "deadline exceeded",
			},
			wantErrCode: "DEADLINE_EXCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := asyncResult(tt.resp, logr.Discard())
			if tt.wantErrCode != "" {
				if result.Error == nil {
					t.Fatalf("expected error code %q, got nil error", tt.wantErrCode)
				}
				if result.Error.Code != tt.wantErrCode {
					t.Fatalf("error code = %q, want %q", result.Error.Code, tt.wantErrCode)
				}
				if result.Response != nil {
					t.Fatalf("expected nil response, got %+v", result.Response)
				}
				return
			}
			if result.Error != nil {
				t.Fatalf("unexpected error: %+v", result.Error)
			}
			if !tt.wantResponse {
				return
			}
			if result.Response == nil {
				t.Fatal("expected response")
			}
			if result.Response.StatusCode != tt.wantStatusCode {
				t.Fatalf("StatusCode = %d, want %d", result.Response.StatusCode, tt.wantStatusCode)
			}
			if result.Response.RequestID != tt.resp.RequestID {
				t.Fatalf("RequestID = %q, want %q", result.Response.RequestID, tt.resp.RequestID)
			}
		})
	}
}

func TestSafeChannelSend(t *testing.T) {
	b := NewResultBroadcaster(nil, logr.Discard())
	result := ResultItem{RequestID: "req-1"}

	t.Run("sends to open channel", func(t *testing.T) {
		ch := make(chan ResultItem, 1)
		b.safeChannelSend(result, ch)
		got := <-ch
		if got.RequestID != "req-1" {
			t.Fatalf("RequestID = %q, want %q", got.RequestID, "req-1")
		}
	})

	t.Run("recovers send on closed channel", func(t *testing.T) {
		ch := make(chan ResultItem)
		close(ch)
		b.safeChannelSend(result, ch)
	})
}

func TestBroadcaster_RetriesTransientError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var callCount atomic.Int32
		transientErr := fmt.Errorf("connection reset")

		client := &fakeAsyncClientWithErrors{
			getResult: func(ctx context.Context) (*inference.GenerateResponse, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				n := callCount.Add(1)
				if n <= 3 {
					return nil, transientErr
				}
				resp, _ := json.Marshal(map[string]any{"ok": true})
				return &inference.GenerateResponse{
					RequestID: "req-1",
					Response:  resp,
				}, nil
			},
		}

		broadcaster := NewResultBroadcaster(client, logr.Discard())
		resultCh := make(chan ResultItem, 10)
		broadcaster.Subscribe(resultCh)

		ctx, cancel := context.WithCancel(t.Context())
		go broadcaster.Run(ctx)

		// Advance past backoff sleeps: 100ms + 200ms + 400ms = 700ms
		time.Sleep(time.Second)
		synctest.Wait()

		select {
		case result := <-resultCh:
			if result.Error != nil {
				t.Fatalf("expected successful result after transient errors, got error: %+v", result.Error)
			}
			if result.RequestID != "req-1" {
				t.Fatalf("RequestID = %q, want %q", result.RequestID, "req-1")
			}
		default:
			t.Fatal("broadcaster stopped after transient error instead of retrying")
		}

		if n := callCount.Load(); n < 4 {
			t.Fatalf("GetResult called %d times, want >= 4 (3 errors + 1 success)", n)
		}

		// Cancel and drain resultCh so broadcaster goroutines can exit
		// before the synctest bubble closes.
		cancel()
		for {
			synctest.Wait()
			select {
			case <-resultCh:
			default:
				return
			}
		}
	})
}

type fakeAsyncClientWithErrors struct {
	getResult func(ctx context.Context) (*inference.GenerateResponse, error)
}

func (c *fakeAsyncClientWithErrors) Submit(_ context.Context, _ *inference.GenerateRequest) *inference.ClientError {
	return nil
}

func (c *fakeAsyncClientWithErrors) GetResult(ctx context.Context) (*inference.GenerateResponse, error) {
	return c.getResult(ctx)
}

func (c *fakeAsyncClientWithErrors) Cancel(_ context.Context, _ []string) error { return nil }
func (c *fakeAsyncClientWithErrors) Close() error                               { return nil }

func TestAsyncDispatcher_ParseError(t *testing.T) {
	client := newFakeAsyncClient()
	resolver := inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{
		"m1": func() inference.AsyncInferenceClient { return client },
	})
	defer func() { _ = resolver.Close() }()

	broadcaster := NewResultBroadcaster(client, logr.Discard())
	broadcasters := NewBroadcasterGroup([]*ResultBroadcaster{broadcaster})
	broadcasterCtx, broadcasterCancel := context.WithCancel(context.Background())
	defer broadcasterCancel()
	go broadcaster.Run(broadcasterCtx)

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "c-1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-bad", CustomID: "req-bad", ParseError: &OutputError{Code: "parse_error", Message: "bad json"}},
		{RequestID: "req-2", CustomID: "c-2", ModelID: "m1", Endpoint: "/v1/chat/completions"},
	}

	pending := NewPendingRequests(0)
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	dispatcher := NewPreDispatcher(NewAsyncDispatcher(resolver,
		broadcasters,
		pending, logr.Discard()))

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		client.deliver("req-1", map[string]any{"ok": true})
		client.deliver("req-2", map[string]any{"ok": true})
	}()

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if counts.Completed != 2 {
		t.Errorf("Completed = %d, want 2", counts.Completed)
	}
	if counts.Failed != 1 {
		t.Errorf("Failed = %d, want 1", counts.Failed)
	}

	outputData := readFile(t, outputFile)
	outputLines := countLines(outputData)
	if outputLines != 2 {
		t.Fatalf("output lines = %d, want 2", outputLines)
	}

	errorData := readFile(t, errorFile)
	errorLines := splitLines(errorData)
	if len(errorLines) != 1 {
		t.Fatalf("error lines = %d, want 1 (parse error)", len(errorLines))
	}
	var errEntry outputLine
	if err := json.Unmarshal(errorLines[0], &errEntry); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if errEntry.Error == nil || errEntry.Error.Code != "parse_error" {
		t.Fatalf("expected parse_error, got %+v", errEntry.Error)
	}
}

func TestAsyncDispatcher_ModelNotFound(t *testing.T) {
	client := newFakeAsyncClient()
	resolver := inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{
		"m1": func() inference.AsyncInferenceClient { return client },
	})
	defer func() { _ = resolver.Close() }()

	broadcaster := NewResultBroadcaster(client, logr.Discard())
	broadcasters := NewBroadcasterGroup([]*ResultBroadcaster{broadcaster})
	broadcasterCtx, broadcasterCancel := context.WithCancel(context.Background())
	defer broadcasterCancel()
	go broadcaster.Run(broadcasterCtx)

	items := []RequestItem{
		{RequestID: "req-1", CustomID: "c-1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		{RequestID: "req-2", CustomID: "c-2", ModelID: "no-such-model", Endpoint: "/v1/chat/completions"},
	}

	pending := NewPendingRequests(0)
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	dispatcher := NewAsyncDispatcher(resolver,
		broadcasters,
		pending, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  collector,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	go func() {
		time.Sleep(50 * time.Millisecond)
		client.deliver("req-1", map[string]any{"ok": true})
	}()

	counts, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	if counts.Completed != 1 {
		t.Errorf("Completed = %d, want 1", counts.Completed)
	}
	if counts.Failed != 1 {
		t.Errorf("Failed = %d, want 1", counts.Failed)
	}

	errorData := readFile(t, errorFile)
	errorLines := splitLines(errorData)
	if len(errorLines) != 1 {
		t.Fatalf("error lines = %d, want 1", len(errorLines))
	}
	var errLine outputLine
	if err := json.Unmarshal(errorLines[0], &errLine); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if errLine.Error == nil || errLine.Error.Code != "model_not_found" {
		t.Fatalf("expected model_not_found, got %+v", errLine.Error)
	}
}

func TestAsyncCancellation(t *testing.T) {
	tests := []struct {
		name        string
		cancelCause error
		wantCode    string
	}{
		{
			name:        "user cancellation",
			cancelCause: context.Canceled,
			wantCode:    "batch_cancelled",
		},
		{
			name:        "deadline expiry",
			cancelCause: context.DeadlineExceeded,
			wantCode:    "batch_expired",
		},
		{
			name:        "batch expiry sentinel",
			cancelCause: batchctx.ErrExpired,
			wantCode:    "batch_expired",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newFakeAsyncClient()
			client.submittedCh = make(chan struct{})
			resolver := inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{
				"m1": func() inference.AsyncInferenceClient { return client },
			})
			defer func() { _ = resolver.Close() }()

			broadcaster := NewResultBroadcaster(client, logr.Discard())
			broadcasters := NewBroadcasterGroup([]*ResultBroadcaster{broadcaster})
			broadcasterCtx, broadcasterCancel := context.WithCancel(context.Background())
			broadcasterDone := make(chan struct{})
			defer func() {
				broadcasterCancel()
				<-broadcasterDone
			}()
			go func() {
				defer close(broadcasterDone)
				broadcaster.Run(broadcasterCtx)
			}()

			items := makeItems(10, "m1")
			pending := NewPendingRequests(0)
			outputFile := tempFile(t)
			errorFile := tempFile(t)
			tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
			collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())
			dispatcher := NewAsyncDispatcher(resolver, broadcasters, pending, logr.Discard())
			executor := NewJobExecutor(JobExecutorConfig{
				Source:     &sliceSource{items: items},
				Dispatcher: dispatcher,
				Collector:  collector,
				Tracker:    tracker,
				Logger:     logr.Discard(),
			})

			parentCtx, parentCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer parentCancel()
			ctx, cancel := context.WithCancelCause(parentCtx)
			deliveryDone := make(chan struct{})
			defer func() {
				cancel(context.Canceled)
				<-deliveryDone
			}()
			go func() {
				defer close(deliveryDone)
				for range items {
					select {
					case <-client.submittedCh:
					case <-ctx.Done():
						return
					}
				}
				for i := 0; i < 3; i++ {
					client.deliver(items[i].RequestID, map[string]any{"ok": true})
				}
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for pending.pending.Load() != int64(len(items)-3) {
					select {
					case <-ticker.C:
					case <-ctx.Done():
						return
					}
				}
				cancel(tt.cancelCause)
			}()

			_, err := executor.Execute(ctx)
			if err != nil && !errors.Is(err, ctx.Err()) {
				t.Fatalf("Execute() error = %v, want nil or %v", err, ctx.Err())
			}
			if parentCtx.Err() != nil {
				t.Fatal("timed out waiting for async submission and partial results")
			}
			if !errors.Is(context.Cause(ctx), tt.cancelCause) {
				t.Fatalf("cancellation cause = %v, want %v", context.Cause(ctx), tt.cancelCause)
			}

			if got := countLines(readFile(t, outputFile)); got != 3 {
				t.Errorf("output lines = %d, want 3", got)
			}
			errorLines := splitLines(readFile(t, errorFile))
			if len(errorLines) != len(items)-3 {
				t.Fatalf("error lines = %d, want %d", len(errorLines), len(items)-3)
			}
			for _, line := range errorLines {
				var entry outputLine
				if err := json.Unmarshal(line, &entry); err != nil {
					t.Fatalf("unmarshal error output: %v", err)
				}
				if entry.Error == nil || entry.Error.Code != tt.wantCode {
					t.Errorf("error = %+v, want code %q", entry.Error, tt.wantCode)
				}
			}

			client.mu.Lock()
			cancelled := append([]string(nil), client.cancelledIDs...)
			client.mu.Unlock()
			wantCancelled := make([]string, 0, len(items)-3)
			for _, item := range items[3:] {
				wantCancelled = append(wantCancelled, item.RequestID)
			}
			slices.Sort(cancelled)
			slices.Sort(wantCancelled)
			if !slices.Equal(cancelled, wantCancelled) {
				t.Errorf("cancelled IDs = %v, want %v", cancelled, wantCancelled)
			}
		})
	}
}
