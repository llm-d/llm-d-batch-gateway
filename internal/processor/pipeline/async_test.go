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

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
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
	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	dispatcher := NewAsyncDispatcher(resolver,
		broadcasters,
		pending, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  checkpointer,
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

	outputData := sinkOutputBytes(t, sink)
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
		broadcaster.Subscribe(resultCh, nil)

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
	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	dispatcher := NewPreDispatcher(NewAsyncDispatcher(resolver,
		broadcasters,
		pending, logr.Discard()))

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  checkpointer,
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

	outputData := sinkOutputBytes(t, sink)
	outputLines := countLines(outputData)
	if outputLines != 2 {
		t.Fatalf("output lines = %d, want 2", outputLines)
	}

	errorData := sinkErrorBytes(t, sink)
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
	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	dispatcher := NewAsyncDispatcher(resolver,
		broadcasters,
		pending, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  checkpointer,
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

	errorData := sinkErrorBytes(t, sink)
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

type requestSourceFunc func(context.Context, chan<- RequestItem) error

var _ RequestSource = requestSourceFunc(nil)

func (f requestSourceFunc) Produce(ctx context.Context, out chan<- RequestItem) error {
	return f(ctx, out)
}

func TestAsyncCancellation(t *testing.T) {
	tests := []struct {
		name string
		// cancelCause trips the abort context (or is returned by the source).
		cancelCause error
		// sourceFailure makes the request source itself fail instead of
		// cancelling the context.
		sourceFailure bool
		wantCode      string
		// wantQueueCancel expects the still-pending submissions to be
		// cancelled in the shared queue. Only terminal aborts (user cancel,
		// expiry) cancel: a stop that leads to a resume must leave the
		// submissions in place, or the queue's CANCELLED results for the
		// same request IDs would poison the resumption.
		wantQueueCancel bool
	}{
		{
			name:            "user cancellation",
			cancelCause:     batchctx.ErrCancelled,
			wantCode:        "batch_cancelled",
			wantQueueCancel: true,
		},
		{
			name:        "system cancellation",
			cancelCause: context.Canceled,
			wantCode:    "batch_failed",
		},
		{
			name:        "shutdown",
			cancelCause: batchctx.ErrShutdown,
			wantCode:    "batch_failed",
		},
		{
			name:          "source failure",
			cancelCause:   errors.New("source read failed"),
			sourceFailure: true,
			wantCode:      "batch_failed",
		},
		{
			name:            "deadline expiry",
			cancelCause:     context.DeadlineExceeded,
			wantCode:        "batch_expired",
			wantQueueCancel: true,
		},
		{
			name:            "batch expiry sentinel",
			cancelCause:     batchctx.ErrExpired,
			wantCode:        "batch_expired",
			wantQueueCancel: true,
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
			sink := &fakeSink{}
			tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
			checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())
			dispatcher := NewAsyncDispatcher(resolver, broadcasters, pending, logr.Discard())
			deliveryDone := make(chan struct{})
			executor := NewJobExecutor(JobExecutorConfig{
				Source: requestSourceFunc(func(ctx context.Context, out chan<- RequestItem) error {
					defer close(out)
					for _, item := range items {
						select {
						case out <- item:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					if tt.sourceFailure {
						// Fail only after submission and partial results have been collected.
						select {
						case <-deliveryDone:
							return tt.cancelCause
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}),
				Dispatcher: dispatcher,
				Collector:  checkpointer,
				Tracker:    tracker,
				Logger:     logr.Discard(),
			})

			parentCtx, parentCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer parentCancel()
			ctx, cancel := context.WithCancelCause(parentCtx)
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
				if !tt.sourceFailure {
					cancel(tt.cancelCause)
				}
			}()

			counts, err := executor.Execute(ctx)
			if parentCtx.Err() != nil {
				t.Fatal("timed out waiting for async submission and partial results")
			}
			if tt.sourceFailure {
				if !errors.Is(err, tt.cancelCause) {
					t.Fatalf("Execute() error = %v, want source error %v", err, tt.cancelCause)
				}
				if ctx.Err() != nil {
					t.Fatalf("source failure should only cancel the executor's context, got %v", ctx.Err())
				}
			} else {
				if err != nil && !errors.Is(err, ctx.Err()) {
					t.Fatalf("Execute() error = %v, want nil or %v", err, ctx.Err())
				}
				if !errors.Is(context.Cause(ctx), tt.cancelCause) {
					t.Fatalf("cancellation cause = %v, want %v", context.Cause(ctx), tt.cancelCause)
				}
			}
			if counts.Completed != 3 || counts.Failed != int64(len(items)-3) {
				t.Errorf("counts = %+v, want 3 completed and %d failed", counts, len(items)-3)
			}

			if got := countLines(sinkOutputBytes(t, sink)); got != 3 {
				t.Errorf("output lines = %d, want 3", got)
			}
			errorLines := splitLines(sinkErrorBytes(t, sink))
			if len(errorLines) != len(items)-3 {
				t.Fatalf("error lines = %d, want %d", len(errorLines), len(items)-3)
			}
			for _, line := range errorLines {
				var entry outputLine
				if err := json.Unmarshal(line, &entry); err != nil {
					t.Fatalf("unmarshal error output: %v", err)
				}
				wantMessage := batch_types.BatchErrorCode(tt.wantCode).Message()
				if entry.Error == nil || entry.Error.Code != tt.wantCode || entry.Error.Message != wantMessage {
					t.Errorf("error = %+v, want code %q and message %q", entry.Error, tt.wantCode, wantMessage)
				}
			}

			client.mu.Lock()
			cancelled := append([]string(nil), client.cancelledIDs...)
			client.mu.Unlock()
			if tt.wantQueueCancel {
				wantCancelled := make([]string, 0, len(items)-3)
				for _, item := range items[3:] {
					wantCancelled = append(wantCancelled, item.RequestID)
				}
				slices.Sort(cancelled)
				slices.Sort(wantCancelled)
				if !slices.Equal(cancelled, wantCancelled) {
					t.Errorf("cancelled IDs = %v, want %v", cancelled, wantCancelled)
				}
			} else if len(cancelled) != 0 {
				t.Errorf("cancelled IDs = %v, want none: a resumable stop must not poison the queue", cancelled)
			}
		})
	}
}

func TestAsyncDispatcher_BeforeSubmit(t *testing.T) {
	newEnv := func(t *testing.T) (*fakeAsyncClient, *inference.AsyncGatewayResolver, *BroadcasterGroup) {
		t.Helper()
		client := newFakeAsyncClient()
		client.submittedCh = make(chan struct{})
		resolver := inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{
			"m1": func() inference.AsyncInferenceClient { return client },
		})
		t.Cleanup(func() { _ = resolver.Close() })

		broadcaster := NewResultBroadcaster(client, logr.Discard())
		broadcasters := NewBroadcasterGroup([]*ResultBroadcaster{broadcaster})
		broadcasterCtx, broadcasterCancel := context.WithCancel(context.Background())
		broadcasterDone := make(chan struct{})
		t.Cleanup(func() {
			broadcasterCancel()
			<-broadcasterDone
		})
		go func() {
			defer close(broadcasterDone)
			broadcaster.Run(broadcasterCtx)
		}()
		return client, resolver, broadcasters
	}

	items := func() []RequestItem {
		return []RequestItem{
			{RequestID: "req-1", CustomID: "c-1", ModelID: "m1", Endpoint: "/v1/chat/completions"},
			{RequestID: "req-2", CustomID: "c-2", ModelID: "m1", Endpoint: "/v1/chat/completions"},
			{RequestID: "req-3", CustomID: "c-3", ModelID: "m1", Endpoint: "/v1/chat/completions"},
		}
	}

	t.Run("ownership conflict stops submitting but keeps draining", func(t *testing.T) {
		client, resolver, broadcasters := newEnv(t)

		pending := NewPendingRequests(0)
		sink := &fakeSink{}
		tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())
		checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

		dispatcher := NewPreDispatcher(NewAsyncDispatcher(resolver, broadcasters, pending, logr.Discard())).
			WithBeforeSubmit(func(msg *RequestItem) (bool, error) {
				if msg.RequestID != "req-1" {
					return false, fmt.Errorf("mark in-flight %s: %w", msg.RequestID, db.ErrConflict)
				}
				return true, nil
			})

		executor := NewJobExecutor(JobExecutorConfig{
			Source:     &sliceSource{items: items()},
			Dispatcher: dispatcher,
			Collector:  checkpointer,
			Tracker:    tracker,
			Logger:     logr.Discard(),
		})

		// The dispatcher stops submitting after the conflict; deliver the one
		// submitted result so the run terminates instead of blocking on it.
		go func() {
			<-client.submittedCh
			client.deliver("req-1", map[string]any{"ok": true})
		}()

		// A deadlock (dispatcher blocked on an undrained source or an
		// unresolved pending entry) surfaces as the deadline firing.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		counts, err := executor.Execute(ctx)
		if err != nil {
			t.Fatalf("Execute() error: %v", err)
		}

		client.mu.Lock()
		submitted := len(client.submitted)
		client.mu.Unlock()
		if submitted != 1 {
			t.Errorf("submitted requests = %d, want 1 (submission stops after ownership loss)", submitted)
		}
		if counts.Completed != 1 || counts.Failed != 0 {
			t.Errorf("counts = completed %d / failed %d, want 1 / 0", counts.Completed, counts.Failed)
		}
		if lines := countLines(sinkOutputBytes(t, sink)); lines != 1 {
			t.Errorf("output lines = %d, want 1", lines)
		}
	})

	t.Run("non-conflict error routes to the per-request error path", func(t *testing.T) {
		client, resolver, broadcasters := newEnv(t)

		pending := NewPendingRequests(0)
		sink := &fakeSink{}
		tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())
		checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

		dispatcher := NewPreDispatcher(NewAsyncDispatcher(resolver, broadcasters, pending, logr.Discard())).
			WithBeforeSubmit(func(msg *RequestItem) (bool, error) {
				if msg.RequestID == "req-2" {
					return true, errors.New("db down")
				}
				return true, nil
			})

		executor := NewJobExecutor(JobExecutorConfig{
			Source:     &sliceSource{items: items()},
			Dispatcher: dispatcher,
			Collector:  checkpointer,
			Tracker:    tracker,
			Logger:     logr.Discard(),
		})

		go func() {
			for range 2 {
				<-client.submittedCh
			}
			client.deliver("req-1", map[string]any{"ok": true})
			client.deliver("req-3", map[string]any{"ok": true})
		}()

		counts, err := executor.Execute(context.Background())
		if err != nil {
			t.Fatalf("Execute() error: %v", err)
		}

		client.mu.Lock()
		submitted := len(client.submitted)
		client.mu.Unlock()
		if submitted != 2 {
			t.Errorf("submitted requests = %d, want 2 (only req-2 is rejected)", submitted)
		}
		if counts.Completed != 2 || counts.Failed != 1 {
			t.Errorf("counts = completed %d / failed %d, want 2 / 1", counts.Completed, counts.Failed)
		}

		errorLines := splitLines(sinkErrorBytes(t, sink))
		if len(errorLines) != 1 {
			t.Fatalf("error lines = %d, want 1", len(errorLines))
		}
		var errLine outputLine
		if err := json.Unmarshal(errorLines[0], &errLine); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if errLine.Error == nil || errLine.Error.Code != "server_error" {
			t.Fatalf("expected server_error, got %+v", errLine.Error)
		}
	})

	t.Run("terminal entry drops without submitting", func(t *testing.T) {
		client, resolver, broadcasters := newEnv(t)

		pending := NewPendingRequests(0)
		sink := &fakeSink{}
		tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())
		checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

		dispatcher := NewPreDispatcher(NewAsyncDispatcher(resolver, broadcasters, pending, logr.Discard())).
			WithBeforeSubmit(func(msg *RequestItem) (bool, error) {
				// req-2's result was checkpointed by an earlier delivery.
				return msg.RequestID != "req-2", nil
			})

		executor := NewJobExecutor(JobExecutorConfig{
			Source:     &sliceSource{items: items()},
			Dispatcher: dispatcher,
			Collector:  checkpointer,
			Tracker:    tracker,
			Logger:     logr.Discard(),
		})

		go func() {
			for range 2 {
				<-client.submittedCh
			}
			client.deliver("req-1", map[string]any{"ok": true})
			client.deliver("req-3", map[string]any{"ok": true})
		}()

		counts, err := executor.Execute(context.Background())
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		client.mu.Lock()
		submitted := len(client.submitted)
		client.mu.Unlock()
		if submitted != 2 {
			t.Errorf("submitted requests = %d, want 2 (req-2 is dropped)", submitted)
		}
		if counts.Completed != 2 || counts.Failed != 0 {
			t.Errorf("counts = completed %d / failed %d, want 2 / 0", counts.Completed, counts.Failed)
		}
	})
}

// fakeDurableAsyncClient serves results through the durable-delivery
// capability instead of GetResult.
type fakeDurableAsyncClient struct {
	receive func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error)
}

var _ inference.AsyncInferenceClient = (*fakeDurableAsyncClient)(nil)
var _ inference.DurableResultClient = (*fakeDurableAsyncClient)(nil)

func (c *fakeDurableAsyncClient) Submit(_ context.Context, _ *inference.GenerateRequest) *inference.ClientError {
	return nil
}

func (c *fakeDurableAsyncClient) GetResult(_ context.Context) (*inference.GenerateResponse, error) {
	return nil, errors.New("GetResult must not be used on a durable client")
}

func (c *fakeDurableAsyncClient) ReceiveResult(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
	return c.receive(ctx)
}

func (c *fakeDurableAsyncClient) Cancel(_ context.Context, _ []string) error { return nil }
func (c *fakeDurableAsyncClient) Close() error                               { return nil }

func durableResponse(requestID string) *inference.GenerateResponse {
	resp, _ := json.Marshal(map[string]any{"ok": true})
	return &inference.GenerateResponse{RequestID: requestID, Response: resp}
}

// TestBroadcaster_DurableDeliveryCarriesAck verifies the broadcaster prefers
// the durable leased delivery and hands its acknowledgement to the consumer
// with the result, so the ack happens only after the durable checkpoint.
func TestBroadcaster_DurableDeliveryCarriesAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var acked atomic.Int32
		client := &fakeDurableAsyncClient{
			receive: func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
				return durableResponse("req-1"), func(context.Context) error {
					acked.Add(1)
					return nil
				}, nil
			},
		}

		broadcaster := NewResultBroadcaster(client, logr.Discard())
		resultCh := make(chan ResultItem, 10)
		broadcaster.Subscribe(resultCh, nil)

		ctx, cancel := context.WithCancel(t.Context())
		go broadcaster.Run(ctx)
		synctest.Wait()

		select {
		case result := <-resultCh:
			if result.RequestID != "req-1" {
				t.Fatalf("RequestID = %q, want req-1", result.RequestID)
			}
			if result.Ack == nil {
				t.Fatal("durable delivery must carry its acknowledgement")
			}
			if err := result.Ack(context.Background()); err != nil {
				t.Fatalf("ack: %v", err)
			}
			if acked.Load() != 1 {
				t.Fatalf("ack reached the delivery %d times, want 1", acked.Load())
			}
		default:
			t.Fatal("no result broadcast from the durable delivery")
		}

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

// TestBroadcaster_AcksResidueWithoutSubscribers covers the residue shape: a
// delivery that arrives when no job is subscribed belongs to no live
// consumer, so the broadcaster acknowledges it instead of letting its lease
// bounce forever — the destructive transport consumed it the same way.
func TestBroadcaster_AcksResidueWithoutSubscribers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		acked := make(chan string, 1)
		client := &fakeDurableAsyncClient{
			receive: func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
				return durableResponse("req-residue"), func(context.Context) error {
					acked <- "req-residue"
					return nil
				}, nil
			},
		}

		broadcaster := NewResultBroadcaster(client, logr.Discard())
		// No subscriber: the residue shape.

		ctx, cancel := context.WithCancel(t.Context())
		go broadcaster.Run(ctx)
		synctest.Wait()

		select {
		case id := <-acked:
			if id != "req-residue" {
				t.Fatalf("acked = %q, want req-residue", id)
			}
		default:
			t.Fatal("broadcaster did not consume the unowned delivery")
		}

		cancel()
	})
}

// TestBroadcaster_FallsBackWhenDurableUnavailable verifies the additive
// capability: a client whose transport lacks durable delivery falls back to
// the destructive GetResult contract.
func TestBroadcaster_FallsBackWhenDurableUnavailable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var getResultCalls atomic.Int32
		client := &fakeDurableAsyncClient{
			receive: func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
				return nil, nil, inference.ErrDurableResultsUnavailable
			},
		}
		// The same client type routes the fallback through GetResult when the
		// durable capability reports itself unavailable.
		fallback := &durableFallbackClient{
			fakeDurableAsyncClient: client,
			getResult: func(ctx context.Context) (*inference.GenerateResponse, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				getResultCalls.Add(1)
				return durableResponse("req-1"), nil
			},
		}

		broadcaster := NewResultBroadcaster(fallback, logr.Discard())
		resultCh := make(chan ResultItem, 10)
		broadcaster.Subscribe(resultCh, nil)

		ctx, cancel := context.WithCancel(t.Context())
		go broadcaster.Run(ctx)
		synctest.Wait()

		select {
		case result := <-resultCh:
			if result.RequestID != "req-1" {
				t.Fatalf("RequestID = %q, want req-1", result.RequestID)
			}
			if result.Ack != nil {
				t.Fatal("destructive fallback must not carry an ack")
			}
		default:
			t.Fatal("no result broadcast from the fallback")
		}
		if getResultCalls.Load() == 0 {
			t.Fatal("GetResult was not used as the fallback")
		}

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

// durableFallbackClient pairs an unavailable durable capability with a
// working destructive GetResult, the shape of a client whose transport lacks
// durable delivery behind a client that always implements the capability
// interface.
type durableFallbackClient struct {
	*fakeDurableAsyncClient
	getResult func(ctx context.Context) (*inference.GenerateResponse, error)
}

func (c *durableFallbackClient) GetResult(ctx context.Context) (*inference.GenerateResponse, error) {
	return c.getResult(ctx)
}

// TestBroadcaster_RoutesPerRequest covers the per-request routing: a result
// reaches only the subscriber that owns its request, and a delivery no live
// subscriber owns is acknowledged as residue even while another job is
// subscribed to the same route — the bouncing-residue window of the plain
// broadcast demux.
func TestBroadcaster_RoutesPerRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ownerPending := NewPendingRequests(0)
		ownerPending.Store(RequestItem{RequestID: "req-owned", CustomID: "c-owned"})
		foreignPending := NewPendingRequests(0)
		foreignPending.Store(RequestItem{RequestID: "req-foreign", CustomID: "c-foreign"})

		var delivered atomic.Int32
		residueAcked := make(chan string, 1)
		client := &fakeDurableAsyncClient{
			receive: func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
				switch delivered.Add(1) {
				case 1:
					return durableResponse("req-owned"), nil, nil
				default:
					return durableResponse("req-residue"), func(context.Context) error {
						residueAcked <- "req-residue"
						return nil
					}, nil
				}
			},
		}

		broadcaster := NewResultBroadcaster(client, logr.Discard())
		ownerCh := make(chan ResultItem, 10)
		foreignCh := make(chan ResultItem, 10)
		broadcaster.Subscribe(ownerCh, ownerPending.Has)
		broadcaster.Subscribe(foreignCh, foreignPending.Has)

		ctx, cancel := context.WithCancel(t.Context())
		go broadcaster.Run(ctx)
		synctest.Wait()

		// The owned result reaches its owner alone.
		select {
		case result := <-ownerCh:
			if result.RequestID != "req-owned" {
				t.Fatalf("owner received %q, want req-owned", result.RequestID)
			}
		default:
			t.Fatal("the owning subscriber did not receive its result")
		}
		select {
		case result := <-foreignCh:
			t.Fatalf("foreign subscriber received %q, want nothing", result.RequestID)
		default:
		}

		// The residue is consumed by the broadcaster itself, without waiting
		// for the route to go idle.
		select {
		case id := <-residueAcked:
			if id != "req-residue" {
				t.Fatalf("acked residue = %q, want req-residue", id)
			}
		default:
			t.Fatal("residue was not acknowledged while a live subscriber was routed around")
		}
		select {
		case result := <-ownerCh:
			t.Fatalf("owner received residue %q, want nothing", result.RequestID)
		default:
		}

		cancel()
	})
}

// TestBroadcaster_BlockedSendDoesNotStallSubscriptions pins the liveness
// contract of the per-request routing: a send blocked on a subscriber that
// stopped receiving must not hold the subscribers lock, so other jobs on
// the same route can still subscribe and unsubscribe.
func TestBroadcaster_BlockedSendDoesNotStallSubscriptions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		blockedPending := NewPendingRequests(0)
		blockedPending.Store(RequestItem{RequestID: "req-blocked", CustomID: "c-blocked"})

		var delivered atomic.Int32
		client := &fakeDurableAsyncClient{
			receive: func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
				if ctx.Err() != nil {
					return nil, nil, ctx.Err()
				}
				if delivered.Add(1) == 1 {
					return durableResponse("req-blocked"), nil, nil
				}
				<-ctx.Done()
				return nil, nil, ctx.Err()
			},
		}

		broadcaster := NewResultBroadcaster(client, logr.Discard())
		// blockedCh is never drained, so Run parks sending req-blocked to it.
		blockedCh := make(chan ResultItem)
		broadcaster.Subscribe(blockedCh, blockedPending.Has)
		otherPending := NewPendingRequests(0)
		otherCh := make(chan ResultItem, 1)
		broadcaster.Subscribe(otherCh, otherPending.Has)

		ctx, cancel := context.WithCancel(t.Context())
		go broadcaster.Run(ctx)
		synctest.Wait()

		subscribed := make(chan struct{})
		go func() {
			defer close(subscribed)
			broadcaster.Subscribe(make(chan ResultItem, 1), otherPending.Has)
		}()
		unsubscribed := make(chan struct{})
		go func() {
			defer close(unsubscribed)
			broadcaster.Unsubscribe(otherCh)
		}()
		synctest.Wait()

		select {
		case <-subscribed:
		default:
			t.Fatal("Subscribe stalled behind a send blocked on a full subscriber channel")
		}
		select {
		case <-unsubscribed:
		default:
			t.Fatal("Unsubscribe stalled behind a send blocked on a full subscriber channel")
		}

		// Release the parked send — the delivery itself — so Run can observe
		// the cancellation and exit.
		result := <-blockedCh
		if result.RequestID != "req-blocked" {
			t.Fatalf("blocked subscriber received %q, want req-blocked", result.RequestID)
		}
		cancel()
	})
}

// TestBroadcaster_ResidueDurableVerification covers the residue gate: an
// unrouted delivery is acknowledged only when the durable check proves no
// open entry can still account for it. The regression this pins: an
// uncheckpointed result arriving before its recovering job has subscribed
// must not be acknowledged — its lease redelivers it once the owner is
// reachable, so the completed inference is recovered instead of discarded.
func TestBroadcaster_ResidueDurableVerification(t *testing.T) {
	park := func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}

	t.Run("open entry holds the delivery for the recovering owner", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ownerPending := NewPendingRequests(0)

			var delivered atomic.Int32
			acked := make(chan string, 2)
			ackFn := func(context.Context) error {
				acked <- "req-recover"
				return nil
			}
			// The second delivery (the lease redelivery) waits until the
			// recovering job has subscribed and pre-stored the request.
			release := make(chan struct{})
			client := &fakeDurableAsyncClient{
				receive: func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
					if ctx.Err() != nil {
						return nil, nil, ctx.Err()
					}
					switch delivered.Add(1) {
					case 1:
						return durableResponse("req-recover"), ackFn, nil
					case 2:
						select {
						case <-release:
							return durableResponse("req-recover"), ackFn, nil
						case <-ctx.Done():
							return nil, nil, ctx.Err()
						}
					default:
						return park(ctx)
					}
				},
			}

			broadcaster := NewResultBroadcaster(client, logr.Discard()).
				WithResidueSafetyCheck(func(context.Context, string) (bool, error) {
					return false, nil // the entry is still open
				})
			ctx, cancel := context.WithCancel(t.Context())
			go broadcaster.Run(ctx)
			synctest.Wait()

			// No subscriber yet: the delivery must stay unacked.
			select {
			case id := <-acked:
				t.Fatalf("uncheckpointed result %q was acknowledged before recovery", id)
			default:
			}

			// The recovering job subscribes and pre-stores the request; the
			// lease redelivery routes to it, and only the owner (after its
			// checkpoint) would release the ack — never the broadcaster.
			ch := make(chan ResultItem, 2)
			broadcaster.Subscribe(ch, ownerPending.Has)
			ownerPending.Store(RequestItem{RequestID: "req-recover", CustomID: "c-recover"})
			close(release)
			synctest.Wait()

			select {
			case result := <-ch:
				if result.RequestID != "req-recover" {
					t.Fatalf("owner received %q, want req-recover", result.RequestID)
				}
			default:
				t.Fatal("the redelivery did not reach the recovering owner")
			}
			select {
			case id := <-acked:
				t.Fatalf("broadcaster released the ack for routed delivery %q", id)
			default:
			}

			cancel()
		})
	})

	t.Run("terminal entry is acknowledged as residue", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var delivered atomic.Int32
			acked := make(chan string, 1)
			client := &fakeDurableAsyncClient{
				receive: func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
					if ctx.Err() != nil {
						return nil, nil, ctx.Err()
					}
					if delivered.Add(1) > 1 {
						return park(ctx)
					}
					return durableResponse("req-late"), func(context.Context) error {
						acked <- "req-late"
						return nil
					}, nil
				},
			}

			broadcaster := NewResultBroadcaster(client, logr.Discard()).
				WithResidueSafetyCheck(func(context.Context, string) (bool, error) {
					return true, nil // no open entry can account for it
				})
			ctx, cancel := context.WithCancel(t.Context())
			go broadcaster.Run(ctx)
			synctest.Wait()

			select {
			case id := <-acked:
				if id != "req-late" {
					t.Fatalf("acked residue = %q, want req-late", id)
				}
			default:
				t.Fatal("residue with a durably verified terminal entry was not acknowledged")
			}

			cancel()
		})
	})

	t.Run("failed check is not safe", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			var delivered atomic.Int32
			acked := make(chan string, 1)
			client := &fakeDurableAsyncClient{
				receive: func(ctx context.Context) (*inference.GenerateResponse, inference.ResultAck, error) {
					if ctx.Err() != nil {
						return nil, nil, ctx.Err()
					}
					if delivered.Add(1) > 1 {
						return park(ctx)
					}
					return durableResponse("req-unknown"), func(context.Context) error {
						acked <- "req-unknown"
						return nil
					}, nil
				},
			}

			broadcaster := NewResultBroadcaster(client, logr.Discard()).
				WithResidueSafetyCheck(func(context.Context, string) (bool, error) {
					return false, errors.New("entry store unavailable")
				})
			ctx, cancel := context.WithCancel(t.Context())
			go broadcaster.Run(ctx)
			synctest.Wait()

			select {
			case id := <-acked:
				t.Fatalf("unverifiable delivery %q was acknowledged", id)
			default:
			}

			cancel()
		})
	})
}
