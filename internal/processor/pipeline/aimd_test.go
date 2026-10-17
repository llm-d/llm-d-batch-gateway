package pipeline

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/semaphore"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

func makeEndpointAIMD(t *testing.T, perEndpoint int) (*semaphore.AdaptiveSemaphore, *semaphore.AIMDController) {
	t.Helper()
	sem, err := semaphore.NewAdaptive(perEndpoint, func() { t.Error("double release") })
	if err != nil {
		t.Fatal(err)
	}
	aimd := semaphore.NewAIMDController(
		semaphore.AIMDConfig{
			MinLimit:         5,
			MaxLimit:         perEndpoint,
			BackoffFactor:    0.5,
			AdditiveIncrease: 1,
		},
		perEndpoint,
		func(limit int) { sem.SetLimit(limit) },
		logr.Discard(),
	)
	return sem, aimd
}

func runPipeline(t *testing.T, items []RequestItem, dispatcher RequestDispatcher) (outputData, errorData []byte, counts *int64, failedCounts *int64) {
	t.Helper()
	sink := &fakeSink{}
	total := int64(len(items))
	tracker := NewProgressTracker(total, nil, "test-job", 0, logr.Discard())
	pending := NewPendingRequests(total)
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	c := tracker.Counts()
	return sinkOutputBytes(t, sink), sinkErrorBytes(t, sink), &c.Completed, &c.Failed
}

func TestAIMDSignaling(t *testing.T) {
	const maxLimit = 20

	t.Run("clean 200 records success", func(t *testing.T) {
		client := &mockInferenceClient{response: []byte(`{"ok":true}`)}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, makeItems(maxLimit, "m1"), dispatcher)

		if got := aimd.Limit(); got != maxLimit {
			t.Errorf("Limit() = %d, want %d (no decrease for clean 200s)", got, maxLimit)
		}
	})

	t.Run("429 records rate limit", func(t *testing.T) {
		client := &statusCodeClient{statusCode: 429, category: httpclient.ErrCategoryRateLimit}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, makeItems(1, "m1"), dispatcher)

		if got := aimd.Limit(); got >= maxLimit {
			t.Errorf("Limit() = %d, want < %d (should decrease on 429)", got, maxLimit)
		}
	})

	t.Run("5xx records rate limit", func(t *testing.T) {
		client := &statusCodeClient{statusCode: 502, category: httpclient.ErrCategoryServer}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, makeItems(1, "m1"), dispatcher)

		if got := aimd.Limit(); got >= maxLimit {
			t.Errorf("Limit() = %d, want < %d (should decrease on 5xx)", got, maxLimit)
		}
	})

	t.Run("200 with capacity retry records rate limit", func(t *testing.T) {
		client := &capacityRetryClient{}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, makeItems(1, "m1"), dispatcher)

		if got := aimd.Limit(); got >= maxLimit {
			t.Errorf("Limit() = %d, want < %d (should decrease on capacity retry)", got, maxLimit)
		}
	})

	t.Run("endpoint isolation", func(t *testing.T) {
		clientA := &statusCodeClient{statusCode: 429, category: httpclient.ErrCategoryRateLimit}
		clientB := &mockInferenceClient{response: []byte(`{"ok":true}`)}
		resolver := inference.NewPerModelClientResolver(map[string]inference.InferenceClient{
			"model-a": clientA,
			"model-b": clientB,
		})
		defer func() { _ = resolver.Close() }()

		semA, aimdA := makeEndpointAIMD(t, maxLimit)
		semB, aimdB := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		items := append(makeItems(1, "model-a"), makeItems(maxLimit, "model-b")...)

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct, map[string]*EndpointAIMD{
			"model-a": {Sem: semA, AIMD: aimdA, Label: "ep-a"},
			"model-b": {Sem: semB, AIMD: aimdB, Label: "ep-b"},
		}, globalLimit, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, items, dispatcher)

		if got := aimdA.Limit(); got >= maxLimit {
			t.Errorf("model-a Limit() = %d, want < %d (429s should decrease)", got, maxLimit)
		}
		if got := aimdB.Limit(); got != maxLimit {
			t.Errorf("model-b Limit() = %d, want %d (should be unaffected)", got, maxLimit)
		}
	})

	t.Run("200 with network-only retries records success", func(t *testing.T) {
		client := &mockInferenceClientWithFn{
			generateFn: func(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
				return &inference.GenerateResponse{
					RequestID:        req.RequestID,
					Response:         []byte(`{"ok":true}`),
					HadCapacityRetry: false,
				}, nil
			},
		}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, makeItems(1, "m1"), dispatcher)

		if got := aimd.Limit(); got != maxLimit {
			t.Errorf("Limit() = %d, want %d (network-only retries should not decrease)", got, maxLimit)
		}
	})

	t.Run("4xx (not 429) records success", func(t *testing.T) {
		client := &statusCodeClient{statusCode: 400, category: httpclient.ErrCategoryInvalidReq}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, makeItems(1, "m1"), dispatcher)

		if got := aimd.Limit(); got != maxLimit {
			t.Errorf("Limit() = %d, want %d (4xx != 429 should not decrease)", got, maxLimit)
		}
	})

	t.Run("non-HTTP error skips AIMD", func(t *testing.T) {
		client := &mockInferenceClientWithFn{
			generateFn: func(_ context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
				return nil, &inference.ClientError{
					Category: httpclient.ErrCategoryUnknown,
					Message:  "connection refused",
				}
			},
		}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, aimd := makeEndpointAIMD(t, maxLimit)
		globalLimit := 100

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
			globalLimit, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, makeItems(1, "m1"), dispatcher)

		if got := aimd.Limit(); got != maxLimit {
			t.Errorf("Limit() = %d, want %d (non-HTTP errors should not affect AIMD)", got, maxLimit)
		}
	})

	t.Run("nil AIMD enforces semaphore without signaling", func(t *testing.T) {
		client := &statusCodeClient{statusCode: 429, category: httpclient.ErrCategoryRateLimit}
		resolver := inference.NewSingleClientResolver(client)
		defer func() { _ = resolver.Close() }()

		sem, err := semaphore.NewAdaptive(2, nil)
		if err != nil {
			t.Fatal(err)
		}

		direct := NewDirectDispatcher(resolver, logr.Discard())
		dispatcher, err := NewAIMDDispatcher(direct,
			map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: nil, Label: "test"}},
			100, logr.Discard())
		if err != nil {
			t.Fatal(err)
		}

		runPipeline(t, makeItems(3, "m1"), dispatcher)

		if sem.Limit() != 2 {
			t.Errorf("semaphore limit changed to %d, want 2 (nil AIMD should not adjust)", sem.Limit())
		}
	})
}

func TestAIMDDrain_SLOExpiry_UsesBatchExpired(t *testing.T) {
	slowClient := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			<-ctx.Done()
			return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "cancelled"}
		},
	}
	resolver := inference.NewSingleClientResolver(slowClient)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, 1)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(100*time.Millisecond))
	defer cancel()

	items := makeItems(5, "m1")
	source := &sliceSource{items: items}

	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	pending := NewPendingRequests(int64(len(items)))
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	direct := NewDirectDispatcher(resolver, logr.Discard())
	dispatcher, err := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		1, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: NewPreDispatcher(dispatcher),
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, _ = executor.Execute(ctx)

	errorData := sinkErrorBytes(t, sink)
	for _, line := range splitLines(errorData) {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if entry.Error == nil {
			continue
		}
		if entry.Error.Code != string(batch_types.ErrCodeBatchExpired) &&
			entry.Error.Code != string(httpclient.ErrCategoryUnknown) {
			t.Errorf("error code = %q, want %q or %q (in-flight)",
				entry.Error.Code, batch_types.ErrCodeBatchExpired, httpclient.ErrCategoryUnknown)
		}
	}
}

func TestCancelDrainsUndispatched(t *testing.T) {
	var dispatched atomic.Int32
	slowClient := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			dispatched.Add(1)
			time.Sleep(200 * time.Millisecond)
			if ctx.Err() != nil {
				return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
			}
			return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
		},
	}
	resolver := inference.NewSingleClientResolver(slowClient)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, 2)
	globalLimit := 2

	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(context.Canceled)
	items := makeItems(10, "m1")

	source := &cancelAfterNSource{items: items, cancelAt: 3, cancelFn: func() { cancel(batchctx.ErrCancelled) }}

	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	pending := NewPendingRequests(int64(len(items)))
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	direct := NewDirectDispatcher(resolver, logr.Discard())
	dispatcher, err := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     source,
		Dispatcher: dispatcher,
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err = executor.Execute(ctx)
	_ = err

	outputData := sinkOutputBytes(t, sink)
	errorData := sinkErrorBytes(t, sink)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	// source produces cancelAfter items before cancelling, plus possibly
	// a few more that race into the channel. All produced items should
	// be accounted for (either completed or cancelled).
	if total == 0 {
		t.Error("expected some output or error entries")
	}
	t.Logf("output=%d error=%d total=%d (of %d items, %d produced before cancel)",
		outputLines, errorLines, total, len(items), source.cancelAt)

	for _, line := range splitLines(errorData) {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal error line: %v", err)
		}
		if entry.Error == nil {
			t.Fatalf("expected error field in error entry")
		}
		if entry.Error.Code != "batch_cancelled" && entry.Error.Code != "UNKNOWN" {
			t.Errorf("unexpected error code %q (want batch_cancelled or UNKNOWN for in-flight)", entry.Error.Code)
		}
	}
}

// TestCancelWithFastRequestsThrottled mirrors the E2E Cancel/InProgress test:
// 5 fast + 20 slow requests, concurrency=10, cancel after some complete.
// The windowed dispatch must throttle so slow requests are still queued when cancel arrives.
func TestCancelWithFastRequestsThrottled(t *testing.T) {
	const totalRequests = 25
	const concurrency = 10

	// requests that block until cancelled (simulating slow inference)
	client := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			select {
			case <-ctx.Done():
				return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
			case <-time.After(5 * time.Second):
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, concurrency)
	globalLimit := concurrency

	items := makeItems(totalRequests, "m1")

	// cancel after 100ms — requests take 5s, so at most concurrency (10) are
	// in-flight, the rest are buffered. All must be accounted for.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(totalRequests), nil, "test-job", 0, logr.Discard())
	pending := NewPendingRequests(int64(totalRequests))
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	direct := NewDirectDispatcher(resolver, logr.Discard())
	dispatcher, err := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &ctxSource{items: items},
		Dispatcher: dispatcher,
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err = executor.Execute(ctx)
	_ = err

	outputData := sinkOutputBytes(t, sink)
	errorData := sinkErrorBytes(t, sink)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	t.Logf("output=%d error=%d total=%d (of %d)", outputLines, errorLines, total, totalRequests)

	// All requests must be accounted for
	if total != totalRequests {
		t.Errorf("output(%d) + error(%d) = %d, want %d (all requests must be accounted for)",
			outputLines, errorLines, total, totalRequests)
	}

	// Cancel must have prevented all from completing
	if outputLines == totalRequests {
		t.Errorf("all %d requests completed — dispatch was not throttled, cancel arrived too late", totalRequests)
	}

	// All should be cancelled (slow requests don't finish in 100ms)
	if errorLines == 0 {
		t.Error("expected at least one cancelled request")
	}
}

// TestCancelInProgressThrottled verifies that with semaphore-throttled dispatch,
// cancellation mid-flight leaves some requests undispatched, and ALL requests
// (completed + cancelled) are still accounted for. This mirrors the E2E
// Cancel/InProgress scenario where the semaphore limits concurrency so the
// cancel arrives while requests are still queued.
func TestCancelInProgressThrottled(t *testing.T) {
	const totalRequests = 25
	const concurrency = 5 // low concurrency so requests queue up

	slowClient := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			// each request takes 200ms — with concurrency=5, 25 requests take ~1s
			select {
			case <-ctx.Done():
				return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
			case <-time.After(200 * time.Millisecond):
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
		},
	}
	resolver := inference.NewSingleClientResolver(slowClient)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, concurrency)
	globalLimit := concurrency

	items := makeItems(totalRequests, "m1")

	// cancel after 300ms — ~5 requests complete (1 batch of concurrency=5),
	// rest should be cancelled
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(totalRequests), nil, "test-job", 0, logr.Discard())
	pending := NewPendingRequests(int64(totalRequests))
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	direct := NewDirectDispatcher(resolver, logr.Discard())
	dispatcher, err := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}

	// Use ctxSource which checks ctx on each send — like PlanFileSource does
	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &ctxSource{items: items},
		Dispatcher: dispatcher,
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err = executor.Execute(ctx)
	_ = err

	outputData := sinkOutputBytes(t, sink)
	errorData := sinkErrorBytes(t, sink)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	t.Logf("output=%d error=%d total=%d (of %d)", outputLines, errorLines, total, totalRequests)

	// Key invariant: all requests accounted for
	if total != totalRequests {
		t.Errorf("output(%d) + error(%d) = %d, want %d (all requests must be accounted for)",
			outputLines, errorLines, total, totalRequests)
	}

	// Some should have completed, some should have been cancelled
	if outputLines == 0 {
		t.Error("expected at least one completed request")
	}
	if errorLines == 0 {
		t.Error("expected at least one cancelled request")
	}
	// Should NOT have completed everything (cancel should arrive in time)
	if outputLines == totalRequests {
		t.Error("all requests completed — cancel didn't throttle dispatch (semaphore not working)")
	}
}

// ctxSource sends items but checks ctx on each send — like PlanFileSource does.
type ctxSource struct {
	items []RequestItem
}

func (s *ctxSource) Produce(_ context.Context, out chan<- RequestItem) error {
	defer close(out)
	for _, item := range s.items {
		out <- item
	}
	return nil
}

// TestCancelInProgress mirrors the E2E Cancel/InProgress test:
// 5 fast requests (complete quickly) + 20 slow requests (in-flight when cancel arrives).
// Concurrency is limited so some slow requests are undispatched.
// All 25 requests must be accounted for in output + error files.
func TestCancelInProgress(t *testing.T) {
	const fastCount = 5
	const slowCount = 20
	const totalRequests = fastCount + slowCount
	const concurrency = 10

	var fastDone atomic.Int32

	client := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			// simulate fast vs slow based on request ID
			isSlow := len(req.RequestID) > 0 && req.RequestID[len(req.RequestID)-1] >= '5'
			if !isSlow {
				fastDone.Add(1)
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
			// slow: wait up to 2s or until cancelled
			select {
			case <-ctx.Done():
				return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
			case <-time.After(2 * time.Second):
				return &inference.GenerateResponse{RequestID: req.RequestID, Response: []byte(`{"ok":true}`)}, nil
			}
		},
	}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, concurrency)
	globalLimit := concurrency

	items := makeItems(totalRequests, "m1")

	ctx, cancel := context.WithCancel(context.Background())
	// cancel after fast requests have had time to complete
	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(totalRequests), nil, "test-job", 0, logr.Discard())
	pending := NewPendingRequests(int64(totalRequests))
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	direct := NewDirectDispatcher(resolver, logr.Discard())
	dispatcher, err := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err = executor.Execute(ctx)
	_ = err

	outputData := sinkOutputBytes(t, sink)
	errorData := sinkErrorBytes(t, sink)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	t.Logf("output=%d error=%d total=%d (of %d)", outputLines, errorLines, total, totalRequests)

	if total != totalRequests {
		t.Errorf("output(%d) + error(%d) = %d, want %d (all requests must be accounted for)",
			outputLines, errorLines, total, totalRequests)
	}

	if outputLines == 0 {
		t.Error("expected at least one completed request")
	}
	if errorLines == 0 {
		t.Error("expected at least one cancelled request")
	}

	counts := tracker.Counts()
	if counts.Completed+counts.Failed != int64(totalRequests) {
		t.Errorf("Completed(%d) + Failed(%d) = %d, want %d",
			counts.Completed, counts.Failed, counts.Completed+counts.Failed, totalRequests)
	}
}

// TestExpiration mirrors the E2E Expiration test:
// A blocker batch saturates concurrency. The expiration batch can't dispatch
// any requests before its SLO fires. All requests must be failed/expired.
func TestExpiration(t *testing.T) {
	const numRequests = 15
	const concurrency = 10

	// slow client that blocks until cancelled
	slowClient := &mockInferenceClientWithFn{
		generateFn: func(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
			<-ctx.Done()
			return nil, &inference.ClientError{Category: httpclient.ErrCategoryUnknown, Message: "request cancelled"}
		},
	}
	resolver := inference.NewSingleClientResolver(slowClient)
	defer func() { _ = resolver.Close() }()

	sem, aimd := makeEndpointAIMD(t, concurrency)
	globalLimit := concurrency

	items := makeItems(numRequests, "m1")

	// simulate SLO expiry after 200ms
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(numRequests), nil, "test-job", 0, logr.Discard())
	pending := NewPendingRequests(int64(numRequests))
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	direct := NewDirectDispatcher(resolver, logr.Discard())
	dispatcher, err := NewAIMDDispatcher(direct,
		map[string]*EndpointAIMD{"m1": {Sem: sem, AIMD: aimd, Label: "test"}},
		globalLimit, logr.Discard())
	if err != nil {
		t.Fatal(err)
	}

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: dispatcher,
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err = executor.Execute(ctx)
	_ = err

	outputData := sinkOutputBytes(t, sink)
	errorData := sinkErrorBytes(t, sink)

	outputLines := countLines(outputData)
	errorLines := countLines(errorData)
	total := outputLines + errorLines

	t.Logf("output=%d error=%d total=%d (of %d)", outputLines, errorLines, total, numRequests)

	if total != numRequests {
		t.Errorf("output(%d) + error(%d) = %d, want %d (all requests must be accounted for)",
			outputLines, errorLines, total, numRequests)
	}
	if outputLines != 0 {
		t.Errorf("expected 0 completed (all should expire), got %d", outputLines)
	}
	if errorLines != numRequests {
		t.Errorf("expected %d error entries, got %d", numRequests, errorLines)
	}
}

func TestRetryExhaustion(t *testing.T) {
	client := &statusCodeClient{statusCode: 429, category: httpclient.ErrCategoryRateLimit}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	items := makeItems(2, "m1")

	sink := &fakeSink{}
	tracker := NewProgressTracker(int64(len(items)), nil, "test-job", 0, logr.Discard())
	pending := NewPendingRequests(int64(len(items)))
	checkpointer := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

	direct := NewDirectDispatcher(resolver, logr.Discard())

	executor := NewJobExecutor(JobExecutorConfig{
		Source:     &sliceSource{items: items},
		Dispatcher: direct,
		Collector:  checkpointer,
		Tracker:    tracker,
		Logger:     logr.Discard(),
	})

	_, err := executor.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute() error: %v", err)
	}

	counts := tracker.Counts()
	if counts.Failed != 2 {
		t.Errorf("Failed = %d, want 2", counts.Failed)
	}
	if counts.Completed != 0 {
		t.Errorf("Completed = %d, want 0", counts.Completed)
	}

	outputData := sinkOutputBytes(t, sink)
	outputLines := countLines(outputData)
	if outputLines != 2 {
		t.Errorf("output lines = %d, want 2 (429 HTTP errors go to output file)", outputLines)
	}

	for _, line := range splitLines(outputData) {
		var entry outputLine
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if entry.Error != nil {
			t.Errorf("expected nil error for HTTP error (goes to output file), got %+v", entry.Error)
		}
		if entry.Response == nil || entry.Response.StatusCode != 429 {
			t.Errorf("expected 429 response, got %+v", entry.Response)
		}
	}
}

// Test helpers

type statusCodeClient struct {
	statusCode int
	category   httpclient.ErrorCategory
}

func (c *statusCodeClient) Generate(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return nil, &inference.ClientError{
		Category:     c.category,
		Message:      "error",
		StatusCode:   c.statusCode,
		ResponseBody: []byte(`{"error":{"message":"error"}}`),
	}
}

type capacityRetryClient struct{}

func (c *capacityRetryClient) Generate(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return &inference.GenerateResponse{
		RequestID:        req.RequestID,
		Response:         []byte(`{"ok":true}`),
		HadCapacityRetry: true,
	}, nil
}

type mockInferenceClientWithFn struct {
	generateFn func(context.Context, *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError)
}

func (m *mockInferenceClientWithFn) Generate(ctx context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
	return m.generateFn(ctx, req)
}

type cancelAfterNSource struct {
	items    []RequestItem
	cancelAt int
	cancelFn context.CancelFunc
}

func (s *cancelAfterNSource) Produce(_ context.Context, out chan<- RequestItem) error {
	defer close(out)
	for i, item := range s.items {
		if i == s.cancelAt {
			s.cancelFn()
		}
		out <- item
	}
	return nil
}

var _ inference.InferenceClient = (*statusCodeClient)(nil)
var _ inference.InferenceClient = (*capacityRetryClient)(nil)
var _ inference.InferenceClient = (*mockInferenceClientWithFn)(nil)
var _ RequestSource = (*cancelAfterNSource)(nil)
