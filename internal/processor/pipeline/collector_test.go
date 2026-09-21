package pipeline

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

func TestResultCollector_RoutesToCorrectFile(t *testing.T) {
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	results := []ResultItem{
		{
			RequestID: "req-1",
			CustomID:  "c-1",
			Response:  &batch_types.ResponseData{StatusCode: 200, RequestID: "req-1", Body: map[string]any{"ok": true}},
		},
		{
			RequestID: "req-2",
			CustomID:  "c-2",
			Response:  &batch_types.ResponseData{StatusCode: 422, RequestID: "req-2", Body: map[string]any{"error": "bad"}},
		},
		{
			RequestID: "req-3",
			CustomID:  "c-3",
			Error:     &OutputError{Code: "SERVER_ERROR", Message: "connection refused"},
		},
	}

	for _, r := range results {
		pending.Store(RequestItem{RequestID: r.RequestID, CustomID: r.CustomID})
	}

	ch := make(chan ResultItem, len(results))
	for _, r := range results {
		ch <- r
	}
	close(ch)

	if err := collector.Drain(context.Background(), ch); err != nil {
		t.Fatalf("Drain error: %v", err)
	}

	outputData := readFile(t, outputFile)
	outputLines := splitLines(outputData)
	if len(outputLines) != 2 {
		t.Fatalf("output lines = %d, want 2 (200 success + 422 HTTP error)", len(outputLines))
	}

	errorData := readFile(t, errorFile)
	errorLines := splitLines(errorData)
	if len(errorLines) != 1 {
		t.Fatalf("error lines = %d, want 1 (non-HTTP error)", len(errorLines))
	}

	var errEntry outputLine
	if err := json.Unmarshal(errorLines[0], &errEntry); err != nil {
		t.Fatalf("unmarshal error line: %v", err)
	}
	if errEntry.Error == nil || errEntry.Error.Code != "SERVER_ERROR" {
		t.Fatalf("expected SERVER_ERROR, got %+v", errEntry.Error)
	}
}

func TestResultCollector_DrainSkipsUnknownPending(t *testing.T) {
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(1, nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	ch := make(chan ResultItem, 1)
	ch <- ResultItem{
		RequestID: "unknown-req",
	}
	close(ch)

	if err := collector.Drain(context.Background(), ch); err != nil {
		t.Fatalf("Drain error: %v", err)
	}

	outputData := readFile(t, outputFile)
	errorData := readFile(t, errorFile)
	if len(trimBytes(outputData)) != 0 || len(trimBytes(errorData)) != 0 {
		t.Fatal("expected no output for unknown pending request")
	}
}

func TestResultCollector_CheckpointsBeforeAck(t *testing.T) {
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(1, nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	var order []string
	collector.SetCheckpoint(func(_ context.Context, requestID string, result []byte) error {
		order = append(order, "checkpoint")
		if requestID != "req-1" || len(result) == 0 {
			t.Fatalf("unexpected checkpoint: %q %q", requestID, result)
		}
		return nil
	})
	pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1"})
	ch := make(chan ResultItem, 1)
	ch <- ResultItem{
		RequestID: "req-1",
		CustomID:  "c-1",
		Response:  &batch_types.ResponseData{StatusCode: 200, RequestID: "req-1", Body: map[string]any{"ok": true}},
		Ack: func(context.Context) error {
			order = append(order, "ack")
			return nil
		},
	}
	close(ch)

	if err := collector.Drain(context.Background(), ch); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	if fmt.Sprint(order) != "[checkpoint ack]" {
		t.Fatalf("order=%v, want checkpoint before ack", order)
	}
}

func TestResultCollector_DrainProcessesAllResultsAfterCancel(t *testing.T) {
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	results := []ResultItem{
		{RequestID: "req-1", CustomID: "c-1", Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-1", Body: map[string]any{"ok": true}}},
		{RequestID: "req-2", CustomID: "c-2", Error: &OutputError{Code: "batch_cancelled", Message: "cancelled"}},
		{RequestID: "req-3", CustomID: "c-3", Error: &OutputError{Code: "batch_cancelled", Message: "cancelled"}},
	}
	for _, r := range results {
		pending.Store(RequestItem{RequestID: r.RequestID, CustomID: r.CustomID})
	}

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan ResultItem, len(results))

	// Send first result, then cancel, then send the rest.
	ch <- results[0]
	cancel()
	ch <- results[1]
	ch <- results[2]
	close(ch)

	err := collector.Drain(ctx, ch)
	if err != nil {
		t.Fatalf("Drain error = %v, want nil (drain completed despite cancelled ctx)", err)
	}

	outputData := readFile(t, outputFile)
	outputLines := splitLines(outputData)
	if len(outputLines) != 1 {
		t.Fatalf("output lines = %d, want 1", len(outputLines))
	}

	errorData := readFile(t, errorFile)
	errorLines := splitLines(errorData)
	if len(errorLines) != 2 {
		t.Fatalf("error lines = %d, want 2 (cancelled requests must still be written)", len(errorLines))
	}
}

type failAfterNWriter struct {
	remaining int
}

func (w *failAfterNWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, fmt.Errorf("simulated write failure")
	}
	w.remaining--
	return len(p), nil
}

func TestResultCollector_DrainDecrementsMetricsAfterWriteFailure(t *testing.T) {
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	collector.output = bufio.NewWriterSize(&failAfterNWriter{remaining: 1}, 1)

	now := time.Now()
	results := []ResultItem{
		{RequestID: "req-1", CustomID: "c-1", SubmittedAt: now, Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-1", Body: map[string]any{"ok": true}}},
		{RequestID: "req-2", CustomID: "c-2", SubmittedAt: now, Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-2", Body: map[string]any{"ok": true}}},
		{RequestID: "req-3", CustomID: "c-3", SubmittedAt: now, Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-3", Body: map[string]any{"ok": true}}},
	}
	for _, r := range results {
		pending.Store(RequestItem{RequestID: r.RequestID, CustomID: r.CustomID, SubmittedAt: r.SubmittedAt})
	}

	ch := make(chan ResultItem, len(results))
	for _, r := range results {
		ch <- r
	}
	close(ch)

	err := collector.Drain(context.Background(), ch)
	if err == nil {
		t.Fatal("expected write failure error from Drain")
	}

	var remaining int
	pending.DrainUnresolved(func(_ RequestItem) {
		remaining++
	})
	if remaining != 0 {
		t.Fatalf("pending requests remaining = %d, want 0 (all should be resolved despite write failure)", remaining)
	}
}

func TestResultCollector_DrainCallsOnPersistenceFailure(t *testing.T) {
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	collector.output = bufio.NewWriterSize(&failAfterNWriter{remaining: 1}, 1)

	var callCount int
	collector.onPersistenceFailure = func() { callCount++ }

	results := []ResultItem{
		{RequestID: "req-1", CustomID: "c-1", Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-1", Body: map[string]any{"ok": true}}},
		{RequestID: "req-2", CustomID: "c-2", Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-2", Body: map[string]any{"ok": true}}},
		{RequestID: "req-3", CustomID: "c-3", Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-3", Body: map[string]any{"ok": true}}},
	}
	for _, r := range results {
		pending.Store(RequestItem{RequestID: r.RequestID, CustomID: r.CustomID})
	}

	ch := make(chan ResultItem, len(results))
	for _, r := range results {
		ch <- r
	}
	close(ch)

	err := collector.Drain(context.Background(), ch)
	if err == nil {
		t.Fatal("expected write failure error from Drain")
	}
	if callCount != 1 {
		t.Fatalf("onPersistenceFailure called %d times, want 1", callCount)
	}
}

func TestResultCollector_DrainReturnsNilWhenCtxCancelled(t *testing.T) {
	outputFile := tempFile(t)
	errorFile := tempFile(t)
	pending := NewPendingRequests(0)
	tracker := NewProgressTracker(2, nil, "test-job", 0, logr.Discard())
	collector := NewResultCollector(outputFile, errorFile, pending, tracker, logr.Discard())

	results := []ResultItem{
		{RequestID: "req-1", CustomID: "c-1", Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-1", Body: map[string]any{"ok": true}}},
		{RequestID: "req-2", CustomID: "c-2", Response: &batch_types.ResponseData{StatusCode: 200, RequestID: "req-2", Body: map[string]any{"ok": true}}},
	}
	for _, r := range results {
		pending.Store(RequestItem{RequestID: r.RequestID, CustomID: r.CustomID})
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ch := make(chan ResultItem, len(results))
	for _, r := range results {
		ch <- r
	}
	close(ch)

	err := collector.Drain(ctx, ch)
	if err != nil {
		t.Fatalf("Drain() = %v, want nil (all results drained despite cancelled ctx)", err)
	}

	counts := tracker.Counts()
	if counts.Completed != 2 {
		t.Fatalf("Completed = %d, want 2", counts.Completed)
	}
}

func TestProgressTracker_AddFailed(t *testing.T) {
	tracker := NewProgressTracker(10, nil, "test-job", 0, logr.Discard())
	tracker.AddFailed(5)
	counts := tracker.Counts()
	if counts.Failed != 5 {
		t.Fatalf("Failed = %d, want 5", counts.Failed)
	}
	if counts.Total != 10 {
		t.Fatalf("Total = %d, want 10", counts.Total)
	}
}

func TestProgressTracker_RecordSuccessAndFailure(t *testing.T) {
	tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())
	tracker.RecordSuccess(ResultItem{RequestID: "r1"})
	tracker.RecordSuccess(ResultItem{RequestID: "r2"})
	tracker.RecordFailure(nil)

	counts := tracker.Counts()
	if counts.Completed != 2 {
		t.Fatalf("Completed = %d, want 2", counts.Completed)
	}
	if counts.Failed != 1 {
		t.Fatalf("Failed = %d, want 1", counts.Failed)
	}
}

type countingUpdater struct {
	mu    sync.Mutex
	calls int
	last  *openai.BatchRequestCounts
}

func (u *countingUpdater) UpdateProgressCounts(_ context.Context, _ string, counts *openai.BatchRequestCounts) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	u.last = counts
	return nil
}

func (u *countingUpdater) getCalls() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

func (u *countingUpdater) getLast() *openai.BatchRequestCounts {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.last
}

func TestProgressTracker_Throttle(t *testing.T) {
	updater := &countingUpdater{}
	tracker := NewProgressTracker(100, updater, "test-job", 50*time.Millisecond, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = tracker.Run(ctx)
		close(done)
	}()

	for i := range 100 {
		if i%2 == 0 {
			tracker.RecordSuccess(ResultItem{RequestID: "r"})
		} else {
			tracker.RecordFailure(nil)
		}
	}

	// Let at least one tick fire.
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done

	calls := updater.getCalls()
	if calls == 0 {
		t.Fatal("expected at least one push")
	}
	if calls >= 100 {
		t.Fatalf("push calls = %d, want < 100 (throttling should batch updates)", calls)
	}

	last := updater.getLast()
	if last.Completed+last.Failed != 100 {
		t.Fatalf("final counts: completed=%d + failed=%d = %d, want 100",
			last.Completed, last.Failed, last.Completed+last.Failed)
	}
}

func TestProgressTracker_FlushOnCancel(t *testing.T) {
	updater := &countingUpdater{}
	tracker := NewProgressTracker(5, updater, "test-job", time.Hour, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = tracker.Run(ctx)
		close(done)
	}()

	tracker.RecordSuccess(ResultItem{RequestID: "r1"})
	tracker.RecordSuccess(ResultItem{RequestID: "r2"})
	tracker.RecordFailure(nil)

	// Interval is 1 hour, so no tick-based push will fire.
	// Cancel triggers the final push.
	cancel()
	<-done

	calls := updater.getCalls()
	if calls != 1 {
		t.Fatalf("push calls = %d, want 1 (final push on cancel)", calls)
	}
	last := updater.getLast()
	if last.Completed != 2 || last.Failed != 1 {
		t.Fatalf("final counts: completed=%d failed=%d, want completed=2 failed=1",
			last.Completed, last.Failed)
	}
}
