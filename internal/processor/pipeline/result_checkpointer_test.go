package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

type checkpointRecord struct {
	msg  ResultItem
	line []byte
	sha  string
	size int64
}

// fakeSink records every checkpoint. fn, when set, decides the outcome
// instead of the default commit; records are kept only for committed ones.
type fakeSink struct {
	mu      sync.Mutex
	records []checkpointRecord
	fn      func(msg *ResultItem) (bool, error)
}

var _ ResultSink = (*fakeSink)(nil)

func (s *fakeSink) Checkpoint(_ context.Context, msg *ResultItem, line []byte, sha string, size int64) (bool, error) {
	if s.fn != nil {
		committed, err := s.fn(msg)
		if committed && err == nil {
			s.record(msg, line, sha, size)
		}
		return committed, err
	}
	s.record(msg, line, sha, size)
	return true, nil
}

func (s *fakeSink) record(msg *ResultItem, line []byte, sha string, size int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = append(s.records, checkpointRecord{msg: *msg, line: line, sha: sha, size: size})
}

func (s *fakeSink) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

// recorded returns a copy of the committed checkpoint records.
func (s *fakeSink) recorded() []checkpointRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]checkpointRecord(nil), s.records...)
}

func successResult(id string) ResultItem {
	return ResultItem{
		RequestID: id,
		Response:  &batch_types.ResponseData{StatusCode: 200, RequestID: id, Body: map[string]any{"ok": true}},
	}
}

func TestResultCheckpointer_Drain(t *testing.T) {
	t.Run("checkpoints each result before resolving it", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := NewProgressTracker(3, nil, "test-job", 0, logr.Discard())

		// The sink verifies the ordering contract: while Checkpoint runs,
		// the request must still be pending (Resolve comes afterwards).
		sink := &fakeSink{}
		sink.fn = func(msg *ResultItem) (bool, error) {
			if !pending.Has(msg.RequestID) {
				return false, fmt.Errorf("checkpoint for %s ran after resolve", msg.RequestID)
			}
			return true, nil
		}
		c := NewResultCheckpointer(sink, pending, tracker, 1, logr.Discard())

		results := []ResultItem{successResult("req-1"), successResult("req-2"), {
			RequestID: "req-3",
			Error:     &OutputError{Code: "server_error", Message: "boom"},
		}}
		for _, r := range results {
			pending.Store(RequestItem{RequestID: r.RequestID, CustomID: "c-" + r.RequestID, ModelID: "m1"})
		}

		ch := make(chan ResultItem, len(results))
		for _, r := range results {
			ch <- r
		}
		close(ch)

		if err := c.Drain(context.Background(), ch); err != nil {
			t.Fatalf("Drain error: %v", err)
		}

		if got := sink.len(); got != 3 {
			t.Fatalf("checkpointed = %d, want 3", got)
		}
		if ids := pending.IDs(); len(ids) != 0 {
			t.Fatalf("pending after drain = %v, want empty", ids)
		}
		counts := tracker.Counts()
		if counts.Completed != 2 || counts.Failed != 1 {
			t.Fatalf("counts = completed %d / failed %d, want 2 / 1", counts.Completed, counts.Failed)
		}
	})

	t.Run("passes enriched metadata, line bytes, digest and size", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := NewProgressTracker(1, nil, "test-job", 0, logr.Discard())
		sink := &fakeSink{}
		c := NewResultCheckpointer(sink, pending, tracker, 1, logr.Discard())

		pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1", ModelID: "m1"})
		ch := make(chan ResultItem, 1)
		ch <- successResult("req-1")
		close(ch)

		if err := c.Drain(context.Background(), ch); err != nil {
			t.Fatalf("Drain error: %v", err)
		}

		sink.mu.Lock()
		rec := sink.records[0]
		sink.mu.Unlock()

		if rec.msg.CustomID != "c-1" || rec.msg.ModelID != "m1" {
			t.Fatalf("sink got unenriched result: %+v", rec.msg)
		}
		var line outputLine
		if err := json.Unmarshal(rec.line, &line); err != nil {
			t.Fatalf("line is not valid JSON: %v", err)
		}
		if line.CustomID != "c-1" || line.ID != "req-1" {
			t.Fatalf("line = %+v, want custom_id c-1 and id req-1", line)
		}
		if rec.line[len(rec.line)-1] != '\n' {
			t.Fatal("line has no trailing newline")
		}
		if rec.size != int64(len(rec.line)) {
			t.Fatalf("size = %d, want %d", rec.size, len(rec.line))
		}
		sum := sha256.Sum256(rec.line)
		if rec.sha != hex.EncodeToString(sum[:]) {
			t.Fatalf("sha256 mismatch: got %s", rec.sha)
		}
	})

	t.Run("failed checkpoint leaves the request pending", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := NewProgressTracker(1, nil, "test-job", 0, logr.Discard())
		sink := &fakeSink{}
		sink.fn = func(*ResultItem) (bool, error) { return false, errors.New("s3 down") }
		c := NewResultCheckpointer(sink, pending, tracker, 1, logr.Discard())

		var hookCalls int
		c.onPersistenceFailure = func() { hookCalls++ }

		pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1"})
		ch := make(chan ResultItem, 1)
		ch <- successResult("req-1")
		close(ch)

		if err := c.Drain(context.Background(), ch); err == nil {
			t.Fatal("expected checkpoint failure to surface from Drain")
		}
		if ids := pending.IDs(); len(ids) != 1 || ids[0] != "req-1" {
			t.Fatalf("pending after failed checkpoint = %v, want [req-1]", ids)
		}
		if hookCalls != 1 {
			t.Fatalf("onPersistenceFailure called %d times, want 1", hookCalls)
		}
		if counts := tracker.Counts(); counts.Completed != 0 || counts.Failed != 0 {
			t.Fatalf("counts = %+v, want zero (entry not committed)", counts)
		}
	})

	t.Run("first failure is sticky", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := NewProgressTracker(2, nil, "test-job", 0, logr.Discard())
		sink := &fakeSink{}
		var calls int
		sink.fn = func(*ResultItem) (bool, error) {
			calls++
			if calls == 1 {
				return false, errors.New("s3 down")
			}
			return true, nil
		}
		c := NewResultCheckpointer(sink, pending, tracker, 1, logr.Discard())

		for _, id := range []string{"req-1", "req-2"} {
			pending.Store(RequestItem{RequestID: id, CustomID: "c-" + id})
		}
		ch := make(chan ResultItem, 2)
		ch <- successResult("req-1")
		ch <- successResult("req-2")
		close(ch)

		if err := c.Drain(context.Background(), ch); err == nil {
			t.Fatal("expected sticky error from Drain")
		}
		if calls != 1 {
			t.Fatalf("checkpoint attempts = %d, want 1 (no retries after failure)", calls)
		}
		if got := sink.len(); got != 0 {
			t.Fatalf("committed checkpoints = %d, want 0", got)
		}
	})

	t.Run("duplicate delivery resolves without recording", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := NewProgressTracker(1, nil, "test-job", 0, logr.Discard())
		sink := &fakeSink{}
		sink.fn = func(*ResultItem) (bool, error) { return false, nil }
		c := NewResultCheckpointer(sink, pending, tracker, 1, logr.Discard())

		pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1"})
		ch := make(chan ResultItem, 1)
		ch <- successResult("req-1")
		close(ch)

		if err := c.Drain(context.Background(), ch); err != nil {
			t.Fatalf("Drain error: %v", err)
		}
		if ids := pending.IDs(); len(ids) != 0 {
			t.Fatalf("pending after duplicate = %v, want empty (still resolved)", ids)
		}
		if counts := tracker.Counts(); counts.Completed != 0 || counts.Failed != 0 {
			t.Fatalf("counts = %+v, want zero (already recorded by the first delivery)", counts)
		}
	})

	t.Run("ownership conflict aborts dispatch and returns ErrConflict", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := NewProgressTracker(1, nil, "test-job", 0, logr.Discard())
		sink := &fakeSink{}
		sink.fn = func(*ResultItem) (bool, error) {
			return false, fmt.Errorf("checkpoint entry: %w", db.ErrConflict)
		}
		c := NewResultCheckpointer(sink, pending, tracker, 1, logr.Discard())

		var hookCalls int
		c.onPersistenceFailure = func() { hookCalls++ }

		pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1"})
		ch := make(chan ResultItem, 1)
		ch <- successResult("req-1")
		close(ch)

		err := c.Drain(context.Background(), ch)
		if !errors.Is(err, db.ErrConflict) {
			t.Fatalf("Drain error = %v, want db.ErrConflict", err)
		}
		if hookCalls != 1 {
			t.Fatalf("dispatch abort hook called %d times, want 1", hookCalls)
		}
	})
}

// sinkOutputBytes reconstructs the committed response-carrying result lines,
// mirroring the bytes the legacy pipeline wrote to output.jsonl.
func sinkOutputBytes(t *testing.T, sink *fakeSink) []byte {
	t.Helper()
	return sinkLines(t, sink, false)
}

// sinkErrorBytes reconstructs the committed error-object result lines,
// mirroring the bytes the legacy pipeline wrote to error.jsonl.
func sinkErrorBytes(t *testing.T, sink *fakeSink) []byte {
	t.Helper()
	return sinkLines(t, sink, true)
}

func sinkLines(t *testing.T, sink *fakeSink, wantErr bool) []byte {
	t.Helper()
	var buf []byte
	for _, r := range sink.recorded() {
		if (r.msg.Error != nil) != wantErr {
			continue
		}
		line, _, _, err := MarshalResultLine(&r.msg)
		if err != nil {
			t.Fatalf("marshal result line: %v", err)
		}
		buf = append(buf, line...)
	}
	return buf
}

// ackTracker records durable-delivery acknowledgements and their ordering
// against checkpoint events.
type ackTracker struct {
	mu     sync.Mutex
	events []string
}

func (t *ackTracker) checkpoint(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, "checkpoint:"+id)
}

func (t *ackTracker) ackFor(id string) func(context.Context) error {
	return func(context.Context) error {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.events = append(t.events, "ack:"+id)
		return nil
	}
}

func (t *ackTracker) acked() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	var ids []string
	for _, e := range t.events {
		if id, ok := strings.CutPrefix(e, "ack:"); ok {
			ids = append(ids, id)
		}
	}
	return ids
}

func (t *ackTracker) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]string(nil), t.events...)
}

// TestResultCheckpointer_AckAfterCheckpoint covers the durable-delivery
// acknowledgement contract: an ack is released only once the result is
// durably accounted for — never before the checkpoint, never for a failed
// checkpoint, and never for a result another job owns.
func TestResultCheckpointer_AckAfterCheckpoint(t *testing.T) {
	newCheckpointer := func(sink *fakeSink, pending *PendingRequests) *ResultCheckpointer {
		return NewResultCheckpointer(sink, pending, NewProgressTracker(4, nil, "test-job", 0, logr.Discard()), 1, logr.Discard())
	}

	t.Run("acks a committed checkpoint and a dropped duplicate, after the checkpoint", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := new(ackTracker)
		sink := &fakeSink{}
		sink.fn = func(msg *ResultItem) (bool, error) {
			tracker.checkpoint(msg.RequestID)
			if msg.RequestID == "req-dup" {
				return false, nil // duplicate delivery: already terminal
			}
			return true, nil
		}
		c := newCheckpointer(sink, pending)

		for _, id := range []string{"req-1", "req-dup"} {
			pending.Store(RequestItem{RequestID: id, CustomID: "c-" + id})
		}
		ch := make(chan ResultItem, 2)
		for _, id := range []string{"req-1", "req-dup"} {
			r := successResult(id)
			r.Ack = tracker.ackFor(id)
			ch <- r
		}
		close(ch)

		if err := c.Drain(context.Background(), ch); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if got := tracker.acked(); len(got) != 2 {
			t.Fatalf("acked = %v, want both results (a duplicate is durably accounted by its earlier delivery)", got)
		}
		// The ack of each result follows its checkpoint.
		events := tracker.snapshot()
		for _, id := range []string{"req-1", "req-dup"} {
			var cp, ak = -1, -1
			for i, e := range events {
				if e == "checkpoint:"+id {
					cp = i
				}
				if e == "ack:"+id {
					ak = i
				}
			}
			if cp == -1 || ak == -1 || ak < cp {
				t.Fatalf("events = %v: %s must be acked after its checkpoint", events, id)
			}
		}
	})

	t.Run("leaves a failed checkpoint unacked for redelivery", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := new(ackTracker)
		sink := &fakeSink{}
		sink.fn = func(msg *ResultItem) (bool, error) {
			tracker.checkpoint(msg.RequestID)
			return false, errors.New("s3 down")
		}
		c := newCheckpointer(sink, pending)

		pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1"})
		ch := make(chan ResultItem, 1)
		r := successResult("req-1")
		r.Ack = tracker.ackFor("req-1")
		ch <- r
		close(ch)

		if err := c.Drain(context.Background(), ch); err == nil {
			t.Fatal("expected the checkpoint failure to surface")
		}
		if got := tracker.acked(); len(got) != 0 {
			t.Fatalf("acked = %v, want none (lease expiry must redeliver the result)", got)
		}
	})

	t.Run("leaves an ownership conflict unacked", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := new(ackTracker)
		sink := &fakeSink{}
		sink.fn = func(msg *ResultItem) (bool, error) {
			return false, fmt.Errorf("entry %s: %w", msg.RequestID, db.ErrConflict)
		}
		c := newCheckpointer(sink, pending)

		pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1"})
		ch := make(chan ResultItem, 1)
		r := successResult("req-1")
		r.Ack = tracker.ackFor("req-1")
		ch <- r
		close(ch)

		if err := c.Drain(context.Background(), ch); !errors.Is(err, db.ErrConflict) {
			t.Fatalf("Drain error = %v, want ErrConflict", err)
		}
		if got := tracker.acked(); len(got) != 0 {
			t.Fatalf("acked = %v, want none (the new owner consumes the redelivery)", got)
		}
	})

	t.Run("does not acknowledge a result another job owns", func(t *testing.T) {
		pending := NewPendingRequests(0)
		tracker := new(ackTracker)
		c := newCheckpointer(&fakeSink{}, pending)

		// req-foreign was never submitted by this job. The per-request
		// routing keeps foreign results out of this channel; the guard is
		// defense in depth for any path that still delivers one.
		ch := make(chan ResultItem, 1)
		r := successResult("req-foreign")
		r.CustomID = "" // async shape: enriched only when pending
		r.Ack = tracker.ackFor("req-foreign")
		ch <- r
		close(ch)

		if err := c.Drain(context.Background(), ch); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if got := tracker.acked(); len(got) != 0 {
			t.Fatalf("acked = %v, want none (the owning job acknowledges)", got)
		}
	})

	t.Run("a failed ack is tolerated", func(t *testing.T) {
		pending := NewPendingRequests(0)
		sink := &fakeSink{}
		c := newCheckpointer(sink, pending)

		pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1"})
		ch := make(chan ResultItem, 1)
		r := successResult("req-1")
		r.Ack = func(context.Context) error { return errors.New("lease expired") }
		ch <- r
		close(ch)

		if err := c.Drain(context.Background(), ch); err != nil {
			t.Fatalf("Drain: %v (a failed ack only risks a duplicate redelivery)", err)
		}
		if got := sink.len(); got != 1 {
			t.Fatalf("committed checkpoints = %d, want 1", got)
		}
	})
}

// TestResultCheckpointer_Concurrency covers the parallel drain: distinct
// requests are checkpointed concurrently up to the configured bound, while
// deliveries of one request stay serial.
func TestResultCheckpointer_Concurrency(t *testing.T) {
	t.Run("checkpoints distinct requests in parallel up to the bound", func(t *testing.T) {
		const bound, total = 4, 12
		pending := NewPendingRequests(0)
		var mu sync.Mutex
		running, peak := 0, 0
		atBound := make(chan struct{})
		sink := &fakeSink{}
		sink.fn = func(*ResultItem) (bool, error) {
			mu.Lock()
			running++
			if running > peak {
				peak = running
			}
			if running == bound {
				select {
				case <-atBound:
				default:
					close(atBound)
				}
			}
			mu.Unlock()
			// Hold until the bound is reached: a serial drain never gets there.
			select {
			case <-atBound:
			case <-time.After(5 * time.Second):
			}
			mu.Lock()
			running--
			mu.Unlock()
			return true, nil
		}
		c := NewResultCheckpointer(sink, pending, NewProgressTracker(total, nil, "test-job", 0, logr.Discard()), bound, logr.Discard())

		ch := make(chan ResultItem, total)
		for i := range total {
			id := fmt.Sprintf("req-%d", i)
			pending.Store(RequestItem{RequestID: id, CustomID: "c-" + id})
			ch <- successResult(id)
		}
		close(ch)

		if err := c.Drain(context.Background(), ch); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if peak != bound {
			t.Fatalf("peak concurrent checkpoints = %d, want %d", peak, bound)
		}
		if got := len(sink.records); got != total {
			t.Fatalf("checkpointed %d results, want %d", got, total)
		}
		if n := pending.pendingCount(); n != 0 {
			t.Fatalf("%d requests still pending after drain", n)
		}
	})

	t.Run("checkpoints duplicate deliveries of one request serially", func(t *testing.T) {
		pending := NewPendingRequests(0)
		var mu sync.Mutex
		running, overlapped, calls := 0, false, 0
		sink := &fakeSink{}
		sink.fn = func(*ResultItem) (bool, error) {
			mu.Lock()
			calls++
			running++
			if running > 1 {
				overlapped = true
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
			mu.Lock()
			running--
			mu.Unlock()
			return true, nil
		}
		c := NewResultCheckpointer(sink, pending, NewProgressTracker(1, nil, "test-job", 0, logr.Discard()), 8, logr.Discard())

		pending.Store(RequestItem{RequestID: "req-1", CustomID: "c-1"})
		ch := make(chan ResultItem, 3)
		for range 3 {
			ch <- successResult("req-1")
		}
		close(ch)

		if err := c.Drain(context.Background(), ch); err != nil {
			t.Fatalf("Drain: %v", err)
		}
		if overlapped {
			t.Fatal("deliveries of the same request were checkpointed concurrently")
		}
		// The first delivery resolves the request; the later ones no longer
		// match a pending entry and are dropped without a checkpoint.
		if calls != 1 {
			t.Fatalf("sink called %d times, want 1", calls)
		}
	})

	// A checkpoint fails while another is running: the running one completes
	// and is acknowledged, and one that has not started is skipped and left
	// unacknowledged for redelivery.
	failures := []struct {
		name string
		err  error
	}{
		{name: "persistence failure with another checkpoint in flight", err: errors.New("s3 down")},
		{name: "ownership conflict with another checkpoint in flight", err: fmt.Errorf("checkpoint entry: %w", db.ErrConflict)},
	}
	for _, tc := range failures {
		t.Run(tc.name, func(t *testing.T) {
			pending := NewPendingRequests(0)
			acks := new(ackTracker)
			inFlight := make(chan struct{})
			failed := make(chan struct{})
			var mu sync.Mutex
			var calls []string
			sink := &fakeSink{}
			sink.fn = func(msg *ResultItem) (bool, error) {
				mu.Lock()
				calls = append(calls, msg.RequestID)
				mu.Unlock()
				switch msg.RequestID {
				case "req-running":
					close(inFlight)
					// Complete only after the other checkpoint has failed.
					<-failed
					return true, nil
				case "req-failing":
					<-inFlight
					return false, tc.err
				default:
					return true, nil
				}
			}
			c := NewResultCheckpointer(sink, pending, NewProgressTracker(3, nil, "test-job", 0, logr.Discard()), 2, logr.Discard())
			var hookCalls int
			// The hook runs once the failure is recorded.
			c.onPersistenceFailure = func() {
				hookCalls++
				close(failed)
			}

			// With a bound of 2, req-late starts only when the failing
			// checkpoint has released its slot.
			ids := []string{"req-running", "req-failing", "req-late"}
			ch := make(chan ResultItem, len(ids))
			for _, id := range ids {
				pending.Store(RequestItem{RequestID: id, CustomID: "c-" + id})
				r := successResult(id)
				r.Ack = acks.ackFor(id)
				ch <- r
			}
			close(ch)

			if err := c.Drain(context.Background(), ch); !errors.Is(err, tc.err) {
				t.Fatalf("Drain error = %v, want %v", err, tc.err)
			}
			if hookCalls != 1 {
				t.Fatalf("dispatch abort hook called %d times, want 1", hookCalls)
			}
			if got := calls; len(got) != 2 {
				t.Fatalf("sink called for %v, want only the two checkpoints that started before the failure", got)
			}
			recorded := sink.recorded()
			if len(recorded) != 1 || recorded[0].msg.RequestID != "req-running" {
				t.Fatalf("committed checkpoints = %d, want only req-running", len(recorded))
			}
			if got := acks.acked(); len(got) != 1 || got[0] != "req-running" {
				t.Fatalf("acked = %v, want [req-running]", got)
			}
			left := pending.IDs()
			slices.Sort(left)
			if !slices.Equal(left, []string{"req-failing", "req-late"}) {
				t.Fatalf("pending after drain = %v, want [req-failing req-late]", left)
			}
		})
	}
}
