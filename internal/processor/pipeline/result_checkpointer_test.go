package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

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
			if _, ok := pending.m.Load(msg.RequestID); !ok {
				return false, fmt.Errorf("checkpoint for %s ran after resolve", msg.RequestID)
			}
			return true, nil
		}
		c := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

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
		c := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

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
		c := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

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
		c := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

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
		c := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

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
		c := NewResultCheckpointer(sink, pending, tracker, logr.Discard())

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
		return NewResultCheckpointer(sink, pending, NewProgressTracker(4, nil, "test-job", 0, logr.Discard()), logr.Discard())
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
