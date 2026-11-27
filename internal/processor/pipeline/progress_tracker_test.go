package pipeline

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

func TestProgressTracker_Run(t *testing.T) {
	tests := []struct {
		name          string
		total         int64
		preFailed     int64
		preCompleted  int64
		wantFailed    int64
		wantCompleted int64
	}{
		{name: "pushes total before the first tick", total: 5},
		{name: "initial push includes rejected lines recorded before Run", total: 5, preFailed: 2, wantFailed: 2},
		{name: "initial push includes checkpointed successes recorded before Run", total: 5, preCompleted: 2, wantCompleted: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updater := &countingUpdater{}
			// The interval is long, so a push seen here cannot come from a tick.
			tracker := NewProgressTracker(tt.total, updater, "test-job", time.Hour, logr.Discard())
			tracker.AddFailed(tt.preFailed)
			tracker.AddCompleted(tt.preCompleted)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = tracker.Run(ctx)
			}()
			defer func() {
				cancel()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Error("Run did not return after cancel")
				}
			}()

			deadline := time.Now().Add(2 * time.Second)
			for updater.getCalls() == 0 && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if got := updater.getCalls(); got != 1 {
				t.Fatalf("push calls before first tick = %d, want 1", got)
			}

			last := updater.getLast()
			if last.Total != tt.total {
				t.Errorf("Total = %d, want %d", last.Total, tt.total)
			}
			if last.Completed != tt.wantCompleted {
				t.Errorf("Completed = %d, want %d", last.Completed, tt.wantCompleted)
			}
			if last.Failed != tt.wantFailed {
				t.Errorf("Failed = %d, want %d", last.Failed, tt.wantFailed)
			}
		})
	}
}

type countingUpdater struct {
	mu     sync.Mutex
	calls  int
	checks int
	last   *openai.BatchRequestCounts
	// failUpdates is the number of upcoming UpdateProgressCounts calls to fail.
	failUpdates int
}

func (u *countingUpdater) UpdateProgressCounts(_ context.Context, _ string, counts *openai.BatchRequestCounts) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.calls++
	if u.failUpdates > 0 {
		u.failUpdates--
		return errors.New("transient write failure")
	}
	u.last = counts
	return nil
}

func (u *countingUpdater) CheckJobStatus(context.Context, string) error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.checks++
	return nil
}

func (u *countingUpdater) getCalls() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.calls
}

func (u *countingUpdater) getChecks() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.checks
}

func (u *countingUpdater) getLast() *openai.BatchRequestCounts {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.last
}

// signalingUpdater records every push on a channel, so a test can wait for
// the exact push it cares about instead of polling.
type signalingUpdater struct {
	pushes chan *openai.BatchRequestCounts
}

func (u *signalingUpdater) UpdateProgressCounts(_ context.Context, _ string, counts *openai.BatchRequestCounts) error {
	u.pushes <- counts
	return nil
}

// CheckJobStatus is a no-op: this test exercises the push path only.
func (u *signalingUpdater) CheckJobStatus(context.Context, string) error { return nil }

// TestProgressTracker_MidRunCountsPushedOnTick verifies the tracker's core
// throttling contract: counts recorded while Run is executing are pushed on
// the next tick, not held until the final push. This is the deterministic
// replacement for the removed ProgressPolling e2e assertion (INFERENG-11391):
// the e2e test raced the deployment's progress interval against simulator
// response lengths, neither of which the test controlled.
func TestProgressTracker_MidRunCountsPushedOnTick(t *testing.T) {
	// The pushes channel is buffered well beyond this test's three pushes
	// so the tracker's tick loop never blocks on the test reader between
	// assertions (an unbuffered channel would stall Run on its next push).
	updater := &signalingUpdater{pushes: make(chan *openai.BatchRequestCounts, 16)}
	tracker := NewProgressTracker(2, updater, "test-job", 10*time.Millisecond, logr.Discard())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = tracker.Run(ctx)
	}()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	}()

	// First push: the initial counts (total visible before any result).
	initial := receivePush(t, updater.pushes)
	if initial.Total != 2 || initial.Completed != 0 || initial.Failed != 0 {
		t.Fatalf("initial push = %+v, want total=2 completed=0 failed=0", initial)
	}

	// A result recorded mid-run must be pushed by the next tick while Run is
	// still executing, not deferred to the final push.
	tracker.RecordSuccess(ResultItem{RequestID: "r1"})

	midRun := receivePush(t, updater.pushes)
	if midRun.Total != 2 || midRun.Completed != 1 || midRun.Failed != 0 {
		t.Fatalf("mid-run push = %+v, want total=2 completed=1 failed=0", midRun)
	}

	// Cancelling pushes the final counts and stops Run.
	cancel()
	<-done
	final := receivePush(t, updater.pushes)
	if final.Total != 2 || final.Completed != 1 || final.Failed != 0 {
		t.Fatalf("final push = %+v, want total=2 completed=1 failed=0", final)
	}
}

// receivePush waits for one push with a generous deadline; the tracker only
// pushes when dirty, so the arrival itself is the assertion.
func receivePush(t *testing.T, pushes <-chan *openai.BatchRequestCounts) *openai.BatchRequestCounts {
	t.Helper()
	select {
	case counts := <-pushes:
		return counts
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a progress push")
		return nil
	}
}

func TestProgressTracker_Tick(t *testing.T) {
	tests := []struct {
		name        string
		failUpdates int
		record      bool
		// done reports whether the tracker has reached the expected state.
		done func(u *countingUpdater) bool
		// wantCalls is the exact number of pushes expected once done.
		wantCalls int
	}{
		{
			name: "checks job status on quiet ticks without writing",
			done: func(u *countingUpdater) bool { return u.getChecks() >= 3 },
			// Only the initial push from Run; quiet ticks add none.
			wantCalls: 1,
		},
		{
			name:        "retries a failed push on the next tick without new results",
			failUpdates: 1,
			record:      true,
			done:        func(u *countingUpdater) bool { return u.getLast() != nil },
			wantCalls:   2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updater := &countingUpdater{failUpdates: tt.failUpdates}
			tracker := NewProgressTracker(1, updater, "test-job", 10*time.Millisecond, logr.Discard())
			if tt.record {
				tracker.RecordSuccess(ResultItem{RequestID: "r1"})
			}

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				_ = tracker.Run(ctx)
				close(done)
			}()

			deadline := time.Now().Add(5 * time.Second)
			for !tt.done(updater) {
				if time.Now().After(deadline) {
					cancel()
					<-done
					t.Fatalf("tracker did not reach the expected state: pushes=%d checks=%d", updater.getCalls(), updater.getChecks())
				}
				time.Sleep(5 * time.Millisecond)
			}
			// Read before cancel: the final push on cancel adds a call.
			calls := updater.getCalls()
			cancel()
			<-done

			if calls != tt.wantCalls {
				t.Fatalf("pushes = %d, want %d", calls, tt.wantCalls)
			}
		})
	}
}
