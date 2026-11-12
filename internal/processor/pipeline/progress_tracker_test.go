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
