package pipeline

import (
	"context"
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
