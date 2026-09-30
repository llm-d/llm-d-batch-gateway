package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

const defaultProgressInterval = time.Second

// ProgressUpdater pushes progress counts to a status store.
type ProgressUpdater interface {
	UpdateProgressCounts(ctx context.Context, jobID string, counts *openai.BatchRequestCounts) error
}

// ProgressTracker tracks request completion counts and pushes throttled
// updates to the status store.
type ProgressTracker struct {
	mu        sync.Mutex
	total     int64
	completed int64
	failed    int64
	dirty     bool
	updater   ProgressUpdater
	jobID     string
	interval  time.Duration
	logger    logr.Logger
}

func NewProgressTracker(total int64, updater ProgressUpdater, jobID string, interval time.Duration, logger logr.Logger) *ProgressTracker {
	if interval <= 0 {
		interval = defaultProgressInterval
	}
	return &ProgressTracker{
		total:    total,
		updater:  updater,
		jobID:    jobID,
		interval: interval,
		logger:   logger,
	}
}

// RecordSuccess records a successful result.
func (pt *ProgressTracker) RecordSuccess(msg ResultItem) {
	pt.mu.Lock()
	pt.completed++
	pt.dirty = true
	pt.mu.Unlock()
}

// RecordFailure records a failed request.
func (pt *ProgressTracker) RecordFailure(err error) {
	pt.mu.Lock()
	pt.failed++
	pt.dirty = true
	pt.mu.Unlock()
}

// Run starts the ticker that pushes throttled updates to the status store.
// Returns when ctx is cancelled, after pushing final counts.
func (pt *ProgressTracker) Run(ctx context.Context) error {
	ticker := time.NewTicker(pt.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			pt.push(finalCtx)
			return nil
		case <-ticker.C:
			pt.mu.Lock()
			dirty := pt.dirty
			pt.dirty = false
			pt.mu.Unlock()
			if dirty {
				pt.push(ctx)
			}
		}
	}
}

// Counts returns the current request counts. Safe to call after Run returns.
func (pt *ProgressTracker) Counts() *openai.BatchRequestCounts {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return &openai.BatchRequestCounts{
		Total:     pt.total,
		Completed: pt.completed,
		Failed:    pt.failed,
	}
}

// AddFailed adds to the failed counter and marks the tracker as dirty
// so the next tick pushes the update.
func (pt *ProgressTracker) AddFailed(n int64) {
	pt.mu.Lock()
	pt.failed += n
	pt.dirty = true
	pt.mu.Unlock()
}

// Seed restores counts already durably checkpointed by an earlier processor.
func (pt *ProgressTracker) Seed(completed, failed int64) {
	pt.mu.Lock()
	pt.completed += completed
	pt.failed += failed
	pt.dirty = true
	pt.mu.Unlock()
}

func (pt *ProgressTracker) push(ctx context.Context) {
	if pt.updater == nil {
		return
	}
	if err := pt.updater.UpdateProgressCounts(ctx, pt.jobID, pt.Counts()); err != nil {
		pt.logger.Error(err, "Failed to update progress counts (best-effort)")
	}
}
