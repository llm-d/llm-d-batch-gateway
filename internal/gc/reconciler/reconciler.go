/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package reconciler detects and recovers orphaned batch jobs that are stuck
// in non-terminal states because their processor crashed or was deleted.
package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batch_utils"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

const pageSize = 100

// Result contains the outcome of a single reconciliation cycle.
type Result struct {
	Expired    int
	ReEnqueued int
	Conflicts  int
	Errors     int
	Duration   time.Duration
}

// Reconciler detects orphaned batch jobs and recovers them. A job is
// considered orphaned when it has a processor_id set but that processor
// is no longer in the live set (maintained by the pod watcher).
//
// The reconciler runs on two triggers:
//   - Event-driven: the pod watcher calls Trigger() on pod deletion
//   - Periodic: a backstop timer fires every interval as a safety net
type Reconciler struct {
	batchDB         db.BatchDBClient
	queue           db.BatchPriorityQueueClient
	interval        time.Duration
	dryRun          bool
	onCycleComplete func(*Result)
	// refreshLive, when set, re-reads the set of currently-Ready processor pods.
	// It is invoked at the start of each cycle so the live set reflects actual
	// pod readiness rather than only the last StatefulSet-stable snapshot — a
	// freshly-Ready replica is then never mistaken for a dead owner.
	refreshLive func(ctx context.Context) (map[string]bool, error)

	mu             sync.RWMutex
	liveProcessors map[string]bool
	snapshotReady  bool

	triggerCh chan struct{}
}

// NewReconciler creates a new orphan reconciler.
func NewReconciler(
	batchDB db.BatchDBClient,
	queue db.BatchPriorityQueueClient,
	interval time.Duration,
	dryRun bool,
	onCycleComplete func(*Result),
) (*Reconciler, error) {
	if batchDB == nil {
		return nil, fmt.Errorf("batchDB client is required")
	}
	if queue == nil {
		return nil, fmt.Errorf("queue client is required")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("interval must be positive, got %v", interval)
	}
	return &Reconciler{
		batchDB:         batchDB,
		queue:           queue,
		interval:        interval,
		dryRun:          dryRun,
		onCycleComplete: onCycleComplete,
		liveProcessors:  make(map[string]bool),
		triggerCh:       make(chan struct{}, 1),
	}, nil
}

// SetLiveProcessors updates the set of currently alive processor pod names.
// Called by the pod watcher on add/delete events.
func (r *Reconciler) SetLiveProcessors(processors map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.liveProcessors = processors
	r.snapshotReady = true
}

// SetLivePodRefresher installs a callback that re-reads the currently-Ready
// processor pods. The reconciler invokes it at the start of each cycle so the
// live set stays fresh even before the StatefulSet reports stable.
func (r *Reconciler) SetLivePodRefresher(fn func(ctx context.Context) (map[string]bool, error)) {
	r.refreshLive = fn
}

func (r *Reconciler) hasSnapshot() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.snapshotReady
}

// Trigger requests an immediate reconciliation cycle. Non-blocking.
func (r *Reconciler) Trigger() {
	select {
	case r.triggerCh <- struct{}{}:
	default:
	}
}

func (r *Reconciler) isProcessorAlive(processorID string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.liveProcessors[processorID]
}

// RunLoop runs the reconciler on both event triggers and a periodic timer.
// It blocks until the context is cancelled.
func (r *Reconciler) RunLoop(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("Reconciler: starting loop", "interval", r.interval)

	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Reconciler: loop stopped")
			return ctx.Err()
		case <-ticker.C:
			r.run(ctx)
		case <-r.triggerCh:
			r.run(ctx)
		}
	}
}

// run executes a single reconciliation cycle.
func (r *Reconciler) run(ctx context.Context) {
	logger := logr.FromContextOrDiscard(ctx)
	start := time.Now()
	result := &Result{}

	defer func() {
		result.Duration = time.Since(start)
		logger.Info("Reconciler: cycle completed",
			"expired", result.Expired,
			"reEnqueued", result.ReEnqueued,
			"conflicts", result.Conflicts,
			"errors", result.Errors,
			"duration", result.Duration,
		)
		r.notifyCycle(result)
	}()

	// Refresh the live set from currently-Ready pods before triaging. This keeps
	// a freshly-Ready replica (whose StatefulSet hasn't reported stable yet) from
	// being mistaken for a dead owner. On error, fall back to the last snapshot.
	if r.refreshLive != nil {
		live, err := r.refreshLive(ctx)
		if err != nil {
			logger.Error(err, "Reconciler: live pod refresh failed; using last snapshot")
		} else {
			r.SetLiveProcessors(live)
		}
	}

	if !r.hasSnapshot() {
		logger.Info("Reconciler: no live processor snapshot yet, skipping cycle")
		return
	}

	jobs, err := r.fetchNonTerminalJobs(ctx)
	if err != nil {
		logger.Error(err, "Reconciler: failed to fetch non-terminal jobs")
		result.Errors++
		return
	}

	for _, job := range jobs {
		if !r.isProcessorAlive(job.ProcessorID) {
			r.triageOrphan(ctx, job, result)
		}
	}
}

func (r *Reconciler) notifyCycle(result *Result) {
	if r.onCycleComplete != nil {
		r.onCycleComplete(result)
	}
}

// fetchNonTerminalJobs paginates through non-terminal batch items that are
// owned by a processor (processor_id IS NOT NULL). Queued jobs (processor_id
// IS NULL) are excluded since they are not orphans.
func (r *Reconciler) fetchNonTerminalJobs(ctx context.Context) ([]*db.BatchItem, error) {
	var all []*db.BatchItem
	cursor := 0
	for {
		items, nextCursor, more, err := r.batchDB.DBGet(ctx,
			&db.BatchQuery{NonTerminal: true, HasProcessorID: true},
			false, cursor, pageSize)
		if err != nil {
			return nil, err
		}
		all = append(all, items...)
		if !more {
			break
		}
		cursor = nextCursor
	}
	return all, nil
}

func (r *Reconciler) triageOrphan(ctx context.Context, job *db.BatchItem, result *Result) {
	ctx = logr.NewContext(ctx, logr.FromContextOrDiscard(ctx).WithValues("jobId", job.ID))
	ctx, span := uotel.StartSpan(ctx, "reconciler.triage")
	defer span.End()
	span.SetAttributes(attribute.String(uotel.AttrBatchID, job.ID))
	logger := logr.FromContextOrDiscard(ctx)

	var statusInfo openai.BatchStatusInfo
	if err := json.Unmarshal(job.Status, &statusInfo); err != nil {
		logger.Error(err, "Reconciler: failed to unmarshal orphan status")
		result.Errors++
		return
	}
	span.SetAttributes(attribute.String("batch.status", string(statusInfo.Status)))

	switch statusInfo.Status {
	case openai.BatchStatusCancelling:
		r.transitionOrphan(ctx, job, &statusInfo, openai.BatchStatusCancelled, result, logger)
	default:
		if isSLOExpired(job) {
			r.transitionOrphan(ctx, job, &statusInfo, openai.BatchStatusFailed, result, logger)
		} else {
			r.reEnqueueOrphan(ctx, job, result, logger)
		}
	}
}

// transitionOrphan transitions an orphaned job to the given terminal status.
func (r *Reconciler) transitionOrphan(ctx context.Context, job *db.BatchItem, statusInfo *openai.BatchStatusInfo, target openai.BatchStatus, result *Result, logger logr.Logger) {
	updatedStatus, err := batch_utils.BuildUpdatedStatusInfo(statusInfo, target, nil, nil)
	if err != nil {
		logger.Error(err, "Reconciler: failed to build target status", "target", target)
		result.Errors++
		return
	}

	updatedBytes, err := json.Marshal(updatedStatus)
	if err != nil {
		logger.Error(err, "Reconciler: failed to marshal target status", "target", target)
		result.Errors++
		return
	}

	if r.dryRun {
		logger.Info("Reconciler: dry-run: would transition orphan", "target", target)
		result.Expired++
		return
	}

	updateItem := &db.BatchItem{
		BaseIndexes:  db.BaseIndexes{ID: job.ID},
		BaseContents: db.BaseContents{Status: updatedBytes},
		Epoch:        job.Epoch,
		BumpEpoch:    true,
	}
	if err := r.batchDB.DBUpdate(ctx, updateItem, job.Status); err != nil {
		if errors.Is(err, db.ErrConflict) {
			logger.Info("Reconciler: CAS conflict during orphan transition", "target", target)
			result.Conflicts++
		} else {
			logger.Error(err, "Reconciler: failed to transition orphan", "target", target)
			result.Errors++
		}
		return
	}

	logger.Info("Reconciler: orphan transitioned", "target", target)
	result.Expired++
}

// reEnqueueOrphan re-enqueues an orphaned job whose SLO is still valid.
func (r *Reconciler) reEnqueueOrphan(ctx context.Context, job *db.BatchItem, result *Result, logger logr.Logger) {
	if job.Priority <= 0 {
		logger.Error(fmt.Errorf("missing priority"), "Reconciler: cannot re-enqueue orphan without priority")
		result.Errors++
		return
	}

	slo := time.UnixMicro(job.Priority)

	if r.dryRun {
		logger.Info("Reconciler: dry-run: would re-enqueue orphan", "slo", slo)
		result.ReEnqueued++
		return
	}

	task := &db.BatchJobPriority{
		ID:  job.ID,
		SLO: slo,
	}
	if err := r.queue.PQEnqueue(ctx, task); err != nil {
		logger.Error(err, "Reconciler: failed to re-enqueue orphan")
		result.Errors++
		return
	}

	logger.Info("Reconciler: orphan re-enqueued", "slo", slo)
	result.ReEnqueued++
}

// isSLOExpired checks whether the job's SLO deadline has passed.
func isSLOExpired(job *db.BatchItem) bool {
	if job.Priority <= 0 {
		return false
	}
	return time.Now().After(time.UnixMicro(job.Priority))
}
