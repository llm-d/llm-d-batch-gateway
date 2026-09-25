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

package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

const testInterval = 60 * time.Minute

func futureSLO() int64 {
	return time.Now().Add(24 * time.Hour).UnixMicro()
}

func expiredSLO() int64 {
	return time.Now().Add(-1 * time.Hour).UnixMicro()
}

func newTestBatchItem(id, processorID string, status openai.BatchStatus, priority int64) *db.BatchItem {
	statusBytes, _ := json.Marshal(openai.BatchStatusInfo{Status: status})
	return &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID: id,
		},
		BaseContents: db.BaseContents{
			Status: statusBytes,
		},
		ProcessorID: processorID,
		Priority:    priority,
	}
}

func newTestReconciler(
	t *testing.T,
	batchDB db.BatchDBClient,
	queue db.BatchPriorityQueueClient,
) (*Reconciler, chan *Result) {
	t.Helper()
	resultCh := make(chan *Result, 1)
	r, err := NewReconciler(batchDB, queue, testInterval, false, func(res *Result) {
		resultCh <- res
	})
	if err != nil {
		t.Fatalf("failed to create reconciler: %v", err)
	}
	return r, resultCh
}

func newTestDryRunReconciler(
	t *testing.T,
	batchDB db.BatchDBClient,
	queue db.BatchPriorityQueueClient,
) (*Reconciler, chan *Result) {
	t.Helper()
	resultCh := make(chan *Result, 1)
	r, err := NewReconciler(batchDB, queue, testInterval, true, func(res *Result) {
		resultCh <- res
	})
	if err != nil {
		t.Fatalf("failed to create dry-run reconciler: %v", err)
	}
	return r, resultCh
}

func storeItems(t *testing.T, batchDB db.BatchDBClient, items ...*db.BatchItem) {
	t.Helper()
	ctx := context.Background()
	for _, item := range items {
		if err := batchDB.DBStore(ctx, item); err != nil {
			t.Fatalf("failed to store item %s: %v", item.ID, err)
		}
	}
}

func assertJobStatus(t *testing.T, batchDB db.BatchDBClient, jobID string, expected openai.BatchStatus) {
	t.Helper()
	items, _, _, err := batchDB.DBGet(context.Background(),
		&db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, false, 0, 10)
	if err != nil {
		t.Fatalf("failed to get job %s: %v", jobID, err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item for %s, got %d", jobID, len(items))
	}
	var info openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &info); err != nil {
		t.Fatalf("failed to unmarshal status: %v", err)
	}
	if info.Status != expected {
		t.Errorf("expected status %s for job %s, got %s", expected, jobID, info.Status)
	}
}

// newMockBatchDB creates a mock batch DB that filters by HasProcessorID when set.
func newMockBatchDB() *mock.MockDBClient[db.BatchItem, db.BatchQuery] {
	m := mock.NewMockDBClient[db.BatchItem, db.BatchQuery](
		func(item *db.BatchItem) string { return item.ID },
		func(query *db.BatchQuery) *db.BaseQuery { return &query.BaseQuery },
	)
	m.QueryFilter = func(item *db.BatchItem, query *db.BatchQuery) bool {
		if query.HasProcessorID && item.ProcessorID == "" {
			return false
		}
		return true
	}
	return m
}

// casConflictBatchDB is a minimal mock that always returns ErrConflict on DBUpdate.
type casConflictBatchDB struct{}

func (c *casConflictBatchDB) DBStore(_ context.Context, _ *db.BatchItem) error { return nil }
func (c *casConflictBatchDB) DBGet(_ context.Context, _ *db.BatchQuery, _ bool, _, _ int) ([]*db.BatchItem, int, bool, error) {
	item := newTestBatchItem("job-cas", "dead-processor", openai.BatchStatusInProgress, expiredSLO())
	return []*db.BatchItem{item}, 1, false, nil
}
func (c *casConflictBatchDB) DBUpdate(_ context.Context, _ *db.BatchItem, _ []byte) error {
	return fmt.Errorf("DBUpdate: %w", db.ErrConflict)
}
func (c *casConflictBatchDB) DBDelete(_ context.Context, _ []string) ([]string, error) {
	return nil, nil
}
func (c *casConflictBatchDB) DBUpdateProgress(_ context.Context, _ string, _ int64, _ db.BatchRequestCounts) error {
	return nil
}
func (c *casConflictBatchDB) Close() error { return nil }
func (c *casConflictBatchDB) GetContext(_ context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.Background(), func() {}
}

func TestTriageOrphan(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name            string
		status          openai.BatchStatus
		priority        int64
		wantExpired     int
		wantReEnqueued  int
		wantFinalStatus openai.BatchStatus
		checkQueue      bool
		wantInQueue     bool
	}{
		{
			name:            "orphan with expired SLO transitions to failed",
			status:          openai.BatchStatusValidating,
			priority:        expiredSLO(),
			wantExpired:     1,
			wantReEnqueued:  0,
			wantFinalStatus: openai.BatchStatusFailed,
		},
		{
			name:            "orphan with future SLO is re-enqueued",
			status:          openai.BatchStatusValidating,
			priority:        futureSLO(),
			wantExpired:     0,
			wantReEnqueued:  1,
			wantFinalStatus: openai.BatchStatusValidating, // status unchanged on re-enqueue
			checkQueue:      true,
			wantInQueue:     true,
		},
		{
			name:            "in_progress orphan with expired SLO transitions to failed",
			status:          openai.BatchStatusInProgress,
			priority:        expiredSLO(),
			wantExpired:     1,
			wantReEnqueued:  0,
			wantFinalStatus: openai.BatchStatusFailed,
		},
		{
			name:            "in_progress orphan with future SLO is re-enqueued",
			status:          openai.BatchStatusInProgress,
			priority:        futureSLO(),
			wantExpired:     0,
			wantReEnqueued:  1,
			wantFinalStatus: openai.BatchStatusInProgress,
			checkQueue:      true,
			wantInQueue:     true,
		},
		{
			name:            "cancelling orphan transitions to cancelled regardless of SLO",
			status:          openai.BatchStatusCancelling,
			priority:        futureSLO(),
			wantExpired:     1,
			wantReEnqueued:  0,
			wantFinalStatus: openai.BatchStatusCancelled,
		},
		{
			name:            "cancelling orphan with expired SLO transitions to cancelled",
			status:          openai.BatchStatusCancelling,
			priority:        expiredSLO(),
			wantExpired:     1,
			wantReEnqueued:  0,
			wantFinalStatus: openai.BatchStatusCancelled,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batchDB := newMockBatchDB()
			queue := mock.NewMockBatchPriorityQueueClient()

			item := newTestBatchItem("job-1", "dead-processor", tc.status, tc.priority)
			storeItems(t, batchDB, item)

			r, resultCh := newTestReconciler(t, batchDB, queue)
			// Mark "dead-processor" as not alive by not including it in live set.
			r.SetLiveProcessors(map[string]bool{})
			r.run(ctx)

			result := <-resultCh
			if result.Expired != tc.wantExpired {
				t.Errorf("expected %d expired, got %d", tc.wantExpired, result.Expired)
			}
			if result.ReEnqueued != tc.wantReEnqueued {
				t.Errorf("expected %d re-enqueued, got %d", tc.wantReEnqueued, result.ReEnqueued)
			}

			assertJobStatus(t, batchDB, "job-1", tc.wantFinalStatus)

			if tc.checkQueue {
				queuedIDs, _ := queue.PQGetIDs(ctx)
				if tc.wantInQueue && !queuedIDs["job-1"] {
					t.Error("expected job-1 to be in queue after re-enqueue")
				}
				if !tc.wantInQueue && queuedIDs["job-1"] {
					t.Error("expected job-1 NOT to be in queue")
				}
			}
		})
	}
}

func TestSkipNonOrphans(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name        string
		processorID string
		liveSet     map[string]bool
	}{
		{
			name:        "job with no processor_id is skipped",
			processorID: "",
			liveSet:     map[string]bool{},
		},
		{
			name:        "job with alive processor is skipped",
			processorID: "alive-processor",
			liveSet:     map[string]bool{"alive-processor": true},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batchDB := newMockBatchDB()
			queue := mock.NewMockBatchPriorityQueueClient()

			item := newTestBatchItem("job-1", tc.processorID, openai.BatchStatusValidating, futureSLO())
			storeItems(t, batchDB, item)

			r, resultCh := newTestReconciler(t, batchDB, queue)
			r.SetLiveProcessors(tc.liveSet)
			r.run(ctx)

			result := <-resultCh
			if result.Expired != 0 || result.ReEnqueued != 0 || result.Conflicts != 0 || result.Errors != 0 {
				t.Errorf("expected no actions for non-orphan job, got %+v", result)
			}
		})
	}
}

func TestRunCycle_RefreshesLiveSetBeforeTriage(t *testing.T) {
	ctx := context.Background()

	batchDB := newMockBatchDB()
	queue := mock.NewMockBatchPriorityQueueClient()

	// Owned by "fresh-replica", which is absent from the stale snapshot but is
	// Ready per the refresher. The cycle must refresh the live set first so this
	// is not triaged as a false orphan.
	item := newTestBatchItem("job-1", "fresh-replica", openai.BatchStatusInProgress, futureSLO())
	storeItems(t, batchDB, item)

	r, resultCh := newTestReconciler(t, batchDB, queue)
	r.SetLiveProcessors(map[string]bool{"stale-processor": true}) // predates fresh-replica

	refreshed := false
	r.SetLivePodRefresher(func(context.Context) (map[string]bool, error) {
		refreshed = true
		return map[string]bool{"stale-processor": true, "fresh-replica": true}, nil
	})

	r.run(ctx)
	result := <-resultCh

	if !refreshed {
		t.Fatal("expected the live set to be refreshed during the reconcile cycle")
	}
	if result.Expired != 0 || result.ReEnqueued != 0 || result.Conflicts != 0 || result.Errors != 0 {
		t.Errorf("fresh Ready replica must not be triaged as an orphan, got %+v", result)
	}
}

func TestRunCycleMixedJobs(t *testing.T) {
	ctx := context.Background()

	t.Run("only dead-processor jobs are triaged", func(t *testing.T) {
		batchDB := newMockBatchDB()
		queue := mock.NewMockBatchPriorityQueueClient()

		storeItems(t, batchDB,
			// Queued (no processor_id) — should be skipped.
			newTestBatchItem("queued-job", "", openai.BatchStatusValidating, futureSLO()),
			// Alive processor — should be skipped.
			newTestBatchItem("alive-job", "processor-0", openai.BatchStatusInProgress, futureSLO()),
			// Dead processor, valid SLO — should be re-enqueued.
			newTestBatchItem("dead-valid", "processor-1", openai.BatchStatusInProgress, futureSLO()),
			// Dead processor, expired SLO — should be failed.
			newTestBatchItem("dead-expired", "processor-2", openai.BatchStatusInProgress, expiredSLO()),
		)

		r, resultCh := newTestReconciler(t, batchDB, queue)
		r.SetLiveProcessors(map[string]bool{"processor-0": true})
		r.run(ctx)

		result := <-resultCh
		if result.ReEnqueued != 1 {
			t.Errorf("expected 1 re-enqueued, got %d", result.ReEnqueued)
		}
		if result.Expired != 1 {
			t.Errorf("expected 1 expired, got %d", result.Expired)
		}
		if result.Errors != 0 {
			t.Errorf("expected 0 errors, got %d", result.Errors)
		}

		// Verify the queued and alive jobs were not touched.
		assertJobStatus(t, batchDB, "queued-job", openai.BatchStatusValidating)
		assertJobStatus(t, batchDB, "alive-job", openai.BatchStatusInProgress)
	})
}

func TestCASConflict(t *testing.T) {
	ctx := context.Background()

	t.Run("CAS conflict is counted as conflict not error", func(t *testing.T) {
		batchDB := &casConflictBatchDB{}
		queue := mock.NewMockBatchPriorityQueueClient()

		resultCh := make(chan *Result, 1)
		r, err := NewReconciler(batchDB, queue, testInterval, false, func(res *Result) {
			resultCh <- res
		})
		if err != nil {
			t.Fatalf("failed to create reconciler: %v", err)
		}
		// Ensure "dead-processor" is not in live set so the job is orphaned.
		r.SetLiveProcessors(map[string]bool{})
		r.run(ctx)

		result := <-resultCh
		if result.Conflicts != 1 {
			t.Errorf("expected 1 conflict from CAS, got %d", result.Conflicts)
		}
		if result.Errors != 0 {
			t.Errorf("expected 0 errors (CAS is a conflict, not an error), got %d", result.Errors)
		}
		if result.Expired != 0 {
			t.Errorf("expected 0 expired (CAS failed), got %d", result.Expired)
		}
	})
}

func TestEpochFencing(t *testing.T) {
	ctx := context.Background()

	t.Run("zombie write fails after GC bumps epoch", func(t *testing.T) {
		// Simulate: processor owns job at epoch=3, GC transitions to failed
		// (bumping epoch to 4), then zombie tries to write with epoch=3.
		batchDB := newMockBatchDB()
		queue := mock.NewMockBatchPriorityQueueClient()

		item := newTestBatchItem("job-epoch", "dead-processor", openai.BatchStatusInProgress, expiredSLO())
		item.Epoch = 3
		storeItems(t, batchDB, item)

		r, resultCh := newTestReconciler(t, batchDB, queue)
		r.SetLiveProcessors(map[string]bool{})
		r.run(ctx)

		result := <-resultCh
		if result.Expired != 1 {
			t.Fatalf("expected 1 expired, got %d", result.Expired)
		}

		// Verify GC set BumpEpoch — the mock doesn't actually increment,
		// but in production the epoch would be bumped. Verify the job
		// was transitioned to failed.
		assertJobStatus(t, batchDB, "job-epoch", openai.BatchStatusFailed)
	})

	t.Run("processor write with matching epoch succeeds", func(t *testing.T) {
		batchDB := newMockBatchDB()

		statusBytes, _ := json.Marshal(openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
		item := &db.BatchItem{
			BaseIndexes:  db.BaseIndexes{ID: "job-match"},
			BaseContents: db.BaseContents{Status: statusBytes},
			ProcessorID:  "processor-0",
			Epoch:        5,
		}
		storeItems(t, batchDB, item)

		// Update with matching epoch should succeed.
		newStatus, _ := json.Marshal(openai.BatchStatusInfo{Status: openai.BatchStatusFinalizing})
		err := batchDB.DBUpdate(ctx, &db.BatchItem{
			BaseIndexes:  db.BaseIndexes{ID: "job-match"},
			BaseContents: db.BaseContents{Status: newStatus},
			Epoch:        5,
		}, nil)
		if err != nil {
			t.Fatalf("expected epoch-matched update to succeed, got %v", err)
		}

		assertJobStatus(t, batchDB, "job-match", openai.BatchStatusFinalizing)
	})

	t.Run("processor write with stale epoch fails", func(t *testing.T) {
		batchDB := newMockBatchDB()

		statusBytes, _ := json.Marshal(openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
		item := &db.BatchItem{
			BaseIndexes:  db.BaseIndexes{ID: "job-stale"},
			BaseContents: db.BaseContents{Status: statusBytes},
			ProcessorID:  "processor-0",
			Epoch:        5,
		}
		storeItems(t, batchDB, item)

		// Simulate GC bumping epoch to 6 by directly updating the stored item.
		items, _, _, _ := batchDB.DBGet(ctx,
			&db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-stale"}}},
			true, 0, 1)
		items[0].Epoch = 6
		_ = batchDB.DBUpdate(ctx, items[0], nil)

		// Zombie write with stale epoch=5 should fail.
		newStatus, _ := json.Marshal(openai.BatchStatusInfo{Status: openai.BatchStatusCompleted})
		err := batchDB.DBUpdate(ctx, &db.BatchItem{
			BaseIndexes:  db.BaseIndexes{ID: "job-stale"},
			BaseContents: db.BaseContents{Status: newStatus},
			Epoch:        5,
		}, nil)
		if err == nil {
			t.Fatal("expected epoch-mismatched update to fail")
		}
		if !errors.Is(err, db.ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}

		// Job should still be in_progress (zombie write rejected).
		assertJobStatus(t, batchDB, "job-stale", openai.BatchStatusInProgress)
	})
}

func TestRunLoop(t *testing.T) {
	t.Run("stops on context cancel", func(t *testing.T) {
		batchDB := newMockBatchDB()
		queue := mock.NewMockBatchPriorityQueueClient()

		ran := make(chan struct{}, 1)
		r, err := NewReconciler(batchDB, queue, testInterval, false, func(*Result) {
			select {
			case ran <- struct{}{}:
			default:
			}
		})
		if err != nil {
			t.Fatalf("failed to create reconciler: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() { done <- r.RunLoop(ctx) }()

		// The first cycle is deferred until Trigger() is called (normally
		// by the pod watcher after cache sync).
		r.Trigger()

		<-ran
		cancel()

		if err := <-done; err != nil && err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

func TestNewReconcilerValidation(t *testing.T) {
	batchDB := newMockBatchDB()
	queue := mock.NewMockBatchPriorityQueueClient()

	tests := []struct {
		name     string
		batchDB  db.BatchDBClient
		queue    db.BatchPriorityQueueClient
		interval time.Duration
	}{
		{
			name:     "nil batchDB",
			batchDB:  nil,
			queue:    queue,
			interval: testInterval,
		},
		{
			name:     "nil queue",
			batchDB:  batchDB,
			queue:    nil,
			interval: testInterval,
		},
		{
			name:     "zero interval",
			batchDB:  batchDB,
			queue:    queue,
			interval: 0,
		},
		{
			name:     "negative interval",
			batchDB:  batchDB,
			queue:    queue,
			interval: -time.Minute,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewReconciler(tc.batchDB, tc.queue, tc.interval, false, nil)
			if err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestRunCycle_DBFailure(t *testing.T) {
	ctx := context.Background()

	t.Run("DB error is counted and cycle returns early", func(t *testing.T) {
		queue := mock.NewMockBatchPriorityQueueClient()
		failDB := &failGetBatchDB{err: fmt.Errorf("connection refused")}

		r, resultCh := newTestReconciler(t, failDB, queue)
		r.SetLiveProcessors(map[string]bool{})
		r.run(ctx)

		result := <-resultCh
		if result.Errors != 1 {
			t.Errorf("expected 1 error, got %d", result.Errors)
		}
		if result.Expired != 0 || result.ReEnqueued != 0 {
			t.Errorf("expected no actions on DB failure, got expired=%d reEnqueued=%d", result.Expired, result.ReEnqueued)
		}
	})
}

// failGetBatchDB always returns an error on DBGet.
type failGetBatchDB struct {
	err error
}

func (f *failGetBatchDB) DBStore(_ context.Context, _ *db.BatchItem) error { return nil }
func (f *failGetBatchDB) DBGet(_ context.Context, _ *db.BatchQuery, _ bool, _, _ int) ([]*db.BatchItem, int, bool, error) {
	return nil, 0, false, f.err
}
func (f *failGetBatchDB) DBUpdate(_ context.Context, _ *db.BatchItem, _ []byte) error { return nil }
func (f *failGetBatchDB) DBDelete(_ context.Context, _ []string) ([]string, error)    { return nil, nil }
func (f *failGetBatchDB) DBUpdateProgress(_ context.Context, _ string, _ int64, _ db.BatchRequestCounts) error {
	return nil
}
func (f *failGetBatchDB) Close() error { return nil }
func (f *failGetBatchDB) GetContext(_ context.Context, _ time.Duration) (context.Context, context.CancelFunc) {
	return context.Background(), func() {}
}

func TestDryRun(t *testing.T) {
	ctx := context.Background()

	t.Run("expire is counted but DB is not mutated", func(t *testing.T) {
		batchDB := newMockBatchDB()
		queue := mock.NewMockBatchPriorityQueueClient()

		item := newTestBatchItem("job-1", "dead-processor", openai.BatchStatusInProgress, expiredSLO())
		storeItems(t, batchDB, item)

		r, resultCh := newTestDryRunReconciler(t, batchDB, queue)
		r.SetLiveProcessors(map[string]bool{})
		r.run(ctx)

		result := <-resultCh
		if result.Expired != 1 {
			t.Errorf("expected 1 expired, got %d", result.Expired)
		}
		// In dry-run mode the DB status should remain unchanged.
		assertJobStatus(t, batchDB, "job-1", openai.BatchStatusInProgress)
	})

	t.Run("re-enqueue is counted but queue is not mutated", func(t *testing.T) {
		batchDB := newMockBatchDB()
		queue := mock.NewMockBatchPriorityQueueClient()

		item := newTestBatchItem("job-1", "dead-processor", openai.BatchStatusValidating, futureSLO())
		storeItems(t, batchDB, item)

		r, resultCh := newTestDryRunReconciler(t, batchDB, queue)
		r.SetLiveProcessors(map[string]bool{})
		r.run(ctx)

		result := <-resultCh
		if result.ReEnqueued != 1 {
			t.Errorf("expected 1 re-enqueued, got %d", result.ReEnqueued)
		}

		queuedIDs, _ := queue.PQGetIDs(ctx)
		if queuedIDs["job-1"] {
			t.Error("expected job-1 NOT to be in queue in dry-run mode")
		}
	})
}

func TestSetLiveProcessors(t *testing.T) {
	tests := []struct {
		name      string
		liveSet   map[string]bool
		processor string
		wantAlive bool
	}{
		{
			name:      "processor in live set is alive",
			liveSet:   map[string]bool{"proc-1": true, "proc-2": true},
			processor: "proc-1",
			wantAlive: true,
		},
		{
			name:      "processor not in live set is not alive",
			liveSet:   map[string]bool{"proc-1": true},
			processor: "proc-2",
			wantAlive: false,
		},
		{
			name:      "empty live set means no processor is alive",
			liveSet:   map[string]bool{},
			processor: "proc-1",
			wantAlive: false,
		},
		{
			name:      "updating live set replaces previous set",
			liveSet:   map[string]bool{"proc-new": true},
			processor: "proc-old",
			wantAlive: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			batchDB := newMockBatchDB()
			queue := mock.NewMockBatchPriorityQueueClient()

			r, _ := newTestReconciler(t, batchDB, queue)

			// Set an initial live set then overwrite it.
			r.SetLiveProcessors(map[string]bool{"proc-old": true})
			r.SetLiveProcessors(tc.liveSet)

			got := r.isProcessorAlive(tc.processor)
			if got != tc.wantAlive {
				t.Errorf("isProcessorAlive(%q) = %v, want %v", tc.processor, got, tc.wantAlive)
			}
		})
	}
}

func TestTrigger(t *testing.T) {
	t.Run("trigger causes immediate reconciliation", func(t *testing.T) {
		batchDB := newMockBatchDB()
		queue := mock.NewMockBatchPriorityQueueClient()

		// Use a very long interval so the ticker never fires.
		resultCh := make(chan *Result, 10)
		r, err := NewReconciler(batchDB, queue, 24*time.Hour, false, func(res *Result) {
			resultCh <- res
		})
		if err != nil {
			t.Fatalf("failed to create reconciler: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- r.RunLoop(ctx) }()

		r.Trigger()

		select {
		case <-resultCh:
			// Trigger caused the cycle — success.
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for triggered reconciliation cycle")
		}

		cancel()
		<-done
	})

	t.Run("trigger is non-blocking", func(t *testing.T) {
		batchDB := newMockBatchDB()
		queue := mock.NewMockBatchPriorityQueueClient()

		r, _ := newTestReconciler(t, batchDB, queue)

		// Multiple triggers should not block even without a consumer.
		r.Trigger()
		r.Trigger()
		r.Trigger()
		// If we reach here without deadlocking, the test passes.
	})
}
