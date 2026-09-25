package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

type dbProgressErrWrapper struct {
	db.BatchProgressDBClient
	err error
}

func (d *dbProgressErrWrapper) DBUpdateProgress(ctx context.Context, id string, epoch int64, counts db.BatchRequestCounts) error {
	return d.err
}

type dbUpdateErrWrapper struct {
	inner db.BatchProgressDBClient
	err   error
}

func (d *dbUpdateErrWrapper) DBStore(ctx context.Context, item *db.BatchItem) error {
	return d.inner.DBStore(ctx, item)
}
func (d *dbUpdateErrWrapper) DBGet(ctx context.Context, query *db.BatchQuery, includeStatic bool, start, limit int) ([]*db.BatchItem, int, bool, error) {
	return d.inner.DBGet(ctx, query, includeStatic, start, limit)
}
func (d *dbUpdateErrWrapper) DBUpdate(ctx context.Context, item *db.BatchItem, expectedStatus []byte) error {
	return d.err
}
func (d *dbUpdateErrWrapper) DBUpdateProgress(ctx context.Context, id string, epoch int64, counts db.BatchRequestCounts) error {
	return d.inner.DBUpdateProgress(ctx, id, epoch, counts)
}
func (d *dbUpdateErrWrapper) DBDelete(ctx context.Context, IDs []string) ([]string, error) {
	return d.inner.DBDelete(ctx, IDs)
}
func (d *dbUpdateErrWrapper) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}
func (d *dbUpdateErrWrapper) Close() error {
	return d.inner.Close()
}

// dbUpdateFailOnceWrapper fails the first N DBUpdate calls, then delegates to inner.
type dbUpdateFailOnceWrapper struct {
	inner     db.BatchProgressDBClient
	failCount int
	calls     int
}

func (d *dbUpdateFailOnceWrapper) DBStore(ctx context.Context, item *db.BatchItem) error {
	return d.inner.DBStore(ctx, item)
}
func (d *dbUpdateFailOnceWrapper) DBGet(ctx context.Context, query *db.BatchQuery, includeStatic bool, start, limit int) ([]*db.BatchItem, int, bool, error) {
	return d.inner.DBGet(ctx, query, includeStatic, start, limit)
}
func (d *dbUpdateFailOnceWrapper) DBUpdate(ctx context.Context, item *db.BatchItem, expectedStatus []byte) error {
	d.calls++
	if d.calls <= d.failCount {
		return fmt.Errorf("simulated DB update failure (call %d)", d.calls)
	}
	return d.inner.DBUpdate(ctx, item, expectedStatus)
}
func (d *dbUpdateFailOnceWrapper) DBUpdateProgress(ctx context.Context, id string, epoch int64, counts db.BatchRequestCounts) error {
	return d.inner.DBUpdateProgress(ctx, id, epoch, counts)
}
func (d *dbUpdateFailOnceWrapper) DBDelete(ctx context.Context, IDs []string) ([]string, error) {
	return d.inner.DBDelete(ctx, IDs)
}
func (d *dbUpdateFailOnceWrapper) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}
func (d *dbUpdateFailOnceWrapper) Close() error {
	return d.inner.Close()
}

func TestUpdateProgressCounts_NilCounts_ReturnsError(t *testing.T) {
	updater := NewStatusUpdater(newMockBatchDBClient())

	if err := updater.UpdateProgressCounts(context.Background(), "job-1", 0, nil); err == nil {
		t.Fatalf("expected error for nil requestCounts")
	}
}

func TestUpdateProgressCounts_StatusSetError_ReturnsError(t *testing.T) {
	statusErr := errors.New("progress update failed")
	updater := NewStatusUpdater(&dbProgressErrWrapper{BatchProgressDBClient: newMockBatchDBClient(), err: statusErr})

	err := updater.UpdateProgressCounts(context.Background(), "job-1", 0, &openai.BatchRequestCounts{Total: 1})
	if !errors.Is(err, statusErr) {
		t.Fatalf("expected progress error, got %v", err)
	}
}

func TestUpdateProgressCounts_Success_WritesPayload(t *testing.T) {
	ctx := context.Background()
	dbClient := newMockBatchDBClient()
	updater := NewStatusUpdater(dbClient)

	seed := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: "job-1"},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}
	if err := dbClient.DBStore(ctx, seed); err != nil {
		t.Fatalf("DBStore seed: %v", err)
	}

	if err := updater.UpdateProgressCounts(ctx, "job-1", 0, &openai.BatchRequestCounts{
		Total: 10, Completed: 7, Failed: 3,
	}); err != nil {
		t.Fatalf("UpdateProgressCounts: %v", err)
	}

	items, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{"job-1"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.RequestCounts.Total != 10 || got.RequestCounts.Completed != 7 || got.RequestCounts.Failed != 3 {
		t.Fatalf("unexpected request counts: %+v", got.RequestCounts)
	}
}

func TestUpdatePersistentStatus_InputValidationErrors(t *testing.T) {
	updater := NewStatusUpdater(newMockBatchDBClient())

	if err := updater.UpdatePersistentStatus(context.Background(), nil, openai.BatchStatusFailed, nil, nil); err == nil {
		t.Fatalf("expected error for nil dbJob")
	}

	err := updater.UpdatePersistentStatus(context.Background(), &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: "job-1"},
	}, openai.BatchStatusFailed, nil, nil)
	if err == nil {
		t.Fatalf("expected error for empty dbJob.Status")
	}
}

func TestUpdatePersistentStatus_UnmarshalError(t *testing.T) {
	updater := NewStatusUpdater(newMockBatchDBClient())

	err := updater.UpdatePersistentStatus(context.Background(), &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: "job-1"},
		BaseContents: db.BaseContents{
			Status: []byte("{invalid"),
		},
	}, openai.BatchStatusFailed, nil, nil)
	if err == nil {
		t.Fatalf("expected unmarshal error")
	}
}

func TestUpdatePersistentStatus_DBUpdateError(t *testing.T) {
	updateErr := errors.New("db update failed")
	dbClient := &dbUpdateErrWrapper{
		inner: newMockBatchDBClient(),
		err:   updateErr,
	}
	updater := NewStatusUpdater(dbClient)

	err := updater.UpdatePersistentStatus(context.Background(), &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: "job-1"},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}, openai.BatchStatusFailed, nil, nil)
	if !errors.Is(err, updateErr) {
		t.Fatalf("expected db update error, got %v", err)
	}
}

func TestUpdatePersistentStatus_Success(t *testing.T) {
	ctx := context.Background()
	dbClient := newMockBatchDBClient()
	updater := NewStatusUpdater(dbClient)
	jobID := "job-update-success"

	seed := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobID},
		BaseContents: db.BaseContents{
			Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress}),
		},
	}
	if err := dbClient.DBStore(ctx, seed); err != nil {
		t.Fatalf("DBStore seed: %v", err)
	}

	if err := updater.UpdatePersistentStatus(ctx, seed, openai.BatchStatusFailed, nil, nil); err != nil {
		t.Fatalf("UpdatePersistentStatus: %v", err)
	}

	items, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet updated item: err=%v len=%d", err, len(items))
	}

	var got openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &got); err != nil {
		t.Fatalf("unmarshal status: %v", err)
	}
	if got.Status != openai.BatchStatusFailed {
		t.Fatalf("expected status failed, got %s", got.Status)
	}
}

func TestUpdatePersistentStatus_PreservesPriorTimestamps(t *testing.T) {
	stamps := map[string]func(*openai.BatchStatusInfo) *int64{
		"cancelled_at":   func(s *openai.BatchStatusInfo) *int64 { return s.CancelledAt },
		"cancelling_at":  func(s *openai.BatchStatusInfo) *int64 { return s.CancellingAt },
		"completed_at":   func(s *openai.BatchStatusInfo) *int64 { return s.CompletedAt },
		"expired_at":     func(s *openai.BatchStatusInfo) *int64 { return s.ExpiredAt },
		"failed_at":      func(s *openai.BatchStatusInfo) *int64 { return s.FailedAt },
		"finalizing_at":  func(s *openai.BatchStatusInfo) *int64 { return s.FinalizingAt },
		"in_progress_at": func(s *openai.BatchStatusInfo) *int64 { return s.InProgressAt },
	}

	tests := []struct {
		name        string
		transitions []openai.BatchStatus
		wantSet     []string
	}{
		{
			name:        "completed run keeps in_progress and finalizing",
			transitions: []openai.BatchStatus{openai.BatchStatusInProgress, openai.BatchStatusFinalizing, openai.BatchStatusCompleted},
			wantSet:     []string{"in_progress_at", "finalizing_at", "completed_at"},
		},
		{
			name:        "failed run keeps in_progress",
			transitions: []openai.BatchStatus{openai.BatchStatusInProgress, openai.BatchStatusFailed},
			wantSet:     []string{"in_progress_at", "failed_at"},
		},
		{
			// The worker never writes cancelling itself: the API server records that
			// before sending the cancel event, so the worker only writes cancelled.
			name:        "cancelled run keeps in_progress",
			transitions: []openai.BatchStatus{openai.BatchStatusInProgress, openai.BatchStatusCancelled},
			wantSet:     []string{"in_progress_at", "cancelled_at"},
		},
		{
			name:        "expired run keeps in_progress",
			transitions: []openai.BatchStatus{openai.BatchStatusInProgress, openai.BatchStatusExpired},
			wantSet:     []string{"in_progress_at", "expired_at"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			dbClient := newMockBatchDBClient()
			updater := NewStatusUpdater(dbClient)
			jobID := "job-preserve-timestamps"

			// Seed what CreateBatch writes: validating, every timestamp unset.
			jobItem := &db.BatchItem{
				BaseIndexes: db.BaseIndexes{ID: jobID},
				BaseContents: db.BaseContents{
					Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusValidating}),
				},
			}
			if err := dbClient.DBStore(ctx, jobItem); err != nil {
				t.Fatalf("DBStore seed: %v", err)
			}

			// Every transition reuses jobItem, as the real call sites do.
			for _, status := range tt.transitions {
				if err := updater.UpdatePersistentStatus(ctx, jobItem, status, nil, nil); err != nil {
					t.Fatalf("UpdatePersistentStatus(%s): %v", status, err)
				}
			}

			items, _, _, err := dbClient.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{jobID}}}, true, 0, 1)
			if err != nil || len(items) != 1 {
				t.Fatalf("DBGet updated item: err=%v len=%d", err, len(items))
			}
			var got openai.BatchStatusInfo
			if err := json.Unmarshal(items[0].Status, &got); err != nil {
				t.Fatalf("unmarshal status: %v", err)
			}

			want := make(map[string]bool, len(tt.wantSet))
			for _, name := range tt.wantSet {
				want[name] = true
			}
			for name, get := range stamps {
				switch value := get(&got); {
				case want[name] && value == nil:
					t.Errorf("%s: want a timestamp, got null", name)
				case !want[name] && value != nil:
					t.Errorf("%s: want null, got %d", name, *value)
				}
			}
		})
	}
}
