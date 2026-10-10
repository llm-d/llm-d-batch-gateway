package worker

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// checkCounter counts the status checks that reach the inner updater.
type checkCounter struct {
	epochProgressUpdater
	checks atomic.Int32
}

func (c *checkCounter) CheckJobStatus(ctx context.Context, jobID string, epoch int64) (openai.BatchStatus, error) {
	defer c.checks.Add(1)
	return c.epochProgressUpdater.CheckJobStatus(ctx, jobID, epoch)
}

// TestProgressTrackerQuietIntervalPostgres changes a running job's row during
// a quiet interval (no new counts to write) and verifies the progress tracker
// still notices, without writing the row on quiet ticks.
func TestProgressTrackerQuietIntervalPostgres(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := testLoggerCtx(t)

	batchDB, err := postgresql.NewPostgresBatchDBClient(ctx, &postgresql.PostgreSQLConfig{Url: url})
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	const (
		jobID = "job-quiet-interval"
		epoch = int64(5)
	)
	rowVersion := func(t *testing.T) string {
		t.Helper()
		var xmin string
		if err := pool.QueryRow(ctx, "SELECT xmin::text FROM batch_items WHERE id = $1", jobID).Scan(&xmin); err != nil {
			t.Fatalf("read row version: %v", err)
		}
		return xmin
	}
	waitFor := func(t *testing.T, what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}

	tests := []struct {
		name           string
		change         string // SQL applied to the row in a quiet interval; empty for none
		wantFencedOut  bool
		wantCancelling bool
	}{
		{
			name: "unchanged row is checked without being written",
		},
		{
			name:          "epoch bump signals ownership lost",
			change:        "UPDATE batch_items SET epoch = epoch + 1 WHERE id = $1",
			wantFencedOut: true,
		},
		{
			name:           "cancelling status signals a user cancel",
			change:         `UPDATE batch_items SET status = jsonb_set(status, '{status}', '"cancelling"') WHERE id = $1`,
			wantCancelling: true,
		},
		{
			name:          "terminal status signals ownership lost",
			change:        `UPDATE batch_items SET status = jsonb_set(status, '{status}', '"failed"') WHERE id = $1`,
			wantFencedOut: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := batchDB.DBDelete(ctx, []string{jobID}); err != nil {
				t.Fatalf("delete: %v", err)
			}
			if err := batchDB.DBStore(ctx, &api.BatchItem{
				BaseIndexes:  api.BaseIndexes{ID: jobID, TenantID: "tenant-1"},
				BaseContents: api.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
				Epoch:        epoch,
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}
			t.Cleanup(func() { _, _ = batchDB.DBDelete(context.Background(), []string{jobID}) })

			var fencedOut, cancelling atomic.Bool
			counter := &checkCounter{epochProgressUpdater: NewStatusUpdater(batchDB)}
			tracker := pipeline.NewProgressTracker(10, jobProgressUpdater{
				inner:        counter,
				jobID:        jobID,
				epoch:        epoch,
				onFencedOut:  func() { fencedOut.Store(true) },
				onCancelling: func() { cancelling.Store(true) },
			}, jobID, 20*time.Millisecond, testLogger(t))
			// Like executeJobAsync: the first tick writes the initial counts.
			tracker.AddFailed(0)

			runCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				_ = tracker.Run(runCtx)
				close(done)
			}()
			defer func() {
				cancel()
				<-done
			}()

			waitFor(t, "the first tick", func() bool { return counter.checks.Load() > 0 })
			before := rowVersion(t)

			if tt.change == "" {
				checks := counter.checks.Load()
				waitFor(t, "three more status checks", func() bool { return counter.checks.Load() >= checks+3 })
				if after := rowVersion(t); after != before {
					t.Fatalf("quiet ticks wrote the row: xmin %s -> %s", before, after)
				}
			} else {
				if _, err := pool.Exec(ctx, tt.change, jobID); err != nil {
					t.Fatalf("change row: %v", err)
				}
				waitFor(t, "the tracker to notice the change", func() bool { return fencedOut.Load() || cancelling.Load() })
			}

			if fencedOut.Load() != tt.wantFencedOut {
				t.Errorf("onFencedOut called = %v, want %v", fencedOut.Load(), tt.wantFencedOut)
			}
			if cancelling.Load() != tt.wantCancelling {
				t.Errorf("onCancelling called = %v, want %v", cancelling.Load(), tt.wantCancelling)
			}
		})
	}
}
