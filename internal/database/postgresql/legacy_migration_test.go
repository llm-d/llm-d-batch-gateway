package postgresql

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

// Schema shipped before the queue columns existed.
const preQueueSchema = `
CREATE TABLE IF NOT EXISTS batch_items (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    expiry     BIGINT,
    tags       JSONB,
    spec       JSONB,
    status     JSONB
);`

func TestLegacyRowsAfterMigration(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()

	if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS batch_items"); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if _, err := pool.Exec(ctx, preQueueSchema); err != nil {
		t.Fatalf("old schema: %v", err)
	}
	// Simulate the queue index installed before the resumable marker existed.
	// The migration must create the replacement predicate before dropping it.
	if _, err := pool.Exec(ctx, `
		ALTER TABLE batch_items ADD COLUMN processor_id TEXT;
		ALTER TABLE batch_items ADD COLUMN priority BIGINT;
		CREATE INDEX idx_batch_items_queue ON batch_items (priority ASC)
			WHERE processor_id IS NULL
			  AND status IS NOT NULL
			  AND status::jsonb->>'status' = 'validating'`); err != nil {
		t.Fatalf("old queue index: %v", err)
	}
	slo := time.Now().Add(24 * time.Hour).UTC().UnixMicro()
	tags := `{"slo_unix_micro":"` + strconv.FormatInt(slo, 10) + `"}`
	for _, row := range []struct{ id, status string }{
		{"legacy-queued", "validating"},
		{"legacy-inflight", "in_progress"},
	} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO batch_items (id, tenant_id, expiry, tags, spec, status) VALUES ($1, 'tenant-1', 0, $2::jsonb, '{}'::jsonb, $3::jsonb)`,
			row.id, tags, `{"status":"`+row.status+`"}`); err != nil {
			t.Fatalf("insert %s: %v", row.id, err)
		}
	}

	cfg := &PostgreSQLConfig{Url: url}
	batchDB, err := NewPostgresBatchDBClient(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient (migration): %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })
	queue, err := NewPostgresBatchQueueClient(ctx, cfg, "processor-0")
	if err != nil {
		t.Fatalf("NewPostgresBatchQueueClient: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })

	t.Run("queued legacy row is dequeued with its SLO", func(t *testing.T) {
		jobs, err := queue.PQDequeue(ctx, 0, 10)
		if err != nil {
			var pid *string
			_ = pool.QueryRow(ctx, "SELECT processor_id FROM batch_items WHERE id = 'legacy-queued'").Scan(&pid)
			claimed := "NULL"
			if pid != nil {
				claimed = *pid
			}
			t.Fatalf("PQDequeue: %v (row now has processor_id=%s, so it was claimed and lost)", err, claimed)
		}
		if len(jobs) != 1 || jobs[0].ID != "legacy-queued" {
			t.Fatalf("expected legacy-queued, got %+v", jobs)
		}
		if jobs[0].SLO.UnixMicro() != slo {
			t.Errorf("SLO not carried over from slo_unix_micro tag: got %d want %d", jobs[0].SLO.UnixMicro(), slo)
		}
	})

	t.Run("in-flight legacy row is visible to the reconciler", func(t *testing.T) {
		items, _, _, err := batchDB.DBGet(ctx, &api.BatchQuery{NonTerminal: true, HasProcessorID: true}, false, 0, 10)
		if err != nil {
			t.Fatalf("DBGet: %v", err)
		}
		for _, it := range items {
			if it.ID == "legacy-inflight" {
				if it.Resumable {
					t.Error("legacy row must default to non-resumable")
				}
				return
			}
		}
		t.Errorf("legacy-inflight (in_progress, no processor_id) is invisible to both the queue and the reconciler; got %d owned items", len(items))
	})

	t.Run("in-flight legacy row can be re-enqueued at epoch zero", func(t *testing.T) {
		err := queue.PQEnqueue(ctx, &api.BatchJobPriority{
			ID:             "legacy-inflight",
			Epoch:          0,
			ProcessorID:    "pre-migration",
			ExpectedStatus: "in_progress",
		})
		if err != nil {
			t.Fatalf("PQEnqueue: %v", err)
		}
	})

	t.Run("queue index is replaced with resumable predicate", func(t *testing.T) {
		var definition string
		if err := pool.QueryRow(ctx,
			`SELECT indexdef FROM pg_indexes WHERE tablename = 'batch_items' AND indexname = 'idx_batch_items_queue_non_resumable'`,
		).Scan(&definition); err != nil {
			t.Fatalf("new queue index: %v", err)
		}
		if !strings.Contains(definition, "resumable = false") {
			t.Fatalf("new queue index lacks resumable predicate: %s", definition)
		}

		var oldIndexCount int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM pg_indexes WHERE tablename = 'batch_items' AND indexname = 'idx_batch_items_queue'`,
		).Scan(&oldIndexCount); err != nil {
			t.Fatalf("old queue index lookup: %v", err)
		}
		if oldIndexCount != 0 {
			t.Fatal("pre-resumable queue index was not removed")
		}
	})
}
