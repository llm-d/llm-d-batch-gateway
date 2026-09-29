package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func TestActivateResumableBatchDoesNotPartiallyActivateOnManifestConflict(t *testing.T) {
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

	client, err := NewPostgresBatchDBClient(ctx, &PostgreSQLConfig{Url: url})
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	const batchID = "manifest-conflict-batch"
	if _, err := pool.Exec(ctx, "DELETE FROM batch_items WHERE id = $1", batchID); err != nil {
		t.Fatalf("delete batch: %v", err)
	}
	oldStatus := []byte(`{"status":"validating"}`)
	newStatus := []byte(`{"status":"in_progress"}`)
	if _, err := pool.Exec(ctx, `
		INSERT INTO batch_items (id, tenant_id, status, processor_id, epoch, resumable)
		VALUES ($1, 'tenant-1', $2::jsonb, 'processor-0', 7, FALSE)`, batchID, oldStatus); err != nil {
		t.Fatalf("insert batch: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO batch_manifests (batch_id, version, owner_epoch, entries)
		VALUES ($1, $2, 7, '[]'::jsonb)`, batchID, api.BatchManifestVersion); err != nil {
		t.Fatalf("insert manifest: %v", err)
	}

	err = client.ActivateResumableBatch(ctx, &api.BatchItem{
		BaseIndexes:        api.BaseIndexes{ID: batchID},
		BaseContents:       api.BaseContents{Status: newStatus},
		ProcessorID:        "processor-0",
		OwnerInstanceID:    "owner-a",
		OwnerLeaseDuration: 30 * time.Second,
		Epoch:              7,
	}, oldStatus, &api.BatchManifest{BatchID: batchID, Version: api.BatchManifestVersion})
	if !errors.Is(err, api.ErrConflict) {
		t.Fatalf("ActivateResumableBatch error = %v, want ErrConflict", err)
	}

	var (
		status    []byte
		resumable bool
	)
	if err := pool.QueryRow(ctx,
		"SELECT status, resumable FROM batch_items WHERE id = $1", batchID,
	).Scan(&status, &resumable); err != nil {
		t.Fatalf("read batch: %v", err)
	}
	var statusInfo struct {
		Status string `json:"status"`
	}
	if err := json.Unmarshal(status, &statusInfo); err != nil {
		t.Fatalf("decode batch status: %v", err)
	}
	if statusInfo.Status != "validating" || resumable {
		t.Fatalf("batch was partially activated: status=%s resumable=%t", status, resumable)
	}
}

func TestResumableLeaseTakeoverFencesStaleOwner(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}

	ctx := context.Background()
	client, err := NewPostgresBatchDBClient(ctx, &PostgreSQLConfig{Url: url})
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	defer client.Close()

	const batchID = "lease-takeover-batch"
	if _, err := client.pool.Exec(ctx, "DELETE FROM batch_items WHERE id = $1", batchID); err != nil {
		t.Fatalf("delete batch: %v", err)
	}
	if _, err := client.pool.Exec(ctx, `
		INSERT INTO batch_items (id, tenant_id, status, processor_id, owner_instance_id, owner_lease_expires_at, epoch, resumable)
		VALUES ($1, 'tenant-1', '{"status":"in_progress"}'::jsonb, 'processor-a', 'owner-a', NOW() - interval '1 second', 7, TRUE)`, batchID); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	claimed, err := client.ClaimExpiredResumableBatches(ctx, "owner-b", 30*time.Second)
	if err != nil {
		t.Fatalf("claim expired batch: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != batchID || claimed[0].Epoch != 8 {
		t.Fatalf("claimed = %#v, want batch %q at epoch 8", claimed, batchID)
	}
	if err := client.RenewResumableBatchLease(ctx, &api.ResumableBatchLease{
		BatchID: batchID, OwnerInstanceID: "owner-a", Epoch: 7, LeaseDuration: 30 * time.Second,
	}); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("stale owner renewal error = %v, want ErrConflict", err)
	}
}
