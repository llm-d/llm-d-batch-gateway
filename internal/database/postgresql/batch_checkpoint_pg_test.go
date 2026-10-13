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

package postgresql

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func TestCheckpointWritesRequireLiveOwner(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}

	ctx := context.Background()
	client, err := NewPostgresBatchDBClient(ctx, &PostgreSQLConfig{Url: url})
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	defer func() { _ = client.Close() }()

	const batchID = "checkpoint-live-owner-batch"
	if _, err := client.pool.Exec(ctx, "DELETE FROM batch_items WHERE id = $1", batchID); err != nil {
		t.Fatalf("delete batch: %v", err)
	}
	if _, err := client.pool.Exec(ctx, `
		INSERT INTO batch_items (id, tenant_id, status, processor_id, owner_instance_id, owner_lease_expires_at, epoch, resumable)
		VALUES ($1, 'tenant-1', '{"status":"in_progress"}'::jsonb, 'processor-a', 'owner-a', NOW() + interval '1 minute', 7, TRUE)`, batchID); err != nil {
		t.Fatalf("insert batch: %v", err)
	}

	checkpoint := func(owner string, epoch int64, requestID string, result string) error {
		return client.CheckpointBatchResult(ctx, &api.BatchResultCheckpoint{
			BatchID: batchID, RequestID: requestID, OwnerInstanceID: owner, OwnerEpoch: epoch, Result: []byte(result),
		})
	}
	attempt := func(owner string, epoch int64, requestID string) error {
		return client.RecordBatchRequestAttempt(ctx, &api.BatchRequestAttempt{
			BatchID: batchID, RequestID: requestID, Attempt: 1, OwnerInstanceID: owner, OwnerEpoch: epoch,
		})
	}

	if err := attempt("owner-a", 7, "request-live"); err != nil {
		t.Fatalf("live owner attempt: %v", err)
	}
	if err := checkpoint("owner-a", 7, "request-live", `{"id":"result"}`); err != nil {
		t.Fatalf("live owner checkpoint: %v", err)
	}
	if err := checkpoint("owner-a", 7, "request-live", `{"id":"result"}`); err != nil {
		t.Fatalf("live owner duplicate checkpoint: %v", err)
	}
	if err := checkpoint("owner-a", 7, "request-live", `{"id":"conflict"}`); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("conflicting duplicate checkpoint = %v, want ErrConflict", err)
	}

	if _, err := client.pool.Exec(ctx, "UPDATE batch_items SET owner_lease_expires_at = NOW() - interval '1 second' WHERE id = $1", batchID); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
	if err := checkpoint("owner-a", 7, "request-expired", `{"id":"expired"}`); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("expired owner checkpoint = %v, want ErrConflict", err)
	}
	if err := attempt("owner-a", 7, "request-expired"); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("expired owner attempt = %v, want ErrConflict", err)
	}

	claimed, err := client.ClaimExpiredResumableBatches(ctx, "owner-b", time.Minute)
	if err != nil {
		t.Fatalf("claim expired batch: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != batchID || claimed[0].OwnerInstanceID != "owner-b" || claimed[0].Epoch != 8 {
		t.Fatalf("claimed = %#v, want %q owned by owner-b at epoch 8", claimed, batchID)
	}
	if err := checkpoint("owner-a", 7, "request-takeover", `{"id":"stale"}`); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("previous owner checkpoint = %v, want ErrConflict", err)
	}
	if err := attempt("owner-a", 7, "request-takeover"); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("previous owner attempt = %v, want ErrConflict", err)
	}
	if err := checkpoint("owner-b", 8, "request-takeover", `{"id":"current"}`); err != nil {
		t.Fatalf("takeover winner checkpoint: %v", err)
	}
	if err := checkpoint("owner-b", 8, "request-live", `{"id":"result"}`); err != nil {
		t.Fatalf("checkpoint before takeover remains idempotent: %v", err)
	}
}