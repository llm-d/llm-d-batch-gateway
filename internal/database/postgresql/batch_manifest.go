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
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

var _ api.ResumableBatchStore = (*PostgresBatchDBClient)(nil)
var _ api.ResumableBatchLeaseStore = (*PostgresBatchDBClient)(nil)

// ActivateResumableBatch commits the immutable dispatch manifest and activates
// recovery in one PostgreSQL statement. The eligible row lock makes the manifest
// insert and status transition indivisible from competing ownership mutations.
func (c *PostgresBatchDBClient) ActivateResumableBatch(
	ctx context.Context,
	item *api.BatchItem,
	expectedStatus []byte,
	manifest *api.BatchManifest,
) error {
	if item == nil || manifest == nil {
		return fmt.Errorf("item and manifest are required")
	}
	if item.ID == "" || manifest.BatchID != item.ID {
		return fmt.Errorf("manifest batch ID must match item ID")
	}
	if item.ProcessorID == "" {
		return fmt.Errorf("processor ID is required")
	}
	if item.Epoch < 0 {
		return fmt.Errorf("epoch must not be negative")
	}
	if len(item.Status) == 0 || len(expectedStatus) == 0 {
		return fmt.Errorf("current and expected status are required")
	}
	if manifest.Version != api.BatchManifestVersion {
		return fmt.Errorf("unsupported manifest version %d", manifest.Version)
	}
	entries, err := json.Marshal(manifest.Entries)
	if err != nil {
		return fmt.Errorf("marshal manifest entries: %w", err)
	}

	const query = `
WITH candidate AS (
		SELECT id, epoch
			FROM batch_items
		 WHERE id = $2
			 AND processor_id = $3
			 AND epoch = $4
			 AND status = $5
			 AND resumable = FALSE
			 AND NOT EXISTS (SELECT 1 FROM batch_manifests WHERE batch_id = $2)
		 FOR UPDATE
), manifested AS (
		INSERT INTO batch_manifests (batch_id, version, owner_epoch, entries)
		SELECT id, $6, epoch, $7::jsonb
			FROM candidate
		RETURNING batch_id
), activated AS (
		UPDATE batch_items
			 SET status = $1,
					 resumable = TRUE
			FROM manifested
		 WHERE batch_items.id = manifested.batch_id
		RETURNING batch_items.id
)
SELECT id FROM activated`

	var activatedID string
	if err := c.pool.QueryRow(ctx, query,
		item.Status, item.ID, item.ProcessorID, item.Epoch, expectedStatus,
		manifest.Version, entries,
	).Scan(&activatedID); err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("ActivateResumableBatch: %w", api.ErrConflict)
		}
		return err
	}
	return nil
}

func (c *PostgresBatchDBClient) GetBatchManifest(ctx context.Context, batchID string) (*api.BatchManifest, error) {
	if batchID == "" {
		return nil, fmt.Errorf("batch ID is required")
	}
	const query = `SELECT version, entries FROM batch_manifests WHERE batch_id = $1`
	var version int
	var entriesJSON []byte
	if err := c.pool.QueryRow(ctx, query, batchID).Scan(&version, &entriesJSON); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	var entries []api.BatchManifestEntry
	if err := json.Unmarshal(entriesJSON, &entries); err != nil {
		return nil, fmt.Errorf("decode manifest entries: %w", err)
	}
	return &api.BatchManifest{BatchID: batchID, Version: version, Entries: entries}, nil
}

// ClaimExpiredResumableBatches atomically transfers every expired resumable
// lease to one replacement Processor. The returned epoch fences the old owner.
func (c *PostgresBatchDBClient) ClaimExpiredResumableBatches(
	ctx context.Context,
	ownerInstanceID string,
	leaseDuration time.Duration,
) ([]*api.BatchItem, error) {
	if ownerInstanceID == "" {
		return nil, fmt.Errorf("owner instance ID is required")
	}
	if leaseDuration <= 0 {
		return nil, fmt.Errorf("lease duration must be positive")
	}

	rows, err := c.pool.Query(ctx, `
UPDATE batch_items
   SET owner_instance_id = $1,
       owner_lease_expires_at = NOW() + $2::interval,
       epoch = epoch + 1,
       recovery_attempts = recovery_attempts + 1
 WHERE resumable = TRUE
   AND status IS NOT NULL
   AND `+nonTerminalCondition+`
   AND (owner_lease_expires_at IS NULL OR owner_lease_expires_at <= NOW())
 RETURNING id, processor_id, owner_instance_id, owner_lease_expires_at, epoch,
           recovery_attempts, status`, ownerInstanceID, leaseDuration.String())
	if err != nil {
		return nil, fmt.Errorf("claim expired resumable batches: %w", err)
	}
	defer rows.Close()

	var batches []*api.BatchItem
	for rows.Next() {
		var batch api.BatchItem
		var leaseExpiry time.Time
		if err := rows.Scan(
			&batch.ID,
			&batch.ProcessorID,
			&batch.OwnerInstanceID,
			&leaseExpiry,
			&batch.Epoch,
			&batch.RecoveryAttempts,
			&batch.Status,
		); err != nil {
			return nil, fmt.Errorf("scan claimed resumable batch: %w", err)
		}
		batch.OwnerLeaseExpiresAt = &leaseExpiry
		batch.Resumable = true
		batches = append(batches, &batch)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("claim expired resumable batches rows: %w", err)
	}
	return batches, nil
}

// RenewResumableBatchLease extends the caller's lease only while it remains
// the current owner at the supplied fencing epoch.
func (c *PostgresBatchDBClient) RenewResumableBatchLease(ctx context.Context, lease *api.ResumableBatchLease) error {
	if lease == nil || lease.BatchID == "" || lease.OwnerInstanceID == "" || lease.Epoch <= 0 {
		return fmt.Errorf("valid resumable batch lease is required")
	}
	if lease.LeaseDuration <= 0 {
		return fmt.Errorf("lease duration must be positive")
	}
	tag, err := c.pool.Exec(ctx, `
UPDATE batch_items
   SET owner_lease_expires_at = NOW() + $4::interval
 WHERE id = $1
   AND owner_instance_id = $2
   AND epoch = $3
   AND resumable = TRUE
   AND owner_lease_expires_at > NOW()`,
		lease.BatchID, lease.OwnerInstanceID, lease.Epoch, lease.LeaseDuration.String())
	if err != nil {
		return fmt.Errorf("renew resumable batch lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("renew resumable batch lease: %w", api.ErrConflict)
	}
	return nil
}
