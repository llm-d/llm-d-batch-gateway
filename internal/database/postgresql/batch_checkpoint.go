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
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

var _ api.BatchCheckpointStore = (*PostgresBatchDBClient)(nil)

func (c *PostgresBatchDBClient) RecordBatchRequestAttempt(ctx context.Context, attempt *api.BatchRequestAttempt) error {
	if attempt == nil || attempt.BatchID == "" || attempt.RequestID == "" || attempt.Attempt <= 0 {
		return fmt.Errorf("valid batch request attempt is required")
	}
	const query = `
INSERT INTO batch_request_attempts (batch_id, request_id, attempt, owner_epoch)
SELECT id, $2, $3, epoch
  FROM batch_items
 WHERE id = $1 AND epoch = $4 AND resumable = TRUE
ON CONFLICT (batch_id, request_id, attempt) DO NOTHING`
	tag, err := c.pool.Exec(ctx, query, attempt.BatchID, attempt.RequestID, attempt.Attempt, attempt.OwnerEpoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		// An existing attempt is accepted only while the caller still owns the
		// batch. Check ownership separately so a stale owner cannot continue.
		var owned bool
		if err := c.pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM batch_items WHERE id = $1 AND epoch = $2 AND resumable = TRUE)`,
			attempt.BatchID, attempt.OwnerEpoch,
		).Scan(&owned); err != nil {
			return err
		}
		if !owned {
			return fmt.Errorf("RecordBatchRequestAttempt: %w", api.ErrConflict)
		}
	}
	return nil
}

func (c *PostgresBatchDBClient) CheckpointBatchResult(ctx context.Context, checkpoint *api.BatchResultCheckpoint) error {
	if checkpoint == nil || checkpoint.BatchID == "" || checkpoint.RequestID == "" || len(checkpoint.Result) == 0 {
		return fmt.Errorf("valid batch result checkpoint is required")
	}
	const query = `
WITH eligible AS (
    SELECT id, epoch FROM batch_items
     WHERE id = $1 AND epoch = $2 AND resumable = TRUE
), checkpointed AS (
    INSERT INTO batch_result_checkpoints (batch_id, request_id, owner_epoch, result)
    SELECT id, $3, epoch, $4::jsonb FROM eligible
    ON CONFLICT (batch_id, request_id) DO UPDATE
       SET result = batch_result_checkpoints.result
    RETURNING request_id
)
SELECT request_id FROM checkpointed`
	var requestID string
	if err := c.pool.QueryRow(ctx, query,
		checkpoint.BatchID, checkpoint.OwnerEpoch, checkpoint.RequestID, checkpoint.Result,
	).Scan(&requestID); err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("CheckpointBatchResult: %w", api.ErrConflict)
		}
		return err
	}
	return nil
}

func (c *PostgresBatchDBClient) CompletedBatchRequestIDs(ctx context.Context, batchID string) (map[string]bool, error) {
	checkpoints, err := c.BatchResultCheckpoints(ctx, batchID)
	if err != nil {
		return nil, err
	}
	completed := make(map[string]bool, len(checkpoints))
	for _, checkpoint := range checkpoints {
		completed[checkpoint.RequestID] = true
	}
	return completed, nil
}

func (c *PostgresBatchDBClient) BatchResultCheckpoints(ctx context.Context, batchID string) ([]*api.BatchResultCheckpoint, error) {
	if batchID == "" {
		return nil, fmt.Errorf("batch ID is required")
	}
	rows, err := c.pool.Query(ctx,
		`SELECT request_id, owner_epoch, result FROM batch_result_checkpoints WHERE batch_id = $1 ORDER BY request_id`, batchID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var checkpoints []*api.BatchResultCheckpoint
	for rows.Next() {
		checkpoint := &api.BatchResultCheckpoint{BatchID: batchID}
		if err := rows.Scan(&checkpoint.RequestID, &checkpoint.OwnerEpoch, &checkpoint.Result); err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return checkpoints, nil
}
