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
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

// PostgresBatchQueueClient implements api.BatchPriorityQueueClient using
// the batch_items table as a native Postgres queue. Jobs with status
// 'validating' and processor_id IS NULL are the queue. Dequeue atomically
// claims jobs using a CTE with SELECT FOR UPDATE SKIP LOCKED + UPDATE
// in a single statement.
type PostgresBatchQueueClient struct {
	*pgCore
	processorID string
}

var _ api.BatchPriorityQueueClient = (*PostgresBatchQueueClient)(nil)

func NewPostgresBatchQueueClient(ctx context.Context, config *PostgreSQLConfig, processorID string) (*PostgresBatchQueueClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	pgCore, err := newPgCore(ctx, config, batchDescriptor{})
	if err != nil {
		return nil, err
	}

	logr.FromContextOrDiscard(ctx).Info("NewPostgresBatchQueueClient: client created successfully")
	return &PostgresBatchQueueClient{pgCore: pgCore, processorID: processorID}, nil
}

func (c *PostgresBatchQueueClient) Close() error {
	return c.close()
}

// PQEnqueue re-enqueues a previously claimed job back to the queue (e.g. from
// GC recovery or processor graceful shutdown). It resets status to 'validating',
// clears processor_id so the job becomes visible to PQDequeue again, and bumps
// epoch to fence out any zombie writes from the previous owner.
// The UPDATE is guarded to only affect non-resumable, non-terminal jobs that
// are currently claimed by a processor. Resumable jobs are exclusively owned
// by startup recovery and must never be reset through a queue re-enqueue.
// If the guard matches nothing (job already terminal, or already back in the
// queue) it returns api.ErrConflict so callers can distinguish a no-op from a
// real re-enqueue.
func (c *PostgresBatchQueueClient) PQEnqueue(ctx context.Context, jobPriority *api.BatchJobPriority) error {
	if jobPriority == nil {
		return fmt.Errorf("PQEnqueue: nil job priority")
	}
	if jobPriority.ID == "" {
		return fmt.Errorf("PQEnqueue: empty job ID")
	}
	if jobPriority.Epoch < 0 {
		return fmt.Errorf("PQEnqueue: invalid epoch for %s", jobPriority.ID)
	}
	if jobPriority.ProcessorID == "" {
		return fmt.Errorf("PQEnqueue: empty processor ID for %s", jobPriority.ID)
	}
	if jobPriority.ExpectedStatus == "" {
		return fmt.Errorf("PQEnqueue: empty expected status for %s", jobPriority.ID)
	}
	result, err := c.pool.Exec(ctx,
		`WITH re_enqueued AS (
			UPDATE batch_items
			SET processor_id = NULL,
			    status = jsonb_set(status, '{status}', '"validating"'),
			    epoch = epoch + 1
			WHERE id = $1
			  AND epoch = $2
			  AND processor_id = $3
			  AND status::jsonb->>'status' = $4
			  AND resumable = FALSE
			  AND `+nonTerminalCondition+`
			RETURNING id
		)
		-- TODO: the processor polling loop (worker.go) could LISTEN on this channel
		-- to wake up immediately instead of waiting for the next poll interval.
		SELECT pg_notify('batch_jobs_available', '') FROM re_enqueued`,
		jobPriority.ID, jobPriority.Epoch, jobPriority.ProcessorID, jobPriority.ExpectedStatus,
	)
	if err != nil {
		return fmt.Errorf("PQEnqueue: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("PQEnqueue: %s is resumable, not owned, or already terminal: %w", jobPriority.ID, api.ErrConflict)
	}
	return nil
}

// PQDequeue atomically claims up to maxItems unclaimed jobs. A single CTE
// statement selects validating jobs with no processor_id (ordered by priority,
// earliest SLO first), locks them with FOR UPDATE SKIP LOCKED, and sets
// processor_id in one atomic SQL statement.
//
// The timeout parameter is unused — the Postgres implementation is a
// non-blocking query. The caller (worker polling loop) controls retry cadence.
func (c *PostgresBatchQueueClient) PQDequeue(ctx context.Context, _ time.Duration, maxItems int) ([]*api.BatchJobPriority, error) {
	if c.processorID == "" {
		return nil, fmt.Errorf("PQDequeue: processor ID is empty, only processors can dequeue")
	}
	logger := logr.FromContextOrDiscard(ctx)

	rows, err := c.pool.Query(ctx,
		`WITH claimed AS (
			SELECT id FROM batch_items
			WHERE processor_id IS NULL
			  AND resumable = FALSE
			  AND status IS NOT NULL
			  AND status::jsonb->>'status' = 'validating'
			ORDER BY priority ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE batch_items
		SET processor_id = $2,
		    epoch = epoch + 1,
		    recovery_attempts = 0
		FROM claimed
		WHERE batch_items.id = claimed.id
		RETURNING batch_items.id, batch_items.priority, batch_items.epoch`,
		maxItems, c.processorID,
	)
	if err != nil {
		return nil, fmt.Errorf("PQDequeue: %w", err)
	}
	defer rows.Close()

	var result []*api.BatchJobPriority
	for rows.Next() {
		var id string
		var priority, epoch int64
		if err := rows.Scan(&id, &priority, &epoch); err != nil {
			return nil, fmt.Errorf("PQDequeue: scan: %w", err)
		}
		result = append(result, &api.BatchJobPriority{
			ID:             id,
			SLO:            time.UnixMicro(priority),
			Epoch:          epoch,
			ProcessorID:    c.processorID,
			ExpectedStatus: "validating",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("PQDequeue: rows: %w", err)
	}

	if len(result) > 0 {
		logger.V(logging.DEBUG).Info("PQDequeue: claimed jobs", "count", len(result))
	}
	return result, nil
}

// PQClaimOwned bumps the epoch and recovery counter of every non-terminal job
// this processor already owns and returns them for startup recovery.
func (c *PostgresBatchQueueClient) PQClaimOwned(ctx context.Context) ([]*api.BatchJobPriority, error) {
	if c.processorID == "" {
		return nil, fmt.Errorf("PQClaimOwned: processor ID is empty, only processors can claim")
	}
	rows, err := c.pool.Query(ctx,
		`UPDATE batch_items
		SET epoch = epoch + 1,
		    recovery_attempts = recovery_attempts + 1
		WHERE processor_id = $1
		  AND status IS NOT NULL
		  AND `+nonTerminalCondition+`
		RETURNING id, COALESCE(priority, 0), epoch, recovery_attempts`,
		c.processorID,
	)
	if err != nil {
		return nil, fmt.Errorf("PQClaimOwned: %w", err)
	}
	defer rows.Close()

	var result []*api.BatchJobPriority
	for rows.Next() {
		var id string
		var priority, epoch, attempts int64
		if err := rows.Scan(&id, &priority, &epoch, &attempts); err != nil {
			return nil, fmt.Errorf("PQClaimOwned: scan: %w", err)
		}
		result = append(result, &api.BatchJobPriority{
			ID:               id,
			SLO:              time.UnixMicro(priority),
			Epoch:            epoch,
			RecoveryAttempts: attempts,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("PQClaimOwned: rows: %w", err)
	}
	return result, nil
}

// PQDelete atomically removes a job from the queue by transitioning it to
// cancelled with a cancelled_at timestamp, but only if it is non-resumable and
// still unclaimed (validating with no processor_id).
// Returns 1 if the job was cancelled, 0 if it was already claimed by a processor.
// The FOR UPDATE SKIP LOCKED prevents races with concurrent PQDequeue calls.
func (c *PostgresBatchQueueClient) PQDelete(ctx context.Context, jobPriority *api.BatchJobPriority) (int, error) {
	if jobPriority == nil {
		return 0, fmt.Errorf("PQDelete: nil job priority")
	}

	now := time.Now().UTC().Unix()
	result, err := c.pool.Exec(ctx,
		`WITH queued AS (
			SELECT id FROM batch_items
			WHERE id = $1
			  AND processor_id IS NULL
			  AND resumable = FALSE
			  AND status IS NOT NULL
			  AND status::jsonb->>'status' = 'validating'
			FOR UPDATE SKIP LOCKED
		)
		UPDATE batch_items
		SET status = jsonb_set(
			jsonb_set(status, '{status}', '"cancelled"'),
			'{cancelled_at}', to_jsonb($2::bigint)
		),
		    epoch = epoch + 1
		FROM queued
		WHERE batch_items.id = queued.id`,
		jobPriority.ID, now,
	)
	if err != nil {
		return 0, fmt.Errorf("PQDelete: %w", err)
	}

	return int(result.RowsAffected()), nil
}

// PQGetIDs returns the set of all job IDs currently in the queue
// (validating with no processor_id).
func (c *PostgresBatchQueueClient) PQGetIDs(ctx context.Context) (map[string]bool, error) {
	rows, err := c.pool.Query(ctx,
		`SELECT id FROM batch_items
		 WHERE processor_id IS NULL
		   AND status IS NOT NULL
		   AND status::jsonb->>'status' = 'validating'`,
	)
	if err != nil {
		return nil, fmt.Errorf("PQGetIDs: %w", err)
	}
	defer rows.Close()

	ids := make(map[string]bool)
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("PQGetIDs: scan: %w", err)
		}
		ids[id] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("PQGetIDs: rows: %w", err)
	}
	return ids, nil
}
