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
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

// PostgresResumableStore implements the resumable batch, lease, and entry
// stores over batch_items and batch_entries. All writes are fenced on the
// calling owner's identity and epoch, and lease checks use the database clock
// (NOW()) so correctness never depends on processor clock sync.
type PostgresResumableStore struct {
	*pgCore
}

var _ api.ResumableStore = (*PostgresResumableStore)(nil)

func NewPostgresResumableStore(ctx context.Context, config *PostgreSQLConfig) (*PostgresResumableStore, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	pgCore, err := newPgCore(ctx, config, batchDescriptor{})
	if err != nil {
		return nil, err
	}

	logr.FromContextOrDiscard(ctx).Info("NewPostgresResumableStore: client created successfully")
	return &PostgresResumableStore{pgCore}, nil
}

func (c *PostgresResumableStore) Close() error {
	return c.close()
}

// ownerLockCTE is the shared ownership fence for entry writes: the batch is
// owned by the given processor at the given epoch with an unexpired lease,
// is still an activated resumable batch, and has not reached a terminal
// status — mirrors MockResumableStore.ownedFence. The FOR UPDATE lock is the
// point: UPDATE ... FROM reads the FROM relation at the statement snapshot,
// so without it a RequeueResumableOrphan/FinalizeResumableBatch that commits
// while an entry write is in flight would not invalidate the fence, and a
// stale owner could still mutate entries after ownership moved. Locking the
// row serializes the two: whichever commits first wins. It cannot deadlock —
// the batch row is the first lock every writer takes.
var ownerLockCTE = `WITH owner AS (
	SELECT b.id FROM batch_items b
	WHERE b.id = $1 AND b.processor_id = $3 AND b.epoch = $4
	  AND b.owner_lease_expires_at > NOW() AND b.resumable = TRUE
	  AND ` + buildNonTerminalCondition("b."+colStatus) + `
	FOR UPDATE
)`

// ActivateResumableBatch performs the atomic validating → in_progress
// transition: it inserts the entry set from a JSON array and flips
// resumable/lease in one statement, fenced on the dequeuing processor's
// processor_id + epoch. RowsAffected == 0 means the fence failed.
func (c *PostgresResumableStore) ActivateResumableBatch(ctx context.Context, activation *api.ResumableActivation) error {
	if activation == nil {
		return fmt.Errorf("ActivateResumableBatch: nil activation")
	}
	if activation.BatchID == "" || activation.ProcessorID == "" {
		return fmt.Errorf("ActivateResumableBatch: batch ID and processor ID are required")
	}
	if activation.LeaseTTL <= 0 {
		return fmt.Errorf("ActivateResumableBatch: lease TTL must be > 0")
	}
	if len(activation.Entries) == 0 {
		return fmt.Errorf("ActivateResumableBatch: empty entry set for %s", activation.BatchID)
	}

	entriesJSON, err := json.Marshal(activation.Entries)
	if err != nil {
		return fmt.Errorf("ActivateResumableBatch: marshal entries: %w", err)
	}

	result, err := c.pool.Exec(ctx,
		`WITH candidate AS (
			SELECT id FROM batch_items
			WHERE id = $1 AND processor_id = $2 AND epoch = $3
			  AND status::jsonb->>'status' = 'validating' AND resumable = FALSE
			  AND NOT EXISTS (SELECT 1 FROM batch_entries WHERE batch_id = $1)
			FOR UPDATE
		), inserted AS (
			INSERT INTO batch_entries (batch_id, request_id, ordinal, custom_id, status, attempts,
				result_key, result_bytes, result_sha256, error_code, completed_at)
			SELECT $1, r.request_id, r.ordinal, r.custom_id, r.status,
				-- attempts counts dispatch attempts; an activation-rejected
				-- line is never dispatched, so it seeds 0 — the manifest
				-- integrity check relies on this to tell a rejection (failed
				-- with zero attempts) from an attempted failure.
				0,
				r.result_key, r.result_bytes, r.result_sha256, r.error_code,
				CASE WHEN r.status = 'failed' THEN NOW() END
			FROM candidate, jsonb_to_recordset($4::jsonb) AS r(request_id TEXT, ordinal BIGINT,
				custom_id TEXT, status TEXT, error_code TEXT, result_key TEXT, result_bytes BIGINT,
				result_sha256 TEXT)
		), activated AS (
			UPDATE batch_items
			SET status = jsonb_set(
					jsonb_set(status, '{status}', '"in_progress"'),
					-- FLOOR matches Go's time.Unix() truncation; a plain
					-- numeric cast rounds to nearest, which can stamp
					-- in_progress_at one second past a completed_at written
					-- later in the same wall-clock second.
					'{in_progress_at}', to_jsonb(FLOOR(EXTRACT(EPOCH FROM NOW()))::bigint)),
				resumable = TRUE,
				owner_lease_expires_at = NOW() + make_interval(secs => $5)
			FROM candidate WHERE batch_items.id = candidate.id
			RETURNING batch_items.id
		)
		SELECT id FROM activated`,
		activation.BatchID, activation.ProcessorID, activation.Epoch, string(entriesJSON),
		int64(activation.LeaseTTL.Seconds()),
	)
	if err != nil {
		return fmt.Errorf("ActivateResumableBatch: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("ActivateResumableBatch: %s is not owned at epoch %d or already activated: %w",
			activation.BatchID, activation.Epoch, api.ErrConflict)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("ActivateResumableBatch: succeeded",
		"batchId", activation.BatchID, "entries", len(activation.Entries))
	return nil
}

// RenewResumableBatchLease extends the lease of an owned, non-terminal batch.
// A stale epoch, a changed owner, or a lapsed lease matches no rows.
func (c *PostgresResumableStore) RenewResumableBatchLease(ctx context.Context, batchID, processorID string, epoch int64, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("RenewResumableBatchLease: lease TTL must be > 0")
	}
	result, err := c.pool.Exec(ctx,
		`UPDATE batch_items
		SET owner_lease_expires_at = NOW() + make_interval(secs => $4)
		WHERE id = $1 AND processor_id = $2 AND epoch = $3
		  AND resumable = TRUE AND owner_lease_expires_at > NOW()
		  AND `+nonTerminalCondition,
		batchID, processorID, epoch, int64(ttl.Seconds()),
	)
	if err != nil {
		return fmt.Errorf("RenewResumableBatchLease: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("RenewResumableBatchLease: %s is not owned by %s at epoch %d: %w",
			batchID, processorID, epoch, api.ErrConflict)
	}
	return nil
}

// RequeueResumableOrphan clears ownership of an orphaned resumable batch so it
// becomes dequeuable again, keeping status and entries. The fenced CAS on
// (processor_id, epoch) makes a race with a renewal or finalization a no-op.
// bumpAttempts=false leaves the recovery budget untouched — the startup
// recovery path uses it because PQClaimOwned already counted the restart.
func (c *PostgresResumableStore) RequeueResumableOrphan(ctx context.Context, batchID, processorID string, epoch int64, requireExpiredLease, bumpAttempts bool) error {
	query := `UPDATE batch_items
		SET processor_id = NULL, owner_lease_expires_at = NULL,
		    epoch = epoch + 1,
		    recovery_attempts = CASE WHEN $4 THEN recovery_attempts + 1 ELSE recovery_attempts END
		WHERE id = $1 AND processor_id = $2 AND epoch = $3
		  AND resumable = TRUE AND ` + nonTerminalCondition
	if requireExpiredLease {
		query += ` AND owner_lease_expires_at <= NOW()`
	}

	result, err := c.pool.Exec(ctx, query, batchID, processorID, epoch, bumpAttempts)
	if err != nil {
		return fmt.Errorf("RequeueResumableOrphan: %w", err)
	}
	if result.RowsAffected() == 0 {
		// Raced with a renewal, requeue, or terminal transition: the snapshot
		// is stale and the current state wins.
		logr.FromContextOrDiscard(ctx).V(logging.DEBUG).Info("RequeueResumableOrphan: no-op",
			"batchId", batchID, "epoch", epoch)
		return nil
	}

	// The orphaned owner submitted the batch's in-flight requests, so their
	// unacknowledged result deliveries are stranded on its per-processor
	// route. Record it as their submitter so the next owner recovers them
	// from there. Rows written before submitter tracking keep NULL — the
	// next owner re-submits those directly (the pre-recovery behavior).
	if _, err := c.pool.Exec(ctx,
		`UPDATE batch_entries SET submitted_by = $2
		 WHERE batch_id = $1 AND status = 'in_flight' AND submitted_by IS NULL`,
		batchID, processorID,
	); err != nil {
		return fmt.Errorf("RequeueResumableOrphan: record submitter: %w", err)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("RequeueResumableOrphan: requeued",
		"batchId", batchID, "epoch", epoch)
	return nil
}

// FinalizeResumableBatch performs the fenced terminal transition of an owned,
// activated batch: it persists the terminal status document and clears
// resumable/lease (processor_id stays for audit). RowsAffected == 0 means the
// fence failed or an earlier attempt already committed — a row persisting
// exactly the given status document is an idempotent success, anything else a
// conflict. A failed verification query is neither: ownership is undetermined
// and the transient error is returned so the caller retries instead of
// abandoning a finalization it may still own.
func (c *PostgresResumableStore) FinalizeResumableBatch(ctx context.Context, fin *api.ResumableFinalization) error {
	if fin == nil {
		return fmt.Errorf("FinalizeResumableBatch: nil finalization")
	}
	if fin.BatchID == "" || fin.ProcessorID == "" {
		return fmt.Errorf("FinalizeResumableBatch: batch ID and processor ID are required")
	}
	if fin.ExpectedStatus == "" || len(fin.Status) == 0 {
		return fmt.Errorf("FinalizeResumableBatch: expected and target status are required")
	}

	result, err := c.pool.Exec(ctx,
		`UPDATE batch_items
		SET status = $5, resumable = FALSE, owner_lease_expires_at = NULL
		WHERE id = $1 AND processor_id = $2 AND epoch = $3
		  AND resumable = TRUE AND owner_lease_expires_at > NOW()
		  AND status::jsonb->>'status' = $4`,
		fin.BatchID, fin.ProcessorID, fin.Epoch, fin.ExpectedStatus, fin.Status,
	)
	if err != nil {
		return fmt.Errorf("FinalizeResumableBatch: %w", err)
	}
	if result.RowsAffected() > 0 {
		logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("FinalizeResumableBatch: finalized",
			"batchId", fin.BatchID, "epoch", fin.Epoch)
		return nil
	}

	// Zero rows: the fence failed, or an earlier attempt committed the
	// transition but its response was lost. Only the latter is a success;
	// jsonb canonicalizes the stored document, so the comparison is
	// semantic rather than bytewise.
	var committed []byte
	switch err := c.pool.QueryRow(ctx,
		`SELECT status FROM batch_items WHERE id = $1`, fin.BatchID,
	).Scan(&committed); {
	case errors.Is(err, pgx.ErrNoRows):
		// The row is gone: nothing proves an earlier commit of this
		// document, so the fence failed.
	case err != nil:
		// The verification itself failed. Ownership is undetermined — the
		// fenced UPDATE matched no rows, but this processor may still own the
		// batch — so surface the transient error instead of a conflict: the
		// caller retries the finalization rather than abandoning one it may
		// still hold.
		return fmt.Errorf("FinalizeResumableBatch: verify committed status of %s: %w", fin.BatchID, err)
	case jsonEqual(committed, fin.Status):
		logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("FinalizeResumableBatch: already finalized",
			"batchId", fin.BatchID, "epoch", fin.Epoch)
		return nil
	}
	return fmt.Errorf("FinalizeResumableBatch: %s is not owned by %s at epoch %d in status %q: %w",
		fin.BatchID, fin.ProcessorID, fin.Epoch, fin.ExpectedStatus, api.ErrConflict)
}

// jsonEqual reports whether two JSON documents hold the same value,
// ignoring key order and whitespace.
func jsonEqual(a, b []byte) bool {
	var av, bv any
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

// MarkEntryInFlight records an imminent submission. It returns false when the
// entry is already terminal (duplicate delivery); a failed ownership fence
// returns ErrConflict.
func (c *PostgresResumableStore) MarkEntryInFlight(ctx context.Context, batchID, requestID, processorID string, epoch int64) (bool, error) {
	result, err := c.pool.Exec(ctx,
		ownerLockCTE+`UPDATE batch_entries e
		SET status = 'in_flight', attempts = attempts + 1, submitted_by = $3, updated_at = NOW()
		FROM owner
		WHERE e.batch_id = $1 AND e.request_id = $2
		  AND e.status IN ('pending','in_flight')`,
		batchID, requestID, processorID, epoch,
	)
	if err != nil {
		return false, fmt.Errorf("MarkEntryInFlight: %w", err)
	}
	if result.RowsAffected() > 0 {
		return true, nil
	}
	return false, c.entryNotUpdatable(ctx, "MarkEntryInFlight", batchID, requestID, processorID, epoch)
}

// CheckpointEntryResult commits a result to a pending/in_flight entry
// (first-wins). It returns false when the entry is already terminal
// (duplicate delivery); a failed ownership fence returns ErrConflict.
func (c *PostgresResumableStore) CheckpointEntryResult(ctx context.Context, batchID, requestID, processorID string, epoch int64, result *api.ResumableResult) (bool, error) {
	if result == nil {
		return false, fmt.Errorf("CheckpointEntryResult: nil result")
	}
	// Only terminal statuses commit: a non-terminal one would leave the entry
	// updatable, letting a later checkpoint overwrite the first-wins result.
	if result.Status != api.ResumableEntryCompleted && result.Status != api.ResumableEntryFailed {
		return false, fmt.Errorf("CheckpointEntryResult: status must be %q or %q, got %q",
			api.ResumableEntryCompleted, api.ResumableEntryFailed, result.Status)
	}
	var errorCode any
	if result.ErrorCode != "" {
		errorCode = result.ErrorCode
	}
	var resultKey any
	if result.ResultKey != "" {
		resultKey = result.ResultKey
	}
	var resultSHA256 any
	if result.ResultSHA256 != "" {
		resultSHA256 = result.ResultSHA256
	}
	var resultBytes any
	if result.ResultBytes > 0 {
		resultBytes = result.ResultBytes
	}

	updated, err := c.pool.Exec(ctx,
		ownerLockCTE+`UPDATE batch_entries e
		SET status = $5, result_key = $6, result_bytes = $7, result_sha256 = $8,
		    error_code = $9, completed_at = NOW(), updated_at = NOW()
		FROM owner
		WHERE e.batch_id = $1 AND e.request_id = $2
		  AND e.status IN ('pending','in_flight')`,
		batchID, requestID, processorID, epoch,
		string(result.Status), resultKey, resultBytes, resultSHA256, errorCode,
	)
	if err != nil {
		return false, fmt.Errorf("CheckpointEntryResult: %w", err)
	}
	if updated.RowsAffected() > 0 {
		return true, nil
	}
	return false, c.entryNotUpdatable(ctx, "CheckpointEntryResult", batchID, requestID, processorID, epoch)
}

// entryNotUpdatable disambiguates a zero-row fenced entry write: a terminal
// entry is a duplicate delivery (no error), anything else is ownership loss.
func (c *PostgresResumableStore) entryNotUpdatable(ctx context.Context, op, batchID, requestID, processorID string, epoch int64) error {
	var status api.ResumableEntryStatus
	err := c.pool.QueryRow(ctx,
		`SELECT e.status FROM batch_entries e
		JOIN batch_items b ON b.id = e.batch_id
		WHERE e.batch_id = $1 AND e.request_id = $2 AND b.processor_id = $3 AND b.epoch = $4`,
		batchID, requestID, processorID, epoch,
	).Scan(&status)
	if err != nil {
		// The entry is gone or the ownership fence no longer holds.
		return fmt.Errorf("%s: %s/%s is not owned by %s at epoch %d: %w",
			op, batchID, requestID, processorID, epoch, api.ErrConflict)
	}
	if status.IsTerminal() {
		logr.FromContextOrDiscard(ctx).V(logging.DEBUG).Info(op+": entry already terminal, dropping duplicate",
			"batchId", batchID, "requestId", requestID, "status", string(status))
		return nil
	}
	return fmt.Errorf("%s: %s/%s is %q but not updatable by %s at epoch %d: %w",
		op, batchID, requestID, status, processorID, epoch, api.ErrConflict)
}

// EntryResumeState aggregates the entry statuses of a batch and lists the
// terminal request IDs (the resume skip set).
func (c *PostgresResumableStore) EntryResumeState(ctx context.Context, batchID string) (*api.ResumableResumeState, error) {
	state := &api.ResumableResumeState{}
	err := c.pool.QueryRow(ctx,
		`SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'in_flight'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed')
		FROM batch_entries WHERE batch_id = $1`,
		batchID,
	).Scan(&state.Pending, &state.InFlight, &state.Completed, &state.Failed)
	if err != nil {
		return nil, fmt.Errorf("EntryResumeState: %w", err)
	}

	rows, err := c.pool.Query(ctx,
		`SELECT request_id FROM batch_entries
		WHERE batch_id = $1 AND status IN ('completed','failed')
		ORDER BY ordinal`,
		batchID,
	)
	if err != nil {
		return nil, fmt.Errorf("EntryResumeState: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var requestID string
		if err := rows.Scan(&requestID); err != nil {
			return nil, fmt.Errorf("EntryResumeState: scan: %w", err)
		}
		state.TerminalRequestIDs = append(state.TerminalRequestIDs, requestID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("EntryResumeState: rows: %w", err)
	}
	return state, nil
}

const entrySelectColumns = `request_id, ordinal, custom_id, status, attempts, submitted_by,
	result_key, result_bytes, result_sha256, error_code`

// scanEntryRow scans one batch_entries row; scan is rows.Scan or QueryRow.Scan.
func scanEntryRow(scan func(dest ...any) error, batchID string) (*api.ResumableEntry, error) {
	var entry api.ResumableEntry
	var resultKey, resultSHA256, errorCode, submittedBy *string
	var resultBytes *int64
	if err := scan(
		&entry.RequestID, &entry.Ordinal, &entry.CustomID, &entry.Status, &entry.Attempts, &submittedBy,
		&resultKey, &resultBytes, &resultSHA256, &errorCode,
	); err != nil {
		return nil, err
	}
	entry.BatchID = batchID
	if submittedBy != nil {
		entry.SubmittedBy = *submittedBy
	}
	if resultKey != nil {
		entry.ResultKey = *resultKey
	}
	if resultSHA256 != nil {
		entry.ResultSHA256 = *resultSHA256
	}
	if errorCode != nil {
		entry.ErrorCode = *errorCode
	}
	if resultBytes != nil {
		entry.ResultBytes = *resultBytes
	}
	return &entry, nil
}

// ListBatchEntries returns up to limit entries with ordinal greater than
// afterOrdinal in ascending ordinal order.
func (c *PostgresResumableStore) ListBatchEntries(ctx context.Context, batchID string, afterOrdinal int64, limit int) ([]*api.ResumableEntry, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("ListBatchEntries: limit must be > 0")
	}
	rows, err := c.pool.Query(ctx,
		`SELECT `+entrySelectColumns+` FROM batch_entries
		WHERE batch_id = $1 AND ordinal > $2
		ORDER BY ordinal ASC LIMIT $3`,
		batchID, afterOrdinal, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("ListBatchEntries: %w", err)
	}
	defer rows.Close()

	var entries []*api.ResumableEntry
	for rows.Next() {
		entry, err := scanEntryRow(rows.Scan, batchID)
		if err != nil {
			return nil, fmt.Errorf("ListBatchEntries: scan: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ListBatchEntries: rows: %w", err)
	}
	return entries, nil
}

// FindEntryByRequestID returns the entry, or nil when it does not exist.
func (c *PostgresResumableStore) FindEntryByRequestID(ctx context.Context, batchID, requestID string) (*api.ResumableEntry, error) {
	entry, err := scanEntryRow(func(dest ...any) error {
		return c.pool.QueryRow(ctx,
			`SELECT `+entrySelectColumns+` FROM batch_entries
			WHERE batch_id = $1 AND request_id = $2`,
			batchID, requestID,
		).Scan(dest...)
	}, batchID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("FindEntryByRequestID: %w", err)
	}
	return entry, nil
}

// HasOpenEntryByRequest reports whether any batch still awaits the result of
// the request ID (pending or in_flight entry). Request IDs are stable and
// batch-scoped, so at most one batch matches; the EXISTS over all batches is
// conservative if duplicates ever existed.
func (c *PostgresResumableStore) HasOpenEntryByRequest(ctx context.Context, requestID string) (bool, error) {
	var open bool
	if err := c.pool.QueryRow(ctx,
		`SELECT EXISTS(
			SELECT 1 FROM batch_entries
			WHERE request_id = $1 AND status IN ('pending','in_flight'))`,
		requestID,
	).Scan(&open); err != nil {
		return false, fmt.Errorf("HasOpenEntryByRequest: %w", err)
	}
	return open, nil
}
