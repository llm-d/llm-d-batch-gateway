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
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

// resumablePgEnv bundles the real clients and pool the pg-backed tests use.
type resumablePgEnv struct {
	ctx     context.Context
	url     string
	store   *PostgresResumableStore
	batchDB *PostgresBatchDBClient
	queue   *PostgresBatchQueueClient
	pool    *pgxpool.Pool
}

// newResumablePgEnv prepares a clean database and the clients under test.
func newResumablePgEnv(t *testing.T) *resumablePgEnv {
	t.Helper()
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()

	cfg := &PostgreSQLConfig{Url: url}
	env := &resumablePgEnv{ctx: ctx, url: url}
	var err error
	if env.store, err = NewPostgresResumableStore(ctx, cfg); err != nil {
		t.Fatalf("NewPostgresResumableStore: %v", err)
	}
	if env.batchDB, err = NewPostgresBatchDBClient(ctx, cfg); err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	if env.queue, err = NewPostgresBatchQueueClient(ctx, cfg, testProcessorID); err != nil {
		t.Fatalf("NewPostgresBatchQueueClient: %v", err)
	}
	if env.pool, err = pgxpool.New(ctx, url); err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(func() {
		env.pool.Close()
		_ = env.queue.Close()
		_ = env.batchDB.Close()
		_ = env.store.Close()
	})

	// CASCADE: batch_entries holds a foreign key to batch_items.
	if _, err := env.pool.Exec(ctx, "TRUNCATE batch_items CASCADE"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return env
}

// seedValidatingBatch stores a queued (validating, unclaimed) batch row.
func (env *resumablePgEnv) seedValidatingBatch(t *testing.T, id string) {
	t.Helper()
	if err := env.batchDB.DBStore(env.ctx, &api.BatchItem{
		BaseIndexes: api.BaseIndexes{ID: id, TenantID: "tenant-1"},
		BaseContents: api.BaseContents{
			Status: []byte(`{"status":"validating"}`),
			Spec:   []byte(`{}`),
		},
		Priority: time.Now().Add(24 * time.Hour).UnixMicro(),
	}); err != nil {
		t.Fatalf("seed batch %s: %v", id, err)
	}
}

// claimFreshBatch seeds a validating batch and claims it through the queue,
// returning the post-dequeue epoch the activation must be fenced on.
func (env *resumablePgEnv) claimFreshBatch(t *testing.T, id string) int64 {
	t.Helper()
	env.seedValidatingBatch(t, id)

	jobs, err := env.queue.PQDequeue(env.ctx, 0, 10)
	if err != nil {
		t.Fatalf("PQDequeue: %v", err)
	}
	for _, job := range jobs {
		if job.ID == id {
			return job.Epoch
		}
	}
	t.Fatalf("dequeue did not claim %s (got %+v)", id, jobs)
	return 0
}

func testActivation(batchID string, epoch int64, entries ...api.ResumableActivationEntry) *api.ResumableActivation {
	return &api.ResumableActivation{
		BatchID:     batchID,
		ProcessorID: testProcessorID,
		Epoch:       epoch,
		LeaseTTL:    5 * time.Minute,
		Entries:     entries,
	}
}

func activationEntries() []api.ResumableActivationEntry {
	return []api.ResumableActivationEntry{
		{RequestID: "batch_req_a", Ordinal: 0, CustomID: "a", Status: "pending"},
		{RequestID: "batch_req_b", Ordinal: 1, CustomID: "b", Status: "pending"},
		{RequestID: "batch_req_r", Ordinal: 2, CustomID: "r", Status: "failed",
			ErrorCode: "model_not_found", ResultKey: "results/j1/batch_req_r.jsonl",
			ResultBytes: 42, ResultSHA256: "sha-r"},
	}
}

func TestActivateResumableBatchPG(t *testing.T) {
	env := newResumablePgEnv(t)
	const batchID = "batch-activate"

	t.Run("inserts entries and flips ownership atomically", func(t *testing.T) {
		epoch := env.claimFreshBatch(t, batchID)

		if err := env.store.ActivateResumableBatch(env.ctx, testActivation(batchID, epoch, activationEntries()...)); err != nil {
			t.Fatalf("ActivateResumableBatch: %v", err)
		}

		var status string
		var resumable bool
		var processor *string
		var leaseExpiry *time.Time
		if err := env.pool.QueryRow(env.ctx,
			`SELECT status->>'status', resumable, processor_id, owner_lease_expires_at
			 FROM batch_items WHERE id = $1`, batchID,
		).Scan(&status, &resumable, &processor, &leaseExpiry); err != nil {
			t.Fatalf("read batch: %v", err)
		}
		if status != "in_progress" || !resumable || processor == nil || *processor != testProcessorID {
			t.Fatalf("batch not activated: status=%s resumable=%v processor=%v", status, resumable, processor)
		}
		if leaseExpiry == nil || !leaseExpiry.After(time.Now()) {
			t.Fatalf("lease not set in the future: %v", leaseExpiry)
		}

		entries, err := env.store.ListBatchEntries(env.ctx, batchID, -1, 10)
		if err != nil {
			t.Fatalf("ListBatchEntries: %v", err)
		}
		if len(entries) != 3 {
			t.Fatalf("expected 3 entries, got %d", len(entries))
		}
		rejected := entries[2]
		if rejected.Status != api.ResumableEntryFailed || rejected.ErrorCode != "model_not_found" ||
			rejected.ResultKey != "results/j1/batch_req_r.jsonl" || rejected.ResultBytes != 42 ||
			rejected.ResultSHA256 != "sha-r" || rejected.Attempts != 0 {
			t.Fatalf("rejected entry not persisted with its error object: %+v", rejected)
		}
	})

	t.Run("conflicts on a stale epoch", func(t *testing.T) {
		epoch := env.claimFreshBatch(t, "batch-stale-epoch")
		err := env.store.ActivateResumableBatch(env.ctx, testActivation("batch-stale-epoch", epoch+1, activationEntries()...))
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("conflicts when another processor owns the row", func(t *testing.T) {
		epoch := env.claimFreshBatch(t, "batch-other-owner")
		act := testActivation("batch-other-owner", epoch, activationEntries()...)
		act.ProcessorID = "processor-other"
		err := env.store.ActivateResumableBatch(env.ctx, act)
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("conflicts when already activated and leaves entries untouched", func(t *testing.T) {
		const id = "batch-double-activate"
		epoch := env.claimFreshBatch(t, id)
		if err := env.store.ActivateResumableBatch(env.ctx, testActivation(id, epoch, activationEntries()...)); err != nil {
			t.Fatalf("first activation: %v", err)
		}
		err := env.store.ActivateResumableBatch(env.ctx, testActivation(id, epoch, activationEntries()...))
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("expected ErrConflict on re-activation, got %v", err)
		}
		entries, err := env.store.ListBatchEntries(env.ctx, id, -1, 10)
		if err != nil || len(entries) != 3 {
			t.Fatalf("re-activation must not touch entries: %v %+v", err, entries)
		}
	})

	t.Run("rolls back everything on a malformed entry set", func(t *testing.T) {
		const id = "batch-atomic"
		epoch := env.claimFreshBatch(t, id)
		// Duplicate request IDs violate the entries primary key; the whole
		// statement must fail without flipping the batch.
		bad := []api.ResumableActivationEntry{
			{RequestID: "batch_req_a", Ordinal: 0, CustomID: "a", Status: "pending"},
			{RequestID: "batch_req_a", Ordinal: 1, CustomID: "b", Status: "pending"},
		}
		if err := env.store.ActivateResumableBatch(env.ctx, testActivation(id, epoch, bad...)); err == nil {
			t.Fatal("expected error for duplicate request IDs")
		}

		var status string
		var resumable bool
		if err := env.pool.QueryRow(env.ctx,
			`SELECT status->>'status', resumable FROM batch_items WHERE id = $1`, id,
		).Scan(&status, &resumable); err != nil {
			t.Fatalf("read batch: %v", err)
		}
		if status != "validating" || resumable {
			t.Fatalf("failed activation must not flip the batch: status=%s resumable=%v", status, resumable)
		}
		entries, err := env.store.ListBatchEntries(env.ctx, id, -1, 10)
		if err != nil || len(entries) != 0 {
			t.Fatalf("failed activation must not insert entries: %v %+v", err, entries)
		}
	})
}

func (env *resumablePgEnv) activate(t *testing.T, id string) int64 {
	t.Helper()
	epoch := env.claimFreshBatch(t, id)
	if err := env.store.ActivateResumableBatch(env.ctx, testActivation(id, epoch, activationEntries()...)); err != nil {
		t.Fatalf("ActivateResumableBatch: %v", err)
	}
	return epoch
}

func (env *resumablePgEnv) expireLease(t *testing.T, batchID string) {
	t.Helper()
	if _, err := env.pool.Exec(env.ctx,
		`UPDATE batch_items SET owner_lease_expires_at = NOW() - interval '1 second' WHERE id = $1`,
		batchID,
	); err != nil {
		t.Fatalf("expire lease: %v", err)
	}
}

func TestResumableLeasePG(t *testing.T) {
	env := newResumablePgEnv(t)

	t.Run("renew extends the lease", func(t *testing.T) {
		const id = "batch-renew"
		epoch := env.activate(t, id)

		if err := env.store.RenewResumableBatchLease(env.ctx, id, testProcessorID, epoch, time.Minute); err != nil {
			t.Fatalf("RenewResumableBatchLease: %v", err)
		}
		var expiry time.Time
		if err := env.pool.QueryRow(env.ctx,
			`SELECT owner_lease_expires_at FROM batch_items WHERE id = $1`, id,
		).Scan(&expiry); err != nil {
			t.Fatalf("read lease: %v", err)
		}
		if !expiry.After(time.Now().Add(30 * time.Second)) {
			t.Fatalf("lease not extended: %v", expiry)
		}
	})

	t.Run("renew conflicts on stale epoch or foreign owner", func(t *testing.T) {
		const id = "batch-renew-fence"
		epoch := env.activate(t, id)

		if err := env.store.RenewResumableBatchLease(env.ctx, id, testProcessorID, epoch+1, time.Minute); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("stale epoch: expected ErrConflict, got %v", err)
		}
		if err := env.store.RenewResumableBatchLease(env.ctx, id, "instance-other", epoch, time.Minute); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("foreign owner: expected ErrConflict, got %v", err)
		}
	})

	t.Run("renew conflicts on a lapsed lease", func(t *testing.T) {
		const id = "batch-renew-lapsed"
		epoch := env.activate(t, id)
		env.expireLease(t, id)

		if err := env.store.RenewResumableBatchLease(env.ctx, id, testProcessorID, epoch, time.Minute); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("lapsed lease: expected ErrConflict, got %v", err)
		}
	})
}

func TestRequeueResumableOrphanPG(t *testing.T) {
	env := newResumablePgEnv(t)

	type rowState struct {
		processor       *string
		epoch, attempts int64
		status          string
	}
	readRow := func(t *testing.T, id string) rowState {
		t.Helper()
		var s rowState
		if err := env.pool.QueryRow(env.ctx,
			`SELECT processor_id, epoch, recovery_attempts, status->>'status'
			 FROM batch_items WHERE id = $1`, id,
		).Scan(&s.processor, &s.epoch, &s.attempts, &s.status); err != nil {
			t.Fatalf("read row: %v", err)
		}
		return s
	}

	t.Run("clears ownership, bumps epoch and attempts, keeps status and entries", func(t *testing.T) {
		const id = "batch-requeue"
		epoch := env.activate(t, id)

		if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, epoch, false, true); err != nil {
			t.Fatalf("RequeueResumableOrphan: %v", err)
		}

		s := readRow(t, id)
		if s.processor != nil {
			t.Fatalf("ownership not cleared: processor=%v", s.processor)
		}
		if s.epoch != epoch+1 {
			t.Fatalf("epoch not bumped: got %d want %d", s.epoch, epoch+1)
		}
		if s.attempts != 1 {
			t.Fatalf("recovery_attempts not bumped: got %d", s.attempts)
		}
		if s.status != "in_progress" {
			t.Fatalf("status must survive the requeue, got %s", s.status)
		}
		entries, err := env.store.ListBatchEntries(env.ctx, id, -1, 10)
		if err != nil || len(entries) != 3 {
			t.Fatalf("entries must survive the requeue: %v %+v", err, entries)
		}
	})

	t.Run("startup requeue leaves the recovery budget to the claim", func(t *testing.T) {
		// PQClaimOwned already bumps recovery_attempts when startup recovery
		// claims the owned row, so the hand-back requeue must not bump again:
		// one restart costs one attempt, not two — after two rollouts a
		// healthy batch sits at 2, inside the default budget of 3.
		const id = "batch-requeue-startup"
		epoch := env.activate(t, id)
		// The claim's bump, as PQClaimOwned would leave it.
		if _, err := env.pool.Exec(env.ctx,
			`UPDATE batch_items SET recovery_attempts = 1 WHERE id = $1`, id,
		); err != nil {
			t.Fatalf("stage claim bump: %v", err)
		}

		if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, epoch, false, false); err != nil {
			t.Fatalf("RequeueResumableOrphan: %v", err)
		}

		s := readRow(t, id)
		if s.processor != nil {
			t.Fatalf("ownership not cleared: processor=%v", s.processor)
		}
		if s.epoch != epoch+1 {
			t.Fatalf("epoch not bumped: got %d want %d", s.epoch, epoch+1)
		}
		if s.attempts != 1 {
			t.Fatalf("recovery_attempts = %d, want 1 (only the claim's bump)", s.attempts)
		}
	})

	t.Run("no-ops when the snapshot is stale", func(t *testing.T) {
		const id = "batch-requeue-race"
		epoch := env.activate(t, id)

		if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, epoch+5, false, true); err != nil {
			t.Fatalf("stale snapshot must be a no-op success, got %v", err)
		}
		if s := readRow(t, id); s.processor == nil || *s.processor != testProcessorID {
			t.Fatal("stale requeue must not clear ownership")
		}
	})

	t.Run("wedged-owner trigger requires a lapsed lease", func(t *testing.T) {
		const id = "batch-requeue-wedged"
		epoch := env.activate(t, id)

		if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, epoch, true, true); err != nil {
			t.Fatalf("RequeueResumableOrphan: %v", err)
		}
		if s := readRow(t, id); s.processor == nil {
			t.Fatal("unexpired lease must not be requeued by the wedged-owner trigger")
		}

		env.expireLease(t, id)
		if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, epoch, true, true); err != nil {
			t.Fatalf("RequeueResumableOrphan: %v", err)
		}
		if s := readRow(t, id); s.processor != nil {
			t.Fatal("lapsed lease must be requeued")
		}
	})

	t.Run("ignores non-resumable rows", func(t *testing.T) {
		const id = "batch-requeue-legacy"
		env.seedValidatingBatch(t, id)
		if _, err := env.pool.Exec(env.ctx,
			`UPDATE batch_items SET processor_id = $2 WHERE id = $1`, id, testProcessorID,
		); err != nil {
			t.Fatalf("claim: %v", err)
		}

		if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, 0, false, true); err != nil {
			t.Fatalf("RequeueResumableOrphan: %v", err)
		}
		if s := readRow(t, id); s.processor == nil || *s.processor != testProcessorID {
			t.Fatal("non-resumable row must not be touched")
		}
	})
}

func TestFinalizeResumableBatchPG(t *testing.T) {
	env := newResumablePgEnv(t)
	doc := []byte(`{"status":"completed","request_counts":{"total":3,"completed":3}}`)

	// setBatchStatus rewrites the row's status name, staging the transitions
	// the row store performs (in_progress → finalizing, the cancel CAS, …).
	setBatchStatus := func(t *testing.T, id, status string) {
		t.Helper()
		if _, err := env.pool.Exec(env.ctx,
			`UPDATE batch_items SET status = jsonb_set(status, '{status}', $2::jsonb) WHERE id = $1`,
			id, `"`+status+`"`,
		); err != nil {
			t.Fatalf("set status %s: %v", status, err)
		}
	}

	t.Run("persists the terminal status and releases ownership", func(t *testing.T) {
		const id = "batch-fin"
		epoch := env.activate(t, id)

		if err := env.store.FinalizeResumableBatch(env.ctx, &api.ResumableFinalization{
			BatchID: id, ProcessorID: testProcessorID, Epoch: epoch,
			ExpectedStatus: "in_progress", Status: doc,
		}); err != nil {
			t.Fatalf("FinalizeResumableBatch: %v", err)
		}

		var status []byte
		var resumable bool
		var processor *string
		var lease *time.Time
		if err := env.pool.QueryRow(env.ctx,
			`SELECT status, resumable, processor_id, owner_lease_expires_at
			 FROM batch_items WHERE id = $1`, id,
		).Scan(&status, &resumable, &processor, &lease); err != nil {
			t.Fatalf("read row: %v", err)
		}
		// jsonb canonicalizes the document, so compare semantically.
		var got, want any
		if err := json.Unmarshal(status, &got); err != nil {
			t.Fatalf("stored status is not JSON: %v", err)
		}
		if err := json.Unmarshal(doc, &want); err != nil {
			t.Fatalf("test document is not JSON: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("status = %s, want the terminal document %s", status, doc)
		}
		if resumable || lease != nil || processor == nil || *processor != testProcessorID {
			t.Fatalf("ownership not released: resumable=%v processor=%v lease=%v", resumable, processor, lease)
		}
		// The released owner fences out of entry writes.
		if _, err := env.store.MarkEntryInFlight(env.ctx, id, "batch_req_a", testProcessorID, epoch); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("entry write after finalize: expected ErrConflict, got %v", err)
		}
	})

	t.Run("transition table over source statuses", func(t *testing.T) {
		tests := []struct {
			name     string
			source   string // row status before the finalization
			expected string // expected status the finalizer passes
			wantErr  bool
		}{
			{name: "in_progress", source: "in_progress", expected: "in_progress"},
			{name: "finalizing", source: "finalizing", expected: "finalizing"},
			{name: "cancelling", source: "cancelling", expected: "cancelling"},
			{name: "cancel raced the finalizer", source: "cancelling", expected: "finalizing", wantErr: true},
			{name: "finalizer believes a stale status", source: "finalizing", expected: "in_progress", wantErr: true},
			{name: "already terminal", source: "completed", expected: "finalizing", wantErr: true},
		}
		for i, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				id := fmt.Sprintf("batch-fin-table-%d", i)
				epoch := env.activate(t, id)
				setBatchStatus(t, id, tt.source)

				err := env.store.FinalizeResumableBatch(env.ctx, &api.ResumableFinalization{
					BatchID: id, ProcessorID: testProcessorID, Epoch: epoch,
					ExpectedStatus: tt.expected, Status: doc,
				})
				if tt.wantErr && !errors.Is(err, api.ErrConflict) {
					t.Fatalf("expected ErrConflict, got %v", err)
				}
				if !tt.wantErr && err != nil {
					t.Fatalf("FinalizeResumableBatch: %v", err)
				}
			})
		}
	})

	t.Run("fenced out by stale epoch, foreign owner, and lapsed lease", func(t *testing.T) {
		const id = "batch-fin-fence"
		epoch := env.activate(t, id)

		fin := &api.ResumableFinalization{
			BatchID: id, ProcessorID: testProcessorID, Epoch: epoch,
			ExpectedStatus: "in_progress", Status: doc,
		}
		stale := *fin
		stale.Epoch = epoch + 1
		if err := env.store.FinalizeResumableBatch(env.ctx, &stale); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("stale epoch: expected ErrConflict, got %v", err)
		}
		foreign := *fin
		foreign.ProcessorID = "proc-other"
		if err := env.store.FinalizeResumableBatch(env.ctx, &foreign); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("foreign owner: expected ErrConflict, got %v", err)
		}
		env.expireLease(t, id)
		if err := env.store.FinalizeResumableBatch(env.ctx, fin); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("lapsed lease: expected ErrConflict, got %v", err)
		}
	})

	t.Run("requeued zombie cannot finalize", func(t *testing.T) {
		const id = "batch-fin-zombie"
		epoch := env.activate(t, id)
		if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, epoch, false, true); err != nil {
			t.Fatalf("RequeueResumableOrphan: %v", err)
		}

		err := env.store.FinalizeResumableBatch(env.ctx, &api.ResumableFinalization{
			BatchID: id, ProcessorID: testProcessorID, Epoch: epoch,
			ExpectedStatus: "in_progress", Status: doc,
		})
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("zombie finalize: expected ErrConflict, got %v", err)
		}
		var status string
		if err := env.pool.QueryRow(env.ctx,
			`SELECT status->>'status' FROM batch_items WHERE id = $1`, id,
		).Scan(&status); err != nil {
			t.Fatalf("read row: %v", err)
		}
		if status != "in_progress" {
			t.Fatalf("zombie must not touch the row, status = %s", status)
		}
	})

	t.Run("retry with the identical status document is idempotent", func(t *testing.T) {
		const id = "batch-fin-idempotent"
		epoch := env.activate(t, id)

		fin := &api.ResumableFinalization{
			BatchID: id, ProcessorID: testProcessorID, Epoch: epoch,
			ExpectedStatus: "in_progress", Status: doc,
		}
		if err := env.store.FinalizeResumableBatch(env.ctx, fin); err != nil {
			t.Fatalf("first finalize: %v", err)
		}
		// The retry re-sends the same document after an ambiguous response.
		if err := env.store.FinalizeResumableBatch(env.ctx, fin); err != nil {
			t.Fatalf("identical retry: %v", err)
		}
		// A diverging document over the committed terminal state conflicts.
		diverging := *fin
		diverging.Status = []byte(`{"status":"cancelled"}`)
		if err := env.store.FinalizeResumableBatch(env.ctx, &diverging); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("diverging retry: expected ErrConflict, got %v", err)
		}
	})

	t.Run("verification query failure is transient, not a conflict", func(t *testing.T) {
		const id = "batch-fin-verify-error"
		epoch := env.activate(t, id)
		// Stage a fence failure the verification must distinguish from an
		// idempotent retry: the row is owned but holds a different status.
		setBatchStatus(t, id, "finalizing")

		fin := &api.ResumableFinalization{
			BatchID: id, ProcessorID: testProcessorID, Epoch: epoch,
			ExpectedStatus: "in_progress", Status: doc,
		}

		// Without the injected fault this is a plain conflict.
		if err := env.store.FinalizeResumableBatch(env.ctx, fin); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("fence failure: expected ErrConflict, got %v", err)
		}

		// A transient failure of the verification query must surface as that
		// error, not as ownership loss: this processor may still own the batch,
		// so the caller must retry rather than abandon the finalization.
		injected := errors.New("injected verification query failure")
		store := &PostgresResumableStore{pgCore: &pgCore{pool: &failingQueryRowPool{
			pgxPool: env.pool,
			failSQL: "SELECT status FROM batch_items WHERE id = $1",
			err:     injected,
		}}}
		err := store.FinalizeResumableBatch(env.ctx, fin)
		if !errors.Is(err, injected) {
			t.Fatalf("verification failure: expected the injected error, got %v", err)
		}
		if errors.Is(err, api.ErrConflict) {
			t.Fatal("verification failure must not be classified as a conflict")
		}

		// The row was not touched by either attempt.
		var status string
		if err := env.pool.QueryRow(env.ctx,
			`SELECT status->>'status' FROM batch_items WHERE id = $1`, id,
		).Scan(&status); err != nil {
			t.Fatalf("read row: %v", err)
		}
		if status != "finalizing" {
			t.Fatalf("row status = %s, want finalizing (untouched)", status)
		}
	})
}

// failingQueryRowPool wraps a pgxPool and fails every QueryRow whose SQL
// contains failSQL with err — used to inject a transient verification-query
// failure deterministically.
type failingQueryRowPool struct {
	pgxPool
	failSQL string
	err     error
}

func (p *failingQueryRowPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	if strings.Contains(sql, p.failSQL) {
		return failedRow{err: p.err}
	}
	return p.pgxPool.QueryRow(ctx, sql, args...)
}

// failedRow is a pgx.Row that always fails to scan.
type failedRow struct {
	err error
}

func (r failedRow) Scan(dest ...any) error { return r.err }

func TestResumableEntryWritesPG(t *testing.T) {
	env := newResumablePgEnv(t)

	const batchID = "batch-entries"
	epoch := env.activate(t, batchID)

	t.Run("mark in flight then checkpoint first-wins", func(t *testing.T) {
		submit, err := env.store.MarkEntryInFlight(env.ctx, batchID, "batch_req_a", testProcessorID, epoch)
		if err != nil || !submit {
			t.Fatalf("MarkEntryInFlight: submit=%v err=%v", submit, err)
		}

		committed, err := env.store.CheckpointEntryResult(env.ctx, batchID, "batch_req_a", testProcessorID, epoch,
			&api.ResumableResult{Status: api.ResumableEntryCompleted,
				ResultKey: "results/j1/batch_req_a.jsonl", ResultBytes: 10, ResultSHA256: "sha-a"})
		if err != nil || !committed {
			t.Fatalf("CheckpointEntryResult: committed=%v err=%v", committed, err)
		}

		// A duplicate delivery of the same result must drop without a commit.
		committed, err = env.store.CheckpointEntryResult(env.ctx, batchID, "batch_req_a", testProcessorID, epoch,
			&api.ResumableResult{Status: api.ResumableEntryCompleted,
				ResultKey: "results/j1/batch_req_a.jsonl", ResultBytes: 10, ResultSHA256: "sha-a"})
		if err != nil || committed {
			t.Fatalf("duplicate checkpoint: committed=%v err=%v", committed, err)
		}

		// A duplicate submission of a terminal entry must be dropped.
		submit, err = env.store.MarkEntryInFlight(env.ctx, batchID, "batch_req_a", testProcessorID, epoch)
		if err != nil || submit {
			t.Fatalf("duplicate MarkEntryInFlight: submit=%v err=%v", submit, err)
		}

		entry, err := env.store.FindEntryByRequestID(env.ctx, batchID, "batch_req_a")
		if err != nil || entry == nil {
			t.Fatalf("FindEntryByRequestID: %v %+v", err, entry)
		}
		if entry.Status != api.ResumableEntryCompleted || entry.Attempts != 1 ||
			entry.ResultKey != "results/j1/batch_req_a.jsonl" || entry.ResultBytes != 10 ||
			entry.ResultSHA256 != "sha-a" || entry.ErrorCode != "" {
			t.Fatalf("checkpoint not persisted faithfully: %+v", entry)
		}
	})

	t.Run("zombie epoch is fenced out", func(t *testing.T) {
		if _, err := env.store.MarkEntryInFlight(env.ctx, batchID, "batch_req_b", testProcessorID, epoch+3); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("stale epoch MarkEntryInFlight: expected ErrConflict, got %v", err)
		}
		_, err := env.store.CheckpointEntryResult(env.ctx, batchID, "batch_req_b", testProcessorID, epoch+3,
			&api.ResumableResult{Status: api.ResumableEntryCompleted, ResultKey: "k", ResultBytes: 1, ResultSHA256: "s"})
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("stale epoch CheckpointEntryResult: expected ErrConflict, got %v", err)
		}

		entry, err := env.store.FindEntryByRequestID(env.ctx, batchID, "batch_req_b")
		if err != nil || entry == nil || entry.Status != api.ResumableEntryPending {
			t.Fatalf("zombie must not touch the entry: %v %+v", err, entry)
		}
	})

	t.Run("lapsed lease fences entry writes", func(t *testing.T) {
		env.expireLease(t, batchID)
		if _, err := env.store.MarkEntryInFlight(env.ctx, batchID, "batch_req_b", testProcessorID, epoch); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("lapsed lease MarkEntryInFlight: expected ErrConflict, got %v", err)
		}
		_, err := env.store.CheckpointEntryResult(env.ctx, batchID, "batch_req_b", testProcessorID, epoch,
			&api.ResumableResult{Status: api.ResumableEntryFailed, ErrorCode: "err"})
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("lapsed lease CheckpointEntryResult: expected ErrConflict, got %v", err)
		}
	})

	t.Run("terminal batch fences entry writes even with ownership untouched", func(t *testing.T) {
		const id = "batch-terminal-owned"
		epoch := env.activate(t, id)

		// Terminalize the row directly, the way a finalizer commit does,
		// without clearing processor_id/epoch/owner_lease_expires_at/resumable.
		// A late write from the batch's own (still-recorded) owner must still
		// be rejected: mirrors MockResumableStore.ownedFence.
		if _, err := env.pool.Exec(env.ctx,
			`UPDATE batch_items SET status = '{"status":"completed"}'::jsonb WHERE id = $1`, id,
		); err != nil {
			t.Fatalf("terminalize: %v", err)
		}

		if _, err := env.store.MarkEntryInFlight(env.ctx, id, "batch_req_b", testProcessorID, epoch); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("terminal batch MarkEntryInFlight: expected ErrConflict, got %v", err)
		}
		_, err := env.store.CheckpointEntryResult(env.ctx, id, "batch_req_a", testProcessorID, epoch,
			&api.ResumableResult{Status: api.ResumableEntryCompleted,
				ResultKey: "results/j1/batch_req_a.jsonl", ResultBytes: 10, ResultSHA256: "sha-a"})
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("terminal batch CheckpointEntryResult: expected ErrConflict, got %v", err)
		}
	})

	t.Run("rejects non-terminal checkpoint statuses", func(t *testing.T) {
		for _, status := range []api.ResumableEntryStatus{api.ResumableEntryPending, api.ResumableEntryInFlight} {
			committed, err := env.store.CheckpointEntryResult(env.ctx, batchID, "batch_req_b", testProcessorID, epoch,
				&api.ResumableResult{Status: status, ResultKey: "k", ResultBytes: 1, ResultSHA256: "s"})
			if err == nil || committed {
				t.Fatalf("status %q: expected a validation error, got committed=%v err=%v", status, committed, err)
			}
		}
		entry, ferr := env.store.FindEntryByRequestID(env.ctx, batchID, "batch_req_b")
		if ferr != nil || entry == nil || entry.Status != api.ResumableEntryPending || entry.Attempts != 0 {
			t.Fatalf("rejected checkpoints must not touch the entry: %v %+v", ferr, entry)
		}
	})

	t.Run("entry writes serialize with a concurrent requeue of the batch row", func(t *testing.T) {
		const id = "batch-entry-lock"
		epoch := env.activate(t, id)

		conn, err := env.pool.Acquire(env.ctx)
		if err != nil {
			t.Fatalf("acquire conn: %v", err)
		}
		defer conn.Release()
		tx, err := conn.Begin(env.ctx)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		// The requeue's UPDATE, held uncommitted so it owns the batch row lock.
		if _, err := tx.Exec(env.ctx,
			`UPDATE batch_items
			 SET processor_id = NULL, owner_lease_expires_at = NULL,
			     epoch = epoch + 1, recovery_attempts = recovery_attempts + 1
			 WHERE id = $1`, id,
		); err != nil {
			t.Fatalf("lock batch row: %v", err)
		}

		type outcome struct {
			committed bool
			err       error
		}
		done := make(chan outcome, 1)
		go func() {
			committed, err := env.store.CheckpointEntryResult(env.ctx, id, "batch_req_a", testProcessorID, epoch,
				&api.ResumableResult{Status: api.ResumableEntryCompleted, ResultKey: "k", ResultBytes: 1, ResultSHA256: "s"})
			done <- outcome{committed, err}
		}()

		// The checkpoint must block on the batch-row lock: committing against
		// the pre-requeue snapshot would be exactly the fenced-out stale write.
		select {
		case o := <-done:
			t.Fatalf("checkpoint completed while the requeue held the batch row: committed=%v err=%v", o.committed, o.err)
		case <-time.After(500 * time.Millisecond):
		}

		if err := tx.Commit(env.ctx); err != nil {
			t.Fatalf("commit requeue: %v", err)
		}

		select {
		case o := <-done:
			if !errors.Is(o.err, api.ErrConflict) {
				t.Fatalf("expected ErrConflict after the requeue committed, got committed=%v err=%v", o.committed, o.err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("checkpoint did not return after the requeue committed")
		}
	})
}

func TestResumableEntryQueriesPG(t *testing.T) {
	env := newResumablePgEnv(t)

	const batchID = "batch-queries"
	epoch := env.activate(t, batchID)
	if _, err := env.store.CheckpointEntryResult(env.ctx, batchID, "batch_req_a", testProcessorID, epoch,
		&api.ResumableResult{Status: api.ResumableEntryCompleted, ResultKey: "k-a", ResultBytes: 1, ResultSHA256: "s-a"}); err != nil {
		t.Fatalf("checkpoint a: %v", err)
	}
	if _, err := env.store.MarkEntryInFlight(env.ctx, batchID, "batch_req_b", testProcessorID, epoch); err != nil {
		t.Fatalf("mark b: %v", err)
	}

	t.Run("resume state aggregates counts and terminal IDs", func(t *testing.T) {
		state, err := env.store.EntryResumeState(env.ctx, batchID)
		if err != nil {
			t.Fatalf("EntryResumeState: %v", err)
		}
		if state.Pending != 0 || state.InFlight != 1 || state.Completed != 1 || state.Failed != 1 {
			t.Fatalf("unexpected counts: %+v", state)
		}
		if len(state.TerminalRequestIDs) != 2 ||
			state.TerminalRequestIDs[0] != "batch_req_a" || state.TerminalRequestIDs[1] != "batch_req_r" {
			t.Fatalf("unexpected terminal IDs: %+v", state.TerminalRequestIDs)
		}
	})

	t.Run("list paginates by ordinal", func(t *testing.T) {
		first, err := env.store.ListBatchEntries(env.ctx, batchID, -1, 2)
		if err != nil || len(first) != 2 || first[0].Ordinal != 0 || first[1].Ordinal != 1 {
			t.Fatalf("first page: %v %+v", err, first)
		}
		second, err := env.store.ListBatchEntries(env.ctx, batchID, first[1].Ordinal, 2)
		if err != nil || len(second) != 1 || second[0].RequestID != "batch_req_r" {
			t.Fatalf("second page: %v %+v", err, second)
		}
	})

	t.Run("find by request ID", func(t *testing.T) {
		entry, err := env.store.FindEntryByRequestID(env.ctx, batchID, "batch_req_b")
		if err != nil || entry == nil || entry.CustomID != "b" || entry.Status != api.ResumableEntryInFlight {
			t.Fatalf("FindEntryByRequestID: %v %+v", err, entry)
		}
		missing, err := env.store.FindEntryByRequestID(env.ctx, batchID, "batch_req_missing")
		if err != nil || missing != nil {
			t.Fatalf("missing entry: %v %+v", err, missing)
		}
	})

	t.Run("entries cascade on batch delete and enforce key uniqueness", func(t *testing.T) {
		if _, err := env.pool.Exec(env.ctx,
			`INSERT INTO batch_entries (batch_id, request_id, ordinal, custom_id)
			 VALUES ($1, 'batch_req_a', 99, 'dup')`, batchID,
		); err == nil {
			t.Fatal("expected primary key violation for duplicate (batch_id, request_id)")
		}

		if _, err := env.pool.Exec(env.ctx, `DELETE FROM batch_items WHERE id = $1`, batchID); err != nil {
			t.Fatalf("delete batch: %v", err)
		}
		var n int
		if err := env.pool.QueryRow(env.ctx, `SELECT COUNT(*) FROM batch_entries WHERE batch_id = $1`, batchID).Scan(&n); err != nil {
			t.Fatalf("count entries: %v", err)
		}
		if n != 0 {
			t.Fatalf("entries must cascade with the batch row, %d remain", n)
		}
	})
}

// TestSchemaBackfillSkipsRequeuedRows verifies that the migration backfill —
// which hands ownerless in-flight rows to the 'pre-migration' sentinel and
// runs on every client initialization — leaves requeued resumable rows alone:
// such a row legitimately has no owner while it waits for the next dequeue,
// and stamping it would make it unclaimable until the reconciler's next
// cycle.
func TestSchemaBackfillSkipsRequeuedRows(t *testing.T) {
	env := newResumablePgEnv(t)

	// A requeued resumable row: activated, then handed back — ownership
	// cleared, non-terminal, waiting for the next dequeue.
	const id = "batch-backfill-requeued"
	epoch := env.activate(t, id)
	if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, epoch, false, true); err != nil {
		t.Fatalf("RequeueResumableOrphan: %v", err)
	}

	// Any pod (re)starting runs the embedded schema, backfill included.
	batchDB, err := NewPostgresBatchDBClient(env.ctx, &PostgreSQLConfig{Url: env.url})
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient (schema re-run): %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })

	// The waiting row was not stamped and remains claimable by the queue.
	var processor *string
	if err := env.pool.QueryRow(env.ctx,
		`SELECT processor_id FROM batch_items WHERE id = $1`, id,
	).Scan(&processor); err != nil {
		t.Fatalf("read row: %v", err)
	}
	if processor != nil {
		t.Fatalf("requeued row was stamped %q by the migration backfill: it becomes unclaimable until the reconciler cycle", *processor)
	}

	tasks, err := env.queue.PQDequeue(env.ctx, time.Minute, 10)
	if err != nil {
		t.Fatalf("PQDequeue: %v", err)
	}
	found := false
	for _, task := range tasks {
		if task.ID == id {
			found = true
		}
	}
	if !found {
		t.Fatalf("requeued row %s was not claimable after a schema re-run: %+v", id, tasks)
	}
}

// TestResumableSubmitterTrackingPG pins the submitter bookkeeping that
// cross-processor recovery routes on: MarkEntryInFlight records the
// submitting processor, an orphan requeue backfills it for entries written
// before submitter tracking, and HasOpenEntryByRequest resolves an unrouted
// delivery to the open entry that can still account for it.
func TestResumableSubmitterTrackingPG(t *testing.T) {
	env := newResumablePgEnv(t)

	const batchID = "batch-submitter"
	epoch := env.activate(t, batchID)

	t.Run("mark in flight records the submitter", func(t *testing.T) {
		if _, err := env.store.MarkEntryInFlight(env.ctx, batchID, "batch_req_a", testProcessorID, epoch); err != nil {
			t.Fatalf("MarkEntryInFlight: %v", err)
		}
		entry, err := env.store.FindEntryByRequestID(env.ctx, batchID, "batch_req_a")
		if err != nil || entry == nil {
			t.Fatalf("FindEntryByRequestID: entry=%v err=%v", entry, err)
		}
		if entry.SubmittedBy != testProcessorID {
			t.Fatalf("SubmittedBy = %q, want %q", entry.SubmittedBy, testProcessorID)
		}
	})

	t.Run("has open entry by request", func(t *testing.T) {
		open, err := env.store.HasOpenEntryByRequest(env.ctx, "batch_req_a")
		if err != nil || !open {
			t.Fatalf("HasOpenEntryByRequest(in-flight) = %v (err %v), want true", open, err)
		}
		open, err = env.store.HasOpenEntryByRequest(env.ctx, "batch_req_b")
		if err != nil || !open {
			t.Fatalf("HasOpenEntryByRequest(pending) = %v (err %v), want true", open, err)
		}
		open, err = env.store.HasOpenEntryByRequest(env.ctx, "batch_req_unknown")
		if err != nil || open {
			t.Fatalf("HasOpenEntryByRequest(unknown) = %v (err %v), want false", open, err)
		}

		// A checkpointed entry can no longer account for a delivery.
		if _, err := env.store.MarkEntryInFlight(env.ctx, batchID, "batch_req_b", testProcessorID, epoch); err != nil {
			t.Fatalf("MarkEntryInFlight: %v", err)
		}
		if _, err := env.store.CheckpointEntryResult(env.ctx, batchID, "batch_req_b", testProcessorID, epoch,
			&api.ResumableResult{Status: api.ResumableEntryCompleted,
				ResultKey: "results/j1/batch_req_b.jsonl", ResultBytes: 10, ResultSHA256: "sha-b"}); err != nil {
			t.Fatalf("CheckpointEntryResult: %v", err)
		}
		open, err = env.store.HasOpenEntryByRequest(env.ctx, "batch_req_b")
		if err != nil || open {
			t.Fatalf("HasOpenEntryByRequest(terminal) = %v (err %v), want false", open, err)
		}
	})

	t.Run("orphan requeue backfills the submitter of in-flight entries", func(t *testing.T) {
		// batch_req_a is in flight; simulate a row written before submitter
		// tracking by clearing its stamp.
		if _, err := env.pool.Exec(env.ctx,
			`UPDATE batch_entries SET submitted_by = NULL WHERE batch_id = $1 AND request_id = $2`,
			batchID, "batch_req_a"); err != nil {
			t.Fatalf("clear submitted_by: %v", err)
		}

		// The batch is requeued as an orphan of its owner.
		if err := env.store.RequeueResumableOrphan(env.ctx, batchID, testProcessorID, epoch, false, true); err != nil {
			t.Fatalf("RequeueResumableOrphan: %v", err)
		}

		entry, err := env.store.FindEntryByRequestID(env.ctx, batchID, "batch_req_a")
		if err != nil || entry == nil {
			t.Fatalf("FindEntryByRequestID: entry=%v err=%v", entry, err)
		}
		if entry.SubmittedBy != testProcessorID {
			t.Fatalf("SubmittedBy after requeue = %q, want the orphaned owner %q", entry.SubmittedBy, testProcessorID)
		}
	})
}
