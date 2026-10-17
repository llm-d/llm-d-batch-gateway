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

package mock

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

const (
	activationEpoch    = 3
	activationLeaseTTL = 5 * time.Minute
)

// frozenClock drives the store's Now hook so lease expiry is deterministic.
type frozenClock struct{ now time.Time }

func (c *frozenClock) Now() time.Time { return c.now }

func (c *frozenClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

// seedActivated claims a batch for proc-1 at activationEpoch and activates it
// with two pending entries and one rejected entry, all under a frozen clock.
func seedActivated(t *testing.T) (*MockResumableStore, *frozenClock) {
	t.Helper()

	clock := &frozenClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	store := NewMockResumableStore()
	store.Now = clock.Now
	store.SeedBatch("job-1", "proc-1", activationEpoch)

	activation := &api.ResumableActivation{
		BatchID:     "job-1",
		ProcessorID: "proc-1",
		Epoch:       activationEpoch,
		LeaseTTL:    activationLeaseTTL,
		Entries: []api.ResumableActivationEntry{
			{RequestID: "r-1", Ordinal: 0, CustomID: "c-1", Status: "pending"},
			{RequestID: "r-2", Ordinal: 1, CustomID: "c-2", Status: "pending"},
			{RequestID: "r-3", Ordinal: 2, CustomID: "c-3", Status: "failed", ErrorCode: "model_not_found",
				ResultKey: "results/job-1/r-3.jsonl", ResultBytes: 42, ResultSHA256: "abc123"},
		},
	}
	if err := store.ActivateResumableBatch(context.Background(), activation); err != nil {
		t.Fatalf("ActivateResumableBatch: %v", err)
	}
	return store, clock
}

func TestMockResumableStore_Activation(t *testing.T) {
	ctx := context.Background()

	t.Run("inserts entries and flips ownership", func(t *testing.T) {
		store, _ := seedActivated(t)
		state, err := store.EntryResumeState(ctx, "job-1")
		if err != nil {
			t.Fatalf("EntryResumeState: %v", err)
		}
		if state.Pending != 2 || state.Failed != 1 || len(state.TerminalRequestIDs) != 1 {
			t.Fatalf("state = %+v, want 2 pending / 1 failed", state)
		}
	})

	t.Run("double activation conflicts", func(t *testing.T) {
		store, _ := seedActivated(t)
		err := store.ActivateResumableBatch(ctx, &api.ResumableActivation{
			BatchID: "job-1", ProcessorID: "proc-1", Epoch: activationEpoch,
			LeaseTTL: activationLeaseTTL,
			Entries:  []api.ResumableActivationEntry{{RequestID: "r-9", Status: "pending"}},
		})
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("second activation: expected ErrConflict, got %v", err)
		}
	})

	t.Run("fences on processor and epoch", func(t *testing.T) {
		for _, act := range []*api.ResumableActivation{
			{BatchID: "job-2", ProcessorID: "proc-1", Epoch: activationEpoch - 1, LeaseTTL: time.Minute,
				Entries: []api.ResumableActivationEntry{{RequestID: "r", Status: "pending"}}},
			{BatchID: "job-2", ProcessorID: "wrong", Epoch: activationEpoch, LeaseTTL: time.Minute,
				Entries: []api.ResumableActivationEntry{{RequestID: "r", Status: "pending"}}},
		} {
			store := NewMockResumableStore()
			store.SeedBatch("job-2", "proc-1", activationEpoch)
			if err := store.ActivateResumableBatch(ctx, act); !errors.Is(err, api.ErrConflict) {
				t.Fatalf("activation %+v: expected ErrConflict, got %v", act, err)
			}
		}
	})

	t.Run("validates input", func(t *testing.T) {
		store := NewMockResumableStore()
		store.SeedBatch("job-3", "proc-1", 1)
		for _, act := range []*api.ResumableActivation{
			nil,
			{BatchID: "job-3", ProcessorID: "proc-1", Epoch: 1, LeaseTTL: time.Minute},
			{BatchID: "job-3", ProcessorID: "proc-1", Epoch: 1,
				Entries: []api.ResumableActivationEntry{{RequestID: "r", Status: "pending"}}},
		} {
			if err := store.ActivateResumableBatch(ctx, act); err == nil {
				t.Fatalf("activation %+v: expected validation error, got nil", act)
			}
		}
	})
}

func TestMockResumableStore_Lease(t *testing.T) {
	ctx := context.Background()

	t.Run("renew extends an unexpired lease", func(t *testing.T) {
		store, clock := seedActivated(t)
		clock.Advance(activationLeaseTTL - time.Minute)
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); err != nil {
			t.Fatalf("renew: %v", err)
		}
		clock.Advance(activationLeaseTTL - time.Minute)
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); err != nil {
			t.Fatalf("renew after extension: %v", err)
		}
	})

	t.Run("conflicts on lapsed lease, wrong owner, stale epoch, terminal batch", func(t *testing.T) {
		store, clock := seedActivated(t)
		clock.Advance(activationLeaseTTL + time.Minute)
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("lapsed lease: expected ErrConflict, got %v", err)
		}

		store, _ = seedActivated(t)
		if err := store.RenewResumableBatchLease(ctx, "job-1", "other", activationEpoch, activationLeaseTTL); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("wrong owner: expected ErrConflict, got %v", err)
		}
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch+1, activationLeaseTTL); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("stale epoch: expected ErrConflict, got %v", err)
		}

		store.MarkTerminal("job-1")
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("terminal batch: expected ErrConflict, got %v", err)
		}
	})
}

func TestMockResumableStore_Finalize(t *testing.T) {
	ctx := context.Background()
	doc := []byte(`{"status":"completed","request_counts":{"total":2,"completed":2}}`)

	finOf := func(expected string) *api.ResumableFinalization {
		return &api.ResumableFinalization{
			BatchID: "job-1", ProcessorID: "proc-1", Epoch: activationEpoch,
			ExpectedStatus: expected, Status: doc,
		}
	}

	t.Run("persists the terminal status and releases ownership", func(t *testing.T) {
		store, _ := seedActivated(t)
		if err := store.FinalizeResumableBatch(ctx, finOf("in_progress")); err != nil {
			t.Fatalf("FinalizeResumableBatch: %v", err)
		}
		// The released owner fences out of every subsequent write.
		if _, err := store.MarkEntryInFlight(ctx, "job-1", "r-1", "proc-1", activationEpoch); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("entry write after finalize: expected ErrConflict, got %v", err)
		}
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("renew after finalize: expected ErrConflict, got %v", err)
		}
	})

	t.Run("fenced transitions conflict", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(store *MockResumableStore, clock *frozenClock)
			fin    *api.ResumableFinalization
		}{
			{name: "expected status does not match the row",
				fin: finOf("finalizing")},
			{name: "stale epoch",
				fin: func() *api.ResumableFinalization {
					f := finOf("in_progress")
					f.Epoch = activationEpoch + 1
					return f
				}()},
			{name: "foreign owner",
				fin: func() *api.ResumableFinalization {
					f := finOf("in_progress")
					f.ProcessorID = "proc-other"
					return f
				}()},
			{name: "lapsed lease",
				mutate: func(_ *MockResumableStore, clock *frozenClock) {
					clock.Advance(activationLeaseTTL + time.Minute)
				},
				fin: finOf("in_progress")},
			{name: "requeued batch lost its owner",
				mutate: func(store *MockResumableStore, _ *frozenClock) {
					_ = store.RequeueResumableOrphan(ctx, "job-1", "proc-1", activationEpoch, false, true)
				},
				fin: finOf("in_progress")},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				store, clock := seedActivated(t)
				if tt.mutate != nil {
					tt.mutate(store, clock)
				}
				if err := store.FinalizeResumableBatch(ctx, tt.fin); !errors.Is(err, api.ErrConflict) {
					t.Fatalf("expected ErrConflict, got %v", err)
				}
			})
		}
	})

	t.Run("retry with the identical status document is idempotent", func(t *testing.T) {
		store, _ := seedActivated(t)
		if err := store.FinalizeResumableBatch(ctx, finOf("in_progress")); err != nil {
			t.Fatalf("first finalize: %v", err)
		}
		if err := store.FinalizeResumableBatch(ctx, finOf("in_progress")); err != nil {
			t.Fatalf("identical retry: %v", err)
		}
		// A different document over the committed terminal state conflicts.
		other := finOf("in_progress")
		other.Status = []byte(`{"status":"cancelled"}`)
		if err := store.FinalizeResumableBatch(ctx, other); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("diverging retry: expected ErrConflict, got %v", err)
		}
	})

	t.Run("validates input", func(t *testing.T) {
		store, _ := seedActivated(t)
		for _, fin := range []*api.ResumableFinalization{
			nil,
			{BatchID: "job-1", ProcessorID: "proc-1", Epoch: activationEpoch, ExpectedStatus: "in_progress"},
			{BatchID: "job-1", ProcessorID: "proc-1", Epoch: activationEpoch, Status: doc},
		} {
			if err := store.FinalizeResumableBatch(ctx, fin); err == nil {
				t.Fatalf("finalization %+v: expected validation error, got nil", fin)
			}
		}
	})
}

func TestMockResumableStore_Requeue(t *testing.T) {
	ctx := context.Background()

	t.Run("clears ownership, bumps epoch, keeps entries", func(t *testing.T) {
		store, _ := seedActivated(t)
		if err := store.RequeueResumableOrphan(ctx, "job-1", "proc-1", activationEpoch, false, true); err != nil {
			t.Fatalf("requeue: %v", err)
		}
		// The owner's fenced writes no longer match at the old epoch.
		if _, err := store.MarkEntryInFlight(ctx, "job-1", "r-1", "proc-1", activationEpoch); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("write after requeue: expected ErrConflict, got %v", err)
		}
		// Entries survive for the next owner.
		entries, err := store.ListBatchEntries(ctx, "job-1", -1, 10)
		if err != nil || len(entries) != 3 {
			t.Fatalf("entries after requeue: err=%v len=%d", err, len(entries))
		}
	})

	t.Run("bumpAttempts=false leaves the recovery budget untouched", func(t *testing.T) {
		// The startup-recovery requeue: PQClaimOwned already counted the
		// restart, so the requeue must not spend a second attempt — one
		// restart costs one attempt of the budget, not two.
		store, _ := seedActivated(t)
		if err := store.RequeueResumableOrphan(ctx, "job-1", "proc-1", activationEpoch, false, false); err != nil {
			t.Fatalf("requeue: %v", err)
		}
		if _, _, attempts := store.BatchOwnership("job-1"); attempts != 0 {
			t.Fatalf("recovery_attempts = %d, want 0 (the claim already counted the restart)", attempts)
		}

		// The GC requeue still counts.
		store2, _ := seedActivated(t)
		if err := store2.RequeueResumableOrphan(ctx, "job-1", "proc-1", activationEpoch, false, true); err != nil {
			t.Fatalf("requeue: %v", err)
		}
		if _, _, attempts := store2.BatchOwnership("job-1"); attempts != 1 {
			t.Fatalf("recovery_attempts = %d, want 1 for a counted requeue", attempts)
		}
	})

	t.Run("stale snapshot is a no-op", func(t *testing.T) {
		store, _ := seedActivated(t)
		if err := store.RequeueResumableOrphan(ctx, "job-1", "proc-1", activationEpoch-1, false, true); err != nil {
			t.Fatalf("stale requeue: %v", err)
		}
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); err != nil {
			t.Fatalf("owner unaffected by stale requeue: %v", err)
		}
	})

	t.Run("wedged-owner trigger requires a lapsed lease", func(t *testing.T) {
		store, clock := seedActivated(t)
		if err := store.RequeueResumableOrphan(ctx, "job-1", "proc-1", activationEpoch, true, true); err != nil {
			t.Fatalf("requeue with live lease: %v", err)
		}
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); err != nil {
			t.Fatalf("live lease must block the requeue: %v", err)
		}
		clock.Advance(activationLeaseTTL + time.Minute)
		if err := store.RequeueResumableOrphan(ctx, "job-1", "proc-1", activationEpoch, true, true); err != nil {
			t.Fatalf("requeue after lapse: %v", err)
		}
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("owner must be dispossessed after lapse: %v", err)
		}
	})

	t.Run("terminal batch is left alone", func(t *testing.T) {
		store, _ := seedActivated(t)
		store.MarkTerminal("job-1")
		if err := store.RequeueResumableOrphan(ctx, "job-1", "proc-1", activationEpoch, false, true); err != nil {
			t.Fatalf("requeue terminal: %v", err)
		}
		if err := store.RenewResumableBatchLease(ctx, "job-1", "proc-1", activationEpoch, activationLeaseTTL); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("terminal owner unaffected: %v", err)
		}
	})
}

func TestMockResumableStore_EntryWrites(t *testing.T) {
	ctx := context.Background()
	result := func() *api.ResumableResult {
		return &api.ResumableResult{
			Status:       api.ResumableEntryCompleted,
			ResultKey:    "results/job-1/r-1.jsonl",
			ResultBytes:  7,
			ResultSHA256: "def456",
		}
	}

	t.Run("mark and checkpoint are first-wins", func(t *testing.T) {
		store, _ := seedActivated(t)
		submitted, err := store.MarkEntryInFlight(ctx, "job-1", "r-1", "proc-1", activationEpoch)
		if err != nil || !submitted {
			t.Fatalf("MarkEntryInFlight: submitted=%v err=%v", submitted, err)
		}
		committed, err := store.CheckpointEntryResult(ctx, "job-1", "r-1", "proc-1", activationEpoch, result())
		if err != nil || !committed {
			t.Fatalf("CheckpointEntryResult: committed=%v err=%v", committed, err)
		}
		entry, err := store.FindEntryByRequestID(ctx, "job-1", "r-1")
		if err != nil || entry == nil {
			t.Fatalf("FindEntryByRequestID: err=%v entry=%v", err, entry)
		}
		if entry.Status != api.ResumableEntryCompleted || entry.Attempts != 1 ||
			entry.ResultKey != "results/job-1/r-1.jsonl" || entry.ResultBytes != 7 || entry.ResultSHA256 != "def456" {
			t.Fatalf("checkpointed entry = %+v", entry)
		}

		// Duplicates against a terminal entry are dropped without error.
		submitted, err = store.MarkEntryInFlight(ctx, "job-1", "r-1", "proc-1", activationEpoch)
		if err != nil || submitted {
			t.Fatalf("duplicate mark: submitted=%v err=%v", submitted, err)
		}
		committed, err = store.CheckpointEntryResult(ctx, "job-1", "r-1", "proc-1", activationEpoch, result())
		if err != nil || committed {
			t.Fatalf("duplicate checkpoint: committed=%v err=%v", committed, err)
		}
	})

	t.Run("terminal entry under a stale epoch conflicts", func(t *testing.T) {
		store, _ := seedActivated(t)
		if _, err := store.CheckpointEntryResult(ctx, "job-1", "r-1", "proc-1", activationEpoch, result()); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		_, err := store.CheckpointEntryResult(ctx, "job-1", "r-1", "proc-1", activationEpoch+1, result())
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("stale-epoch duplicate: expected ErrConflict, got %v", err)
		}
	})

	t.Run("fences on lease and missing entries", func(t *testing.T) {
		store, clock := seedActivated(t)
		clock.Advance(activationLeaseTTL + time.Minute)
		if _, err := store.MarkEntryInFlight(ctx, "job-1", "r-2", "proc-1", activationEpoch); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("lapsed lease mark: expected ErrConflict, got %v", err)
		}
		if _, err := store.CheckpointEntryResult(ctx, "job-1", "r-2", "proc-1", activationEpoch, result()); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("lapsed lease checkpoint: expected ErrConflict, got %v", err)
		}

		store, _ = seedActivated(t)
		if _, err := store.MarkEntryInFlight(ctx, "job-1", "missing", "proc-1", activationEpoch); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("missing entry: expected ErrConflict, got %v", err)
		}
	})

	t.Run("rejects non-terminal checkpoint statuses", func(t *testing.T) {
		store, _ := seedActivated(t)
		for _, status := range []api.ResumableEntryStatus{api.ResumableEntryPending, api.ResumableEntryInFlight} {
			committed, err := store.CheckpointEntryResult(ctx, "job-1", "r-2", "proc-1", activationEpoch,
				&api.ResumableResult{Status: status})
			if err == nil || committed {
				t.Fatalf("status %q: expected a validation error, got committed=%v err=%v", status, committed, err)
			}
		}
		entry, err := store.FindEntryByRequestID(ctx, "job-1", "r-2")
		if err != nil || entry == nil || entry.Status != api.ResumableEntryPending {
			t.Fatalf("rejected checkpoints must not touch the entry: %v %+v", err, entry)
		}
	})
}

func TestMockResumableStore_Queries(t *testing.T) {
	ctx := context.Background()

	t.Run("resume state aggregates statuses and terminal IDs", func(t *testing.T) {
		store, _ := seedActivated(t)
		if _, err := store.CheckpointEntryResult(ctx, "job-1", "r-1", "proc-1", activationEpoch,
			&api.ResumableResult{Status: api.ResumableEntryCompleted}); err != nil {
			t.Fatalf("checkpoint: %v", err)
		}
		if _, err := store.MarkEntryInFlight(ctx, "job-1", "r-2", "proc-1", activationEpoch); err != nil {
			t.Fatalf("mark: %v", err)
		}
		state, err := store.EntryResumeState(ctx, "job-1")
		if err != nil {
			t.Fatalf("EntryResumeState: %v", err)
		}
		if state.Pending != 0 || state.InFlight != 1 || state.Completed != 1 || state.Failed != 1 {
			t.Fatalf("state = %+v", state)
		}
		want := []string{"r-1", "r-3"} // ordered by ordinal
		if len(state.TerminalRequestIDs) != 2 || state.TerminalRequestIDs[0] != want[0] || state.TerminalRequestIDs[1] != want[1] {
			t.Fatalf("terminal IDs = %v, want %v", state.TerminalRequestIDs, want)
		}
	})

	t.Run("list paginates by ordinal", func(t *testing.T) {
		store, _ := seedActivated(t)
		page, err := store.ListBatchEntries(ctx, "job-1", -1, 2)
		if err != nil || len(page) != 2 || page[0].RequestID != "r-1" || page[1].RequestID != "r-2" {
			t.Fatalf("first page: err=%v entries=%+v", err, page)
		}
		page, err = store.ListBatchEntries(ctx, "job-1", page[1].Ordinal, 2)
		if err != nil || len(page) != 1 || page[0].RequestID != "r-3" {
			t.Fatalf("second page: err=%v entries=%+v", err, page)
		}
		if _, err := store.ListBatchEntries(ctx, "job-1", 0, 0); err == nil {
			t.Fatal("zero limit: expected error")
		}
	})

	t.Run("find returns the entry or nil", func(t *testing.T) {
		store, _ := seedActivated(t)
		entry, err := store.FindEntryByRequestID(ctx, "job-1", "r-3")
		if err != nil || entry == nil {
			t.Fatalf("find r-3: err=%v entry=%v", err, entry)
		}
		if entry.Status != api.ResumableEntryFailed || entry.Attempts != 0 ||
			entry.ErrorCode != "model_not_found" || entry.ResultKey != "results/job-1/r-3.jsonl" ||
			entry.ResultBytes != 42 || entry.ResultSHA256 != "abc123" {
			t.Fatalf("rejected entry = %+v", entry)
		}
		if entry, err = store.FindEntryByRequestID(ctx, "job-1", "missing"); err != nil || entry != nil {
			t.Fatalf("find missing: err=%v entry=%v", err, entry)
		}
	})
}
