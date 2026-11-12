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

// The file provides an in-memory mock implementation of api.ResumableStore
// that mirrors the fencing semantics of the Postgres implementation.
package mock

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

// Compile-time check: MockResumableStore implements api.ResumableStore.
var _ api.ResumableStore = (*MockResumableStore)(nil)

// mockResumableBatch is one in-memory batch_items row relevant to resumable
// ownership.
type mockResumableBatch struct {
	processorID      string
	epoch            int64
	recoveryAttempts int64
	resumable        bool
	terminal         bool
	leaseExpiresAt   time.Time
	leaseRenewals    int
	entries          []*api.ResumableEntry // ordered by ordinal
	statusName       string                // current status name, for expected-status fences
	statusDoc        []byte                // persisted terminal status document
}

// MockResumableStore is a stateful in-memory api.ResumableStore. Writes are
// fenced exactly like the Postgres implementation: on owner identity, epoch,
// and an unexpired lease measured against Now.
type MockResumableStore struct {
	mu      sync.Mutex
	batches map[string]*mockResumableBatch

	// Now resolves the current time for lease checks; defaults to time.Now.
	// Tests can freeze it to exercise lease fencing deterministically.
	Now func() time.Time
}

func NewMockResumableStore() *MockResumableStore {
	return &MockResumableStore{
		batches: make(map[string]*mockResumableBatch),
		Now:     time.Now,
	}
}

func (m *MockResumableStore) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

// SeedBatch registers a claimed, validating batch at the given epoch — the
// state PQDequeue leaves behind — so activation can run against it.
func (m *MockResumableStore) SeedBatch(batchID, processorID string, epoch int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches[batchID] = &mockResumableBatch{processorID: processorID, epoch: epoch}
}

// MarkTerminal flips a batch to a terminal status so ownership writes fence out.
func (m *MockResumableStore) MarkTerminal(batchID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.batches[batchID]; ok {
		b.terminal = true
	}
}

// BatchOwnership exposes a seeded batch's ownership state for test
// assertions: the processor ID (empty once requeued), epoch, and recovery
// attempt count.
func (m *MockResumableStore) BatchOwnership(batchID string) (processorID string, epoch, recoveryAttempts int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	b, ok := m.batches[batchID]
	if !ok {
		return "", 0, 0
	}
	return b.processorID, b.epoch, b.recoveryAttempts
}

// LeaseRenewals returns how many times the batch's lease was successfully
// renewed, for assertions that a renewal goroutine is running.
func (m *MockResumableStore) LeaseRenewals(batchID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.batches[batchID]; ok {
		return b.leaseRenewals
	}
	return 0
}

// SetBatchStatus overrides a seeded batch's status name, staging transitions
// the row store performs outside this mock (e.g. in_progress → finalizing).
func (m *MockResumableStore) SetBatchStatus(batchID, statusName string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if b, ok := m.batches[batchID]; ok {
		b.statusName = statusName
	}
}

func (m *MockResumableStore) Close() error {
	return nil
}

func (m *MockResumableStore) ActivateResumableBatch(ctx context.Context, activation *api.ResumableActivation) error {
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

	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.batches[activation.BatchID]
	if !ok || b.terminal || b.resumable || len(b.entries) > 0 ||
		b.processorID != activation.ProcessorID || b.epoch != activation.Epoch {
		return fmt.Errorf("ActivateResumableBatch: %s is not owned at epoch %d or already activated: %w",
			activation.BatchID, activation.Epoch, api.ErrConflict)
	}

	for _, e := range activation.Entries {
		entry := &api.ResumableEntry{
			BatchID:      activation.BatchID,
			RequestID:    e.RequestID,
			Ordinal:      e.Ordinal,
			CustomID:     e.CustomID,
			Status:       api.ResumableEntryStatus(e.Status),
			ResultKey:    e.ResultKey,
			ResultBytes:  e.ResultBytes,
			ResultSHA256: e.ResultSHA256,
			ErrorCode:    e.ErrorCode,
		}
		b.entries = append(b.entries, entry)
	}
	sort.Slice(b.entries, func(i, j int) bool { return b.entries[i].Ordinal < b.entries[j].Ordinal })

	b.resumable = true
	b.leaseExpiresAt = m.now().Add(activation.LeaseTTL)
	b.statusName = "in_progress"
	return nil
}

func (m *MockResumableStore) RenewResumableBatchLease(ctx context.Context, batchID, processorID string, epoch int64, ttl time.Duration) error {
	if ttl <= 0 {
		return fmt.Errorf("RenewResumableBatchLease: lease TTL must be > 0")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.batches[batchID]
	if !ok || b.terminal || !b.resumable || b.processorID != processorID ||
		b.epoch != epoch || !b.leaseExpiresAt.After(m.now()) {
		return fmt.Errorf("RenewResumableBatchLease: %s is not owned by %s at epoch %d: %w",
			batchID, processorID, epoch, api.ErrConflict)
	}
	b.leaseExpiresAt = m.now().Add(ttl)
	b.leaseRenewals++
	return nil
}

func (m *MockResumableStore) FinalizeResumableBatch(ctx context.Context, fin *api.ResumableFinalization) error {
	if fin == nil {
		return fmt.Errorf("FinalizeResumableBatch: nil finalization")
	}
	if fin.BatchID == "" || fin.ProcessorID == "" {
		return fmt.Errorf("FinalizeResumableBatch: batch ID and processor ID are required")
	}
	if fin.ExpectedStatus == "" || len(fin.Status) == 0 {
		return fmt.Errorf("FinalizeResumableBatch: expected and target status are required")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.batches[fin.BatchID]
	if ok && m.ownedFence(b, fin.ProcessorID, fin.Epoch) && b.statusName == fin.ExpectedStatus {
		b.statusDoc = fin.Status
		b.terminal = true
		b.resumable = false
		b.leaseExpiresAt = time.Time{}
		return nil
	}
	// Zero rows: the fence failed, or an earlier attempt committed the
	// identical status document (ambiguous response). The comparison is
	// semantic, mirroring the jsonb canonicalization of the row store.
	if ok && b.terminal && jsonEqual(b.statusDoc, fin.Status) {
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

func (m *MockResumableStore) RequeueResumableOrphan(ctx context.Context, batchID, processorID string, epoch int64, requireExpiredLease, bumpAttempts bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.batches[batchID]
	if !ok || b.terminal || !b.resumable || b.processorID != processorID || b.epoch != epoch {
		return nil
	}
	if requireExpiredLease && b.leaseExpiresAt.After(m.now()) {
		return nil
	}

	b.processorID = ""
	b.leaseExpiresAt = time.Time{}
	b.epoch++
	if bumpAttempts {
		b.recoveryAttempts++
	}
	// Record the orphaned owner as the submitter of its in-flight entries,
	// mirroring the Postgres requeue backfill.
	for _, e := range b.entries {
		if e.Status == api.ResumableEntryInFlight && e.SubmittedBy == "" {
			e.SubmittedBy = processorID
		}
	}
	return nil
}

// findEntry returns the batch and entry, or nil when either is missing.
func (m *MockResumableStore) findEntry(batchID, requestID string) (*mockResumableBatch, *api.ResumableEntry) {
	b, ok := m.batches[batchID]
	if !ok {
		return nil, nil
	}
	for _, e := range b.entries {
		if e.RequestID == requestID {
			return b, e
		}
	}
	return b, nil
}

// ownedFence reports whether the ownership predicate for entry writes holds.
func (m *MockResumableStore) ownedFence(b *mockResumableBatch, processorID string, epoch int64) bool {
	return b != nil && !b.terminal && b.resumable && b.processorID == processorID &&
		b.epoch == epoch && b.leaseExpiresAt.After(m.now())
}

func (m *MockResumableStore) MarkEntryInFlight(ctx context.Context, batchID, requestID, processorID string, epoch int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, e := m.findEntry(batchID, requestID)
	if e != nil && !e.Status.IsTerminal() && m.ownedFence(b, processorID, epoch) {
		e.Status = api.ResumableEntryInFlight
		e.Attempts++
		e.SubmittedBy = processorID
		return true, nil
	}
	return false, entryNotUpdatable("MarkEntryInFlight", b, e, batchID, requestID, processorID, epoch)
}

func (m *MockResumableStore) CheckpointEntryResult(ctx context.Context, batchID, requestID, processorID string, epoch int64, result *api.ResumableResult) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if result == nil {
		return false, fmt.Errorf("CheckpointEntryResult: nil result")
	}
	// Only terminal statuses commit, mirroring the Postgres implementation.
	if result.Status != api.ResumableEntryCompleted && result.Status != api.ResumableEntryFailed {
		return false, fmt.Errorf("CheckpointEntryResult: status must be %q or %q, got %q",
			api.ResumableEntryCompleted, api.ResumableEntryFailed, result.Status)
	}

	b, e := m.findEntry(batchID, requestID)
	if e != nil && !e.Status.IsTerminal() && m.ownedFence(b, processorID, epoch) {
		e.Status = result.Status
		e.ResultKey = result.ResultKey
		e.ResultBytes = result.ResultBytes
		e.ResultSHA256 = result.ResultSHA256
		e.ErrorCode = result.ErrorCode
		return true, nil
	}
	return false, entryNotUpdatable("CheckpointEntryResult", b, e, batchID, requestID, processorID, epoch)
}

// entryNotUpdatable disambiguates a failed fenced entry write, mirroring the
// Postgres implementation: a terminal entry still owned by the caller is a
// duplicate delivery (no error); anything else is ownership loss.
func entryNotUpdatable(op string, b *mockResumableBatch, e *api.ResumableEntry, batchID, requestID, processorID string, epoch int64) error {
	if e != nil && b != nil && e.Status.IsTerminal() &&
		b.processorID == processorID && b.epoch == epoch {
		return nil
	}
	return fmt.Errorf("%s: %s/%s is not owned by %s at epoch %d: %w",
		op, batchID, requestID, processorID, epoch, api.ErrConflict)
}

func (m *MockResumableStore) EntryResumeState(ctx context.Context, batchID string) (*api.ResumableResumeState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.batches[batchID]
	if !ok {
		return nil, fmt.Errorf("EntryResumeState: unknown batch %s", batchID)
	}

	state := &api.ResumableResumeState{}
	for _, e := range b.entries {
		switch e.Status {
		case api.ResumableEntryPending:
			state.Pending++
		case api.ResumableEntryInFlight:
			state.InFlight++
		case api.ResumableEntryCompleted:
			state.Completed++
			state.TerminalRequestIDs = append(state.TerminalRequestIDs, e.RequestID)
		case api.ResumableEntryFailed:
			state.Failed++
			state.TerminalRequestIDs = append(state.TerminalRequestIDs, e.RequestID)
		}
	}
	return state, nil
}

func (m *MockResumableStore) ListBatchEntries(ctx context.Context, batchID string, afterOrdinal int64, limit int) ([]*api.ResumableEntry, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("ListBatchEntries: limit must be > 0")
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	b, ok := m.batches[batchID]
	if !ok {
		return nil, fmt.Errorf("ListBatchEntries: unknown batch %s", batchID)
	}

	var entries []*api.ResumableEntry
	for _, e := range b.entries {
		if int64(len(entries)) == int64(limit) {
			break
		}
		if e.Ordinal > afterOrdinal {
			// Snapshots, like rows materialized from a Postgres query.
			entry := *e
			entries = append(entries, &entry)
		}
	}
	return entries, nil
}

func (m *MockResumableStore) FindEntryByRequestID(ctx context.Context, batchID, requestID string) (*api.ResumableEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, e := m.findEntry(batchID, requestID)
	if e == nil {
		return nil, nil
	}
	// Return a snapshot: the caller reads it after the lock is released while
	// a fenced checkpoint may still mutate the stored entry.
	entry := *e
	return &entry, nil
}

func (m *MockResumableStore) HasOpenEntryByRequest(_ context.Context, requestID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, b := range m.batches {
		for _, e := range b.entries {
			if e.RequestID == requestID && !e.Status.IsTerminal() {
				return true, nil
			}
		}
	}
	return false, nil
}
