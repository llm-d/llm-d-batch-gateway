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

// This file specifies the types and interfaces for resumable batch jobs:
// per-request entry metadata, batch activation, lease management, and
// fenced entry state transitions.

package api

import (
	"context"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/store"
)

// ResumableEntryStatus is the lifecycle state of a single batch request entry.
type ResumableEntryStatus string

const (
	// ResumableEntryPending is an entry that has not been submitted yet.
	ResumableEntryPending ResumableEntryStatus = "pending"
	// ResumableEntryInFlight is an entry that was submitted but not checkpointed.
	ResumableEntryInFlight ResumableEntryStatus = "in_flight"
	// ResumableEntryCompleted is an entry with a checkpointed successful result.
	ResumableEntryCompleted ResumableEntryStatus = "completed"
	// ResumableEntryFailed is an entry with a checkpointed failure.
	ResumableEntryFailed ResumableEntryStatus = "failed"
)

// IsTerminal reports whether the entry status admits no further transitions.
func (s ResumableEntryStatus) IsTerminal() bool {
	return s == ResumableEntryCompleted || s == ResumableEntryFailed
}

// ResumableEntry is one batch_entries row: per-request metadata for a resumable
// batch. Payloads live only in the file store; ResultKey/ResultBytes/
// ResultSHA256 locate and verify the checkpointed result object.
type ResumableEntry struct {
	BatchID   string
	RequestID string
	Ordinal   int64
	CustomID  string
	Status    ResumableEntryStatus
	Attempts  int64
	// SubmittedBy records the processor that last marked the entry
	// in_flight (the submitter): its per-processor async result route is
	// where an unacknowledged delivery of the entry's result is stranded
	// after a disruption, so the next owner recovers from there.
	SubmittedBy  string
	ResultKey    string
	ResultBytes  int64
	ResultSHA256 string
	// ErrorCode is the code of the entry's error-object line; empty for a
	// failed entry whose line preserves a non-2xx HTTP response (an output
	// line, like the sync pipeline's routing).
	ErrorCode string
}

// ResumableActivationEntry is one entry of the activation set derived from the
// preprocessed input. Accepted lines activate as pending; rejected lines
// (e.g. unregistered model) activate as failed with their error-object metadata.
// JSON field names match the activation SQL's jsonb_to_recordset columns.
type ResumableActivationEntry struct {
	RequestID    string `json:"request_id"`
	Ordinal      int64  `json:"ordinal"`
	CustomID     string `json:"custom_id"`
	Status       string `json:"status"`
	ErrorCode    string `json:"error_code,omitempty"`
	ResultKey    string `json:"result_key,omitempty"`
	ResultBytes  int64  `json:"result_bytes,omitempty"`
	ResultSHA256 string `json:"result_sha256,omitempty"`
}

// ResumableActivation carries the state transition validating → in_progress
// for a resumable batch: insert the entry set and flip ownership atomically.
type ResumableActivation struct {
	BatchID     string
	ProcessorID string
	Epoch       int64
	LeaseTTL    time.Duration
	Entries     []ResumableActivationEntry
}

// ResumableResult is the checkpointed outcome of one request: the result
// object's location and integrity metadata plus the entry status to commit.
type ResumableResult struct {
	Status       ResumableEntryStatus
	ResultKey    string
	ResultBytes  int64
	ResultSHA256 string
	ErrorCode    string
}

// ResumableResumeState aggregates entry counts for a batch and lists the
// request IDs of terminal entries (the resume skip set).
type ResumableResumeState struct {
	Pending            int64
	InFlight           int64
	Completed          int64
	Failed             int64
	TerminalRequestIDs []string
	// InFlightSubmittedBy maps each in-flight entry's request ID to the
	// processor that submitted it. The resuming owner pre-stores these
	// entries and defers their re-submission for a recovery window: their
	// results — completed, failed, or deadline-exceeded — arrive on the
	// submitter's per-processor result route and are recovered through it
	// instead of re-running inference. Entries with an unknown submitter
	// (written before submitter tracking) are absent and re-submit directly.
	InFlightSubmittedBy map[string]string
}

// ResumableBatchStore activates batches into the resumable pipeline.
type ResumableBatchStore interface {
	// ActivateResumableBatch performs the single-statement transition
	// validating → in_progress: it inserts the entry set and sets
	// resumable/lease atomically. It is fenced on the calling
	// processor's queue claim (processor_id + epoch) and requires the batch to
	// be non-resumable with no existing entries. A failed fence returns
	// ErrConflict.
	ActivateResumableBatch(ctx context.Context, activation *ResumableActivation) error
}

// ResumableFinalization is one fenced terminal transition of an activated
// batch: the terminal status document to persist, fenced on the owning
// processor and the source status the transition starts from.
type ResumableFinalization struct {
	BatchID     string
	ProcessorID string
	Epoch       int64
	// ExpectedStatus is the source status name the transition is fenced on
	// (e.g. "finalizing" or "cancelling").
	ExpectedStatus string
	// Status is the terminal status document to persist.
	Status []byte
}

// ResumableBatchLeaseStore manages ownership of an activated batch.
type ResumableBatchLeaseStore interface {
	// RenewResumableBatchLease extends the owner's lease, fenced on the
	// owning processor, epoch, and an unexpired lease. A failed fence (ownership
	// moved or lease lapsed) returns ErrConflict.
	RenewResumableBatchLease(ctx context.Context, batchID, processorID string, epoch int64, ttl time.Duration) error

	// FinalizeResumableBatch transitions an owned, activated batch to a
	// terminal status and releases the lease (resumable/lease cleared;
	// processor_id stays for audit). The write is fenced on the owning
	// processor, epoch, an unexpired lease, and the expected source status. A
	// zero-row update is retried as a verification: a row already persisting
	// exactly the given status document is an idempotent success (an ambiguous
	// response from an earlier attempt); anything else returns ErrConflict.
	FinalizeResumableBatch(ctx context.Context, fin *ResumableFinalization) error

	// RequeueResumableOrphan clears ownership of an orphaned resumable batch
	// (processor_id = NULL, epoch + 1) so it becomes dequeuable again, keeping
	// status and entries. It is a fenced CAS on the snapshot the reconciler
	// fetched: 0 rows means the snapshot is stale (e.g. a renewal raced) and
	// is a no-op success. When requireExpiredLease is set the requeue
	// additionally requires the lease to have lapsed — the wedged-owner
	// trigger; a confirmed pod death requeues regardless.
	//
	// bumpAttempts controls the recovery-budget accounting: the GC requeue
	// of a dead or wedged owner and a processor handing back a claimed batch
	// each count one recovery attempt, but the startup-recovery requeue must
	// pass false — PQClaimOwned already counted the restart, and counting
	// both would spend two attempts of the budget per rollout.
	RequeueResumableOrphan(ctx context.Context, batchID, processorID string, epoch int64, requireExpiredLease, bumpAttempts bool) error
}

// ResumableEntryStore tracks per-request state of a resumable batch.
type ResumableEntryStore interface {
	// MarkEntryInFlight records that the owner is about to submit the request
	// (pending/in_flight → in_flight, attempts++), fenced on batch ownership
	// and an unexpired lease. It returns true when the caller should submit.
	// False means the entry is already terminal — a duplicate delivery that
	// must be dropped without submitting. A failed ownership fence returns
	// ErrConflict.
	MarkEntryInFlight(ctx context.Context, batchID, requestID, processorID string, epoch int64) (bool, error)

	// CheckpointEntryResult commits a result to the entry (first-wins: only
	// pending/in_flight entries accept a commit), fenced on batch ownership
	// and an unexpired lease. It returns true when this call committed the
	// result. False means the entry was already terminal — a duplicate
	// delivery whose result must be dropped. A failed ownership fence returns
	// ErrConflict.
	CheckpointEntryResult(ctx context.Context, batchID, requestID, processorID string, epoch int64, result *ResumableResult) (bool, error)

	// EntryResumeState aggregates the entry statuses of a batch and lists the
	// terminal request IDs.
	EntryResumeState(ctx context.Context, batchID string) (*ResumableResumeState, error)

	// ListBatchEntries returns up to limit entries with ordinal greater than
	// afterOrdinal in ascending ordinal order (keyset pagination).
	ListBatchEntries(ctx context.Context, batchID string, afterOrdinal int64, limit int) ([]*ResumableEntry, error)

	// FindEntryByRequestID returns the entry, or nil when it does not exist.
	FindEntryByRequestID(ctx context.Context, batchID, requestID string) (*ResumableEntry, error)

	// HasOpenEntryByRequest reports whether any batch holds a non-terminal
	// (pending or in_flight) entry for the request ID. The result
	// broadcaster's residue check uses it to decide whether an unrouted
	// delivery may be acknowledged: a delivery whose entry is open can still
	// be accounted for by whichever processor recovers its batch, so it must
	// stay unacknowledged for lease redelivery; only a delivery whose entry
	// is terminal or absent is safe to discard.
	HasOpenEntryByRequest(ctx context.Context, requestID string) (bool, error)
}

// ResumableStore combines the resumable batch, lease, and entry capabilities
// with client lifecycle.
type ResumableStore interface {
	ResumableBatchStore
	ResumableBatchLeaseStore
	ResumableEntryStore
	store.BatchClientAdmin
}
