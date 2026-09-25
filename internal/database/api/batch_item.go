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

package api

// BatchItem is the database item type
type BatchItem struct {
	BaseIndexes
	BaseContents

	// ProcessorID identifies the processor pod that owns this job.
	// Set atomically with the status transition to in_progress during dequeue.
	// Empty when the job is queued (validating) or in a terminal state.
	ProcessorID string

	// Priority determines dequeue order (lower = higher priority).
	// Stores SLO.UnixMicro() — jobs with earlier deadlines are dequeued first.
	Priority int64

	// Epoch is a fencing token incremented on every ownership change (dequeue,
	// recovery, GC reclaim). Processor writes include WHERE epoch = N so a
	// zombie whose lease was reclaimed cannot overwrite the new owner's state.
	Epoch int64

	// BumpEpoch signals that DBUpdate should atomically increment the epoch
	// in addition to checking it. Set by the GC reconciler when reclaiming
	// an orphan — this is an ownership change (like a Raft term bump).
	BumpEpoch bool

	// RecoveryAttempts counts startup recoveries under the current ownership.
	// Reset on dequeue, incremented by PQClaimOwned.
	RecoveryAttempts int64

	// Resumable marks jobs whose durable manifest allows startup recovery to
	// resume work without GC resetting or terminalizing the batch.
	Resumable bool

	// ExpectedResumable optionally fences DBUpdate on the persisted resumable
	// value. It is a mutation precondition and is not itself persisted.
	ExpectedResumable *bool
}

// BatchQuery specifies parameters for retrieving batches from the database.
type BatchQuery struct {
	BaseQuery
	NonTerminal    bool
	ProcessorID    string
	HasProcessorID bool // filter for processor_id IS NOT NULL (owned jobs)
}

// BatchDBClient is the typed database client for batch objects.
type BatchDBClient = DBClient[BatchItem, BatchQuery]
