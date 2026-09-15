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

// This file specifies the interfaces for the batch jobs data structures system.

package api

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/store"
)

// ErrConflict is returned by DBUpdate when expectedStatus is non-nil and
// the current status in the database does not match. Callers can use
// errors.Is(err, ErrConflict) to detect CAS failures.
var ErrConflict = errors.New("status conflict")

// -- Metadata storage --

// DBClient is a generic interface for managing database items in persistent storage.
//
// Each domain type (e.g., BatchItem, FileItem) gets its own typed DBClient implementation.
// Callers are responsible for serializing item contents (Spec, Status) before storing,
// and deserializing them after retrieval.
//
// Example usage:
//
//	type BatchDBClient = api.DBClient[BatchItem, BatchQuery]
//	type FileDBClient = api.DBClient[FileItem, FileQuery]
type DBClient[T any, Q any] interface {
	store.BatchClientAdmin

	// DBStore persists an item.
	DBStore(ctx context.Context, item *T) (err error)

	// DBGet gets the information of items.
	// If IDs are specified, this function will get items by the specified item IDs.
	// If tags are specified, this function will get items by the specified tags.
	// tagsLogicalCond specifies the logical condition to use when searching for the tags per item.
	// If expired is set to true, this function will get expired items.
	// If purpose is specified, this function will get items by the specified purpose.
	// If TenantID is specified, only items belonging to that tenant are returned.
	// Specification of a TenantID can be added to any other condition.
	// If no query conditions are specified, the function will return an empty list of items.
	// includeStatic specifies if to include the static part of an item in the returned output.
	// start and limit specify the pagination details. This is relevant for any query condition except IDs.
	// In the first iteration with pagination specify 0 for 'start', and in any subsequent iteration specify in 'start'
	// the value that was returned by 'cursor' in the previous iteration. The value returned by 'cursor' is an opaque integer.
	// The value specified in 'limit' can be different between iterations, and is a recommendation only.
	// items is a slice of returned items.
	// cursor is an opaque integer that should be given in the next paginated call via the 'start' parameter.
	// expectMore indicates if there are more items to get.
	DBGet(ctx context.Context, query *Q, includeStatic bool, start, limit int) (
		items []*T, cursor int, expectMore bool, err error)

	// DBUpdate updates the dynamic parts of an item.
	// The function will update in the item's record in the database - all the dynamic fields of the item which are not empty
	// in the given item object.
	// Any dynamic field that is empty in the given item object - will not be updated in the item's record in the database.
	//
	// When expectedStatus is non-nil, the update is conditional (CAS): it only
	// succeeds if the current status in the database matches expectedStatus exactly.
	// Returns ErrConflict if the status has changed since it was read.
	// Pass nil to skip the CAS check (unconditional update).
	DBUpdate(ctx context.Context, item *T, expectedStatus []byte) (err error)

	// DBDelete removes items by their IDs.
	DBDelete(ctx context.Context, IDs []string) (deletedIDs []string, err error)
}

// BatchRequestCounts holds the request progress counts for a batch job.
type BatchRequestCounts struct {
	Total     int64 `json:"total"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
}

// BatchProgressDBClient is a BatchDBClient that additionally supports
// in-flight progress updates.
type BatchProgressDBClient interface {
	BatchDBClient

	// DBUpdateProgress updates the in-flight request counts in the item's status
	// column, fenced by the item's current epoch. A write carrying a stale epoch
	// (e.g. from a fenced-out processor incarnation) matches no rows and returns
	// ErrConflict, so the caller knows it is no longer the owner and can abort.
	DBUpdateProgress(ctx context.Context, id string, epoch int64, counts BatchRequestCounts) error
}

// Tags are key-value pairs for filtering items.
type Tags map[string]string

type LogicalCond int

const (
	LogicalCondNone LogicalCond = iota
	LogicalCondAnd
	LogicalCondOr
)

var LogicalCondNames = map[LogicalCond]string{
	LogicalCondAnd: "and",
	LogicalCondOr:  "or",
}

// -- Batch jobs priority queue --

type BatchJobPriority struct {
	ID    string    `json:"id,omitempty"`    // [mandatory] ID of the batch job.
	SLO   time.Time `json:"slo,omitempty"`   // [mandatory] The SLO value determines the priority of the job.
	Data  []byte    `json:"data,omitempty"`  // [optional] User defined data.
	TTL   int       `json:"ttl,omitempty"`   // [optional] TTL in seconds applied on the entire queue. If used, this should be set to a sufficiently large value to prevent premature removal of items.
	Epoch int64     `json:"epoch,omitempty"` // Fencing token incremented on every ownership change.
	// RecoveryAttempts counts startup recoveries of the current ownership.
	RecoveryAttempts int64 `json:"recovery_attempts,omitempty"`
}

func (bj *BatchJobPriority) IsValid() error {
	if len(bj.ID) == 0 {
		return fmt.Errorf("ID is empty")
	}
	if bj.SLO.IsZero() {
		return fmt.Errorf("SLO is zero for ID %s", bj.ID)
	}
	return nil
}

// BatchPriorityQueueClient enables to perform operations on a priority queue of jobs.
type BatchPriorityQueueClient interface {
	store.BatchClientAdmin

	// PQClaimOwned takes ownership of every non-terminal job already assigned to
	// this processor for startup recovery: bumps the fencing epoch and the
	// recovery attempt counter and returns the claimed jobs with the new values.
	PQClaimOwned(ctx context.Context) ([]*BatchJobPriority, error)

	// PQEnqueue adds a job priority object to the queue.
	PQEnqueue(ctx context.Context, jobPriority *BatchJobPriority) (err error)

	// PQDequeue atomically removes and returns the job priority objects at the head of the queue,
	// up to the maximum number of objects specified in maxItems.
	// The function blocks up to the timeout value for a job priority object to be available.
	// If the timeout value is zero or negative, the function returns immediately.
	//
	// Implementations MUST atomically remove dequeued items from the queue. The processor
	// assumes exclusive dequeue semantics: a dequeued job will not be returned by any
	// subsequent PQDequeue call. Non-atomic (peek/lease) implementations will cause the
	// same job to be processed multiple times.
	PQDequeue(ctx context.Context, timeout time.Duration, maxItems int) (
		jobPriorities []*BatchJobPriority, err error)

	// PQDelete deletes a job priority object from the queue.
	// Specify the ID and SLO values for deleting. Other values are not required.
	// It returns the number of deleted objects.
	// An error is returned only if the deletion operation failed.
	PQDelete(ctx context.Context, jobPriority *BatchJobPriority) (nDeleted int, err error)

	// PQGetIDs returns the set of all job IDs currently in the priority queue.
	PQGetIDs(ctx context.Context) (map[string]bool, error)
}

// -- Batch jobs events and channels --

type BatchEventType int

const (
	BatchEventCancel BatchEventType = iota // Cancel a job.
	BatchEventPause                        // Pause a job.
	BatchEventResume                       // Resume a job.
	BatchEventMaxVal                       // [Internal] Indicates the max value for the enum. Don't use this value.
)

type BatchEvent struct {
	ID   string         // [mandatory] ID of the job.
	Type BatchEventType // [mandatory] Event type.
	TTL  int            // [mandatory] TTL in seconds for the event. Must be the same for all the events sent for the same job ID. Set this for sending an event. This is not returned for the event consumer.
}

func (be *BatchEvent) IsValid() error {
	if len(be.ID) == 0 {
		return fmt.Errorf("ID is empty")
	}
	if be.Type < BatchEventCancel || be.Type >= BatchEventMaxVal {
		return fmt.Errorf("event type %d is invalid for ID %s", be.Type, be.ID)
	}
	if be.TTL <= 0 {
		return fmt.Errorf("TTL is invalid for ID %s", be.ID)
	}
	return nil
}

type BatchEventsChan struct {
	ID      string          // ID of the job.
	Events  chan BatchEvent // Channel for receiving events for the job.
	CloseFn func()          // Function for closing the channel and the associated resources. Must be called by the consumer when the job's processing is finished.
}

// BatchEventChannelClient enables to create and use event channels for batch jobs being processed.
type BatchEventChannelClient interface {
	store.BatchClientAdmin

	// ECConsumerGetChannel gets an events channel for the job ID, to be used by a consumer to listen for events.
	// When the caller finishes processing a job - the caller must call the function CloseFn specified in BatchEventsChan,
	// to close the associated resources.
	ECConsumerGetChannel(ctx context.Context, ID string) (batchEventsChan *BatchEventsChan, err error)

	// ECProducerSendEvents sends the specified events via associated event channels.
	// The events are sent and consumed in FIFO order.
	ECProducerSendEvents(ctx context.Context, events []BatchEvent) (sentIDs []string, err error)
}
