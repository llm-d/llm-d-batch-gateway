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

import (
	"context"
	"encoding/json"
)

type BatchRequestAttempt struct {
	BatchID    string
	RequestID  string
	Attempt    int
	OwnerEpoch int64
}

type BatchResultCheckpoint struct {
	BatchID    string
	RequestID  string
	OwnerEpoch int64
	Result     json.RawMessage
}

// BatchCheckpointStore persists dispatch attempts and terminal results under
// the batch ownership epoch. CheckpointBatchResult is idempotent for duplicate
// delivery of the same stable request ID.
type BatchCheckpointStore interface {
	RecordBatchRequestAttempt(ctx context.Context, attempt *BatchRequestAttempt) error
	CheckpointBatchResult(ctx context.Context, checkpoint *BatchResultCheckpoint) error
	BatchResultCheckpoints(ctx context.Context, batchID string) ([]*BatchResultCheckpoint, error)
	CompletedBatchRequestIDs(ctx context.Context, batchID string) (map[string]bool, error)
}

// ResumableBatchFinalizer publishes a terminal status under the active owner
// epoch and atomically removes the resumable ownership marker. Implementations
// must treat an identical already-published terminal status as success so an
// ambiguous database response is safe to retry.
type ResumableBatchFinalizer interface {
	FinalizeResumableBatch(ctx context.Context, batch *BatchItem, expectedStatus []byte) error
}
