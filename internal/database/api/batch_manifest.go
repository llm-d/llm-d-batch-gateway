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

const BatchManifestVersion = 1

// BatchManifestEntry is the durable identity and dispatch input for one request.
// Ordinal preserves input order while RequestID remains stable across takeover.
type BatchManifestEntry struct {
	Ordinal   int64           `json:"ordinal"`
	RequestID string          `json:"request_id"`
	CustomID  string          `json:"custom_id"`
	ModelID   string          `json:"model_id"`
	Payload   json.RawMessage `json:"payload"`
}

// BatchManifest is complete before a batch can be marked resumable. A manifest
// is immutable for one ownership epoch; later slices may append attempt and
// result checkpoints in separate tables without rewriting dispatch identity.
type BatchManifest struct {
	BatchID string               `json:"batch_id"`
	Version int                  `json:"version"`
	Entries []BatchManifestEntry `json:"entries"`
}

// ResumableBatchStore is the PostgreSQL-only capability used by durable
// recovery. Implementations must persist the complete manifest and change the
// batch to resumable/in_progress atomically under the supplied status, owner,
// and epoch preconditions.
type ResumableBatchStore interface {
	ActivateResumableBatch(ctx context.Context, item *BatchItem, expectedStatus []byte, manifest *BatchManifest) error
	GetBatchManifest(ctx context.Context, batchID string) (*BatchManifest, error)
}
