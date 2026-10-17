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

package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/go-logr/logr"
	"github.com/google/uuid"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

// batchResultStore is the durable pipeline's ResultSink for one activated
// batch: each result is stored as its own object under a canonical key and
// then committed to the entry store with a fenced first-wins update. A crash
// between the two leaves an orphan object, never a lost result.
type batchResultStore struct {
	files       filesapi.BatchFilesClient
	entries     db.ResumableEntryStore
	batchID     string
	folder      string
	processorID string
	epoch       int64
}

var _ pipeline.ResultSink = (*batchResultStore)(nil)

func newBatchResultStore(
	files filesapi.BatchFilesClient,
	entries db.ResumableEntryStore,
	batchID, tenantID, processorID string,
	epoch int64,
) (*batchResultStore, error) {
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		return nil, fmt.Errorf("result store for %s: %w", batchID, err)
	}
	return &batchResultStore{
		files:       files,
		entries:     entries,
		batchID:     batchID,
		folder:      folder,
		processorID: processorID,
		epoch:       epoch,
	}, nil
}

// Checkpoint stores the marshaled result line and commits its entry. It
// returns false with nil error for a duplicate delivery (the entry is already
// terminal — its result was committed by an earlier delivery) and an error
// wrapping db.ErrConflict when batch ownership was lost.
func (s *batchResultStore) Checkpoint(ctx context.Context, msg *pipeline.ResultItem, line []byte, digest string, size int64) (bool, error) {
	// A terminal entry is a duplicate delivery; drop it without a PUT.
	entry, err := s.entries.FindEntryByRequestID(ctx, s.batchID, msg.RequestID)
	if err != nil {
		return false, fmt.Errorf("find entry %s: %w", msg.RequestID, err)
	}
	if entry != nil && entry.Status.IsTerminal() {
		return false, nil
	}

	result := &db.ResumableResult{
		Status:       db.ResumableEntryFailed,
		ResultBytes:  size,
		ResultSHA256: digest,
	}
	if msg.Succeeded() {
		result.Status = db.ResumableEntryCompleted
	} else if msg.Error != nil {
		result.ErrorCode = msg.Error.Code
	}

	key, err := s.storeObject(ctx, msg.RequestID, line, digest, size)
	if err != nil {
		return false, err
	}
	result.ResultKey = key

	committed, err := s.entries.CheckpointEntryResult(ctx, s.batchID, msg.RequestID, s.processorID, s.epoch, result)
	if err != nil {
		return false, fmt.Errorf("checkpoint entry %s: %w", msg.RequestID, err)
	}
	if !committed {
		// Lost the first-wins race to a duplicate. A fallback object this
		// call wrote is garbage now — the winner owns the canonical state —
		// so remove it; prefix cleanup is the backstop.
		if key != resumableResultKey(s.batchID, msg.RequestID) {
			if err := s.files.Delete(ctx, key, s.folder); err != nil {
				logr.FromContextOrDiscard(ctx).Error(err, "Failed to delete orphaned fallback result object", "key", key)
			}
		}
		return false, nil
	}
	return true, nil
}

// putRejectedObject stores the error object of a line rejected during
// ingestion. The line is deterministic, so a PUT that finds the key already
// present — an earlier attempt that crashed before activation — verifies the
// bytes instead of failing.
func (s *batchResultStore) putRejectedObject(ctx context.Context, e manifestEntry) error {
	_, err := s.files.Store(ctx, e.ResultKey, s.folder, 0, 0, bytes.NewReader(e.ErrLine))
	if err == nil {
		return nil
	}
	if !errors.Is(err, filesapi.ErrFileExists) {
		return fmt.Errorf("store rejected result %s: %w", e.ResultKey, err)
	}
	if !s.verifyStored(ctx, e.ResultKey, e.ResultSHA256, e.ResultBytes) {
		return fmt.Errorf("rejected result %s holds unexpected bytes", e.ResultKey)
	}
	return nil
}

// storeObject PUTs the line at its canonical result key. The file store is
// create-only: when the key is taken, matching size and digest mean the object
// is ours from an earlier attempt (retried PUT); different content means a
// retried request produced a new result and the line goes to a unique
// fallback key, which the entry commit records.
func (s *batchResultStore) storeObject(ctx context.Context, requestID string, line []byte, digest string, size int64) (string, error) {
	key := resumableResultKey(s.batchID, requestID)
	_, err := s.files.Store(ctx, key, s.folder, 0, 0, bytes.NewReader(line))
	if err == nil {
		return key, nil
	}
	if !errors.Is(err, filesapi.ErrFileExists) {
		return "", fmt.Errorf("store result %s: %w", key, err)
	}
	if s.verifyStored(ctx, key, digest, size) {
		return key, nil
	}
	fallback := fmt.Sprintf("results/%s/%s.%s.jsonl", s.batchID, requestID, uuid.NewString())
	if _, err := s.files.Store(ctx, fallback, s.folder, 0, 0, bytes.NewReader(line)); err != nil {
		return "", fmt.Errorf("store result %s: %w", fallback, err)
	}
	return fallback, nil
}

// verifyStored reports whether the object at key has exactly the given size
// and SHA-256 digest.
func (s *batchResultStore) verifyStored(ctx context.Context, key, digest string, size int64) bool {
	reader, md, err := s.files.Retrieve(ctx, key, s.folder)
	if err != nil {
		return false
	}
	defer reader.Close()
	if md == nil || md.Size != size {
		return false
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		return false
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]) == digest
}
