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
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

// manifestEntry is the per-line metadata accumulated while preprocessing a
// batch. RequestIDs are stable (derived from batch + custom IDs), so the same
// input always yields the same manifest.
type manifestEntry struct {
	Ordinal      int64
	CustomID     string
	RequestID    string
	Rejected     bool
	ErrorCode    string
	ResultKey    string
	ResultBytes  int64
	ResultSHA256 string
	// ErrLine is the marshaled error-object line of a rejected entry
	// (in-memory only), stored as its result object at activation.
	ErrLine []byte
}

// requestManifest is the in-memory entry list of a preprocessed batch: the
// input for resumable activation and the comparison set for recovery's
// integrity check. It is never persisted as a blob.
type requestManifest struct {
	Entries []manifestEntry
}

func (m *requestManifest) add(e manifestEntry) {
	m.Entries = append(m.Entries, e)
}

// rejectedEntry records a line rejected during ingestion (e.g. unregistered
// model), with the metadata and bytes of its error-object line.
func rejectedEntry(batchID string, ordinal int64, customID, requestID, errorCode string, errLine []byte) manifestEntry {
	sum := sha256.Sum256(errLine)
	return manifestEntry{
		Ordinal:      ordinal,
		CustomID:     customID,
		RequestID:    requestID,
		Rejected:     true,
		ErrorCode:    errorCode,
		ResultKey:    resumableResultKey(batchID, requestID),
		ResultBytes:  int64(len(errLine)),
		ResultSHA256: hex.EncodeToString(sum[:]),
		ErrLine:      errLine,
	}
}

// activationEntries converts the manifest into the entry set for resumable
// activation: accepted lines activate as pending, rejected lines as failed
// with the metadata of the error object ingestion wrote for them.
func (m *requestManifest) activationEntries() []db.ResumableActivationEntry {
	entries := make([]db.ResumableActivationEntry, len(m.Entries))
	for i, e := range m.Entries {
		entry := db.ResumableActivationEntry{
			RequestID: e.RequestID,
			Ordinal:   e.Ordinal,
			CustomID:  e.CustomID,
			Status:    string(db.ResumableEntryPending),
		}
		if e.Rejected {
			entry.Status = string(db.ResumableEntryFailed)
			entry.ErrorCode = e.ErrorCode
			entry.ResultKey = e.ResultKey
			entry.ResultBytes = e.ResultBytes
			entry.ResultSHA256 = e.ResultSHA256
		}
		entries[i] = entry
	}
	return entries
}

// resumableResultKey is the file-store key of one request's result object.
func resumableResultKey(batchID, requestID string) string {
	return fmt.Sprintf("results/%s/%s.jsonl", batchID, requestID)
}
