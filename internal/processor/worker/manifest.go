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
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"github.com/google/uuid"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

func stableBatchRequestID(batchID, customID string) string {
	name := []byte(batchID + "\x00" + customID)
	return newBatchRequestID(uuid.NewSHA1(uuid.NameSpaceOID, name).String())
}

// buildBatchManifest records the validated request payloads and their stable
// identities. It consumes the staged input after planning has completed; the
// caller activates the manifest only after this function returns successfully.
func buildBatchManifest(batchID string, input io.Reader) (*db.BatchManifest, error) {
	if batchID == "" {
		return nil, fmt.Errorf("batch ID is required")
	}
	reader := bufio.NewReaderSize(input, 1024*1024)
	manifest := &db.BatchManifest{BatchID: batchID, Version: db.BatchManifestVersion}
	seen := make(map[string]struct{})

	for ordinal := int64(0); ; ordinal++ {
		line, done, err := readNormalizedLine(reader)
		if err != nil {
			return nil, fmt.Errorf("read manifest line %d: %w", ordinal+1, err)
		}
		if done {
			break
		}
		payload := bytes.TrimSuffix(line, []byte{'\n'})
		var request batch_types.Request
		if err := json.Unmarshal(payload, &request); err != nil {
			return nil, fmt.Errorf("decode manifest line %d: %w", ordinal+1, err)
		}
		if request.CustomID == "" {
			return nil, fmt.Errorf("manifest line %d has empty custom_id", ordinal+1)
		}
		if _, exists := seen[request.CustomID]; exists {
			return nil, fmt.Errorf("manifest line %d has duplicate custom_id %q", ordinal+1, request.CustomID)
		}
		seen[request.CustomID] = struct{}{}

		modelID, _ := request.Body["model"].(string)
		manifest.Entries = append(manifest.Entries, db.BatchManifestEntry{
			Ordinal:   ordinal,
			RequestID: stableBatchRequestID(batchID, request.CustomID),
			CustomID:  request.CustomID,
			ModelID:   modelID,
			Payload:   append(json.RawMessage(nil), payload...),
		})
	}

	return manifest, nil
}
