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
	"os"

	"github.com/google/uuid"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

func stableBatchRequestID(batchID, customID string) string {
	name := []byte(batchID + "\x00" + customID)
	return newBatchRequestID(uuid.NewSHA1(uuid.NameSpaceOID, name).String())
}

// restoreManifestArtifacts recreates only the deterministic local execution
// inputs. Result artifacts are rebuilt from durable checkpoints separately.
func (p *Processor) restoreManifestArtifacts(manifest *db.BatchManifest, tenantID string) error {
	if manifest == nil {
		return fmt.Errorf("manifest is required")
	}
	jobRoot, err := p.jobRootDir(manifest.BatchID, tenantID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(jobRoot, 0o700); err != nil {
		return err
	}
	input, _, err := p.createLocalInputFile(manifest.BatchID, tenantID)
	if err != nil {
		return err
	}
	defer input.Close()

	acc := newPlanAccumulator(jobRoot)
	modelToSafe := make(map[string]string)
	used := make(map[string]int)
	var offset int64
	for i, entry := range manifest.Entries {
		line := append(append([]byte(nil), entry.Payload...), '\n')
		meta, err := extractAndValidateLine(line)
		if err != nil {
			return fmt.Errorf("validate manifest entry %d: %w", i, err)
		}
		if _, err := input.Write(line); err != nil {
			return fmt.Errorf("write manifest entry %d: %w", i, err)
		}
		offset = accumulatePlanEntry(acc, meta.ModelID, modelToSafe, used, offset, uint32(len(line)), meta.PrefixHash)
	}
	if err := input.Sync(); err != nil {
		return err
	}
	if err := finalizePlanFiles(acc, modelToSafe); err != nil {
		return err
	}
	if err := writeModelMappings(jobRoot, modelToSafe, int64(len(manifest.Entries)), 0); err != nil {
		return err
	}
	errorPath, err := p.jobErrorFilePath(manifest.BatchID, tenantID)
	if err != nil {
		return err
	}
	errorFile, err := os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	return errorFile.Close()
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
