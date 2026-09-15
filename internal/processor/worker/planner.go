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

// this file contains the plan file accumulation and writing logic for the job processing plan files
package worker

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const modelMapFileName = "model_map.json"

type planRequestLine struct {
	CustomID string `json:"custom_id"`
	Method   string `json:"method"`
	URL      string `json:"url"`
	Body     struct {
		Model    string `json:"model"`
		Stream   *bool  `json:"stream,omitempty"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	} `json:"body"`
}

// messageText extracts only the system text needed for prefix grouping.
// Other content stays encoded and the original input is used for forwarding.
func messageText(content json.RawMessage) (string, error) {
	content = bytes.TrimSpace(content)
	if len(content) == 0 || bytes.Equal(content, []byte("null")) {
		return "", nil
	}
	switch content[0] {
	case '"':
		var text string
		err := json.Unmarshal(content, &text)
		return text, err
	case '[':
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(content, &parts); err != nil {
			return "", err
		}
		var builder strings.Builder
		for _, part := range parts {
			if part.Type == "text" {
				builder.WriteString(part.Text)
			}
		}
		return builder.String(), nil
	default:
		return "", fmt.Errorf("must be a string or an array of content parts")
	}
}

// NoPrefixHash is used when a request has no system prompt.
// Set to MaxUint32 so that no-prompt requests sort last, allowing
// requests with actual system prompts to be dispatched first.
const NoPrefixHash uint32 = math.MaxUint32

// planEntry is a single entry in the plan file.
// 16 bytes:
// Offset [int64 Offset] start byte offset of the request line in input.jsonl
// Length [uint32 Length] length of the request line in input.jsonl
// PrefixHash [uint32 PrefixHash] FNV-32a hash of the request's system prompt, used to group similar requests together during execution. If the system prompt is absent, the hash defaults to NoPrefixHash.
type planEntry struct {
	Offset     int64
	Length     uint32
	PrefixHash uint32
}

// marshalBinary encodes the entry into a fixed-size 16-byte little-endian buffer.
func (e planEntry) marshalBinary() [planEntrySize]byte {
	var buf [planEntrySize]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(e.Offset))
	binary.LittleEndian.PutUint32(buf[8:12], e.Length)
	binary.LittleEndian.PutUint32(buf[12:16], e.PrefixHash)
	return buf
}

// planAccumulator collects plan entries in memory per model during ingestion.
// Once all entries are collected, Finalize sorts each model's entries by PrefixHash and writes them to disk.
type planAccumulator struct {
	jobRootDir string
	entries    map[string][]planEntry // safeModelID -> entries
}

func newPlanAccumulator(jobRootDir string) *planAccumulator {
	return &planAccumulator{
		jobRootDir: jobRootDir,
		entries:    make(map[string][]planEntry),
	}
}

// modelMapFile is a map of modelID to the file name of the plan file
type modelMapFile struct {
	ModelToSafe   map[string]string `json:"model_to_safe"`
	SafeToModel   map[string]string `json:"safe_to_model"`
	LineCount     int64             `json:"line_count"`
	RejectedCount int64             `json:"rejected_count"`
}

func writeModelMapFile(jobRootDir string, modelMapFile modelMapFile) error {
	finalPath := filepath.Join(jobRootDir, modelMapFileName)
	tempPath := finalPath + ".tmp"

	if err := os.MkdirAll(jobRootDir, 0o700); err != nil {
		return fmt.Errorf("failed to create job root directory: %w", err)
	}

	data, err := json.MarshalIndent(modelMapFile, "", "  ") // indent for manual inspection
	if err != nil {
		return fmt.Errorf("failed to marshal model map file: %w", err)
	}

	// temp file creation
	if err := os.WriteFile(tempPath, data, 0o600); err != nil {
		return fmt.Errorf("failed to write model map file: %w", err)
	}

	// atomic commit
	if err := os.Rename(tempPath, finalPath); err != nil {
		// best effort to rename the file + cleanup
		_ = os.Remove(tempPath)
		return fmt.Errorf("failed to rename model map file: %w", err)
	}
	return nil
}

// safeFileNameRegex is a regex to replace invalid file name characters with '_'
var safeFileNameRegex = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

func (a *planAccumulator) plansDir() string {
	return filepath.Join(a.jobRootDir, "plans")
}

func (a *planAccumulator) Append(safeModelID string, entry planEntry) {
	a.entries[safeModelID] = append(a.entries[safeModelID], entry)
}

// internModelID interns the modelID to a safe file name
// it is used to ensure the modelID is a valid file name
// it is also used to avoid conflicts with other modelIDs
func internModelID(modelID string, used map[string]int) string {
	base := safeFileNameRegex.ReplaceAllString(modelID, "_")
	if base == "" {
		base = "unknown"
	}
	if used[base] == 0 {
		used[base] = 1
		return base
	}

	for {
		used[base]++
		candidate := fmt.Sprintf("%s_%d", base, used[base])
		if used[candidate] == 0 {
			// Reserve the final name too: another model may sanitize to it.
			used[candidate] = 1
			return candidate
		}
	}
}

// Finalize sorts each model's entries by PrefixHash and writes them to disk.
// Entries with the same PrefixHash are grouped together, enabling downstream
// inference gateway optimizations such as prefix cache hits.
func (a *planAccumulator) Finalize(modelIDs []string) error {
	if err := os.MkdirAll(a.plansDir(), 0o700); err != nil {
		return err
	}

	for _, safeModelID := range modelIDs {
		entries := a.entries[safeModelID]
		if len(entries) == 0 {
			continue
		}

		sort.Slice(entries, func(i, j int) bool {
			return entries[i].PrefixHash < entries[j].PrefixHash
		})

		tmpPath := filepath.Join(a.plansDir(), safeModelID+".plan.tmp")
		finalPath := filepath.Join(a.plansDir(), safeModelID+".plan")

		f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if err != nil {
			return fmt.Errorf("create plan file %s: %w", safeModelID, err)
		}

		for _, e := range entries {
			buf := e.marshalBinary()
			if _, err := f.Write(buf[:]); err != nil {
				f.Close()
				_ = os.Remove(tmpPath)
				return fmt.Errorf("write plan entry for %s: %w", safeModelID, err)
			}
		}

		if err := f.Close(); err != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("close plan file %s: %w", safeModelID, err)
		}

		if err := os.Rename(tmpPath, finalPath); err != nil {
			_ = os.Remove(tmpPath)
			return fmt.Errorf("rename plan file %s: %w", safeModelID, err)
		}
	}
	return nil
}

// accumulatePlanEntry accumulates a plan entry in memory.
// returns the next offset in the input file
func accumulatePlanEntry(
	acc *planAccumulator,
	modelID string,
	modelToSafe map[string]string,
	used map[string]int,
	offset int64,
	length uint32,
	prefixHash uint32,
) int64 {
	safeModelID, ok := modelToSafe[modelID]
	if !ok {
		safeModelID = internModelID(modelID, used)
		modelToSafe[modelID] = safeModelID
	}

	acc.Append(safeModelID, planEntry{Offset: offset, Length: length, PrefixHash: prefixHash})
	return offset + int64(length)
}

// finalizePlanFiles concludes ingestion by sorting and writing the plan entries to disk.
func finalizePlanFiles(acc *planAccumulator, modelToSafe map[string]string) error {
	modelIDs := make([]string, 0, len(modelToSafe))
	for _, safeID := range modelToSafe {
		modelIDs = append(modelIDs, safeID)
	}
	sort.Strings(modelIDs)
	return acc.Finalize(modelIDs)
}
