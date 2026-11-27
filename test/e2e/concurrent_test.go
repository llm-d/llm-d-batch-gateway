// Copyright 2026 The llm-d Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
)

func testConcurrent(t *testing.T) {
	t.Run("MultipleBatches", doTestConcurrentBatches)
}

// doTestConcurrentBatches submits 5 batches with different input files
// simultaneously and verifies they all complete independently.
func doTestConcurrentBatches(t *testing.T) {
	t.Helper()

	const batchCount = 5

	// Create distinct input files (each with a unique custom_id prefix).
	fileIDs := make([]string, batchCount)
	for i := range batchCount {
		jsonl := strings.Join([]string{
			fmt.Sprintf(`{"custom_id":"concurrent-%d-req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello %d"}]}}`, i, testModel, i),
			fmt.Sprintf(`{"custom_id":"concurrent-%d-req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"World %d"}]}}`, i, testModel, i),
		}, "\n")
		fileIDs[i] = mustCreateFile(t, fmt.Sprintf("test-concurrent-%d-%s.jsonl", i, testRunID), jsonl)
	}

	// Submit all batches.
	batchIDs := make([]string, batchCount)
	for i, fid := range fileIDs {
		batchIDs[i] = mustCreateBatch(t, fid)
		t.Logf("submitted batch %d: %s (file: %s)", i, batchIDs[i], fid)
	}

	// Wait for all batches concurrently.
	for i, bid := range batchIDs {
		i, bid := i, bid
		t.Run(fmt.Sprintf("Batch%d", i), func(t *testing.T) {
			t.Parallel()

			b, _ := waitForBatchStatus(t, bid, 5*time.Minute, openai.BatchStatusCompleted)
			if b.RequestCounts.Total != 2 {
				t.Errorf("batch %d (%s): total = %d, want 2", i, b.ID, b.RequestCounts.Total)
			}
			if b.RequestCounts.Completed != 2 {
				t.Errorf("batch %d (%s): completed = %d, want 2", i, b.ID, b.RequestCounts.Completed)
			}
			if b.RequestCounts.Failed != 0 {
				t.Errorf("batch %d (%s): failed = %d, want 0", i, b.ID, b.RequestCounts.Failed)
			}
			if b.OutputFileID == "" {
				t.Errorf("batch %d (%s): output_file_id is empty", i, b.ID)
			}
			t.Logf("batch %d (%s): completed (completed=%d, failed=%d)",
				i, b.ID, b.RequestCounts.Completed, b.RequestCounts.Failed)
		})
	}
}
