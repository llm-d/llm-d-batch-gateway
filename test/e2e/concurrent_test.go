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
	"sync"
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
	type result struct {
		index int
		batch *openai.Batch
	}
	results := make([]result, batchCount)
	var wg sync.WaitGroup
	wg.Add(batchCount)
	for i, bid := range batchIDs {
		go func(idx int, batchID string) {
			defer wg.Done()
			b, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)
			results[idx] = result{index: idx, batch: b}
		}(i, bid)
	}
	wg.Wait()

	// Verify each batch completed independently.
	for _, r := range results {
		b := r.batch
		if b.RequestCounts.Total != 2 {
			t.Errorf("batch %d (%s): total = %d, want 2", r.index, b.ID, b.RequestCounts.Total)
		}
		if b.RequestCounts.Completed != 2 {
			t.Errorf("batch %d (%s): completed = %d, want 2", r.index, b.ID, b.RequestCounts.Completed)
		}
		if b.RequestCounts.Failed != 0 {
			t.Errorf("batch %d (%s): failed = %d, want 0", r.index, b.ID, b.RequestCounts.Failed)
		}
		if b.OutputFileID == "" {
			t.Errorf("batch %d (%s): output_file_id is empty", r.index, b.ID)
		}
		t.Logf("batch %d (%s): completed (completed=%d, failed=%d)",
			r.index, b.ID, b.RequestCounts.Completed, b.RequestCounts.Failed)
	}
}

// TestWaitForBatchStatusGoroutineSafety guards the INFERENG-10907 fix:
// waitForBatchStatusErr must be safe to call from non-test goroutines. With
// the previous t.Fatalf-based helper, this exact pattern crashed the whole
// test binary with a nil-pointer dereference on any wait failure (a
// transient API error, a wrong terminal status, or a timeout), taking down
// every subtest scheduled after it.
func TestWaitForBatchStatusGoroutineSafety(t *testing.T) {
	const goroutines = 5
	errs := make([]error, goroutines)
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			// A batch ID that does not exist: the first poll fails, which is
			// exactly the failure shape that used to SIGSEGV the binary.
			_, _, errs[idx] = waitForBatchStatusErr(t,
				fmt.Sprintf("batch-goroutine-safety-%s-%d", testRunID, idx),
				30*time.Second, openai.BatchStatusCompleted)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err == nil {
			t.Errorf("goroutine %d: expected an error for a nonexistent batch", i)
		}
	}
	// Reaching here at all is the regression: the fatal-helper version of
	// this pattern never returned from wg.Wait with a live process.
}
