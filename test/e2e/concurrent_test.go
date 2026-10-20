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
	t.Run("WaitGoroutineSafety", doTestWaitForBatchStatusGoroutineSafety)
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

	// Wait for all batches concurrently. The goroutines must use
	// waitForBatchStatusErr rather than waitForBatchStatus: t.Fatalf is only
	// legal on the test goroutine, and called from a spawned goroutine it
	// would exit just that goroutine, leave the result slot nil, and crash
	// the verification loop below with a nil dereference (INFERENG-10907).
	type result struct {
		index int
		batch *openai.Batch
		err   error
	}
	results := make([]result, batchCount)
	var wg sync.WaitGroup
	wg.Add(batchCount)
	for i, bid := range batchIDs {
		go func(idx int, batchID string) {
			defer wg.Done()
			b, _, err := waitForBatchStatusErr(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)
			results[idx] = result{index: idx, batch: b, err: err}
		}(i, bid)
	}
	wg.Wait()

	// Report wait failures from the test goroutine. A failed wait leaves its
	// batch nil, so the verification loop below must skip it instead of
	// dereferencing it.
	for _, r := range results {
		if r.err != nil {
			t.Errorf("batch %d (%s): wait failed: %v", r.index, batchIDs[r.index], r.err)
		}
	}

	// Verify each batch completed independently.
	for _, r := range results {
		if r.err != nil {
			continue
		}
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

// doTestWaitForBatchStatusGoroutineSafety guards the INFERENG-10907 fix:
// waiting on batches from goroutines must go through waitForBatchStatusErr,
// never the t.Fatalf-based waitForBatchStatus wrapper. t.Fatalf only
// terminates the calling goroutine, so a single failed wait (a transient
// API error, a wrong terminal status, or a timeout) used to kill the
// goroutine before it wrote its result slot; the caller's unguarded
// verification loop then dereferenced the nil batch and crashed the whole
// test binary, taking down every subtest scheduled after it.
//
// It mirrors doTestConcurrentBatches' exact shape — per-goroutine result
// slots, errors collected and reported only after wg.Wait, and a
// verification loop that skips failed waits — against batch IDs that do
// not exist, the failure shape that used to crash the binary. If the
// fatal wrapper ever sneaks back into this pattern, the goroutine dies
// before writing its result slot, the zero-valued entry fails the
// "expected an error" assertion below, and the test still catches it —
// without crashing the binary. It runs under TestE2E (not as a standalone
// top-level test) so filtered CI runs that drive TestE2E subtests
// execute it too.
func doTestWaitForBatchStatusGoroutineSafety(t *testing.T) {
	const batchCount = 5
	type result struct {
		index int
		batch *openai.Batch
		err   error
	}
	results := make([]result, batchCount)
	var wg sync.WaitGroup
	wg.Add(batchCount)
	for i := range batchCount {
		go func(idx int) {
			defer wg.Done()
			// A batch ID that does not exist: the first poll fails, which is
			// exactly the failure shape that used to SIGSEGV the binary.
			b, _, err := waitForBatchStatusErr(t,
				fmt.Sprintf("batch-goroutine-safety-%s-%d", testRunID, idx),
				30*time.Second, openai.BatchStatusCompleted)
			results[idx] = result{index: idx, batch: b, err: err}
		}(i)
	}
	wg.Wait()

	// The doTestConcurrentBatches verification shape: failed waits are
	// reported from the test goroutine and their nil batches are skipped,
	// never dereferenced. Every wait must fail here (the batches do not
	// exist), and reaching the end of this test at all is the regression —
	// the fatal-wrapper version of this pattern never returned from wg.Wait
	// with a live process.
	for _, r := range results {
		if r.err != nil {
			t.Logf("batch %d: wait failed as expected: %v", r.index, r.err)
			continue
		}
		t.Errorf("batch %d: expected an error for a nonexistent batch", r.index)
		if r.batch != nil {
			t.Errorf("batch %d: unexpected batch %q for a nonexistent ID", r.index, r.batch.ID)
		}
	}
}
