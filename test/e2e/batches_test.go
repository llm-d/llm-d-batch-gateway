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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/packages/param"
)

func testBatches(t *testing.T) {
	t.Run("Lifecycle", doTestBatchLifecycle)
	t.Run("List", func(t *testing.T) {
		t.Run("Pagination", doTestBatchPagination)
	})
	t.Run("Cancel", func(t *testing.T) {
		t.Run("BeforeProcessing", doTestBatchCancelBeforeProcessing)
		t.Run("InProgress", doTestBatchCancel)
		t.Run("IdempotentRetry", doTestCancelIdempotentRetry)
		t.Run("TerminalBatchRejected", doTestCancelTerminalBatchRejected)
	})
	t.Run("TrailingNewline", doTestBatchTrailingNewline)
	t.Run("MixedSuccessFailure", doTestBatchMixedSuccessFailure)
	t.Run("SharedInputFile", doTestBatchSharedInputFile)
	t.Run("PassThroughHeaders", doTestPassThroughHeaders)
	skipIf(t, testDispatcherDeployed, "requires sync dispatch slot saturation", "Expiration", doTestBatchExpiration)
	skipIf(t, testDispatcherDeployed, "sim-model-b not available in async dispatch", "MultiModel", doTestMultiModelBatch)
	skipIf(t, testDispatcherDeployed, "progress tracking differs in async dispatch", "ProgressEpochFence", doTestProgressEpochFence)
	t.Run("ProgressPolling", doTestProgressPolling)
	t.Run("Ingestion", func(t *testing.T) {
		t.Run("DuplicateCustomID", doTestDuplicateCustomID)
		t.Run("StreamingRejected", doTestStreamingRejected)
		t.Run("AllModelsUnregistered", doTestAllModelsUnregistered)
	})
	t.Run("Validation", func(t *testing.T) {
		t.Run("MissingInputFileID", doTestCreateBatchMissingInputFileID)
		t.Run("InvalidEndpoint", doTestCreateBatchInvalidEndpoint)
		t.Run("NonexistentFile", doTestCreateBatchNonexistentFile)
	})
}

func doTestBatchCancel(t *testing.T) {
	t.Helper()

	// Mix fast and slow requests to guarantee both output and error files exist after cancel:
	//   - Fast requests (max_tokens=1): complete in ~150ms, ensuring output file has entries.
	//   - Slow requests (max_tokens=200): take ~20s each with dev-deploy sim-model
	//     defaults (~50ms TTFT + ~100ms inter-token), ensuring cancel arrives while
	//     they are still in-flight or undispatched.
	//   - 20 slow requests exceed PerModelMaxConcurrency (default 10), guaranteeing some
	//     remain undispatched and get drained to the error file as batch_cancelled.
	var lines []string
	for i := 1; i <= 5; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"fast-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":1,"messages":[{"role":"user","content":"Hi %d"}]}}`, i, testSimModel, i))
	}
	for i := 1; i <= 20; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"slow-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"Tell me a long story %d"}]}}`, i, testSimModel, i))
	}
	slowJSONL := strings.Join(lines, "\n")
	fileID := mustCreateFile(t, fmt.Sprintf("test-batch-cancel-%s.jsonl", testRunID), slowJSONL)
	batchID := mustCreateBatch(t, fileID)

	// Wait for the processor to pick up the batch and start inference.
	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)

	// Wait for at least one fast request to complete before cancelling.
	// This replaces a fixed sleep, making the test deterministic regardless
	// of request-path latency (e.g. with GIE: processor → Envoy → EPP → vllm-sim).
	waitForCompletedRequests(t, batchID, 1, 2*time.Minute)

	// Cancel the batch while slow requests are still in-flight.
	batch, err := newClient().Batches.Cancel(context.Background(), batchID)
	if err != nil {
		t.Fatalf("cancel batch failed: %v", err)
	}
	t.Logf("cancel response status: %s", batch.Status)

	// The cancel response should be cancelling (batch is in_progress, so
	// the apiserver sends a cancel event rather than directly cancelling).
	if batch.Status != openai.BatchStatusCancelling {
		t.Errorf("expected status %q immediately after cancel call, got %q",
			openai.BatchStatusCancelling, batch.Status)
	}

	// Wait for the batch to reach cancelled state.
	finalBatch, _ := waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusCancelled)

	t.Logf("batch %s cancelled (completed=%d, failed=%d, total=%d, output_file_id=%s, error_file_id=%s)",
		batchID,
		finalBatch.RequestCounts.Completed,
		finalBatch.RequestCounts.Failed,
		finalBatch.RequestCounts.Total,
		finalBatch.OutputFileID,
		finalBatch.ErrorFileID)

	// 25 requests total (5 fast + 20 slow). Fast requests should complete before
	// cancel; undispatched slow requests are drained as batch_cancelled, and
	// in-flight slow requests are aborted via abortCtx — both count as failed.
	if finalBatch.RequestCounts.Total != int64(len(lines)) {
		t.Errorf("total = %d, want %d", finalBatch.RequestCounts.Total, len(lines))
	}
	if finalBatch.RequestCounts.Completed == 0 {
		t.Error("expected at least one completed request (fast requests should finish before cancel)")
	}
	if finalBatch.RequestCounts.Failed == 0 {
		t.Error("expected at least one failed request (slow requests should be cancelled)")
	}
	if finalBatch.OutputFileID == "" {
		t.Error("expected output_file_id to be set (fast requests completed)")
	}
	if finalBatch.ErrorFileID == "" {
		t.Error("expected error_file_id to be set (slow requests cancelled)")
	}

	// Best-effort check: look for cancellation log in processor pods.
	// This is informational only — log tail depth and rotation make it unreliable.
	if testKubectlAvailable {
		out, err := exec.Command("kubectl", "logs",
			"-l", fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=processor", testHelmRelease),
			"-n", testNamespace,
			"--tail=500",
		).CombinedOutput()
		if err != nil {
			t.Logf("kubectl logs failed (non-fatal): %v\n%s", err, out)
		} else {
			logs := string(out)
			if strings.Contains(logs, "Request cancelled for request_id") {
				t.Logf("confirmed: processor logs contain in-flight request cancellation entries")
			} else {
				t.Logf("note: 'Request cancelled for request_id' not found in last 500 log lines (may have rotated)")
			}
		}
	}
}

// doTestBatchCancelBeforeProcessing creates a batch and cancels it immediately.
// If the cancel arrives before the processor dequeues the batch, the response
// is "cancelled" (PQDelete path). If the processor was faster, the response is
// "cancelling" (cancel event path). Both are valid due to the inherent race.
// Either way, the batch must eventually reach "cancelled".
func doTestBatchCancelBeforeProcessing(t *testing.T) {
	t.Helper()

	fileID := mustCreateFile(t, fmt.Sprintf("test-batch-cancel-before-%s.jsonl", testRunID), testJSONL)
	batchID := mustCreateBatch(t, fileID)

	// Cancel immediately — the batch is likely still in the queue.
	batch, err := newClient().Batches.Cancel(context.Background(), batchID)
	if err != nil {
		t.Fatalf("cancel batch failed: %v", err)
	}
	t.Logf("cancel response status: %s", batch.Status)

	switch batch.Status {
	case openai.BatchStatusCancelled:
		// PQDelete path: batch was still in queue, cancelled directly.
		t.Log("batch was cancelled directly from queue (PQDelete path)")
	case openai.BatchStatusCancelling:
		// Cancel event path: processor already dequeued the batch.
		t.Log("batch is cancelling via event (processor already dequeued)")
	default:
		t.Errorf("expected status %q or %q after immediate cancel, got %q",
			openai.BatchStatusCancelled, openai.BatchStatusCancelling, batch.Status)
	}

	// Either way, the batch must reach "cancelled" eventually.
	finalBatch, _ := waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusCancelled)

	// Cancelled before processing: no requests should have completed.
	if finalBatch.RequestCounts.Completed != 0 {
		t.Errorf("completed = %d, want 0 (cancelled before processing)", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.OutputFileID != "" {
		t.Errorf("expected empty output_file_id for batch cancelled before processing, got %q", finalBatch.OutputFileID)
	}
}

// psqlExec runs a SQL statement in the deployed PostgreSQL pod and returns the
// trimmed output (no headers).
func psqlExec(t *testing.T, sql string) string {
	t.Helper()

	podName := testDBPod
	if podName == "" {
		podName = fmt.Sprintf("%s-0", testPostgresqlRelease)
	}
	ns := testDBNamespace
	if ns == "" {
		ns = testNamespace
	}
	cmd := fmt.Sprintf(
		`PGPASSWORD="${POSTGRESQL_PASSWORD:-$(cat "$POSTGRES_PASSWORD_FILE" 2>/dev/null)}" psql -U '%s' -d '%s' -t -A -c %q`,
		testDBUser, testDBName, sql,
	)
	out, err := exec.Command("kubectl", "exec",
		podName,
		"-n", ns,
		"--", "bash", "-c", cmd,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl exec psql failed: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

// doTestProgressEpochFence verifies that a processor holding a stale epoch
// cannot write to its job's row. While a slow batch is in_progress, the row's
// epoch is bumped (simulating a new owner's PQClaimOwned claim). Every write
// the live processor issues afterwards — throttled progress flushes and the
// terminal status write — carries the pre-bump epoch, so all of them must be
// fenced out: the batch must stay in_progress with pre-bump counts even after
// every request has completed.
func doTestProgressEpochFence(t *testing.T) {
	t.Helper()

	if !testKubectlAvailable {
		t.Skip("kubectl not available, skipping epoch fence test")
	}
	if testDBClientType != "postgresql" {
		t.Skip("epoch fencing is a PostgreSQL queue feature")
	}

	// 10 slow requests (~60s each at dev-deploy sim defaults, 10 concurrent =>
	// a single ~60s wave) so the job is guaranteed to still be in-flight when
	// the epoch is bumped.
	var lines []string
	for i := 1; i <= 10; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"fence-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":600,"messages":[{"role":"user","content":"Fence story %d"}]}}`, i, testSimModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("test-epoch-fence-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)
	b, err := newClient().Batches.Get(context.Background(), batchID)
	if err != nil || b.Status != openai.BatchStatusInProgress {
		t.Fatalf("batch %s did not reach in_progress: status=%v err=%v", batchID, b, err)
	}
	t.Logf("batch %s in_progress; letting the job settle", batchID)
	time.Sleep(5 * time.Second)

	epoch := psqlExec(t, fmt.Sprintf("SELECT epoch FROM batch_items WHERE id = '%s'", batchID))
	psqlExec(t, fmt.Sprintf("UPDATE batch_items SET epoch = epoch + 1 WHERE id = '%s'", batchID))
	t.Logf("bumped batch %s epoch from %s to %s (simulating a new owner's claim)", batchID, epoch, "epoch+1")

	// The job must still be running for the fence to be observable.
	if b, err := newClient().Batches.Get(context.Background(), batchID); err == nil && terminalBatchStatuses[b.Status] {
		t.Skipf("batch %s reached %s before the epoch bump (sim too fast); fence not observable", batchID, b.Status)
	}

	// The slow wave completes ~60s after dispatch; poll well past that. If the
	// fence were not working, the progress flushes or the terminal write would
	// land and the batch would leave in_progress.
	deadline := time.Now().Add(150 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := newClient().Batches.Get(context.Background(), batchID); err == nil && terminalBatchStatuses[b.Status] {
			t.Fatalf("batch %s reached terminal state %s after the epoch bump — a stale-epoch write landed (fence broken)", batchID, b.Status)
		}
		time.Sleep(5 * time.Second)
	}

	b, err = newClient().Batches.Get(context.Background(), batchID)
	if err != nil {
		t.Fatalf("get batch: %v", err)
	}
	if b.Status != openai.BatchStatusInProgress {
		t.Fatalf("expected batch to remain in_progress while its owner holds a stale epoch, got %s", b.Status)
	}
	if b.RequestCounts.Completed != 0 {
		t.Errorf("expected completed=0 (progress writes fenced), got %d", b.RequestCounts.Completed)
	}
	t.Logf("epoch fence held: batch %s still %s (completed=%d) after all requests completed", batchID, b.Status, b.RequestCounts.Completed)

	// Cleanup: the processor can no longer write a terminal state for this job
	// (its epoch is stale), so mark it terminal directly and let GC collect it.
	now := time.Now().UTC().Unix()
	psqlExec(t, fmt.Sprintf(
		"UPDATE batch_items SET status = '{\"status\":\"failed\",\"failed_at\":%d,\"request_counts\":{\"total\":10,\"completed\":0,\"failed\":10}}', expiry = %d WHERE id = '%s'",
		now, now, batchID))
	t.Log("marked batch terminal in DB for GC collection")
}

// doTestBatchLifecycle creates a fresh batch, verifies list and retrieve operations,
// polls until it reaches a terminal state, then asserts it completed successfully
// and prints the output/error file contents.
func doTestBatchLifecycle(t *testing.T) {
	t.Helper()

	client := newClient()

	// Create
	fileID := mustCreateFile(t, fmt.Sprintf("test-batch-lifecycle-%s.jsonl", testRunID), testJSONL)
	batchID := mustCreateBatch(t, fileID)

	// List
	page, err := client.Batches.List(context.Background(), openai.BatchListParams{})
	if err != nil {
		t.Fatalf("list batches failed: %v", err)
	}
	t.Logf("list batches: got %d items", len(page.Data))

	// Retrieve
	batch, err := client.Batches.Get(context.Background(), batchID)
	if err != nil {
		t.Fatalf("retrieve batch failed: %v", err)
	}
	if batch.ID != batchID {
		t.Errorf("expected ID %q, got %q", batchID, batch.ID)
	}
	if batch.InputFileID != fileID {
		t.Errorf("expected input_file_id %q, got %q", fileID, batch.InputFileID)
	}
	if batch.Endpoint != "/v1/chat/completions" {
		t.Errorf("expected endpoint %q, got %q", "/v1/chat/completions", batch.Endpoint)
	}
	if batch.CompletionWindow != "24h" {
		t.Errorf("expected completion_window %q, got %q", "24h", batch.CompletionWindow)
	}
	for k, wantV := range testBatchMetadata {
		if gotV, ok := batch.Metadata[k]; !ok {
			t.Errorf("metadata key %q missing from retrieve response", k)
		} else if gotV != wantV {
			t.Errorf("metadata[%q] = %q, want %q", k, gotV, wantV)
		}
	}

	// Poll until completion
	finalBatch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)

	// All 2 requests in testJSONL should succeed.
	if finalBatch.RequestCounts.Total != 2 {
		t.Errorf("total = %d, want 2", finalBatch.RequestCounts.Total)
	}
	if finalBatch.RequestCounts.Completed != 2 {
		t.Errorf("completed = %d, want 2", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.RequestCounts.Failed != 0 {
		t.Errorf("failed = %d, want 0", finalBatch.RequestCounts.Failed)
	}
	if finalBatch.OutputFileID == "" {
		t.Error("expected output_file_id to be set for completed batch")
	}
	if finalBatch.ErrorFileID != "" {
		t.Errorf("expected empty error_file_id for fully-successful batch, got %q", finalBatch.ErrorFileID)
	}
}

// doTestBatchSharedInputFile creates two batches from the same input file and
// verifies both complete independently with correct output.
func doTestBatchSharedInputFile(t *testing.T) {
	t.Helper()

	fileID := mustCreateFile(t, fmt.Sprintf("test-shared-input-%s.jsonl", testRunID), testJSONL)

	batchID1 := mustCreateBatch(t, fileID)
	batchID2 := mustCreateBatch(t, fileID)
	t.Logf("created batch1=%s batch2=%s from file=%s", batchID1, batchID2, fileID)

	batch1, _ := waitForBatchStatus(t, batchID1, 5*time.Minute, openai.BatchStatusCompleted)
	batch2, _ := waitForBatchStatus(t, batchID2, 5*time.Minute, openai.BatchStatusCompleted)

	// Both batches use the same 2-request input file and should fully succeed.
	for i, b := range []*openai.Batch{batch1, batch2} {
		label := fmt.Sprintf("batch%d", i+1)
		if b.RequestCounts.Total != 2 {
			t.Errorf("%s: total = %d, want 2", label, b.RequestCounts.Total)
		}
		if b.RequestCounts.Completed != 2 {
			t.Errorf("%s: completed = %d, want 2", label, b.RequestCounts.Completed)
		}
		if b.RequestCounts.Failed != 0 {
			t.Errorf("%s: failed = %d, want 0", label, b.RequestCounts.Failed)
		}
		if b.OutputFileID == "" {
			t.Errorf("%s: expected output_file_id to be set", label)
		}
		if b.ErrorFileID != "" {
			t.Errorf("%s: expected empty error_file_id, got %q", label, b.ErrorFileID)
		}
	}

	// Verify output files are distinct.
	if batch1.OutputFileID == batch2.OutputFileID {
		t.Errorf("both batches produced the same output_file_id %q, expected distinct files", batch1.OutputFileID)
	}
}

// doTestBatchTrailingNewline verifies that a single trailing newline in an
// input file does not inflate the request count or cause parse errors.
func doTestBatchTrailingNewline(t *testing.T) {
	t.Helper()

	fileID := mustCreateFile(t, fmt.Sprintf("test-trailing-newline-%s.jsonl", testRunID), testJSONL+"\n")
	batchID := mustCreateBatch(t, fileID)

	finalBatch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)

	if finalBatch.RequestCounts.Total != 2 {
		t.Errorf("total = %d, want 2", finalBatch.RequestCounts.Total)
	}
	if finalBatch.RequestCounts.Completed != 2 {
		t.Errorf("completed = %d, want 2", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.RequestCounts.Failed != 0 {
		t.Errorf("failed = %d, want 0", finalBatch.RequestCounts.Failed)
	}
	if finalBatch.OutputFileID == "" {
		t.Error("expected output_file_id to be set")
	}
	if finalBatch.ErrorFileID != "" {
		t.Errorf("expected empty error_file_id, got %q", finalBatch.ErrorFileID)
	}
}

// doTestBatchMixedSuccessFailure creates a batch with a mix of valid and invalid
// requests (invalid model), verifies the batch completes with correct
// completed/failed counts, and that output and error files contain the right entries.
func doTestBatchMixedSuccessFailure(t *testing.T) {
	t.Helper()

	mixedJSONL := strings.Join([]string{
		fmt.Sprintf(`{"custom_id":"good-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello"}]}}`, testModel),
		`{"custom_id":"bad-1","method":"POST","url":"/v1/chat/completions","body":{"model":"nonexistent-model","max_tokens":5,"messages":[{"role":"user","content":"Hello"}]}}`,
		fmt.Sprintf(`{"custom_id":"good-2","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"World"}]}}`, testModel),
	}, "\n")

	fileID := mustCreateFile(t, fmt.Sprintf("test-mixed-%s.jsonl", testRunID), mixedJSONL)
	batchID := mustCreateBatch(t, fileID)

	finalBatch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)

	// 3 requests: 2 valid (good-1, good-2) + 1 invalid model (bad-1).
	if finalBatch.RequestCounts.Total != 3 {
		t.Errorf("total = %d, want 3", finalBatch.RequestCounts.Total)
	}
	if finalBatch.RequestCounts.Completed != 2 {
		t.Errorf("completed = %d, want 2", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.RequestCounts.Failed != 1 {
		t.Errorf("failed = %d, want 1", finalBatch.RequestCounts.Failed)
	}
	if finalBatch.OutputFileID == "" {
		t.Error("expected output_file_id to be set (2 requests succeeded)")
	}
	if finalBatch.ErrorFileID == "" {
		t.Error("expected error_file_id to be set (1 request failed)")
	}
}

// doTestPassThroughHeaders creates a batch with pass-through headers, waits for
// completion, then verifies the processor logged the expected header names.
func doTestPassThroughHeaders(t *testing.T) {
	t.Helper()

	// Create batch with pass-through headers
	fileID := mustCreateFile(t, fmt.Sprintf("test-pass-through-headers-%s.jsonl", testRunID), testJSONL)

	var headerOpts []option.RequestOption
	for k, v := range testPassThroughHeaders {
		headerOpts = append(headerOpts, option.WithHeader(k, v))
	}

	batchID := mustCreateBatch(t, fileID, headerOpts...)

	finalBatch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)

	// All 2 requests in testJSONL should succeed.
	if finalBatch.RequestCounts.Total != 2 {
		t.Errorf("total = %d, want 2", finalBatch.RequestCounts.Total)
	}
	if finalBatch.RequestCounts.Completed != 2 {
		t.Errorf("completed = %d, want 2", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.RequestCounts.Failed != 0 {
		t.Errorf("failed = %d, want 0", finalBatch.RequestCounts.Failed)
	}
	if finalBatch.OutputFileID == "" {
		t.Error("expected output_file_id to be set")
	}
	if finalBatch.ErrorFileID != "" {
		t.Errorf("expected empty error_file_id, got %q", finalBatch.ErrorFileID)
	}

	// Verify the processor attached the pass-through header names to the job:
	// the process-batch span for this batch carries them as a tag.
	span := waitForBatchSpan(t, "process-batch", batchID, 60*time.Second)
	got, ok := span.tag(attrPassThroughHeaders)
	if !ok {
		t.Fatalf("process-batch span for %s has no %q tag; tags: %v", batchID, attrPassThroughHeaders, span.Tags)
	}
	for headerName := range testPassThroughHeaders {
		if !strings.Contains(got, headerName) {
			t.Errorf("expected %q in %s=%q on the process-batch span for %s", headerName, attrPassThroughHeaders, got, batchID)
		}
	}
}

// doTestBatchPagination creates 3 batches under an isolated tenant and verifies
// that limit/after pagination returns correct pages with no duplicates.
func doTestBatchPagination(t *testing.T) {
	t.Helper()

	tenant := fmt.Sprintf("pagination-batches-%s", testRunID)
	client := newClientForTenant(tenant)
	ctx := context.Background()

	// Create a shared input file under this tenant.
	filename := fmt.Sprintf("pagination-batch-input-%s.jsonl", testRunID)
	fileID := mustCreateUniqueFileWithClient(t, client, filename, testJSONL)

	// Create 3 batches.
	const count = 3
	createdIDs := make([]string, count)
	for i := range count {
		batch, err := client.Batches.New(ctx, openai.BatchNewParams{
			InputFileID:      fileID,
			Endpoint:         openai.BatchNewParamsEndpointV1ChatCompletions,
			CompletionWindow: openai.BatchNewParamsCompletionWindow24h,
		})
		if err != nil {
			t.Fatalf("create batch %d failed: %v", i, err)
		}
		createdIDs[i] = batch.ID
		t.Logf("created batch %d: %s", i, batch.ID)
	}

	// Page 1: limit=2, no after → expect 2 items, has_more=true
	page1, err := client.Batches.List(ctx, openai.BatchListParams{
		Limit: param.NewOpt(int64(2)),
	})
	if err != nil {
		t.Fatalf("list batches page 1 failed: %v", err)
	}
	if len(page1.Data) != 2 {
		t.Fatalf("page 1: expected 2 items, got %d", len(page1.Data))
	}
	if !page1.HasMore {
		t.Error("page 1: expected has_more=true")
	}

	page1IDs := make([]string, len(page1.Data))
	for i, b := range page1.Data {
		page1IDs[i] = b.ID
	}
	t.Logf("page 1 IDs: %v (has_more=%v)", page1IDs, page1.HasMore)

	// Page 2: limit=2, after="2" (offset) → expect 1 item, has_more=false
	page2, err := client.Batches.List(ctx, openai.BatchListParams{
		Limit: param.NewOpt(int64(2)),
		After: param.NewOpt("2"),
	})
	if err != nil {
		t.Fatalf("list batches page 2 failed: %v", err)
	}
	if len(page2.Data) != 1 {
		t.Fatalf("page 2: expected 1 item, got %d", len(page2.Data))
	}
	if page2.HasMore {
		t.Error("page 2: expected has_more=false")
	}

	page2IDs := make([]string, len(page2.Data))
	for i, b := range page2.Data {
		page2IDs[i] = b.ID
	}
	t.Logf("page 2 IDs: %v (has_more=%v)", page2IDs, page2.HasMore)

	// Verify no overlap and full coverage.
	allIDs := append(page1IDs, page2IDs...)
	assertSliceEqual(t, createdIDs, allIDs)
}

// doTestBatchExpiration creates a batch with slow requests and a short
// completion_window so the SLO fires before any requests are dispatched. A
// blocker batch saturates the processor first, so the expiration batch's
// requests all remain undispatched and are drained as batch_expired.
// Verifies: expired status, correct timestamps, completed==0, no output file,
// and an error file with the expired entries.
//
// With dev-deploy sim-model defaults (~50ms TTFT + ~100ms inter-token), each
// slow request (max_tokens=200) takes ~20s. The 10s completion_window is
// longer than the processor poll interval (5s) so the batch is always
// dequeued before its SLO fires; the SLO then fires in-flight while the
// blocker still holds all dispatch slots, and the undelivered requests are
// drained as batch_expired.
func doTestBatchExpiration(t *testing.T) {
	t.Helper()

	client := newClient()
	ctx := context.Background()

	// Step 1: Create a "blocker" batch with many slow requests to saturate the
	// processor's PerModelMaxConcurrency (default 10). This ensures the
	// expiration batch cannot dispatch any requests before its SLO fires.
	var blockerLines []string
	for i := 1; i <= 50; i++ {
		blockerLines = append(blockerLines, fmt.Sprintf(
			`{"custom_id":"blocker-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"Block %d"}]}}`, i, testSimModel, i))
	}
	blockerFileID := mustCreateFile(t, fmt.Sprintf("test-expiration-blocker-%s.jsonl", testRunID), strings.Join(blockerLines, "\n"))
	blockerBatchID := mustCreateBatch(t, blockerFileID)

	// Ensure the blocker batch is cancelled when the test ends (even on failure),
	// so the processor is freed for subsequent tests.
	t.Cleanup(func() {
		_, err := client.Batches.Cancel(ctx, blockerBatchID)
		if err != nil {
			t.Logf("cleanup: cancel blocker batch %s failed (may already be done): %v", blockerBatchID, err)
			return
		}
		waitForBatchStatus(t, blockerBatchID, 2*time.Minute, openai.BatchStatusCancelled)
	})

	// Wait for the blocker to reach in_progress so it holds all worker slots.
	_, _ = waitForBatchStatus(t, blockerBatchID, 2*time.Minute, openai.BatchStatusInProgress)

	// Step 2: Create the expiration batch with a short completion_window.
	// Since the processor is saturated by the blocker, none of these requests
	// can be dispatched before the 10s SLO fires.
	const numRequests = 15
	var lines []string
	for i := 1; i <= numRequests; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"expire-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"Expire %d"}]}}`, i, testSimModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("test-batch-expiration-%s.jsonl", testRunID), strings.Join(lines, "\n"))

	// BatchNewParamsCompletionWindow is a string type; the batch-gateway API
	// accepts Go duration strings like "10s" in addition to the standard "24h".
	batch, err := client.Batches.New(ctx, openai.BatchNewParams{
		InputFileID:      fileID,
		Endpoint:         openai.BatchNewParamsEndpointV1ChatCompletions,
		CompletionWindow: openai.BatchNewParamsCompletionWindow("10s"),
		Metadata:         testBatchMetadata,
	})
	if err != nil {
		t.Fatalf("create batch with short completion_window failed: %v", err)
	}
	batchID := batch.ID
	t.Logf("created expiration batch %s with completion_window=10s (blocker=%s)", batchID, blockerBatchID)

	// Wait for the batch to reach expired status.
	finalBatch, _ := waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusExpired)

	t.Logf("batch %s expired (completed=%d, failed=%d, total=%d, output_file_id=%s, error_file_id=%s)",
		batchID,
		finalBatch.RequestCounts.Completed,
		finalBatch.RequestCounts.Failed,
		finalBatch.RequestCounts.Total,
		finalBatch.OutputFileID,
		finalBatch.ErrorFileID)

	// The processor was saturated by the blocker batch, so none of the
	// expiration batch's requests could be dispatched before the SLO fired.
	if finalBatch.RequestCounts.Total != numRequests {
		t.Errorf("total = %d, want %d", finalBatch.RequestCounts.Total, numRequests)
	}
	if finalBatch.RequestCounts.Completed != 0 {
		t.Errorf("completed = %d, want 0 (processor was saturated)", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.RequestCounts.Failed != finalBatch.RequestCounts.Total {
		t.Errorf("failed = %d, want %d (all requests should expire)", finalBatch.RequestCounts.Failed, finalBatch.RequestCounts.Total)
	}
	if finalBatch.OutputFileID != "" {
		t.Errorf("expected empty output_file_id for fully-expired batch, got %q", finalBatch.OutputFileID)
	}
	if finalBatch.ErrorFileID == "" {
		t.Error("expected error_file_id to be set for expired batch")
	}

	// Blocker batch cleanup is handled by t.Cleanup() registered above.
}

// doTestCancelIdempotentRetry cancels an in-progress batch twice in a row.
// Both calls should succeed (200) and the batch should reach cancelled.
// Guards the idempotent cancel-retry path added.
func doTestCancelIdempotentRetry(t *testing.T) {
	t.Helper()

	var lines []string
	for i := 1; i <= 20; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"retry-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"slow %d"}]}}`, i, testSimModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("test-cancel-retry-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)
	time.Sleep(1 * time.Second)

	client := newClient()
	ctx := context.Background()

	batch1, err := client.Batches.Cancel(ctx, batchID)
	if err != nil {
		t.Fatalf("first cancel failed: %v", err)
	}
	t.Logf("first cancel: status=%s", batch1.Status)

	batch2, err := client.Batches.Cancel(ctx, batchID)
	if err != nil {
		t.Fatalf("second cancel (idempotent retry) failed: %v", err)
	}
	t.Logf("second cancel: status=%s", batch2.Status)

	if batch2.Status != openai.BatchStatusCancelling && batch2.Status != openai.BatchStatusCancelled {
		t.Errorf("expected cancelling or cancelled after retry, got %s", batch2.Status)
	}

	finalBatch, _ := waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusCancelled)
	if finalBatch.Status != openai.BatchStatusCancelled {
		t.Errorf("expected final status cancelled, got %s", finalBatch.Status)
	}
}

// doTestCancelTerminalBatchRejected completes a batch, then attempts to cancel it.
// The API should return 400.
func doTestCancelTerminalBatchRejected(t *testing.T) {
	t.Helper()

	fileID := mustCreateFile(t, fmt.Sprintf("test-cancel-terminal-%s.jsonl", testRunID), testJSONL)
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)

	_, err := newClient().Batches.Cancel(context.Background(), batchID)
	if err == nil {
		t.Fatal("expected error when cancelling a completed batch, got nil")
	}

	var apiErr *openai.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected openai.Error, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", apiErr.StatusCode)
	}
	t.Logf("cancel of completed batch correctly rejected: %d", apiErr.StatusCode)
}

// doTestMultiModelBatch creates a batch with requests targeting two different
// models (testModel and testModelB) and verifies both models appear in the output.
func doTestMultiModelBatch(t *testing.T) {
	t.Helper()

	jsonl := strings.Join([]string{
		fmt.Sprintf(`{"custom_id":"model-a-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello A"}]}}`, testModel),
		fmt.Sprintf(`{"custom_id":"model-a-2","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"World A"}]}}`, testModel),
		fmt.Sprintf(`{"custom_id":"model-b-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello B"}]}}`, testModelB),
		fmt.Sprintf(`{"custom_id":"model-b-2","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"World B"}]}}`, testModelB),
	}, "\n")

	fileID := mustCreateFile(t, fmt.Sprintf("test-multi-model-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)

	finalBatch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)

	if finalBatch.RequestCounts.Total != 4 {
		t.Errorf("total = %d, want 4", finalBatch.RequestCounts.Total)
	}
	if finalBatch.RequestCounts.Completed != 4 {
		t.Errorf("completed = %d, want 4", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.RequestCounts.Failed != 0 {
		t.Errorf("failed = %d, want 0", finalBatch.RequestCounts.Failed)
	}
	if finalBatch.OutputFileID == "" {
		t.Fatal("expected output_file_id to be set")
	}

	resp, err := newClient().Files.Content(context.Background(), finalBatch.OutputFileID)
	if err != nil {
		t.Fatalf("download output file failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	modelsFound := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
		var result struct {
			Response *struct {
				Body struct {
					Model string `json:"model"`
				} `json:"body"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(line), &result); err != nil {
			t.Errorf("invalid output line: %v", err)
			continue
		}
		if result.Response != nil {
			modelsFound[result.Response.Body.Model]++
		}
	}

	if modelsFound[testModel] != 2 {
		t.Errorf("expected 2 responses from %s, got %d", testModel, modelsFound[testModel])
	}
	if modelsFound[testModelB] != 2 {
		t.Errorf("expected 2 responses from %s, got %d", testModelB, modelsFound[testModelB])
	}
	t.Logf("multi-model output: %v", modelsFound)
}

// doTestProgressPolling submits a batch with a mix of fast and slow requests
// and verifies that request_counts.completed is non-zero while the batch is
// still in_progress. The fast requests finish quickly, guaranteeing a non-zero
// completed count well before the slow requests finish — this avoids flakiness
// from tight timing windows.
func doTestProgressPolling(t *testing.T) {
	t.Helper()

	// 5 fast requests (max_tokens=1, ~150ms each) complete almost immediately.
	// 15 slow requests (max_tokens=200, ~20s each at the simulator's 100ms
	// inter-token latency) keep the batch in_progress long enough for the
	// 15-second progress ticker to fire and persist intermediate counts to the DB.
	var lines []string
	for i := 1; i <= 5; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"fast-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":1,"messages":[{"role":"user","content":"fast %d"}]}}`, i, testSimModel, i))
	}
	for i := 1; i <= 15; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"slow-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"slow %d"}]}}`, i, testSimModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("test-progress-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)

	client := newClient()
	ctx := context.Background()
	var sawNonZeroCompleted bool
	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		b, err := client.Batches.Get(ctx, batchID)
		if err != nil {
			t.Fatalf("get batch failed: %v", err)
		}
		t.Logf("progress: status=%s completed=%d failed=%d total=%d",
			b.Status, b.RequestCounts.Completed, b.RequestCounts.Failed, b.RequestCounts.Total)

		if b.RequestCounts.Completed > 0 && b.Status == openai.BatchStatusInProgress {
			sawNonZeroCompleted = true
			break
		}
		if terminalBatchStatuses[b.Status] {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	if !sawNonZeroCompleted {
		t.Error("never saw non-zero completed count while batch was in_progress")
	}

	finalBatch, _ := waitForBatchStatus(t, batchID, 3*time.Minute, openai.BatchStatusCompleted)
	if finalBatch.RequestCounts.Completed != 20 {
		t.Errorf("completed = %d, want 20", finalBatch.RequestCounts.Completed)
	}
}

// doTestDuplicateCustomID submits a JSONL with duplicate custom_ids.
// The batch should fail during ingestion.
func doTestDuplicateCustomID(t *testing.T) {
	t.Helper()

	jsonl := strings.Join([]string{
		fmt.Sprintf(`{"custom_id":"dup-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello"}]}}`, testModel),
		fmt.Sprintf(`{"custom_id":"dup-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"World"}]}}`, testModel),
	}, "\n")

	fileID := mustCreateFile(t, fmt.Sprintf("test-dup-customid-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)

	finalBatch := waitForIngestionFailure(t, batchID, 2*time.Minute)
	t.Logf("duplicate custom_id batch reached %s", finalBatch.Status)
}

// doTestStreamingRejected submits a JSONL with stream:true in the body.
// The batch should fail during ingestion.
func doTestStreamingRejected(t *testing.T) {
	t.Helper()

	jsonl := fmt.Sprintf(`{"custom_id":"stream-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"stream":true,"messages":[{"role":"user","content":"Hello"}]}}`, testModel)

	fileID := mustCreateFile(t, fmt.Sprintf("test-streaming-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)

	finalBatch := waitForIngestionFailure(t, batchID, 2*time.Minute)
	t.Logf("streaming batch reached %s", finalBatch.Status)
}

// doTestAllModelsUnregistered submits a JSONL where every request targets a
// nonexistent model. In per-model mode the preprocessor rejects each line with
// model_not_found (written to error.jsonl) during ingestion — the same rejection
// mechanism as MixedSuccessFailure, but here every line is invalid so
// completed stays 0. The batch still reaches "completed" (not "failed") because
// the job finishes normally; only individual requests fail.
// The batch should complete with failed == total.
func doTestAllModelsUnregistered(t *testing.T) {
	t.Helper()

	jsonl := strings.Join([]string{
		`{"custom_id":"bad-1","method":"POST","url":"/v1/chat/completions","body":{"model":"nonexistent-model-xyz","max_tokens":5,"messages":[{"role":"user","content":"Hello"}]}}`,
		`{"custom_id":"bad-2","method":"POST","url":"/v1/chat/completions","body":{"model":"nonexistent-model-xyz","max_tokens":5,"messages":[{"role":"user","content":"World"}]}}`,
	}, "\n")

	fileID := mustCreateFile(t, fmt.Sprintf("test-all-unregistered-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)

	finalBatch, _ := waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusCompleted)

	if finalBatch.RequestCounts.Total != 2 {
		t.Errorf("total = %d, want 2", finalBatch.RequestCounts.Total)
	}
	if finalBatch.RequestCounts.Failed != 2 {
		t.Errorf("failed = %d, want 2", finalBatch.RequestCounts.Failed)
	}
	if finalBatch.RequestCounts.Completed != 0 {
		t.Errorf("completed = %d, want 0", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.ErrorFileID == "" {
		t.Fatal("expected error_file_id to be set")
	}

	resp, err := newClient().Files.Content(context.Background(), finalBatch.ErrorFileID)
	if err != nil {
		t.Fatalf("download error file failed: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if !strings.Contains(string(body), "model_not_found") {
		t.Error("expected error file to contain 'model_not_found'")
	}
	t.Logf("all-unregistered error file contains model_not_found")
}

// doTestCreateBatchMissingInputFileID attempts to create a batch without
// input_file_id. The API should return 400.
func doTestCreateBatchMissingInputFileID(t *testing.T) {
	t.Helper()

	_, err := createBatchRaw(newClient(), openai.BatchNewParams{
		InputFileID:      "",
		Endpoint:         openai.BatchNewParamsEndpointV1ChatCompletions,
		CompletionWindow: openai.BatchNewParamsCompletionWindow24h,
	})
	if err == nil {
		t.Fatal("expected error for missing input_file_id, got nil")
	}

	var apiErr *openai.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected openai.Error, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", apiErr.StatusCode)
	}
	t.Logf("missing input_file_id correctly rejected: %d", apiErr.StatusCode)
}

// doTestCreateBatchInvalidEndpoint attempts to create a batch with an
// unsupported endpoint. The API should return 400.
func doTestCreateBatchInvalidEndpoint(t *testing.T) {
	t.Helper()

	fileID := mustCreateFile(t, fmt.Sprintf("test-invalid-endpoint-%s.jsonl", testRunID), testJSONL)

	_, err := createBatchRaw(newClient(), openai.BatchNewParams{
		InputFileID:      fileID,
		Endpoint:         openai.BatchNewParamsEndpoint("/v1/invalid"),
		CompletionWindow: openai.BatchNewParamsCompletionWindow24h,
	})
	if err == nil {
		t.Fatal("expected error for invalid endpoint, got nil")
	}

	var apiErr *openai.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected openai.Error, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", apiErr.StatusCode)
	}
	t.Logf("invalid endpoint correctly rejected: %d", apiErr.StatusCode)
}

// doTestCreateBatchNonexistentFile attempts to create a batch with a
// file_id that does not exist. The API should return 400.
func doTestCreateBatchNonexistentFile(t *testing.T) {
	t.Helper()

	_, err := createBatchRaw(newClient(), openai.BatchNewParams{
		InputFileID:      "file-does-not-exist-12345",
		Endpoint:         openai.BatchNewParamsEndpointV1ChatCompletions,
		CompletionWindow: openai.BatchNewParamsCompletionWindow24h,
	})
	if err == nil {
		t.Fatal("expected error for nonexistent file, got nil")
	}

	var apiErr *openai.Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected openai.Error, got %T: %v", err, err)
	}
	if apiErr.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", apiErr.StatusCode)
	}
	t.Logf("nonexistent file correctly rejected: %d", apiErr.StatusCode)
}
