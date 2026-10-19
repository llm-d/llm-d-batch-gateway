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
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/packages/param"
)

// testResumable covers the resumable pipeline every batch runs on,
// regardless of dispatch transport: result checkpointing, cancel semantics,
// multi-worker throughput, and recovery from Processor pod loss on single-
// and multi-replica fleets — mid-execution and mid-finalization.
func testResumable(t *testing.T) {
	t.Run("HappyPath", doTestResumableHappyPath)
	t.Run("CancelMidFlight", doTestResumableCancelMidFlight)
	t.Run("MultiWorker", doTestResumableMultiWorker)
	skipIf(t, !testKubectlAvailable, "skipping: kubectl not available", "PodLoss", doTestResumablePodLoss)
	skipIf(t, !testKubectlAvailable, "skipping: kubectl not available", "MultiReplica", doTestResumableMultiReplica)
	skipIf(t, !testKubectlAvailable, "skipping: kubectl not available", "FinalizationInterrupt", doTestResumableFinalizationInterrupt)
}

// doTestResumableHappyPath runs a small batch through the resumable pipeline
// and asserts the checkpointed results are finalized into a complete output
// file with no error entries.
func doTestResumableHappyPath(t *testing.T) {
	const requests = 8
	lines := make([]string, 0, requests)
	for i := 0; i < requests; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"resumable-happy-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello %d"}]}}`,
			i, testModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("resumable-happy-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	batch, results := waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusCompleted)

	if batch.RequestCounts.Total != requests {
		t.Errorf("total = %d, want %d", batch.RequestCounts.Total, requests)
	}
	if batch.RequestCounts.Completed != requests {
		t.Errorf("completed = %d, want %d", batch.RequestCounts.Completed, requests)
	}
	if batch.RequestCounts.Failed != 0 {
		t.Errorf("failed = %d, want 0", batch.RequestCounts.Failed)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
	if results.OutputLines != requests {
		t.Errorf("output lines = %d, want %d", results.OutputLines, requests)
	}
	if results.ErrorLines != 0 {
		t.Errorf("error lines = %d, want 0", results.ErrorLines)
	}
}

// doTestResumableCancelMidFlight cancels a resumable batch while slow
// requests are in flight and asserts completed requests survive in the
// output file while the rest are drained as batch_cancelled errors.
func doTestResumableCancelMidFlight(t *testing.T) {
	// Fast requests (max_tokens=1) complete before the cancel; slow requests
	// (max_tokens=200, ~20s each on dev-deploy sim defaults) are still in
	// flight or undispatched when it lands.
	var lines []string
	for i := 0; i < 4; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"resumable-cancel-fast-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":1,"messages":[{"role":"user","content":"Hi %d"}]}}`,
			i, testSimModel, i))
	}
	for i := 0; i < 16; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"resumable-cancel-slow-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"Tell me a long story %d"}]}}`,
			i, testSimModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("resumable-cancel-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)
	waitForCompletedRequests(t, batchID, 1, 2*time.Minute)

	batch, err := newClient().Batches.Cancel(context.Background(), batchID)
	if err != nil {
		t.Fatalf("cancel batch failed: %v", err)
	}
	if batch.Status != openai.BatchStatusCancelling {
		t.Errorf("expected status %q immediately after cancel call, got %q",
			openai.BatchStatusCancelling, batch.Status)
	}

	finalBatch, results := waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusCancelled)

	if finalBatch.RequestCounts.Total != int64(len(lines)) {
		t.Errorf("total = %d, want %d", finalBatch.RequestCounts.Total, len(lines))
	}
	if finalBatch.RequestCounts.Completed == 0 {
		t.Error("expected at least one completed request (fast requests should finish before cancel)")
	}
	if finalBatch.RequestCounts.Failed == 0 {
		t.Error("expected at least one failed request (slow requests should be cancelled)")
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}

	cancelled := 0
	for _, line := range strings.Split(results.ErrorBody, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rl batchResultLine
		if err := json.Unmarshal([]byte(line), &rl); err != nil {
			t.Fatalf("invalid error line: %v\n%s", err, line)
		}
		if rl.Error != nil && rl.Error.Code == "batch_cancelled" {
			cancelled++
		}
	}
	if cancelled == 0 {
		t.Errorf("expected at least one batch_cancelled error entry, got error file:\n%s", results.ErrorBody)
	}
}

// waitForDispatchQueueDrain waits until the async dispatch queue holds no
// pending or claimed requests. At-least-once dispatch leaves duplicate or
// cancelled submissions in the shared queue after a disruption test's batch
// reaches a terminal status; the dev-deploy dispatcher forwards them serially
// and claims them in batches of ten, so residue already claimed is invisible
// to the queue zset yet still delays the next test's first result.
// Timing-sensitive subtests quiesce the queue before submitting their own
// work.
func waitForDispatchQueueDrain(t *testing.T, queue string, timeout time.Duration) {
	t.Helper()

	// Sync dispatch has no queue; there is nothing to drain.
	if !testDispatcherDeployed {
		return
	}

	rdb := newDispatcherRedisClient(t)
	defer rdb.Close()

	deadline := time.Now().Add(timeout)
	if d, ok := t.Deadline(); ok && d.Before(deadline) {
		deadline = d.Add(-5 * time.Second)
	}
	for time.Now().Before(deadline) {
		pending, err := rdb.ZCard(context.Background(), queue).Result()
		if err != nil {
			t.Fatalf("ZCard %s: %v", queue, err)
		}
		claimed, err := rdb.HLen(context.Background(), queue+":claimed").Result()
		if err != nil {
			t.Fatalf("HLen %s: %v", queue+":claimed", err)
		}
		if pending == 0 && claimed == 0 {
			return
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("dispatch queue %s did not drain within %v", queue, timeout)
}

// doTestResumableMultiWorker creates several batches up front so the
// Processor's worker pool (num_workers default 5) runs them concurrently,
// and asserts every batch finalizes a complete output file. Requests are
// small so the shared deadline is not spent on inference latency: the
// dev-deploy dispatcher forwards serially at ~10 tok/s.
func doTestResumableMultiWorker(t *testing.T) {
	waitForDispatchQueueDrain(t, dispatcherReqQueue, 5*time.Minute)

	const (
		batchCount       = 5
		requestsPerBatch = 8
	)
	batchIDs := make([]string, 0, batchCount)
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		lines := make([]string, 0, requestsPerBatch)
		for requestIndex := 0; requestIndex < requestsPerBatch; requestIndex++ {
			lines = append(lines, fmt.Sprintf(
				`{"custom_id":"resumable-worker-%d-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":25,"messages":[{"role":"user","content":"batch %d request %d"}]}}`,
				batchIndex, requestIndex, testSimModel, batchIndex, requestIndex))
		}
		fileID := mustCreateFile(t,
			fmt.Sprintf("resumable-worker-%d-%s.jsonl", batchIndex, testRunID),
			strings.Join(lines, "\n"))
		batchIDs = append(batchIDs, mustCreateBatch(t, fileID))
	}

	completionDeadline := time.Now().Add(5 * time.Minute)
	for _, batchID := range batchIDs {
		remaining := time.Until(completionDeadline)
		if remaining <= 0 {
			t.Fatalf("batches did not complete within the shared timeout")
		}
		batch, results := waitForBatchStatus(t, batchID, remaining, openai.BatchStatusCompleted)

		if batch.RequestCounts.Total != requestsPerBatch {
			t.Errorf("batch %s: total = %d, want %d", batchID, batch.RequestCounts.Total, requestsPerBatch)
		}
		if batch.RequestCounts.Completed != requestsPerBatch {
			t.Errorf("batch %s: completed = %d, want %d", batchID, batch.RequestCounts.Completed, requestsPerBatch)
		}
		if batch.RequestCounts.Failed != 0 {
			t.Errorf("batch %s: failed = %d, want 0", batchID, batch.RequestCounts.Failed)
		}
		if results == nil {
			t.Fatalf("batch %s: expected non-nil results", batchID)
		}
		if results.OutputLines != requestsPerBatch {
			t.Errorf("batch %s: output lines = %d, want %d", batchID, results.OutputLines, requestsPerBatch)
		}
		validateBatchResults(t, batch, *results)
	}
}

// pinSimLatency pins the simulator's per-request latency for a mid-flight
// disruption test. The processor throttles progress flushes to the DB
// (progressUpdateInterval, 15s by default), so a partial completed set is
// observable only when request completions straddle a flush boundary. The
// latency is pinned because environments vary, and the callers stagger
// max_tokens per line because dispatch concurrency varies too: dev-deploy
// forwards serially while CI's epp job forwards concurrently, and an
// unstaggered workload finished all 12 requests inside one flush interval.
func pinSimLatency(t *testing.T) {
	t.Helper()
	patchEngineConfig(t, testSimService, `{"time_to_first_token": 100, "inter_token_latency": 100}`)
	t.Cleanup(func() {
		if err := tryPatchEngineConfig(t, testSimService, `{"time_to_first_token": 50, "inter_token_latency": 100}`); err != nil {
			t.Logf("restore simulator latency: %v", err)
		}
	})
}

// doTestResumablePodLoss deletes the Processor pod mid-batch and asserts the
// replacement pod reclaims the batch and completes it from its checkpoints,
// with every request accounted for exactly once. The simulator latency is
// pinned and max_tokens staggered per line (~4s..~24s per request): a partial
// completed set must be observable across the processor's progress flush
// interval whether the dispatcher forwards serially (dev-deploy) or
// concurrently (CI), while the batch still drains well within the completion
// window.
func doTestResumablePodLoss(t *testing.T) {
	waitForDispatchQueueDrain(t, dispatcherReqQueue, 5*time.Minute)
	pinSimLatency(t)

	const requests = 12
	lines := make([]string, 0, requests)
	for i := 0; i < requests; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"resumable-pod-loss-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":%d,"messages":[{"role":"user","content":"slow %d"}]}}`,
			i, testSimModel, 40+20*i, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("resumable-pod-loss-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)
	waitForPartialCompletedRequests(t, batchID, requests, 2*time.Minute)

	selector := fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=processor", testHelmRelease)
	if out, err := exec.Command("kubectl", "delete", "pod",
		"-n", testNamespace, "-l", selector,
	).CombinedOutput(); err != nil {
		t.Fatalf("delete processor pod: %v\n%s", err, out)
	}
	t.Logf("deleted processor pod mid-batch; waiting for replacement")

	waitForProcessorReady(t, 2*time.Minute)

	batch, results := waitForBatchStatus(t, batchID, 8*time.Minute, openai.BatchStatusCompleted)

	if batch.RequestCounts.Total != requests {
		t.Errorf("total = %d, want %d", batch.RequestCounts.Total, requests)
	}
	if batch.RequestCounts.Completed != requests {
		t.Errorf("completed = %d, want %d", batch.RequestCounts.Completed, requests)
	}
	if batch.RequestCounts.Failed != 0 {
		t.Errorf("failed = %d, want 0", batch.RequestCounts.Failed)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
	if results.OutputLines != requests {
		t.Errorf("output lines = %d, want %d", results.OutputLines, requests)
	}
}

// doTestResumableMultiReplica scales the Processor to two replicas, deletes
// one pod mid-batch, and asserts the batch still completes exactly once.
// Takeover by the surviving replica may wait for the ownership lease to
// expire, so the completion timeout is generous. Workload sized like PodLoss
// (pinned latency, staggered max_tokens) so a partial completed set is
// observable before the deletion.
func doTestResumableMultiReplica(t *testing.T) {
	// Generous: PodLoss's requeued duplicates must all be forwarded through
	// the serial pipeline before this test starts.
	waitForDispatchQueueDrain(t, dispatcherReqQueue, 8*time.Minute)
	pinSimLatency(t)

	workload := processorWorkload(t)
	originalReplicas := processorReplicaCount(t, workload)
	if originalReplicas != 2 {
		t.Cleanup(func() {
			if err := scaleProcessorWorkload(workload, originalReplicas); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		})
		if err := scaleProcessorWorkload(workload, 2); err != nil {
			t.Fatal(err)
		}
	}

	const requests = 12
	lines := make([]string, 0, requests)
	for i := 0; i < requests; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"resumable-multi-replica-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":%d,"messages":[{"role":"user","content":"slow %d"}]}}`,
			i, testSimModel, 40+20*i, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("resumable-multi-replica-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)
	waitForPartialCompletedRequests(t, batchID, requests, 2*time.Minute)

	pods := processorPodNames(t)
	if len(pods) < 2 {
		t.Fatalf("expected at least 2 processor pods, got %v", pods)
	}
	if out, err := exec.Command("kubectl", "delete", "pod",
		"-n", testNamespace, pods[0],
	).CombinedOutput(); err != nil {
		t.Fatalf("delete processor pod %s: %v\n%s", pods[0], err, out)
	}
	t.Logf("deleted processor pod %s mid-batch; waiting for takeover", pods[0])

	batch, results := waitForBatchStatus(t, batchID, 10*time.Minute, openai.BatchStatusCompleted)

	if batch.RequestCounts.Total != requests {
		t.Errorf("total = %d, want %d", batch.RequestCounts.Total, requests)
	}
	if batch.RequestCounts.Completed != requests {
		t.Errorf("completed = %d, want %d", batch.RequestCounts.Completed, requests)
	}
	if batch.RequestCounts.Failed != 0 {
		t.Errorf("failed = %d, want 0", batch.RequestCounts.Failed)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
	if results.OutputLines != requests {
		t.Errorf("output lines = %d, want %d", results.OutputLines, requests)
	}
}

// finalizationKillWindow identifies the point in a batch's finalization at
// which doTestResumableFinalizationInterrupt deletes the Processor pod.
type finalizationKillWindow int

const (
	// killWindowMissed: the batch reached a terminal status before either
	// window was observed; its finalization was not interrupted.
	killWindowMissed finalizationKillWindow = iota
	// killWindowCommitToTerminal is the precise crash window: the output
	// artifact's file record is visible while the batch is still
	// non-terminal — the observable edge between the artifact commit and
	// the fenced terminal write.
	killWindowCommitToTerminal
	// killWindowFinalizingPhase is the broad fallback: the batch entered
	// the finalizing phase (artifact assembly) without the file record
	// having been observed yet.
	killWindowFinalizingPhase
)

const (
	// killWindowPollInterval is far tighter than the other polling helpers'
	// cadence: the finalizing phase of a small batch lasts only a few
	// hundred milliseconds, so a coarser poll can step over it entirely.
	killWindowPollInterval = 100 * time.Millisecond
	// killWindowListLimit is the files-list page size when watching for the
	// output artifact's record. Listing orders by file ID, not creation
	// time, so the record is not guaranteed to appear on an early page;
	// request the maximum the API accepts.
	killWindowListLimit = 10000
)

// waitForFinalizationKillWindow polls a non-terminal batch until one of the
// finalization kill windows opens and reports which one. The artifact file
// record is sampled before the batch status so that a poll landing in the
// narrow commit-to-terminal-write window classifies as that edge rather
// than as the finalizing phase containing it.
func waitForFinalizationKillWindow(t *testing.T, client *openai.Client, batchID string, timeout time.Duration) finalizationKillWindow {
	t.Helper()

	outputFilename := fmt.Sprintf("batch_output_%s.jsonl", batchID)

	deadline := time.Now().Add(timeout)
	if d, ok := t.Deadline(); ok && d.Before(deadline) {
		deadline = d.Add(-5 * time.Second)
	}
	for time.Now().Before(deadline) {
		// Sample the file record first: it appears during artifact assembly,
		// and a sample that lands in the commit-to-terminal-write window must
		// win over the finalizing-phase fallback checked below.
		page, err := client.Files.List(context.Background(), openai.FileListParams{
			Purpose: param.NewOpt("batch_output"),
			Limit:   param.NewOpt(int64(killWindowListLimit)),
		})
		if err != nil {
			t.Fatalf("list files failed: %v", err)
		}
		recordCommitted := false
		for _, f := range page.Data {
			if f.Filename == outputFilename {
				recordCommitted = true
				break
			}
		}

		b, err := client.Batches.Get(context.Background(), batchID)
		if err != nil {
			t.Fatalf("retrieve batch failed: %v", err)
		}
		if terminalBatchStatuses[b.Status] {
			t.Logf("batch %s finalized (status %q) before a kill window opened", batchID, b.Status)
			return killWindowMissed
		}
		if recordCommitted {
			return killWindowCommitToTerminal
		}
		if b.Status == openai.BatchStatusFinalizing {
			return killWindowFinalizingPhase
		}
		time.Sleep(killWindowPollInterval)
	}
	t.Fatalf("batch %s did not finalize within %v", batchID, timeout)
	return killWindowMissed
}

// doTestResumableFinalizationInterrupt force-deletes the Processor pod while
// a batch is finalizing and asserts the requeued replacement completes the
// batch with every request accounted for exactly once and a complete output
// artifact — the user-visible recovery for an interrupted finalization.
//
// The crash window is the gap between the output artifact's commit and the
// fenced terminal write, observed through the files API: the artifact's
// file record becomes visible while the batch is still non-terminal. The
// pod is deleted the moment that edge is observed. That window is only
// milliseconds wide — the fenced terminal write immediately follows the
// artifact commit — so the watcher falls back to deleting as soon as the
// batch enters the finalizing phase (the whole artifact-assembly window);
// the precise window is deterministically guarded by the Postgres-backed
// worker scenario. When even the finalizing phase completes before the
// delete lands, the batch simply finalized and the completion assertions
// below still hold; the scenario never fails spuriously.
//
// The delete is a force-delete (grace period 0): finalization runs on a
// context detached from SIGTERM by design, so a graceful delete would let
// the dying pod finish its terminal write before it exits. The workload
// follows PodLoss (pinned simulator latency, staggered max_tokens) so
// completion progress is observable whether the dispatcher forwards
// serially (dev-deploy) or concurrently (CI).
func doTestResumableFinalizationInterrupt(t *testing.T) {
	// Generous: MultiReplica's requeued duplicates must all be forwarded
	// through the serial pipeline before this test starts.
	waitForDispatchQueueDrain(t, dispatcherReqQueue, 8*time.Minute)
	pinSimLatency(t)

	const requests = 12
	lines := make([]string, 0, requests)
	for i := 0; i < requests; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"resumable-finalize-interrupt-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":%d,"messages":[{"role":"user","content":"slow %d"}]}}`,
			i, testSimModel, 40+20*i, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("resumable-finalize-interrupt-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)

	// The watcher budget must cover the batch's whole execution, not just
	// its finalization: on a serially-forwarding dispatcher the staggered
	// workload runs ~4-5 minutes before finalizing (same budget class as
	// PodLoss's post-kill completion window).
	window := waitForFinalizationKillWindow(t, newClient(), batchID, 8*time.Minute)
	switch window {
	case killWindowCommitToTerminal:
		t.Logf("batch %s: output artifact committed while still non-terminal; "+
			"force-deleting the processor pod inside the commit-to-terminal-write window", batchID)
	case killWindowFinalizingPhase:
		t.Logf("batch %s: finalization under way; force-deleting the processor pod "+
			"mid-phase before the artifact commit was observed", batchID)
	case killWindowMissed:
		t.Logf("batch %s finalized before a kill window opened; asserting completion "+
			"without an interruption this run", batchID)
	}

	if window != killWindowMissed {
		selector := fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=processor", testHelmRelease)
		if out, err := exec.Command("kubectl", "delete", "pod",
			"-n", testNamespace, "-l", selector,
			"--grace-period=0", "--force", "--wait=true",
		).CombinedOutput(); err != nil {
			t.Fatalf("force-delete processor pod: %v\n%s", err, out)
		}
		t.Logf("force-deleted processor pod mid-finalization; waiting for replacement")

		// Diagnostic only — which side of the race between the delete and
		// the fenced terminal write won must not gate the outcome.
		if b, err := newClient().Batches.Get(context.Background(), batchID); err == nil && !terminalBatchStatuses[b.Status] {
			t.Logf("finalization interrupted: batch %s is still %q after its owner was killed", batchID, b.Status)
		}

		waitForProcessorReady(t, 2*time.Minute)
	}

	batch, results := waitForBatchStatus(t, batchID, 8*time.Minute, openai.BatchStatusCompleted)

	if batch.RequestCounts.Total != requests {
		t.Errorf("total = %d, want %d", batch.RequestCounts.Total, requests)
	}
	if batch.RequestCounts.Completed != requests {
		t.Errorf("completed = %d, want %d", batch.RequestCounts.Completed, requests)
	}
	if batch.RequestCounts.Failed != 0 {
		t.Errorf("failed = %d, want 0", batch.RequestCounts.Failed)
	}
	if batch.OutputFileID == "" || batch.ErrorFileID != "" {
		t.Errorf("terminal files = output:%q error:%q, want non-empty output and empty error",
			batch.OutputFileID, batch.ErrorFileID)
	}
	if results == nil {
		t.Fatal("expected non-nil results")
	}
	if results.OutputLines != requests {
		t.Errorf("output lines = %d, want %d", results.OutputLines, requests)
	}
	if results.ErrorLines != 0 {
		t.Errorf("error lines = %d, want 0", results.ErrorLines)
	}
	// Every request accounted for exactly once: full custom_id coverage
	// across the output and error files, with no duplicates.
	validateBatchResults(t, batch, *results)
}
