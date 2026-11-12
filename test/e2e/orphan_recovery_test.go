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
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
)

// testOrphanRecovery covers the GC reconciler's orphan detection and recovery
// when no processor is running to handle a stranded job.
//
// Unlike testProcessorGracefulShutdown which tests SIGTERM with a replacement
// pod available, these tests scale the processor StatefulSet to 0 replicas,
// ensuring no processor can self-recover the job.
// The reconciler (running in the GC pod) then detects the stale in-flight
// entry and transitions the orphaned job to a terminal state.
//
// Requires:
//   - batch-gc running with reconciler enabled and a short interval (60s in dev-deploy)
//   - kubectl available
func testOrphanRecovery(t *testing.T) {
	t.Run("HardCrashInProgress", doTestHardCrashOrphanRecovery)
	t.Run("CancellingOrphan", doTestCancellingOrphanRecovery)
}

// doTestHardCrashOrphanRecovery submits a batch with long-running requests,
// scales the processor StatefulSet to 0 (which kills the pod), then verifies
// the GC reconciler re-enqueues the orphaned job (SLO is still valid with a
// 24h window). After scaling back to 1, the new processor picks up the
// re-enqueued job and completes it.
//
// Timeline:
//  1. Submit batch → wait for in_progress
//  2. Scale to 0 (kills pod, no replacement available)
//  3. Reconciler detects orphan → requeues it (SLO valid, epoch bumped)
//  4. Scale back to 1 → processor claims the requeued job and completes it
func doTestHardCrashOrphanRecovery(t *testing.T) {
	t.Helper()

	if !testKubectlAvailable {
		t.Skip("kubectl not available, skipping orphan recovery test")
	}

	sts := fmt.Sprintf("%s-processor", testHelmRelease)

	var lines []string
	for i := 1; i <= 10; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"orphan-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"slow %d"}]}}`, i, testSimModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("test-orphan-recovery-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)
	time.Sleep(2 * time.Second)

	killAndScaleDown(t, sts)
	t.Cleanup(func() { scaleUp(t, sts) })

	// The reconciler requeues the resumable orphan: ownership is cleared and
	// the epoch bumped, but the row stays in_progress (the resume path owns
	// the terminal routing). The requeue is not observable through the API,
	// so wait one reconciler interval (30s in dev-deploy) plus margin.
	time.Sleep(45 * time.Second)

	// Scale back to 1 so the processor claims the requeued job and completes
	// it from its checkpoints.
	scaleUp(t, sts)

	finalBatch := waitForOrphanTerminal(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)

	t.Logf("orphan recovery: batch %s reached %s (completed=%d, failed=%d, total=%d)",
		batchID, finalBatch.Status,
		finalBatch.RequestCounts.Completed,
		finalBatch.RequestCounts.Failed,
		finalBatch.RequestCounts.Total)
}

// doTestCancellingOrphanRecovery submits a batch, waits for it to reach
// in_progress, cancels it (status becomes cancelling), then force-kills the
// processor pod and scales to 0 so it cannot complete the cancellation. The
// GC reconciler should detect the orphaned cancelling job and transition it
// to cancelled.
//
// Timeline:
//  1. Submit batch → wait for in_progress
//  2. Wait for at least 1 request to complete (deterministic timing)
//  3. Cancel batch → verify cancelling status
//  4. Force-kill pod + scale to 0 (no replacement, no cancel handling)
//  5. Reconciler detects orphan → requeues it
//  6. Scale back to 1 → the pickup finalizes the cancelling row as cancelled
func doTestCancellingOrphanRecovery(t *testing.T) {
	t.Helper()

	if !testKubectlAvailable {
		t.Skip("kubectl not available, skipping cancelling orphan recovery test")
	}

	sts := fmt.Sprintf("%s-processor", testHelmRelease)

	var lines []string
	for i := 1; i <= 5; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"cancel-orphan-fast-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":1,"messages":[{"role":"user","content":"Hi %d"}]}}`, i, testSimModel, i))
	}
	for i := 1; i <= 20; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"cancel-orphan-slow-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"slow %d"}]}}`, i, testSimModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("test-cancelling-orphan-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)

	waitForCompletedRequests(t, batchID, 1, 2*time.Minute)

	client := newClient()
	batch, err := client.Batches.Cancel(context.Background(), batchID)
	if err != nil {
		t.Fatalf("cancel batch failed: %v", err)
	}
	if batch.Status != openai.BatchStatusCancelling {
		t.Fatalf("expected cancelling after cancel call, got %s", batch.Status)
	}
	t.Logf("batch %s is now cancelling", batchID)

	killAndScaleDown(t, sts)
	t.Cleanup(func() { scaleUp(t, sts) })

	// The reconciler requeues the cancelling orphan like any other; the
	// cancelled finalization happens when a processor picks it up (the
	// pickup routes a cancelling row straight to a cancelled finalization
	// with partial artifacts). Wait one reconciler interval, then bring the
	// processor back to drive the pickup.
	time.Sleep(45 * time.Second)
	scaleUp(t, sts)

	finalBatch := waitForOrphanTerminal(t, batchID, 5*time.Minute, openai.BatchStatusCancelled)

	t.Logf("cancelling orphan recovery: batch %s reached %s (cancelled_at=%d)",
		batchID, finalBatch.Status, finalBatch.CancelledAt)

	if finalBatch.CancelledAt == 0 {
		t.Error("expected cancelled_at to be set")
	}
}

// killAndScaleDown force-kills all processor pods and scales the StatefulSet
// to 0 replicas. The scale-to-0 prevents replacement pods from self-recovering
// the orphaned job, ensuring the GC reconciler handles it.
func killAndScaleDown(t *testing.T, sts string) {
	t.Helper()

	podSelector := fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=processor", testHelmRelease)

	// Scale to 0 first so the StatefulSet controller doesn't recreate the pod.
	scaleOut, scaleErr := exec.Command("kubectl", "scale",
		fmt.Sprintf("statefulset/%s", sts),
		"--replicas=0",
		"-n", testNamespace,
	).CombinedOutput()
	if scaleErr != nil {
		t.Fatalf("kubectl scale --replicas=0 failed: %v\n%s", scaleErr, scaleOut)
	}
	t.Logf("scaled %s to 0: %s", sts, strings.TrimSpace(string(scaleOut)))

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer waitCancel()
	waitOut, waitErr := exec.CommandContext(waitCtx, "kubectl", "wait",
		"--for=delete",
		"pod",
		"-l", podSelector,
		"-n", testNamespace,
		"--timeout=120s",
	).CombinedOutput()
	if waitErr != nil {
		t.Logf("wait for pod deletion (may be already gone): %v\n%s", waitErr, waitOut)
	}
}

// scaleUp scales the given StatefulSet back to 1 replica and waits for it
// to become ready. Used in t.Cleanup to restore the processor for subsequent tests.
func scaleUp(t *testing.T, sts string) {
	t.Helper()

	out, err := exec.Command("kubectl", "scale",
		fmt.Sprintf("statefulset/%s", sts),
		"--replicas=1",
		"-n", testNamespace,
	).CombinedOutput()
	if err != nil {
		t.Logf("kubectl scale --replicas=1 failed (cleanup): %v\n%s", err, out)
		return
	}
	t.Logf("scaled %s back to 1: %s", sts, strings.TrimSpace(string(out)))

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer waitCancel()
	waitOut, waitErr := exec.CommandContext(waitCtx, "kubectl", "wait",
		"--for=condition=Ready",
		"pod",
		"-l", fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=processor", testHelmRelease),
		"-n", testNamespace,
		"--timeout=120s",
	).CombinedOutput()
	if waitErr != nil {
		t.Logf("waiting for processor pod ready (cleanup): %v\n%s", waitErr, waitOut)
		return
	}
	t.Logf("processor pod ready after scale-up: %s", strings.TrimSpace(string(waitOut)))

	waitForProcessorReady(t, 2*time.Minute)
}
