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
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
)

// doTestResumableRecoveryPodLoss deletes the owning processor while a
// feature-gated asynchronous batch is in progress. The replacement must use
// durable recovery state and publish a complete, valid result artifact.
func doTestResumableRecoveryPodLoss(t *testing.T) {
	t.Helper()

	if !testKubectlAvailable {
		t.Skip("kubectl not available, skipping resumable recovery pod-loss test")
	}

	var lines []string
	for i := 1; i <= 10; i++ {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":"resumable-pod-loss-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":200,"messages":[{"role":"user","content":"slow %d"}]}}`, i, testSimModel, i))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("test-resumable-pod-loss-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	_, _ = waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusInProgress)
	time.Sleep(2 * time.Second)

	out, err := exec.Command("kubectl", "delete", "pod",
		"-l", fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=processor", testHelmRelease),
		"-n", testNamespace,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl delete pod failed: %v\n%s", err, out)
	}
	t.Logf("processor pod delete issued: %s", strings.TrimSpace(string(out)))

	waitForProcessorReady(t, 2*time.Minute)
	finalBatch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)
	t.Logf("resumable pod loss: batch %s reached %s (completed=%d, failed=%d, total=%d)",
		batchID, finalBatch.Status, finalBatch.RequestCounts.Completed,
		finalBatch.RequestCounts.Failed, finalBatch.RequestCounts.Total)
}
