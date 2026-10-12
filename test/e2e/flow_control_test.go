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

// Flow control tests verify that the batch-gateway processor correctly sends
// inference headers and handles downstream 429 responses.
//
// Tests are split into two groups:
//
//   - Headers: run without an EPP. They verify batch-gateway's own
//     responsibilities (sending the right headers) against plain vllm-vcr
//     instances.
//
//   - EPP: require a full EPP deployment (ENABLE_GIE=true; legacy flag name). They verify
//     that requests route through EPP, that per-model InferenceObjectives are
//     respected, and that the processor retries and eventually gives up on
//     the responses the EPP emits when it sheds batch traffic under
//     saturation: 429 when it rejects outright, 503 when a queued request's
//     TTL expires. Saturation is provoked through the vllm-vcr control API
//     (see chokeEngine in aimd_test.go); nothing fakes a backpressure status.

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
	"gopkg.in/yaml.v3"
)

func testFlowControl(t *testing.T) {
	t.Run("Headers", func(t *testing.T) {
		if !testKubectlAvailable {
			t.Skip("kubectl not available")
		}
		t.Run("InferenceObjectiveHeader", doTestInferenceObjectiveHeader)
		t.Run("SLOHeader", doTestSLOHeader)
	})

	t.Run("GIE", func(t *testing.T) {
		if !testKubectlAvailable {
			t.Skip("kubectl not available")
		}
		if !detectGIEDeployed(t) {
			t.Skip("EPP not deployed (deploy with ENABLE_GIE=true; legacy flag name)")
		}
		t.Cleanup(func() { deleteE2ECurlPod(t) })
		t.Run("HeaderPropagation", doTestGIEHeaderPropagation)
		t.Run("BatchCompletionThroughEPP", doTestBatchCompletionThroughEPP)
		t.Run("RetryOnShed", doTestRetryOnShed)
		t.Run("RetryExhaustion", doTestRetryExhaustion)
		t.Run("PriorityBandInteraction", doTestPriorityBandInteraction)
	})
}

// ── Header propagation (no EPP required) ────────────────────────────────

// doTestInferenceObjectiveHeader verifies that the processor ConfigMap has the
// expected inferenceObjective for testModel, and that a batch targeting this
// model completes successfully. It does not inspect the request header on the
// wire.
func doTestInferenceObjectiveHeader(t *testing.T) {
	t.Helper()

	expectedObjective := resolveExpectedObjective(t, testModel)
	configuredObjective := getProcessorConfigObjective(t, testModel)
	if configuredObjective != expectedObjective {
		t.Errorf("processor ConfigMap inferenceObjective for %q = %q, want %q",
			testModel, configuredObjective, expectedObjective)
	}

	fileID := mustCreateFile(t, fmt.Sprintf("test-fc-obj-header-%s.jsonl", testRunID), testJSONL)
	batchID := mustCreateBatch(t, fileID)

	batch, _ := waitForBatchStatus(t, batchID, 3*time.Minute, openai.BatchStatusCompleted)
	if batch.RequestCounts.Completed != 2 {
		t.Fatalf("expected 2 completed, got %d", batch.RequestCounts.Completed)
	}

	t.Logf("inferenceObjective=%q configured and batch completed", expectedObjective)
}

// doTestSLOHeader submits a batch with a short completion_window and verifies
// the batch completes before that window expires. It does not read
// x-slo-ttft-ms on the live request. Remaining-ms formatting from a stored
// deadline is covered by TestMergeHeaders in source_planfile_test.go.
func doTestSLOHeader(t *testing.T) {
	t.Helper()

	fileID := mustCreateFile(t, fmt.Sprintf("test-fc-slo-header-%s.jsonl", testRunID), testJSONL)

	client := newClient()
	batch, err := client.Batches.New(t.Context(), openai.BatchNewParams{
		InputFileID:      fileID,
		Endpoint:         openai.BatchNewParamsEndpointV1ChatCompletions,
		CompletionWindow: openai.BatchNewParamsCompletionWindow("10m"),
	})
	if err != nil {
		t.Fatalf("create batch failed: %v", err)
	}

	finalBatch, _ := waitForBatchStatus(t, batch.ID, 3*time.Minute, openai.BatchStatusCompleted)
	if finalBatch.Status != openai.BatchStatusCompleted {
		t.Fatalf("expected batch to complete within SLO window, got status %s", finalBatch.Status)
	}
	if finalBatch.RequestCounts.Completed != 2 {
		t.Fatalf("expected 2 completed, got %d", finalBatch.RequestCounts.Completed)
	}

	t.Logf("batch completed within 10m SLO window")
}

// doTestRetryOnShed verifies the full retry-to-success path:
//  1. Saturate model B until the EPP sheds a request (see
//     submitSaturatingBatch in aimd_test.go).
//  2. Release the engine so the processor's retries succeed.
//  3. Assert every request completed with no failures; the AIMD decrease
//     counter is logged for inspection.
//
// In per-model EPP mode model B is configured with maxRetries=3 and backoff 2s..30s, so
// the engine is released as soon as shedding is observed.
func doTestRetryOnShed(t *testing.T) {
	t.Helper()

	metricsBefore := scrapeProcessorMetrics(t)
	decreasesBefore := parseCounterByEndpoint(t, metricsBefore, "batch_processor_aimd_decreases_total")
	var beforeCount float64
	for endpoint, count := range decreasesBefore {
		if isEPPEndpoint(endpoint, testModelB) {
			beforeCount = count
			break
		}
	}
	errorsBefore := getRequestErrors(t, testModelB)
	outcomesBefore := getEPPBatchOutcomes(t, eppDeploymentFor(testModelB))

	batchID, stopLoad := submitSaturatingBatch(t, "retry-shed")

	t.Log("stopping interactive load and releasing the engine so retries succeed")
	stopLoad()
	if err := releaseEngine(t, testSimServiceB); err != nil {
		t.Fatal(err)
	}

	batch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)
	if batch.RequestCounts.Completed != int64(saturationRequests) {
		t.Fatalf("expected %d completed, got %d (failed=%d)",
			saturationRequests, batch.RequestCounts.Completed, batch.RequestCounts.Failed)
	}
	if batch.RequestCounts.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", batch.RequestCounts.Failed)
	}
	// The EPP shed batch requests (checked in submitSaturatingBatch) and yet
	// every request completed: the processor retried each shed response.
	if shed := assertOutputMatchesEPPOutcomes(t, batch, outcomesBefore, getEPPBatchOutcomes(t, eppDeploymentFor(testModelB))); shed != 0 {
		t.Errorf("expected no shed status in the output after retries, got %d", shed)
	}

	// Each shed request hit at least one shed response before succeeding, so
	// hadCapacityRetry=true triggers an AIMD decrease on completion.
	metricsAfter := scrapeProcessorMetrics(t)
	decreasesAfter := parseCounterByEndpoint(t, metricsAfter, "batch_processor_aimd_decreases_total")
	var afterCount float64
	for endpoint, count := range decreasesAfter {
		if isEPPEndpoint(endpoint, testModelB) {
			afterCount = count
			break
		}
	}
	t.Logf("retry-on-shed: all %d requests completed after retry "+
		"(aimd_decreases: %.0f → %.0f, logged, not asserted)", saturationRequests, beforeCount, afterCount)

	assertNoNewRequestErrors(t, testModelB, errorsBefore)
}

// doTestRetryExhaustion verifies that batch requests the pool cannot serve
// end up as shed-status lines in the output file. Batch traffic alone cannot
// starve itself: every eviction halves the processor's AIMD limit, fewer
// requests sit at the model, the EPP drops out of saturation, and the retries
// get through. What starves batch traffic in production is non-sheddable
// interactive traffic (priority 100, dispatched first), which
// submitSaturatingBatch keeps aimed at model B's choked EPP. Batch requests
// behind it are evicted at every 30s TTL, exhaust their retries (maxRetries=3
// in per-model EPP mode) after roughly four TTL cycles, and the processor records the
// EPP's final answer (503 "request TTL expired", or 429 if it rejected
// outright). The load is then stopped and the engine released so the rest of
// the batch completes, leaving a mix of completed and failed lines.
//
// The test polls manually instead of using waitForBatchStatus because
// validateBatchResults enforces status_code=200 on every output line.
func doTestRetryExhaustion(t *testing.T) {
	t.Helper()

	outcomesBefore := getEPPBatchOutcomes(t, eppDeploymentFor(testModelB))
	batchID, stopLoad := submitSaturatingBatch(t, "exhaust")

	t.Log("waiting for the processor to exhaust retries on a shed request")
	waitForBatchFailures(t, batchID, 8*time.Minute)

	t.Log("stopping interactive load and releasing the engine so the remaining requests complete")
	stopLoad()
	if err := releaseEngine(t, testSimServiceB); err != nil {
		t.Fatal(err)
	}
	finalBatch := waitForRetryExhaustion(t, batchID, 5*time.Minute)

	if finalBatch.Status != openai.BatchStatusCompleted {
		t.Errorf("expected batch status %q (processor finished processing), got %q",
			openai.BatchStatusCompleted, finalBatch.Status)
	}
	if finalBatch.RequestCounts.Failed == 0 {
		t.Errorf("expected retry exhaustion to fail at least one request, got failed=0 completed=%d",
			finalBatch.RequestCounts.Completed)
	}
	if got := finalBatch.RequestCounts.Completed + finalBatch.RequestCounts.Failed; got != int64(saturationRequests) {
		t.Errorf("expected completed+failed = %d, got %d", saturationRequests, got)
	}

	t.Logf("retry exhaustion: status=%s completed=%d failed=%d total=%d",
		finalBatch.Status, finalBatch.RequestCounts.Completed, finalBatch.RequestCounts.Failed, finalBatch.RequestCounts.Total)

	if finalBatch.OutputFileID == "" {
		t.Fatal("expected output file with shed responses, but OutputFileID is empty")
	}
	if shed := assertOutputMatchesEPPOutcomes(t, finalBatch, outcomesBefore, getEPPBatchOutcomes(t, eppDeploymentFor(testModelB))); shed == 0 {
		t.Errorf("expected at least one shed status (503/429) in the output file, found none")
	}

	assertRequestErrors(t, testModelB)
}

// doTestPriorityBandInteraction verifies that the EPP serves the interactive
// band while it sheds the batch band from the same saturated pool:
//
//  1. Saturate model B behind interactive load and submit a batch
//     (submitSaturatingBatch), which returns once a batch request was shed.
//  2. While still saturated, the interactive band's Dispatched counter must
//     keep advancing: the EPP is dispatching priority-100 traffic to the
//     choked engine while it evicts priority -1.
//  3. Stop the load and release the engine: a single interactive request
//     returns 200 and the batch completes with no shed line left in its output.
func doTestPriorityBandInteraction(t *testing.T) {
	t.Helper()

	eppDeployment := eppDeploymentFor(testModelB)
	batchOutcomesBefore := getEPPBatchOutcomes(t, eppDeployment)
	batchID, stopLoad := submitSaturatingBatch(t, "priority-band")

	interactiveBefore := getEPPOutcomes(t, eppDeployment, eppInteractivePriority)[eppOutcomeDispatched]
	batchOutcomesShed := getEPPBatchOutcomes(t, eppDeployment)
	waitForEPPInteractiveDispatch(t, eppDeployment, interactiveBefore, 90*time.Second)
	interactiveAfter := getEPPOutcomes(t, eppDeployment, eppInteractivePriority)[eppOutcomeDispatched]
	t.Logf("under saturation: interactive Dispatched %.0f -> %.0f, batch shed %.0f -> %.0f",
		interactiveBefore, interactiveAfter, shedCount(batchOutcomesBefore), shedCount(batchOutcomesShed))

	t.Log("stopping interactive load and releasing the engine")
	stopLoad()
	if err := releaseEngine(t, testSimServiceB); err != nil {
		t.Fatal(err)
	}

	url := fmt.Sprintf("http://%s.%s.svc.cluster.local:8081/v1/chat/completions", eppDeployment, testNamespace)
	body := fmt.Sprintf(`{"model":%q,"max_tokens":%d,"messages":[{"role":"user","content":"interactive"}]}`, testModelB, saturationMaxTokens)
	if code := curlEPP(t, url, fmt.Sprintf("interactive-default-%s", testModelB), body); code != "200" {
		t.Errorf("interactive request after release returned HTTP %s, want 200", code)
	}

	batch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)
	if batch.RequestCounts.Completed != int64(saturationRequests) {
		t.Fatalf("expected %d completed, got %d (failed=%d)",
			saturationRequests, batch.RequestCounts.Completed, batch.RequestCounts.Failed)
	}
	if shed := assertOutputMatchesEPPOutcomes(t, batch, batchOutcomesBefore, getEPPBatchOutcomes(t, eppDeployment)); shed != 0 {
		t.Errorf("expected no shed status in the output after retries, got %d", shed)
	}
}

// waitForEPPInteractiveDispatch polls until the EPP's interactive-band
// Dispatched counter exceeds before.
func waitForEPPInteractiveDispatch(t *testing.T, deployment string, before float64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last float64
	for time.Now().Before(deadline) {
		last = getEPPOutcomes(t, deployment, eppInteractivePriority)[eppOutcomeDispatched]
		if last > before {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("EPP %s dispatched no interactive request within %v (before=%.0f, last=%.0f)", deployment, timeout, before, last)
}

// startInteractiveLoad keeps `concurrency` interactive chat completions in
// flight against the model's EPP from the in-cluster curl pod, each worker
// sending its next request as soon as the previous one returns, until the
// returned stop function is called (idempotent). The requests carry the
// interactive-default InferenceObjective (priority 100) that dev-deploy
// creates, so the EPP queues rather than sheds them and dispatches them ahead
// of batch traffic.
func startInteractiveLoad(t *testing.T, model string, concurrency int) func() {
	t.Helper()

	ensureE2ECurlPod(t)

	url := fmt.Sprintf("http://%s.%s.svc.cluster.local:8081/v1/chat/completions", eppDeploymentFor(model), testNamespace)
	body := fmt.Sprintf(`{"model":%q,"max_tokens":%d,"messages":[{"role":"user","content":"interactive"}]}`, model, saturationMaxTokens)
	objective := fmt.Sprintf("x-gateway-inference-objective: interactive-default-%s", model)
	script := fmt.Sprintf(`for i in $(seq 1 %d); do (while true; do curl -sS -o /dev/null -m 300 -X POST %s -H 'content-type: application/json' -H %s -d %s; done) & done; wait`,
		concurrency, shQuote(url), shQuote(objective), shQuote(body))

	ctx, cancel := context.WithCancel(t.Context())
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", testNamespace, e2eCurlPod, "--", "sh", "-c", script)
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("failed to start interactive load: %v", err)
	}
	t.Logf("interactive load started: %d concurrent requests at %s", concurrency, url)

	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			_ = cmd.Wait()
			// Cancelling kubectl does not kill the shell loop inside the pod.
			out, err := exec.Command("kubectl", "exec", "-n", testNamespace, e2eCurlPod, "--", "sh", "-c", "pkill -f 'curl -sS' ; pkill -f 'while true'; true").CombinedOutput()
			if err != nil {
				t.Logf("stopping interactive load in pod: %v\n%s", err, out)
			}
			t.Log("interactive load stopped")
		})
	}
	t.Cleanup(stop)
	return stop
}

// waitForBatchFailures polls a batch until RequestCounts.Failed > 0 or it
// reaches a terminal state, failing the test if neither happens in time.
func waitForBatchFailures(t *testing.T, batchID string, timeout time.Duration) {
	t.Helper()

	client := newClient()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		b, err := client.Batches.Get(t.Context(), batchID)
		if err != nil {
			t.Fatalf("retrieve batch failed: %v", err)
		}
		if b.RequestCounts.Failed > 0 {
			t.Logf("batch %s: %d request(s) failed (completed=%d)", batchID, b.RequestCounts.Failed, b.RequestCounts.Completed)
			return
		}
		if terminalBatchStatuses[b.Status] {
			t.Fatalf("batch %s reached %s with no failed requests", batchID, b.Status)
		}
		time.Sleep(5 * time.Second)
	}
	t.Fatalf("batch %s recorded no failed request within %v", batchID, timeout)
}

// ──  EPP integration tests (require ENABLE_GIE=true) ───────────────────
//
// Current coverage: EPP routing smoke tests (header propagation, multi-model
// completion) and shedding under saturation, with the model server choked
// through the vllm-vcr control API and the pool kept saturated by interactive
// load. Against the flow-control scenarios of #402: shedding under saturation
// is RetryExhaustion, mixed load with metrics is RetryOnShed (plus the AIMD
// tests), priority band interaction is PriorityBandInteraction.
//
// Not covered: while two sheddable requests are queued, EPP does not dispatch
// the earlier x-slo-ttft-ms deadline first. Observed on legacy GIE EPP v1.5.0 (FCFS
// even with slo-deadline-ordering-policy). Not rechecked with the llm-d-router
// EPP that dev-deploy now uses.
// When adding the test: saturate, curl long then short SLO on the same
// objective, unsaturate only after both "Item enqueued", assert short
// "Item dispatched" first. Do not use batch CompletedAt.

// detectGIEDeployed (legacy function name) checks whether at least one EPP deployment exists.
// It searches testEPPNamespace (TEST_EPP_NAMESPACE) if set, otherwise
// testNamespace. Set TEST_GIE_DEPLOYED=true to force-enable EPP tests
// when the EPP naming convention differs (e.g. RHOAI).
func detectGIEDeployed(t *testing.T) bool {
	t.Helper()

	if strings.EqualFold(getEnvOrDefault("TEST_GIE_DEPLOYED", ""), "true") {
		return true
	}

	ns := testEPPNamespace
	if ns == "" {
		ns = testNamespace
	}

	out, err := exec.Command("kubectl", "get", "deployments",
		"-n", ns,
		"-o", "name",
	).CombinedOutput()
	if err != nil {
		t.Logf("kubectl get deployments failed: %v", err)
		return false
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "epp-") {
			return true
		}
	}
	return false
}

// doTestGIEHeaderPropagation (legacy function name) verifies that the processor sends requests through
// EPP with x-gateway-inference-objective configured and that the requests
// pass through the flow control dispatch path.
//
// Verification: submit a batch, wait for completion, then check EPP logs for
// evidence that it received, routed, and dispatched the requests via flow control.
func doTestGIEHeaderPropagation(t *testing.T) {
	t.Helper()

	eppDeployment := fmt.Sprintf("%s-%s-epp", getEnvOrDefault("GIE_EPP_RELEASE", "epp"), testModel)
	sinceTime := time.Now().UTC().Format(time.RFC3339Nano)

	fileID := mustCreateFile(t, fmt.Sprintf("flow-control-headers-%s.jsonl", testRunID), testJSONL)
	batchID := mustCreateBatch(t, fileID)

	batch, _ := waitForBatchStatus(t, batchID, 60*time.Second, openai.BatchStatusCompleted)

	if batch.RequestCounts.Completed != 2 {
		t.Fatalf("expected 2 completed requests, got %d", batch.RequestCounts.Completed)
	}
	if batch.RequestCounts.Failed != 0 {
		t.Fatalf("expected 0 failed requests, got %d", batch.RequestCounts.Failed)
	}

	eppLogs := getEPPLogsSince(t, eppDeployment, sinceTime)

	received := strings.Count(eppLogs, "EPP received request")
	if received < 2 {
		t.Errorf("expected EPP to receive >= 2 requests since %s, got %d;\nlog sample:\n%s",
			sinceTime, received, truncateLog(eppLogs, 1000))
	}

	routed := strings.Count(eppLogs, "EPP sent request body response(s) to proxy")
	if routed < 2 {
		t.Errorf("expected EPP to route >= 2 responses since %s, got %d", sinceTime, routed)
	}

	dispatched := strings.Count(eppLogs, "Item dispatched.")
	if dispatched < 2 {
		t.Errorf("expected flow control to dispatch >= 2 items since %s, got %d;\nlog sample:\n%s",
			sinceTime, dispatched, truncateLog(eppLogs, 1000))
	}
}

// doTestBatchCompletionThroughEPP verifies multi-model batch completion through
// separate EPP instances by checking each EPP's dispatched-request metric.
// We prefer metrics here because log sampling via --since-time proved flaky.
func doTestBatchCompletionThroughEPP(t *testing.T) {
	t.Helper()

	t.Cleanup(func() {
		deleteE2ECurlPod(t)
	})

	eppPrefix := getEnvOrDefault("GIE_EPP_RELEASE", "epp")
	eppDeployments := []string{
		fmt.Sprintf("%s-%s-epp", eppPrefix, testModel),
		fmt.Sprintf("%s-%s-epp", eppPrefix, testModelB),
	}
	beforeCounts := make(map[string]float64, len(eppDeployments))
	for _, deployment := range eppDeployments {
		beforeCounts[deployment] = getEPPDispatchedCount(t, deployment)
	}

	multiModelJSONL := strings.Join([]string{
		fmt.Sprintf(`{"custom_id":"fc-req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello from model A"}]}}`, testModel),
		fmt.Sprintf(`{"custom_id":"fc-req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello from model B"}]}}`, testModelB),
	}, "\n")

	fileID := mustCreateFile(t, fmt.Sprintf("flow-control-epp-%s.jsonl", testRunID), multiModelJSONL)
	batchID := mustCreateBatch(t, fileID)

	batch, result := waitForBatchStatus(t, batchID, 60*time.Second, openai.BatchStatusCompleted)

	if batch.RequestCounts.Completed != 2 {
		t.Errorf("expected 2 completed, got %d", batch.RequestCounts.Completed)
	}
	if batch.RequestCounts.Failed != 0 {
		t.Errorf("expected 0 failed, got %d", batch.RequestCounts.Failed)
	}

	validateTerminalBatch(t, batch)
	validateBatchResults(t, batch, *result)

	for _, deployment := range eppDeployments {
		assertEPPDispatchedDelta(t, deployment, beforeCounts[deployment], 1, 15*time.Second)
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────────

// curlEPP sends a chat completion request to EPP with the given objective header
// and returns the HTTP status code string. Does not fail the test on non-200.
// Timeout is 65s (EPP defaultRequestTTL 30s plus buffer) so saturation-path
// EvictedTTL responses are not cut off as transport errors.
func curlEPP(t *testing.T, url, objective, body string) string {
	t.Helper()

	ensureE2ECurlPod(t)
	out, err := exec.Command("kubectl", "exec",
		"-n", testNamespace,
		e2eCurlPod,
		"--",
		"curl", "-sS", "-X", "POST",
		"-H", "Content-Type: application/json",
		"-H", fmt.Sprintf("x-gateway-inference-objective: %s", objective),
		"-d", body,
		"-w", "\n%{http_code}",
		"--max-time", "65",
		url,
	).CombinedOutput()
	if err != nil {
		t.Logf("curl to %s failed: %v\n%s", url, err, out)
		return "000"
	}

	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	return lines[len(lines)-1]
}

// getEPPLogsSince fetches EPP container logs from the given deployment,
// filtered to entries after sinceTime (RFC3339).
func getEPPLogsSince(t *testing.T, deployment, sinceTime string) string {
	t.Helper()

	out, err := exec.Command("kubectl", "logs",
		fmt.Sprintf("deployment/%s", deployment),
		"-n", testNamespace,
		"-c", "epp",
		fmt.Sprintf("--since-time=%s", sinceTime),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl logs for %s failed: %v\n%s", deployment, err, out)
	}
	return string(out)
}

func truncateLog(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

type eppMetricFamily struct {
	prefix                 string
	dispatchedCountPattern *regexp.Regexp
	poolSaturationPattern  *regexp.Regexp
}

var eppMetricFamilies = []eppMetricFamily{
	{
		prefix:                 "llm_d_epp",
		dispatchedCountPattern: regexp.MustCompile(`(?m)^llm_d_epp_flow_control_request_queue_duration_seconds_count\{([^}]*)\}\s+([0-9.e+-]+)$`),
		poolSaturationPattern:  regexp.MustCompile(`(?m)^llm_d_epp_flow_control_pool_saturation\{[^}]*\}\s+([0-9.e+-]+)$`),
	},
	{
		prefix:                 "inference_extension",
		dispatchedCountPattern: regexp.MustCompile(`(?m)^inference_extension_flow_control_request_queue_duration_seconds_count\{([^}]*)\}\s+([0-9.e+-]+)$`),
		poolSaturationPattern:  regexp.MustCompile(`(?m)^inference_extension_flow_control_pool_saturation\{[^}]*\}\s+([0-9.e+-]+)$`),
	},
}

func findEPPDispatchedCountMatches(metrics string) (eppMetricFamily, [][]string) {
	for _, family := range eppMetricFamilies {
		matches := family.dispatchedCountPattern.FindAllStringSubmatch(metrics, -1)
		if len(matches) > 0 {
			return family, matches
		}
	}
	return eppMetricFamily{}, nil
}

func findEPPPoolSaturationMatch(metrics string) ([]string, bool) {
	for _, family := range eppMetricFamilies {
		if match := family.poolSaturationPattern.FindStringSubmatch(metrics); match != nil {
			return match, true
		}
	}
	return nil, false
}

func assertEPPDispatchedDelta(
	t *testing.T,
	deployment string,
	before float64,
	minDelta float64,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var lastCount float64
	var lastSample string
	for time.Now().Before(deadline) {
		lastCount, lastSample = getEPPDispatchedCountAndSample(t, deployment)
		if lastCount-before >= minDelta {
			t.Logf("EPP %s dispatched count advanced from %.0f to %.0f", deployment, before, lastCount)
			return
		}
		time.Sleep(1 * time.Second)
	}

	t.Errorf("EPP %s did not dispatch >= %.0f request(s); before=%.0f after=%.0f\nmetric sample:\n%s",
		deployment, minDelta, before, lastCount, lastSample)
}

func getEPPDispatchedCount(t *testing.T, deployment string) float64 {
	t.Helper()

	count, _ := getEPPDispatchedCountAndSample(t, deployment)
	return count
}

// EPP flow-control outcomes for the batch band, as labelled on the Router
// metric or its legacy GIE equivalent.
const (
	eppOutcomeDispatched       = "Dispatched"
	eppOutcomeEvictedTTL       = "EvictedTTL"       // 503 "request timed out in queue"
	eppOutcomeRejectedCapacity = "RejectedCapacity" // 429, priority band byte budget full
	eppBatchPriority           = "-1"               // the batch-sheddable InferenceObjective
	eppInteractivePriority     = "100"              // the interactive-default InferenceObjective
)

var eppOutcomeLabelPattern = regexp.MustCompile(`outcome="([^"]+)"`)

// getEPPBatchOutcomes returns the EPP's flow-control request counter for the
// batch priority band, keyed by outcome. Interactive traffic (priority 100)
// is excluded so a test can prove that batch requests specifically were
// shed.
func getEPPBatchOutcomes(t *testing.T, deployment string) map[string]float64 {
	t.Helper()

	return getEPPOutcomes(t, deployment, eppBatchPriority)
}

// getEPPOutcomes returns the EPP's flow-control request counter for one
// priority band, keyed by outcome.
func getEPPOutcomes(t *testing.T, deployment, priority string) map[string]float64 {
	t.Helper()

	metrics := scrapeEPPMetrics(t, deployment)
	_, matches := findEPPDispatchedCountMatches(metrics)
	outcomes := make(map[string]float64)
	for _, match := range matches {
		labels := match[1]
		if !strings.Contains(labels, fmt.Sprintf(`priority=%q`, priority)) {
			continue
		}
		outcome := eppOutcomeLabelPattern.FindStringSubmatch(labels)
		if outcome == nil {
			continue
		}
		value, err := strconv.ParseFloat(match[2], 64)
		if err != nil {
			t.Fatalf("failed to parse flow-control count for %s: %v", deployment, err)
		}
		outcomes[outcome[1]] += value
	}
	return outcomes
}

// shedCount sums every batch-band outcome other than Dispatched: the
// requests the EPP answered with 429/503 instead of forwarding.
func shedCount(outcomes map[string]float64) float64 {
	var total float64
	for outcome, count := range outcomes {
		if outcome != eppOutcomeDispatched {
			total += count
		}
	}
	return total
}

// waitForEPPSaturation polls until the EPP's flow-control pool saturation
// gauge exceeds 1 (the pool is past the configured queue-depth threshold).
func waitForEPPSaturation(t *testing.T, deployment string, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last float64
	for time.Now().Before(deadline) {
		if match, ok := findEPPPoolSaturationMatch(scrapeEPPMetrics(t, deployment)); ok {
			value, err := strconv.ParseFloat(match[1], 64)
			if err != nil {
				t.Fatalf("failed to parse pool saturation for %s: %v", deployment, err)
			}
			last = value
			if value > 1 {
				t.Logf("EPP %s pool saturation = %.2f", deployment, value)
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("EPP %s did not report a saturated pool within %v (last=%.2f)", deployment, timeout, last)
}

// waitForEPPShed polls until the EPP has shed at least one batch-band request
// beyond the counts in before.
func waitForEPPShed(t *testing.T, deployment string, before map[string]float64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	var last float64
	for time.Now().Before(deadline) {
		last = shedCount(getEPPBatchOutcomes(t, deployment))
		if last > shedCount(before) {
			t.Logf("EPP %s batch-band shed count advanced from %.0f to %.0f", deployment, shedCount(before), last)
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("EPP %s did not shed any batch request within %v (before=%.0f, last=%.0f)", deployment, timeout, shedCount(before), last)
}

// assertOutputMatchesEPPOutcomes checks that the non-200 lines in a batch's
// output file are exactly the statuses the EPP's batch-band outcome deltas
// account for: 503 only if requests were evicted on TTL, 429 only if
// requests were rejected on band capacity, nothing else, and every failed
// request is one of them. It returns the number of shed lines.
func assertOutputMatchesEPPOutcomes(t *testing.T, batch *openai.Batch, before, after map[string]float64) int {
	t.Helper()

	evicted := after[eppOutcomeEvictedTTL] - before[eppOutcomeEvictedTTL]
	rejected := after[eppOutcomeRejectedCapacity] - before[eppOutcomeRejectedCapacity]
	t.Logf("EPP batch-band deltas: EvictedTTL=%.0f RejectedCapacity=%.0f", evicted, rejected)

	result := fetchOutputFile(t, batch)
	byStatus := make(map[int]int)
	for _, line := range strings.Split(result, "\n") {
		var rl batchResultLine
		if err := json.Unmarshal([]byte(line), &rl); err != nil || rl.Response == nil {
			continue
		}
		byStatus[rl.Response.StatusCode]++
	}
	shed := byStatus[http.StatusServiceUnavailable] + byStatus[http.StatusTooManyRequests]
	for status, n := range byStatus {
		switch status {
		case http.StatusOK:
		case http.StatusServiceUnavailable:
			if evicted == 0 {
				t.Errorf("%d output line(s) with 503 but the EPP evicted no batch request on TTL", n)
			}
		case http.StatusTooManyRequests:
			if rejected == 0 {
				t.Errorf("%d output line(s) with 429 but the EPP rejected no batch request on capacity", n)
			}
		default:
			t.Errorf("%d output line(s) with unexpected status %d", n, status)
		}
	}
	if int64(shed) != batch.RequestCounts.Failed {
		t.Errorf("failed=%d but %d output line(s) carry a shed status (503/429)", batch.RequestCounts.Failed, shed)
	}
	if float64(shed) > evicted+rejected {
		t.Errorf("%d shed lines exceed the EPP's %.0f shed outcomes", shed, evicted+rejected)
	}
	t.Logf("output statuses: %v", byStatus)
	return shed
}

func getEPPDispatchedCountAndSample(t *testing.T, deployment string) (float64, string) {
	t.Helper()

	metrics := scrapeEPPMetrics(t, deployment)
	family, matches := findEPPDispatchedCountMatches(metrics)
	if len(matches) == 0 {
		return 0, truncateLog(metrics, 1000)
	}

	var total float64
	lines := make([]string, 0, len(matches))
	for _, match := range matches {
		labels := match[1]
		if !strings.Contains(labels, `outcome="Dispatched"`) {
			continue
		}
		value, err := strconv.ParseFloat(match[2], 64)
		if err != nil {
			t.Fatalf("failed to parse dispatched count for %s: %v", deployment, err)
		}
		total += value
		lines = append(lines,
			fmt.Sprintf("%s_flow_control_request_queue_duration_seconds_count{%s} %s", family.prefix, labels, match[2]))
	}
	if len(lines) == 0 {
		return 0, truncateLog(metrics, 1000)
	}
	return total, strings.Join(lines, "\n")
}

func scrapeEPPMetrics(t *testing.T, deployment string) string {
	t.Helper()

	ensureE2ECurlPod(t)

	out, err := exec.Command("kubectl", "exec",
		"-n", testNamespace,
		e2eCurlPod,
		"--",
		"curl",
		"-sS",
		fmt.Sprintf("http://%s.%s.svc.cluster.local:9090/metrics", deployment, testNamespace),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("failed to scrape metrics for %s: %v\n%s", deployment, err, out)
	}
	return string(out)
}

// processorObjectiveConfig is the subset of the processor config.yaml that
// decides which inference objective a model gets. The precedence in
// inferenceObjectiveFor mirrors config.ProcessorConfig.InferenceObjectiveFor.
type processorObjectiveConfig struct {
	DispatchMode  string `yaml:"dispatch_mode"`
	AsyncDispatch struct {
		Models map[string]struct {
			InferenceObjective string `yaml:"inference_objective"`
		} `yaml:"models"`
	} `yaml:"async_dispatch"`
	GlobalInferenceGateway *struct {
		InferenceObjective string `yaml:"inference_objective"`
	} `yaml:"global_inference_gateway"`
	ModelGateways map[string]struct {
		InferenceObjective string `yaml:"inference_objective"`
	} `yaml:"model_gateways"`
}

func (c *processorObjectiveConfig) inferenceObjectiveFor(model string) string {
	if c.DispatchMode == "async" {
		return c.AsyncDispatch.Models[model].InferenceObjective
	}
	if c.GlobalInferenceGateway != nil {
		return c.GlobalInferenceGateway.InferenceObjective
	}
	return c.ModelGateways[model].InferenceObjective
}

// getProcessorConfigObjective reads the deployed processor ConfigMap and
// returns the inference objective the processor resolves for the given model.
func getProcessorConfigObjective(t *testing.T, model string) string {
	t.Helper()

	cmName := fmt.Sprintf("%s-processor-config", testHelmRelease)
	cm := kubectlGetConfigMap(t, cmName)

	var cfg processorObjectiveConfig
	if err := yaml.Unmarshal([]byte(cm), &cfg); err != nil {
		t.Fatalf("failed to parse processor ConfigMap %s: %v", cmName, err)
	}
	return cfg.inferenceObjectiveFor(model)
}

// resolveExpectedObjective returns the inference objective value that the
// processor should set for the given model. In per-model EPP mode the objective is
// per-model ("<prefix>-<model>"), otherwise it is the prefix alone.
// TEST_INFERENCE_OBJECTIVE overrides auto-detection.
func resolveExpectedObjective(t *testing.T, model string) string {
	t.Helper()

	if v := getEnvOrDefault("TEST_INFERENCE_OBJECTIVE", ""); v != "" {
		return v
	}
	prefix := getEnvOrDefault("GIE_OBJECTIVE_PREFIX", "batch-sheddable")
	if detectGIEDeployed(t) {
		return prefix + "-" + model
	}
	return prefix
}

// scrapeProcessorMetrics fetches the raw Prometheus text from the processor
// observability endpoint.
func scrapeProcessorMetrics(t *testing.T) string {
	t.Helper()

	_, body, err := readProcessorObsEndpoint(t, "/metrics")
	if err != nil {
		t.Fatalf("failed to scrape processor metrics: %v", err)
	}
	return string(body)
}

// waitForRetryExhaustion polls a batch until it reaches a terminal state.
// Unlike waitForBatchStatus, it skips validateBatchResults because retry
// exhaustion produces 429 responses in the output file which fail the
// standard status_code=200 check.
func waitForRetryExhaustion(t *testing.T, batchID string, timeout time.Duration) *openai.Batch {
	t.Helper()

	client := newClient()
	deadline := time.Now().Add(timeout)
	if d, ok := t.Deadline(); ok && d.Before(deadline) {
		deadline = d.Add(-5 * time.Second)
	}

	for time.Now().Before(deadline) {
		b, err := client.Batches.Get(t.Context(), batchID)
		if err != nil {
			t.Fatalf("retrieve batch failed: %v", err)
		}
		t.Logf("batch %s status: %s (completed=%d, failed=%d)",
			batchID, b.Status, b.RequestCounts.Completed, b.RequestCounts.Failed)

		if terminalBatchStatuses[b.Status] {
			return b
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("batch %s did not reach terminal status within %v", batchID, timeout)
	return nil
}

// getRequestErrors returns the current value of request_errors_by_model_total
// for the given model, or 0 if the metric is not present yet.
func getRequestErrors(t *testing.T, model string) int {
	t.Helper()

	metrics := scrapeProcessorMetrics(t)
	pattern := regexp.MustCompile(fmt.Sprintf(`request_errors_by_model_total\{model=%q\}\s+(\d+)`, model))
	match := pattern.FindStringSubmatch(metrics)
	if match == nil {
		return 0
	}
	val, err := strconv.Atoi(match[1])
	if err != nil {
		t.Fatalf("failed to parse request_errors_by_model_total value %q: %v", match[1], err)
	}
	return val
}

// assertNoNewRequestErrors verifies that request_errors_by_model_total for the
// given model did not increase relative to the provided baseline. This is safe
// to use against long-lived processors where previous test runs may have
// already incremented the counter.
func assertNoNewRequestErrors(t *testing.T, model string, baseline int) {
	t.Helper()

	current := getRequestErrors(t, model)
	if current > baseline {
		t.Errorf("request_errors_by_model_total{model=%q} increased during test "+
			"(before=%d, after=%d); retries should have succeeded transparently",
			model, baseline, current)
	}
}

// assertRequestErrors verifies that request_errors_by_model_total for the
// given model is present and > 0. Used after retry exhaustion to confirm
// that the processor recorded the failures.
func assertRequestErrors(t *testing.T, model string) {
	t.Helper()

	metrics := scrapeProcessorMetrics(t)

	pattern := regexp.MustCompile(fmt.Sprintf(`request_errors_by_model_total\{model=%q\}\s+(\d+)`, model))
	match := pattern.FindStringSubmatch(metrics)
	if match == nil {
		t.Errorf("request_errors_by_model_total{model=%q} not found in metrics, expected > 0", model)
		return
	}
	if match[1] == "0" {
		t.Errorf("expected request_errors_by_model_total{model=%q} > 0, got 0", model)
	}
	t.Logf("request_errors_by_model_total{model=%q} = %s", model, match[1])
}

// getProcessorLogsSince fetches batch-gateway-processor container logs
// filtered to entries after sinceTime (RFC3339Nano).
func getProcessorLogsSince(t *testing.T, sinceTime string) string {
	t.Helper()

	sts := fmt.Sprintf("%s-processor", testHelmRelease)
	out, err := exec.Command("kubectl", "logs",
		fmt.Sprintf("statefulset/%s", sts),
		"-n", testNamespace,
		fmt.Sprintf("--since-time=%s", sinceTime),
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl logs for %s failed: %v\n%s", sts, err, out)
	}
	return string(out)
}
