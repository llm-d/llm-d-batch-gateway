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

// AIMD e2e tests verify endpoint isolation and concurrency recovery under real
// backpressure. TTL shedding covers request outcomes; Recovery uses
// model-server 5xx injection and asserts AIMD metric changes.

package e2e_test

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
	"gopkg.in/yaml.v3"
)

func testAIMD(t *testing.T) {
	if !testKubectlAvailable {
		t.Skip("kubectl not available")
	}
	if !detectGIEDeployed(t) {
		t.Skip("EPP not deployed (deploy with ENABLE_GIE=true; legacy flag name); backpressure comes from EPP shedding")
	}
	t.Cleanup(func() { deleteE2ECurlPod(t) })

	t.Run("TTLSheddingAndIsolation", doTestAIMDTTLSheddingAndIsolation)
	t.Run("Recovery", doTestAIMDRecovery)
}

// saturationRequests is twice the processor's perEndpoint concurrency. It
// keeps enough batch requests in flight to trigger EPP TTL eviction.
const saturationRequests = 40

// saturationMaxTokens is the max_tokens of every saturating request. With
// chokeEngine's 3s decode step a request occupies the single engine slot for
// 15s, so the interactive workers queued behind it keep the pool saturated
// continuously until the EPP's 30s queue TTL evicts batch requests.
const saturationMaxTokens = 5

// chokeEngine caps the simulator's engine at one running request with a
// three-second decode step and registers a cleanup that restores model B's
// dev-deploy latency (200ms TTFT, 500ms ITL). While choked, every additional
// concurrent request waits in the model server's queue, which is what the
// EPP's saturation detector reads.
func chokeEngine(t *testing.T, simService string) {
	t.Helper()

	patchEngineConfig(t, simService, `{"max_num_seqs": 1, "time_to_first_token": 0, "inter_token_latency": 3000}`)
	t.Cleanup(func() {
		if err := releaseEngine(t, simService); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
}

// eppDeploymentFor returns the per-model EPP deployment name.
func eppDeploymentFor(model string) string {
	return fmt.Sprintf("%s-%s-epp", getEnvOrDefault("GIE_EPP_RELEASE", "epp"), model)
}

// interactiveLoadConcurrency is how many non-sheddable interactive requests
// submitSaturatingBatch keeps in flight at model B's EPP. Batch traffic on
// its own gives the EPP a thin saturation margin once AIMD has backed the
// processor off to its floor; a stream of priority-100 traffic keeps the
// choked model's queue full and dispatched ahead of batch, so batch requests
// are the ones evicted whatever the processor's current limit is.
const interactiveLoadConcurrency = 10

// submitSaturatingBatch chokes model B, fills it with interactive load until
// the EPP reports the pool saturated, then submits saturationRequests requests
// to it (plus extra lines, if any) and blocks until the EPP has shed at least
// one request. Every batch request arrives at an EPP that is already
// saturated, so it is held behind the interactive queue and evicted at the
// TTL whatever the processor's current AIMD limit is. It returns the batch id
// and a function that stops the interactive load; the engine is still choked
// and the caller decides when to stop the load and release it.
func submitSaturatingBatch(t *testing.T, prefix string, extra ...string) (string, func()) {
	t.Helper()

	eppDeployment := eppDeploymentFor(testModelB)
	shedBefore := getEPPBatchOutcomes(t, eppDeployment)

	chokeEngine(t, testSimServiceB)
	stopLoad := startInteractiveLoad(t, testModelB, interactiveLoadConcurrency)
	waitForEPPSaturation(t, eppDeployment, 60*time.Second)

	lines := make([]string, 0, saturationRequests+len(extra))
	for i := range saturationRequests {
		lines = append(lines, chatLine(fmt.Sprintf("%s-%d", prefix, i+1), testModelB, fmt.Sprintf("%s %d", prefix, i+1), saturationMaxTokens))
	}
	lines = append(lines, extra...)

	fileID := mustCreateFile(t, fmt.Sprintf("%s-%s.jsonl", prefix, testRunID), strings.Join(lines, "\n"))
	batchID := mustCreateBatch(t, fileID)

	// The first eviction lands one 30s TTL after the first batch request is
	// held.
	waitForEPPShed(t, eppDeployment, shedBefore, 120*time.Second)
	return batchID, stopLoad
}

// releaseEngine undoes chokeEngine.
func releaseEngine(t *testing.T, simService string) error {
	t.Helper()

	return tryPatchEngineConfig(t, simService, `{"max_num_seqs": 128, "time_to_first_token": 200, "inter_token_latency": 500}`)
}

// isEPPEndpoint reports whether an AIMD metric endpoint label is the per-model
// EPP service for the given model (epp-<model>-epp.<ns>...).
func isEPPEndpoint(endpoint, model string) bool {
	return strings.Contains(endpoint, fmt.Sprintf("-%s-epp.", model))
}

// chatLine formats one JSONL batch input line.
func chatLine(customID, model, content string, maxTokens int) string {
	return fmt.Sprintf(
		`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"max_tokens":%d,"messages":[{"role":"user","content":%q}]}}`,
		customID, model, maxTokens, content,
	)
}

// doTestAIMDTTLSheddingAndIsolation verifies that healthy-model requests
// complete while choked-model requests may be recorded as batch_expired errors.
func doTestAIMDTTLSheddingAndIsolation(t *testing.T) {
	const numHealthyRequests = 2

	healthy := make([]string, 0, numHealthyRequests)
	for i := range numHealthyRequests {
		healthy = append(healthy, chatLine(fmt.Sprintf("aimd-ok-%d", i+1), testModel, fmt.Sprintf("AIMD baseline %d", i+1), saturationMaxTokens))
	}
	batchID, stopLoad := submitSaturatingBatch(t, "aimd-choked", healthy...)

	t.Log("stopping interactive load and releasing the engine")
	stopLoad()
	if err := releaseEngine(t, testSimServiceB); err != nil {
		t.Fatal(err)
	}

	batch, results := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)
	total := int64(saturationRequests + numHealthyRequests)
	if got := batch.RequestCounts.Completed + batch.RequestCounts.Failed; got != total {
		t.Fatalf("expected completed+failed = %d, got %d", total, got)
	}
	if batch.RequestCounts.Failed == 0 {
		t.Error("expected at least one TTL-evicted request, got failed=0")
	}
	if got := countBatchExpiredErrors(t, results.ErrorBody); got != batch.RequestCounts.Failed {
		t.Errorf("expected %d batch_expired error line(s), got %d", batch.RequestCounts.Failed, got)
	}
	outputCustomIDs := resultCustomIDs(t, results.OutputBody)
	for i := range numHealthyRequests {
		customID := fmt.Sprintf("aimd-ok-%d", i+1)
		if !outputCustomIDs[customID] {
			t.Errorf("healthy request %q missing from output file", customID)
		}
	}
}

// assertAIMDIsolation reports AIMD state and verifies the healthy endpoint was
// not reduced while the other endpoint was under AIMD decrease pressure.
func assertAIMDIsolation(t *testing.T, phase string, limitBeforeHealthy float64) {
	t.Helper()

	metrics := scrapeProcessorMetrics(t)
	expectedPerEndpoint := getProcessorPerEndpointConcurrency(t)
	aimdLimits := parseGaugeByEndpoint(t, metrics, "batch_processor_aimd_concurrency_limit")
	for endpoint, limit := range aimdLimits {
		switch {
		case isEPPEndpoint(endpoint, testModelB):
			t.Logf("%s: choked endpoint limit=%.0f (perEndpoint=%d)", phase, limit, expectedPerEndpoint)
		case isEPPEndpoint(endpoint, testModel):
			t.Logf("%s: healthy endpoint limit=%.0f (before=%.0f, perEndpoint=%d)",
				phase, limit, limitBeforeHealthy, expectedPerEndpoint)
			if limit < limitBeforeHealthy {
				t.Errorf("healthy endpoint AIMD limit dropped from %.0f to %.0f; isolation violated", limitBeforeHealthy, limit)
			}
		}
	}
}

// getProcessorPerEndpointConcurrency reads the deployed processor ConfigMap and
// returns the configured concurrency.per_endpoint value.
func getProcessorPerEndpointConcurrency(t *testing.T) int {
	t.Helper()

	cmName := fmt.Sprintf("%s-processor-config", testHelmRelease)
	configYAML := kubectlGetConfigMap(t, cmName)

	var root struct {
		Concurrency struct {
			PerEndpoint *int `yaml:"per_endpoint"`
		} `yaml:"concurrency"`
	}
	if err := yaml.Unmarshal([]byte(configYAML), &root); err != nil {
		t.Fatalf("parse processor config.yaml: %v", err)
	}
	if root.Concurrency.PerEndpoint == nil {
		t.Fatalf("concurrency.per_endpoint missing in config:\n%s", configYAML)
	}

	return *root.Concurrency.PerEndpoint
}

// getProcessorAIMDSettings reads the deployed processor ConfigMap and returns
// the configured AIMD minimum limit and backoff factor.
func getProcessorAIMDSettings(t *testing.T) (int, float64) {
	t.Helper()

	cmName := fmt.Sprintf("%s-processor-config", testHelmRelease)
	configYAML := kubectlGetConfigMap(t, cmName)

	var root struct {
		Concurrency struct {
			AIMD struct {
				Min           *int     `yaml:"min"`
				BackoffFactor *float64 `yaml:"backoff_factor"`
			} `yaml:"aimd"`
		} `yaml:"concurrency"`
	}
	if err := yaml.Unmarshal([]byte(configYAML), &root); err != nil {
		t.Fatalf("parse processor config.yaml: %v", err)
	}
	if root.Concurrency.AIMD.Min == nil {
		t.Fatalf("concurrency.aimd.min missing in config:\n%s", configYAML)
	}
	if root.Concurrency.AIMD.BackoffFactor == nil {
		t.Fatalf("concurrency.aimd.backoff_factor missing in config:\n%s", configYAML)
	}

	return *root.Concurrency.AIMD.Min, *root.Concurrency.AIMD.BackoffFactor
}

// parseGaugeByEndpoint extracts all {endpoint="..."} values for a gauge metric.
// Returns a map from endpoint label to the gauge value.
func parseGaugeByEndpoint(t *testing.T, metrics, metricName string) map[string]float64 {
	t.Helper()

	pattern := regexp.MustCompile(fmt.Sprintf(`%s\{[^}]*endpoint="([^"]+)"[^}]*\}\s+([0-9.e+-]+)`, regexp.QuoteMeta(metricName)))
	result := make(map[string]float64)
	for _, match := range pattern.FindAllStringSubmatch(metrics, -1) {
		val, err := strconv.ParseFloat(match[2], 64)
		if err != nil {
			t.Logf("failed to parse %s value %q: %v", metricName, match[2], err)
			continue
		}
		result[match[1]] = val
	}
	return result
}

// parseCounterByEndpoint sums counter values across signal labels for each endpoint.
// For counters like aimd_decreases_total{endpoint="...",signal="..."}, this
// returns the total across all signals per endpoint.
func parseCounterByEndpoint(t *testing.T, metrics, metricName string) map[string]float64 {
	t.Helper()

	pattern := regexp.MustCompile(fmt.Sprintf(`%s\{[^}]*endpoint="([^"]+)"[^}]*\}\s+([0-9.e+-]+)`, regexp.QuoteMeta(metricName)))
	result := make(map[string]float64)
	for _, match := range pattern.FindAllStringSubmatch(metrics, -1) {
		val, err := strconv.ParseFloat(match[2], 64)
		if err != nil {
			t.Logf("failed to parse %s value %q: %v", metricName, match[2], err)
			continue
		}
		result[match[1]] += val
	}
	return result
}

// doTestAIMDRecovery verifies that the AIMD concurrency limit decreases on a
// retryable 5xx response and rises again after the backend recovers.
func doTestAIMDRecovery(t *testing.T) {
	t.Log("phase 1: injecting model B failures to lower the AIMD limit")

	expectedPerEndpoint := getProcessorPerEndpointConcurrency(t)
	metricsBefore := scrapeProcessorMetrics(t)
	limitsBefore := parseGaugeByEndpoint(t, metricsBefore, "batch_processor_aimd_concurrency_limit")
	decreasesBefore := parseCounterByEndpoint(t, metricsBefore, "batch_processor_aimd_decreases_total")

	var aimdEndpoint string
	var baselineLimit, baselineDecreases, limitBeforeHealthy float64
	var foundHealthy bool
	for endpoint, limit := range limitsBefore {
		if isEPPEndpoint(endpoint, testModelB) {
			aimdEndpoint = endpoint
			baselineLimit = limit
			baselineDecreases = decreasesBefore[endpoint]
		}
		if isEPPEndpoint(endpoint, testModel) {
			limitBeforeHealthy = limit
			foundHealthy = true
		}
	}
	if aimdEndpoint == "" {
		t.Fatalf("no AIMD metric found for the EPP endpoint of %q", testModelB)
	}
	if !foundHealthy {
		t.Fatalf("no AIMD metric found for the EPP endpoint of %q", testModel)
	}

	aimdMin, aimdBackoffFactor := getProcessorAIMDSettings(t)
	if baselineLimit <= float64(aimdMin) {
		t.Logf("AIMD limit is at its minimum (%.0f); submitting successful batches to establish a recoverable baseline", baselineLimit)
		submitSuccessfulAIMDBatches(t, "aimd-recov-baseline", aimdMin)
		metricsBefore = scrapeProcessorMetrics(t)
		limitsBefore = parseGaugeByEndpoint(t, metricsBefore, "batch_processor_aimd_concurrency_limit")
		decreasesBefore = parseCounterByEndpoint(t, metricsBefore, "batch_processor_aimd_decreases_total")
		baselineLimit = limitsBefore[aimdEndpoint]
		baselineDecreases = decreasesBefore[aimdEndpoint]
		if baselineLimit <= float64(aimdMin) {
			t.Fatalf("AIMD limit for endpoint %q did not rise above minimum %.0f, got %.0f", aimdEndpoint, float64(aimdMin), baselineLimit)
		}
	}

	patchEngineConfig(t, testSimServiceB, `{"failure_injection_rate":1.0,"failure_types":["error"]}`)
	t.Cleanup(func() {
		if err := tryPatchEngineConfig(t, testSimServiceB, `{"failure_injection_rate":0.0}`); err != nil {
			t.Errorf("cleanup: disable failure injection: %v", err)
		}
	})

	fileID := mustCreateFile(t, fmt.Sprintf("aimd-recov-fail-%s.jsonl", testRunID),
		chatLine("aimd-recov-fail", testModelB, "AIMD recovery failure", 2))
	batchID := mustCreateBatch(t, fileID)
	batch := waitForTerminalBatch(t, batchID, 2*time.Minute)
	if batch.Status != openai.BatchStatusCompleted {
		t.Fatalf("expected failure-injection batch to complete, got status %q", batch.Status)
	}
	results := fetchBatchResults(t, batch)
	if status := outputResponseStatus(t, results.OutputBody); status < http.StatusInternalServerError {
		t.Fatalf("expected retryable 5xx response, got status %d", status)
	}

	expectedDecreaseLimit := math.Min(
		float64(expectedPerEndpoint),
		math.Max(float64(aimdMin), math.Floor(baselineLimit*aimdBackoffFactor)),
	)
	recoveryBaselineLimit, decreasesAfterFailure := waitForAIMDDecreaseSignal(t, aimdEndpoint, expectedDecreaseLimit, baselineDecreases, 30*time.Second)
	if decreasesAfterFailure <= baselineDecreases {
		t.Fatalf("expected AIMD decrease counter above %.0f, got %.0f", baselineDecreases, decreasesAfterFailure)
	}
	t.Logf("phase 1: aimd_concurrency_limit{endpoint=%q} = %.0f (before=%.0f, perEndpoint=%d)", aimdEndpoint, recoveryBaselineLimit, baselineLimit, expectedPerEndpoint)
	assertAIMDIsolation(t, "after injected failure", limitBeforeHealthy)

	patchEngineConfig(t, testSimServiceB, `{"failure_injection_rate":0.0}`)

	numRecoveryRequests := 2*int(recoveryBaselineLimit) + 1
	t.Logf("phase 2: submitting %d single-request batches to trigger AIMD recovery...", numRecoveryRequests)
	submitSuccessfulAIMDBatches(t, "aimd-recov-ok", numRecoveryRequests)

	increasesBefore := parseCounterByEndpoint(t, scrapeProcessorMetrics(t), "batch_processor_aimd_increases_total")[aimdEndpoint]
	limit, increases, decreases, recovered := waitForAIMDRecovery(
		t,
		aimdEndpoint,
		recoveryBaselineLimit,
		increasesBefore,
		decreasesAfterFailure,
		60*time.Second,
		500*time.Millisecond,
	)
	if !recovered {
		t.Fatalf("AIMD limit did not recover above %.0f, got %.0f", recoveryBaselineLimit, limit)
	}
	t.Logf("phase 2: aimd_concurrency_limit{endpoint=%q} = %.0f (perEndpoint=%d)", aimdEndpoint, limit, expectedPerEndpoint)
	t.Logf("aimd_increases_total{endpoint=%q} = %.0f", aimdEndpoint, increases)
	t.Logf("aimd_decreases_total{endpoint=%q} = %.0f", aimdEndpoint, decreases)
}

func submitSuccessfulAIMDBatches(t *testing.T, prefix string, count int) {
	t.Helper()

	for i := range count {
		recovLine := chatLine(fmt.Sprintf("%s-%d", prefix, i+1), testModelB, fmt.Sprintf("%s %d", prefix, i+1), 2)
		recovFileID := mustCreateFile(t, fmt.Sprintf("%s-%s-%02d.jsonl", prefix, testRunID, i+1), recovLine)
		recovBatchID := mustCreateBatch(t, recovFileID)

		recovBatch, _ := waitForBatchStatus(t, recovBatchID, 5*time.Minute, openai.BatchStatusCompleted)
		if recovBatch.RequestCounts.Completed != 1 || recovBatch.RequestCounts.Failed != 0 {
			t.Fatalf("expected successful AIMD batch %d to complete cleanly, got completed=%d failed=%d",
				i+1, recovBatch.RequestCounts.Completed, recovBatch.RequestCounts.Failed)
		}
	}
}

// outputResponseStatus returns the status code from a one-line output file.
func outputResponseStatus(t *testing.T, body string) int {
	t.Helper()

	lines := strings.Split(strings.TrimSpace(body), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected one output line, got %d", len(lines))
	}

	var result batchResultLine
	if err := json.Unmarshal([]byte(lines[0]), &result); err != nil {
		t.Fatalf("parse output line: %v", err)
	}
	if result.Response == nil {
		t.Fatal("output line has no response")
	}
	return result.Response.StatusCode
}

func waitForAIMDDecreaseSignal(
	t *testing.T,
	endpoint string,
	expectedLimit, baselineDecreases float64,
	timeout time.Duration,
) (limit, decreases float64) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		metrics := scrapeProcessorMetrics(t)
		currentLimit := parseGaugeByEndpoint(t, metrics, "batch_processor_aimd_concurrency_limit")[endpoint]
		currentDecreases := parseCounterByEndpoint(t, metrics, "batch_processor_aimd_decreases_total")[endpoint]
		if currentDecreases > baselineDecreases && currentLimit == expectedLimit {
			return currentLimit, currentDecreases
		}
		if time.Now().After(deadline) {
			t.Fatalf("AIMD limit did not reach expected %.0f within %v (after=%.0f, decreases=%.0f, baseline decreases=%.0f)",
				expectedLimit, timeout, currentLimit, currentDecreases, baselineDecreases)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func waitForAIMDRecovery(
	t *testing.T,
	endpoint string,
	baselineLimit, baselineIncreases, baselineDecreases float64,
	timeout, interval time.Duration,
) (limit, increases, decreases float64, recovered bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	limit, increases, decreases = baselineLimit, baselineIncreases, baselineDecreases

	for {
		metrics := scrapeProcessorMetrics(t)
		if v, ok := parseGaugeByEndpoint(t, metrics, "batch_processor_aimd_concurrency_limit")[endpoint]; ok {
			limit = v
		}
		if v, ok := parseCounterByEndpoint(t, metrics, "batch_processor_aimd_increases_total")[endpoint]; ok {
			increases = v
		}
		if v, ok := parseCounterByEndpoint(t, metrics, "batch_processor_aimd_decreases_total")[endpoint]; ok {
			decreases = v
		}
		if decreases > baselineDecreases {
			t.Logf("AIMD recorded new decreases during recovery (baseline=%.0f, now=%.0f)", baselineDecreases, decreases)
		}
		if limit > baselineLimit {
			return limit, increases, decreases, true
		}
		if time.Now().After(deadline) {
			return limit, increases, decreases, false
		}
		time.Sleep(interval)
	}
}
