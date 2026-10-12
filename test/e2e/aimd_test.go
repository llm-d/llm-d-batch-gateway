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

// AIMD e2e tests verify that the adaptive concurrency controller reacts to
// backpressure from downstream inference endpoints and that per-endpoint
// isolation holds.
//
// The backpressure is real: the EPP in front of each model sheds
// batch-sheddable requests (429 on reject, 503 on queue TTL expiry) once its
// saturation detector sees the model server's waiting queue grow past the
// configured threshold. The tests
// provoke that by choking one model's engine through the vllm-vcr control API
// (one running request, multi-second decode) while the other model stays
// healthy. These tests therefore require ENABLE_GIE=true (legacy flag name).
//
// AIMD itself has no independent spec yet, so its gauges (limit, increase and
// decrease counters, per endpoint) are logged for inspection rather than
// asserted. What the tests assert is the mechanism underneath: the EPP shed
// batch requests, and after the engine is released every request completed.
//
// Coverage:
//   - DecreaseAndIsolation: shedding on the choked model with a healthy model
//     in the same batch; logs both endpoints' AIMD state.
//   - Recovery: release the engine, then single-request batches; logs whether
//     the limit climbed back.

package e2e_test

import (
	"fmt"
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

	t.Run("DecreaseAndIsolation", doTestAIMDDecreaseAndIsolation)
	t.Run("Recovery", doTestAIMDRecovery)
}

// saturationRequests is the size of the batch submitted at a saturated
// model: twice the processor's perEndpoint concurrency in per-model EPP mode (20), so
// batch arrivals keep coming as shed requests are retried or give up.
const saturationRequests = 40

// saturationMaxTokens is the max_tokens of every saturating request. With
// chokeEngine's 3s decode step a request occupies the single engine slot for
// 15s, so the interactive workers queued behind it keep the pool saturated
// continuously, for well over the 4 x 30s TTL cycles an exhausting retry
// chain needs.
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

// doTestAIMDDecreaseAndIsolation saturates model B (see submitSaturatingBatch)
// with a couple of requests to model A mixed in, releases the engine once the
// EPP has shed, and waits for the batch to complete. Every shed request was
// retried after a shed response, so its completion is an AIMD capacity_retry
// decrease signal; the resulting AIMD state of both endpoints is logged.
func doTestAIMDDecreaseAndIsolation(t *testing.T) {
	const numHealthyRequests = 2

	metricsBefore := scrapeProcessorMetrics(t)
	decreasesBefore := parseCounterByEndpoint(t, metricsBefore, "batch_processor_aimd_decreases_total")
	limitsBefore := parseGaugeByEndpoint(t, metricsBefore, "batch_processor_aimd_concurrency_limit")

	var decreaseBeforeChoked, limitBeforeHealthy float64
	for endpoint, count := range decreasesBefore {
		if isEPPEndpoint(endpoint, testModelB) {
			decreaseBeforeChoked = count
		}
	}
	for endpoint, limit := range limitsBefore {
		if isEPPEndpoint(endpoint, testModel) {
			limitBeforeHealthy = limit
		}
	}

	healthy := make([]string, 0, numHealthyRequests)
	for i := range numHealthyRequests {
		healthy = append(healthy, chatLine(fmt.Sprintf("aimd-ok-%d", i+1), testModel, fmt.Sprintf("AIMD baseline %d", i+1), saturationMaxTokens))
	}
	batchID, stopLoad := submitSaturatingBatch(t, "aimd-choked", healthy...)

	t.Log("stopping interactive load and releasing the engine so shed requests complete on retry")
	stopLoad()
	if err := releaseEngine(t, testSimServiceB); err != nil {
		t.Fatal(err)
	}

	batch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)
	total := int64(saturationRequests + numHealthyRequests)
	if batch.RequestCounts.Completed != total {
		t.Fatalf("expected %d completed, got completed=%d failed=%d",
			total, batch.RequestCounts.Completed, batch.RequestCounts.Failed)
	}

	logAIMD(t, "after shed batch", decreaseBeforeChoked, limitBeforeHealthy)
}

// logAIMD reports the AIMD gauges and decrease deltas for both EPP endpoints.
// AIMD has no independent spec yet, so these are recorded for inspection and
// not asserted; the test's pass/fail rests on the EPP shed counter, the
// output statuses, and batch completion.
func logAIMD(t *testing.T, phase string, decreaseBeforeChoked, limitBeforeHealthy float64) {
	t.Helper()

	metrics := scrapeProcessorMetrics(t)
	expectedPerEndpoint := getProcessorPerEndpointConcurrency(t)
	aimdLimits := parseGaugeByEndpoint(t, metrics, "batch_processor_aimd_concurrency_limit")
	aimdDecreases := parseCounterByEndpoint(t, metrics, "batch_processor_aimd_decreases_total")
	for endpoint, limit := range aimdLimits {
		switch {
		case isEPPEndpoint(endpoint, testModelB):
			t.Logf("%s: choked endpoint limit=%.0f (perEndpoint=%d) decreases delta=%.0f",
				phase, limit, expectedPerEndpoint, aimdDecreases[endpoint]-decreaseBeforeChoked)
		case isEPPEndpoint(endpoint, testModel):
			t.Logf("%s: healthy endpoint limit=%.0f (before=%.0f, perEndpoint=%d)",
				phase, limit, limitBeforeHealthy, expectedPerEndpoint)
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

// doTestAIMDRecovery verifies that the AIMD concurrency limit recovers after
// backpressure subsides:
//  1. Saturate model B until the EPP sheds, release the engine through the
//     control API (no rollout needed), and let the batch complete; the
//     retried requests drive the limit down as they complete.
//  2. Submit single-request batches to complete several additive-increase
//     windows, then log whether the exported limit rose above its phase-1
//     value and whether new decrease signals appeared.
//
// AIMD increases by +1 per successful window, so a single large recovery batch
// can complete before exported metrics clearly reflect the recovery under CI
// load. Single-request batches make the success windows deterministic.
func doTestAIMDRecovery(t *testing.T) {
	t.Log("phase 1: saturating model B to drive the AIMD limit down...")
	batchID, stopLoad := submitSaturatingBatch(t, "aimd-recov-shed")
	stopLoad()
	if err := releaseEngine(t, testSimServiceB); err != nil {
		t.Fatal(err)
	}
	batch, _ := waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)
	t.Logf("phase 1 batch: completed=%d, failed=%d", batch.RequestCounts.Completed, batch.RequestCounts.Failed)

	expectedPerEndpoint := getProcessorPerEndpointConcurrency(t)
	metrics := scrapeProcessorMetrics(t)
	aimdLimits := parseGaugeByEndpoint(t, metrics, "batch_processor_aimd_concurrency_limit")
	aimdIncreases := parseCounterByEndpoint(t, metrics, "batch_processor_aimd_increases_total")
	aimdDecreases := parseCounterByEndpoint(t, metrics, "batch_processor_aimd_decreases_total")

	var (
		aimdEndpoint      string
		baselineLimit     float64
		baselineIncreases float64
		baselineDecreases float64
	)
	for endpoint, limit := range aimdLimits {
		if isEPPEndpoint(endpoint, testModelB) {
			aimdEndpoint = endpoint
			baselineLimit = limit
			baselineIncreases = aimdIncreases[endpoint]
			baselineDecreases = aimdDecreases[endpoint]
			t.Logf("phase 1: aimd_concurrency_limit{endpoint=%q} = %.0f (perEndpoint=%d)", endpoint, limit, expectedPerEndpoint)
			break
		}
	}
	if aimdEndpoint == "" {
		t.Fatalf("no AIMD metric found for the EPP endpoint of %q", testModelB)
	}

	// A limit of N needs N clean successes to record one additive increase.
	// Send more than one full success window so the exported gauge has time to
	// move above the floor even under CI scrape lag.
	numRecoveryRequests := 2*int(baselineLimit) + 1
	t.Logf("phase 2: submitting %d single-request batches to trigger AIMD recovery...", numRecoveryRequests)
	for i := range numRecoveryRequests {
		recovLine := chatLine(fmt.Sprintf("aimd-recov-ok-%d", i+1), testModelB, fmt.Sprintf("AIMD recover %d", i+1), 2)
		recovFileID := mustCreateFile(t, fmt.Sprintf("aimd-recov-ok-%s-%02d.jsonl", testRunID, i+1), recovLine)
		recovBatchID := mustCreateBatch(t, recovFileID)

		recovBatch, _ := waitForBatchStatus(t, recovBatchID, 5*time.Minute, openai.BatchStatusCompleted)
		if recovBatch.RequestCounts.Completed != 1 || recovBatch.RequestCounts.Failed != 0 {
			t.Fatalf("expected recovery batch %d to complete cleanly, got completed=%d failed=%d",
				i+1, recovBatch.RequestCounts.Completed, recovBatch.RequestCounts.Failed)
		}
	}

	limit, increases, decreases, recovered := waitForAIMDRecovery(
		t,
		aimdEndpoint,
		baselineLimit,
		baselineIncreases,
		baselineDecreases,
		60*time.Second,
		500*time.Millisecond,
	)
	t.Logf("phase 2: aimd_concurrency_limit{endpoint=%q} = %.0f (recovered above %.0f: %v)", aimdEndpoint, limit, baselineLimit, recovered)
	t.Logf("aimd_increases_total{endpoint=%q} = %.0f", aimdEndpoint, increases)
	t.Logf("aimd_decreases_total{endpoint=%q} = %.0f", aimdEndpoint, decreases)
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
