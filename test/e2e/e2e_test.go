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
	"crypto/tls"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3/shared"
)

var (
	testApiserverURL      = getEnvOrDefault("TEST_APISERVER_URL", "https://localhost:8000")
	testApiserverObsURL   = getEnvOrDefault("TEST_APISERVER_OBS_URL", "http://localhost:8081")
	testProcessorObsURL   = getEnvOrDefault("TEST_PROCESSOR_OBS_URL", "")
	testJaegerURL         = getEnvOrDefault("TEST_JAEGER_URL", "http://localhost:16686")
	testAPIKey            = getEnvOrDefault("TEST_API_KEY", "unused")
	testTenantHeader      = getEnvOrDefault("TEST_TENANT_HEADER", "X-MaaS-Username")
	testTenantID          = getEnvOrDefault("TEST_TENANT_ID", "default")
	testNamespace         = getEnvOrDefault("TEST_NAMESPACE", "default")
	testHelmRelease       = getEnvOrDefault("TEST_HELM_RELEASE", "batch-gateway")
	testPostgresqlRelease = getEnvOrDefault("TEST_POSTGRESQL_RELEASE", "postgresql")
	testRedisRelease      = getEnvOrDefault("TEST_REDIS_RELEASE", "redis")
	testDBPod             = getEnvOrDefault("TEST_DB_POD", "")
	testDBNamespace       = getEnvOrDefault("TEST_DB_NAMESPACE", "")
	testDBName            = getEnvOrDefault("TEST_DB_NAME", "postgres")
	testDBUser            = getEnvOrDefault("TEST_DB_USER", "postgres")
	testEPPNamespace      = getEnvOrDefault("TEST_EPP_NAMESPACE", "")

	// testDBClientType and testExchangeClientType are detected from Helm
	// releases at startup; see detectDBClientType / detectExchangeClientType.
	testDBClientType       string
	testExchangeClientType string

	testRunID = fmt.Sprintf("%d", time.Now().UnixNano())

	// testModel is the model name used in batch input; configurable via TEST_MODEL env var.
	testModel  = getEnvOrDefault("TEST_MODEL", "sim-model")
	testModelB = getEnvOrDefault("TEST_MODEL_B", "sim-model-b")
	// testSimModel is the model name used in timing-dependent tests that require
	// controlled latency (cancel, expiration, progress polling, orphan recovery,
	// graceful shutdown). It should always point to a simulator with predictable
	// inter-token latency. Defaults to testModel for dev-deploy where all models
	// are simulated.
	testSimModel = getEnvOrDefault("TEST_SIM_MODEL", testModel)

	// testSimService* hold the Kubernetes service names for the vllm-vcr
	// simulators. Must match VLLM_SIM_*_NAME in dev-common.sh (overridable via
	// env). testSimControlPort is the vllm-vcr control API port on each service.
	testSimService     = getEnvOrDefault("TEST_SIM_SERVICE", "vllm-sim")
	testSimServiceB    = getEnvOrDefault("TEST_SIM_SERVICE_B", "vllm-sim-b")
	testSimControlPort = getEnvOrDefault("TEST_SIM_CONTROL_PORT", "8001")

	// testJSONL is a valid batch input file with two requests.
	// max_tokens is kept small so batches finish quickly. Default TEST_MODEL (sim-model)
	// on dev-deploy uses ~100ms inter-token latency (sim-model-b uses ~500ms).
	testJSONL = strings.Join([]string{
		fmt.Sprintf(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello"}]}}`, testModel),
		fmt.Sprintf(`{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"World"}]}}`, testModel),
	}, "\n")

	// testHTTPClient is used for direct HTTP calls; skips TLS verification for self-signed certs.
	testHTTPClient = &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // dev/test only
		},
		Timeout: 10 * time.Second,
	}

	// testKubectlAvailable is set once at TestE2E startup; when false,
	// verifications that require kubectl (e.g. log grepping) are skipped.
	testKubectlAvailable bool

	// testDispatcherDeployed is set once at TestE2E startup; when true,
	// subtests incompatible with async dispatch mode are skipped.
	testDispatcherDeployed bool

	// testPassThroughHeaders maps header names (matching apiserver pass_through_headers
	// configured by dev-deploy.sh) to the values the e2e client sends when asserting
	// pass-through behavior.
	testPassThroughHeaders = map[string]string{
		"X-E2E-Pass-Through-1": "test-value-1",
		"X-E2E-Pass-Through-2": "test-value-2",
	}

	// testBatchMetadata is attached to every batch created via mustCreateBatch
	// so that metadata round-tripping is verified as part of the lifecycle test.
	testBatchMetadata = shared.Metadata{
		"env":    "e2e-test",
		"run_id": testRunID,
	}
)

func TestE2E(t *testing.T) {
	testDispatcherDeployed = detectDispatcherDeployed(t)

	if out, err := exec.Command("kubectl", "cluster-info").CombinedOutput(); err != nil {
		t.Logf("kubectl not available, some checks will be skipped: %v\n%s", err, out)
	} else {
		testKubectlAvailable = true
	}

	testDBClientType = detectDBClientType(t)
	testExchangeClientType = detectExchangeClientType(t)
	t.Logf("DB client type: %s, exchange client type: %s", testDBClientType, testExchangeClientType)

	waitForServerUp(t, testApiserverURL, 30*time.Second)

	if resp, err := http.Get(testApiserverObsURL + "/ready"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			waitForReady(t, testApiserverObsURL, 30*time.Second)
		}
	}

	t.Run("Files", testFiles)
	t.Run("Batches", testBatches)
	t.Run("Concurrent", testConcurrent)
	t.Run("MultiTenant", testMultiTenant)
	t.Run("GarbageCollection", testGarbageCollection)
	t.Run("SchemaMigrations", testSchemaMigrations)
	t.Run("Observability", testObservability)
	skipIf(t, testDispatcherDeployed, "requires sync processor", "ProcessorGracefulShutdown", testProcessorGracefulShutdown)
	skipIf(t, testDispatcherDeployed, "requires sync processor", "OrphanRecovery", testOrphanRecovery)
	skipIf(t, testDispatcherDeployed, "requires sync processor", "FlowControl", testFlowControl)
	skipIf(t, testDispatcherDeployed, "requires sync processor", "AIMD", testAIMD)
	skipIf(t, testDispatcherDeployed, "requires sync processor", "HelmUpgrade", testHelmUpgrade)
}
