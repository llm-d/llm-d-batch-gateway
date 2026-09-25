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
	"io"
	"net/http"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/producer"
	"github.com/openai/openai-go/v3"
	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"
)

var (
	testRedisURL          = getEnvOrDefault("TEST_REDIS_URL", "redis://localhost:6399")
	testSimURL            = getEnvOrDefault("TEST_SIM_URL", "http://localhost:8099")
	dispatcherPool        = "sim-pool"
	dispatcherReqQueue    = "llm-d-async:requests:" + dispatcherPool
	dispatcherResultQueue = "llm-d-async:results:" + dispatcherPool

	gatePool              = "sim-pool-gate"
	gateReqQueue          = "llm-d-async:requests:" + gatePool
	gateResultQueue       = "llm-d-async:results:" + gatePool
	gateModel             = "sim-model-gate"
	dispatchGateBudgetKey = "dispatch-gate-budget"

	scrapePool        = "sim-pool-scrape"
	scrapeReqQueue    = "llm-d-async:requests:" + scrapePool
	scrapeResultQueue = "llm-d-async:results:" + scrapePool

	promPool        = "sim-pool-prom"
	promReqQueue    = "llm-d-async:requests:" + promPool
	promResultQueue = "llm-d-async:results:" + promPool

	injectPool        = "sim-pool-inject"
	injectReqQueue    = "llm-d-async:requests:" + injectPool
	injectResultQueue = "llm-d-async:results:" + injectPool
	injectModel       = "sim-model-inject"
)

func newDispatcherRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	opts, err := redis.ParseURL(testRedisURL)
	if err != nil {
		t.Fatalf("Failed to parse TEST_REDIS_URL %q: %v", testRedisURL, err)
	}
	client := redis.NewClient(opts)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to Redis at %s: %v", testRedisURL, err)
	}
	return client
}

func newDispatcherProducer(t *testing.T, rdb *redis.Client, poolName string) *producer.RedisSortedSetProducer {
	t.Helper()
	p, err := producer.NewRedisSortedSetProducer(
		producer.RedisSortedSetConfig{
			RequestQueueName: "llm-d-async:requests:" + poolName,
			ResultQueueName:  "llm-d-async:results:" + poolName,
		},
		producer.WithRedisClient(rdb),
	)
	if err != nil {
		t.Fatalf("Failed to create producer for pool %s: %v", poolName, err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// detectDispatcherDeployed checks the processor's active dispatch mode. This
// avoids treating stale llm-d-async deployments as evidence that the processor
// is still configured for async dispatch.
func detectDispatcherDeployed(t *testing.T) bool {
	t.Helper()

	configMap := fmt.Sprintf("%s-processor-config", testHelmRelease)
	out, err := exec.Command("kubectl", "get", "configmap", configMap,
		"-n", testNamespace,
		"-o", "jsonpath={.data.config\\.yaml}",
	).CombinedOutput()
	if err != nil {
		t.Logf("kubectl get processor config failed: %v\n%s", err, out)
		return false
	}
	dispatchMode, err := processorDispatchMode(out)
	if err != nil {
		t.Logf("parse processor config failed: %v", err)
		return false
	}
	return dispatchMode == "async"
}

func processorDispatchMode(data []byte) (string, error) {
	var cfg struct {
		DispatchMode string `yaml:"dispatch_mode"`
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return "", err
	}
	return cfg.DispatchMode, nil
}

func TestProcessorDispatchMode(t *testing.T) {
	tests := []struct {
		name string
		data string
		want string
	}{
		{name: "async", data: `dispatch_mode: "async"`, want: "async"},
		{name: "sync", data: `dispatch_mode: "sync"`, want: "sync"},
		{name: "omitted", data: `poll_interval: "5s"`, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := processorDispatchMode([]byte(tt.data))
			if err != nil {
				t.Fatalf("processorDispatchMode() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("processorDispatchMode() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestDispatcher(t *testing.T) {
	if !detectDispatcherDeployed(t) {
		t.Skip("skipping: dispatcher not deployed")
	}
	rdb := newDispatcherRedisClient(t)
	defer rdb.Close()

	waitForServerUp(t, testApiserverURL, 30*time.Second)

	if resp, err := http.Get(testApiserverObsURL + "/ready"); err == nil {
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			waitForReady(t, testApiserverObsURL, 30*time.Second)
		}
	}

	t.Cleanup(func() {
		ctx := context.Background()
		rdb.Del(ctx, dispatcherReqQueue, dispatcherResultQueue)
		rdb.Del(ctx, gateReqQueue, gateResultQueue)
		rdb.Del(ctx, scrapeReqQueue, scrapeResultQueue)
		rdb.Del(ctx, promReqQueue, promResultQueue)
	})

	t.Run("BatchThroughDispatcher", func(t *testing.T) {
		testDispatcherBatchRoundTrip(t, rdb)
	})
	t.Run("MultiRequestBatch", func(t *testing.T) {
		testDispatcherMultiRequestBatch(t, rdb)
	})
	t.Run("MultiReplicaBatch", func(t *testing.T) {
		testDispatcherMultiReplicaBatch(t)
	})
	t.Run("HTTPErrorStatusPreserved", func(t *testing.T) {
		testDispatcherHTTPErrorStatusPreserved(t, rdb)
	})
	t.Run("BatchCancel", func(t *testing.T) {
		doTestBatchCancel(t)
	})
	t.Run("DispatchGate", func(t *testing.T) {
		testDispatcherRedisGate(t, rdb)
	})
	t.Run("BatchAPIDispatchGate", func(t *testing.T) {
		testBatchAPIDispatchGate(t, rdb)
	})
	t.Run("BatchAPIHardKillRecovery", func(t *testing.T) {
		testBatchAPIHardKillRecovery(t, rdb)
	})
	t.Run("EndpointScrapeGate", func(t *testing.T) {
		testDispatcherEndpointScrapeGate(t, rdb)
	})
	t.Run("PrometheusGate", func(t *testing.T) {
		testDispatcherPrometheusGate(t, rdb)
	})
}

// testDispatcherHTTPErrorStatusPreserved verifies that an HTTP error status
// from the async ResultMessage (e.g. 403 with an empty body) is preserved in
// the batch output file instead of being collapsed into parse_error.
//
// Uses the consumer-less "sim-pool-inject" pool: no llm-d-async subscribes
// to it, so the request stays in the queue deterministically until the test
// removes it and injects a synthetic ResultMessage.
func testDispatcherHTTPErrorStatusPreserved(t *testing.T, rdb *redis.Client) {
	ctx := context.Background()

	rdb.Del(ctx, injectReqQueue, injectResultQueue)
	defer rdb.Del(ctx, injectReqQueue, injectResultQueue)

	jsonl := fmt.Sprintf(
		`{"custom_id":"dreq-403","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"expect 403"}]}}`,
		injectModel)
	fileID := mustCreateFile(t, fmt.Sprintf("dispatcher-http-error-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)
	t.Logf("Created batch %s; waiting for request in %s", batchID, injectReqQueue)

	reqID, resultQueue := waitAndStealQueuedRequest(t, rdb, injectReqQueue, 30*time.Second)
	defer rdb.Del(ctx, resultQueue)
	t.Logf("Got request %s; injecting StatusCode=403 result", reqID)

	resultBytes, err := json.Marshal(asyncapi.ResultMessage{
		ID:         reqID,
		StatusCode: http.StatusForbidden,
		Payload:    "",
	})
	if err != nil {
		t.Fatalf("marshal ResultMessage: %v", err)
	}
	if err := rdb.LPush(ctx, resultQueue, string(resultBytes)).Err(); err != nil {
		t.Fatalf("LPUSH result: %v", err)
	}

	finalBatch := waitForRetryExhaustion(t, batchID, 2*time.Minute)

	if finalBatch.Status != openai.BatchStatusCompleted {
		t.Errorf("expected batch status %q, got %q", openai.BatchStatusCompleted, finalBatch.Status)
	}
	if finalBatch.RequestCounts.Completed != 0 {
		t.Errorf("completed = %d, want 0", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.RequestCounts.Failed != 1 {
		t.Errorf("failed = %d, want 1", finalBatch.RequestCounts.Failed)
	}
	if finalBatch.OutputFileID == "" {
		t.Fatal("expected output_file_id with HTTP 403 response")
	}
	if finalBatch.ErrorFileID != "" {
		t.Errorf("expected empty error_file_id (HTTP errors go to output), got %q", finalBatch.ErrorFileID)
	}

	output := fetchOutputFile(t, finalBatch)
	var found403 bool
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var rl batchResultLine
		if err := json.Unmarshal([]byte(line), &rl); err != nil {
			t.Fatalf("invalid output line: %v\n%s", err, line)
		}
		if rl.Error != nil {
			t.Fatalf("expected HTTP response in output, got error file entry code=%q message=%q",
				rl.Error.Code, rl.Error.Message)
		}
		if rl.Response == nil {
			t.Fatal("expected response object in output line")
		}
		if rl.Response.StatusCode == http.StatusForbidden {
			found403 = true
		} else {
			t.Errorf("status_code = %d, want 403", rl.Response.StatusCode)
		}
	}
	if !found403 {
		t.Errorf("expected status_code 403 in output file, got:\n%s", output)
	}
	t.Logf("Batch %s preserved HTTP 403 in output (no parse_error)", batchID)
}

// waitAndStealQueuedRequest waits until a request appears in the Redis sorted
// set queue, removes it, and returns its request ID and result queue from the
// InternalRequest envelope.
func waitAndStealQueuedRequest(t *testing.T, rdb *redis.Client, queue string, timeout time.Duration) (string, string) {
	t.Helper()

	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		members, err := rdb.ZRange(ctx, queue, 0, 0).Result()
		if err != nil {
			t.Fatalf("ZRange %s: %v", queue, err)
		}
		if len(members) == 0 {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		member := members[0]
		removed, err := rdb.ZRem(ctx, queue, member).Result()
		if err != nil {
			t.Fatalf("ZRem %s: %v", queue, err)
		}
		if removed == 0 {
			// Another consumer won the race; retry.
			continue
		}

		var ir asyncapi.InternalRequest
		if err := json.Unmarshal([]byte(member), &ir); err != nil {
			t.Fatalf("unmarshal InternalRequest: %v\n%s", err, member)
		}
		if ir.PublicRequest == nil {
			t.Fatalf("InternalRequest missing PublicRequest: %s", member)
		}
		reqID := ir.PublicRequest.ReqID()
		if reqID == "" {
			t.Fatalf("empty request ID in queued message: %s", member)
		}
		if ir.ResultQueueName == "" {
			t.Fatalf("empty result queue in queued message: %s", member)
		}
		return reqID, ir.ResultQueueName
	}
	t.Fatalf("no request appeared in %s within %v", queue, timeout)
	return "", ""
}

func testDispatcherBatchRoundTrip(t *testing.T, rdb *redis.Client) {
	jsonl := fmt.Sprintf(
		`{"custom_id":"dreq-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello dispatcher"}]}}`,
		testModel)

	fileID := mustCreateFile(t, fmt.Sprintf("dispatcher-single-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)
	t.Logf("Created batch %s with file %s", batchID, fileID)

	batch, results := waitForBatchStatus(t, batchID, 120*time.Second, openai.BatchStatusCompleted)

	if batch.RequestCounts.Total != 1 {
		t.Errorf("Expected 1 total request, got %d", batch.RequestCounts.Total)
	}
	if batch.RequestCounts.Completed != 1 {
		t.Errorf("Expected 1 completed request, got %d", batch.RequestCounts.Completed)
	}
	if results == nil {
		t.Fatal("Expected non-nil results")
	}
	if results.OutputLines != 1 {
		t.Errorf("Expected 1 output line, got %d", results.OutputLines)
	}

	t.Logf("Batch %s completed via dispatcher", batchID)
}

func testDispatcherMultiRequestBatch(t *testing.T, rdb *redis.Client) {
	jsonl := strings.Join([]string{
		fmt.Sprintf(`{"custom_id":"dreq-m1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello 1"}]}}`, testModel),
		fmt.Sprintf(`{"custom_id":"dreq-m2","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello 2"}]}}`, testModel),
		fmt.Sprintf(`{"custom_id":"dreq-m3","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello 3"}]}}`, testModel),
	}, "\n")

	fileID := mustCreateFile(t, fmt.Sprintf("dispatcher-multi-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)
	t.Logf("Created batch %s with 3 requests", batchID)

	batch, results := waitForBatchStatus(t, batchID, 120*time.Second, openai.BatchStatusCompleted)

	if batch.RequestCounts.Total != 3 {
		t.Errorf("Expected 3 total requests, got %d", batch.RequestCounts.Total)
	}
	if batch.RequestCounts.Completed != 3 {
		t.Errorf("Expected 3 completed requests, got %d", batch.RequestCounts.Completed)
	}
	if results == nil {
		t.Fatal("Expected non-nil results")
	}
	if results.OutputLines != 3 {
		t.Errorf("Expected 3 output lines, got %d", results.OutputLines)
	}

	t.Logf("All 3 requests completed via dispatcher")
}

// testDispatcherMultiReplicaBatch verifies that two healthy Batch Processor
// replicas do not consume and discard one another's async results.
func testDispatcherMultiReplicaBatch(t *testing.T) {
	selector := fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/component=processor", testHelmRelease)
	var workloads []string
	var discoveryErrors []string
	for _, kind := range []string{"deployment", "statefulset"} {
		out, err := exec.Command("kubectl", "get", kind,
			"-n", testNamespace,
			"-l", selector,
			"-o", `jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`,
		).CombinedOutput()
		if err != nil {
			discoveryErrors = append(discoveryErrors, fmt.Sprintf("%s: %v: %s", kind, err, strings.TrimSpace(string(out))))
			continue
		}
		for _, name := range strings.Fields(string(out)) {
			workloads = append(workloads, kind+"/"+name)
		}
	}
	if len(workloads) != 1 {
		t.Fatalf("find one Processor workload with selector %q: found %v; discovery errors: %v", selector, workloads, discoveryErrors)
	}
	workload := workloads[0]
	replicasOut, err := exec.Command("kubectl", "get", workload,
		"-n", testNamespace, "-o", "jsonpath={.spec.replicas}").CombinedOutput()
	if err != nil {
		t.Fatalf("get Processor replica count for %s: %v\n%s", workload, err, replicasOut)
	}
	originalReplicas, err := strconv.Atoi(strings.TrimSpace(string(replicasOut)))
	if err != nil {
		t.Fatalf("parse Processor replica count %q: %v", replicasOut, err)
	}

	scaleProcessor := func(replicas int) error {
		out, scaleErr := exec.Command("kubectl", "scale", workload,
			"-n", testNamespace, fmt.Sprintf("--replicas=%d", replicas)).CombinedOutput()
		if scaleErr != nil {
			return fmt.Errorf("scale Processor to %d replicas: %w\n%s", replicas, scaleErr, out)
		}
		out, scaleErr = exec.Command("kubectl", "rollout", "status", workload,
			"-n", testNamespace, "--timeout=180s").CombinedOutput()
		if scaleErr != nil {
			return fmt.Errorf("wait for %d Processor replicas: %w\n%s", replicas, scaleErr, out)
		}
		return nil
	}

	if originalReplicas != 2 {
		t.Cleanup(func() {
			if err := scaleProcessor(originalReplicas); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		})
		if err := scaleProcessor(2); err != nil {
			t.Fatal(err)
		}
	}

	const (
		batchCount       = 2
		requestsPerBatch = 32
	)
	batchIDs := make([]string, 0, batchCount)
	for batchIndex := 0; batchIndex < batchCount; batchIndex++ {
		lines := make([]string, 0, requestsPerBatch)
		for requestIndex := 0; requestIndex < requestsPerBatch; requestIndex++ {
			lines = append(lines, fmt.Sprintf(
				`{"custom_id":"multi-replica-%d-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":1,"messages":[{"role":"user","content":"batch %d request %d"}]}}`,
				batchIndex, requestIndex, testModel, batchIndex, requestIndex))
		}

		fileID := mustCreateFile(t,
			fmt.Sprintf("dispatcher-multi-replica-%d-%s.jsonl", batchIndex, testRunID),
			strings.Join(lines, "\n"))
		batchIDs = append(batchIDs, mustCreateBatch(t, fileID))
	}

	completionDeadline := time.Now().Add(3 * time.Minute)
	for _, batchID := range batchIDs {
		remaining := time.Until(completionDeadline)
		if remaining <= 0 {
			t.Fatalf("batches did not complete within the shared timeout")
		}
		batch, results := waitForBatchStatus(t, batchID, remaining, openai.BatchStatusCompleted)

		if batch.RequestCounts.Total != requestsPerBatch {
			t.Errorf("batch %s: total requests = %d, want %d", batchID, batch.RequestCounts.Total, requestsPerBatch)
		}
		if batch.RequestCounts.Completed != requestsPerBatch {
			t.Errorf("batch %s: completed requests = %d, want %d", batchID, batch.RequestCounts.Completed, requestsPerBatch)
		}
		if batch.RequestCounts.Failed != 0 {
			t.Errorf("batch %s: failed requests = %d, want 0", batchID, batch.RequestCounts.Failed)
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

func testDispatcherRedisGate(t *testing.T, rdb *redis.Client) {
	ctx := context.Background()

	p := newDispatcherProducer(t, rdb, gatePool)

	// Close the gate before submitting any request
	rdb.Set(ctx, dispatchGateBudgetKey, "0.0", 0)
	defer rdb.Del(ctx, dispatchGateBudgetKey)
	t.Log("Gate closed (budget=0.0)")

	// Wait for the dispatcher to see the closed gate (poll interval is 500ms)
	time.Sleep(2 * time.Second)

	// Enqueue a request via the producer (bypass the processor
	// so we can observe the gate independently of BRPOP timeouts)
	reqID := fmt.Sprintf("gate-test-%s", testRunID)
	err := p.SubmitRequest(ctx, &asyncapi.RequestMessage{
		ID:       reqID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Minute).Unix(),
		Payload:  map[string]any{"model": testModel, "prompt": "Hello gate", "max_tokens": 5},
		Endpoint: "/v1/completions",
	})
	if err != nil {
		t.Fatalf("Failed to submit request: %v", err)
	}
	t.Logf("Enqueued request %s while gate is closed", reqID)

	// Verify request stays in the queue (gate blocks dispatch)
	time.Sleep(3 * time.Second)
	queueDepth, _ := rdb.ZCard(ctx, gateReqQueue).Result()
	if queueDepth == 0 {
		t.Fatal("Expected request in dispatcher queue while gate is closed, but queue is empty")
	}
	t.Logf("Confirmed: request stuck in queue (depth=%d, gate closed)", queueDepth)

	// Open the gate
	rdb.Set(ctx, dispatchGateBudgetKey, "1.0", 0)
	t.Log("Gate opened (budget=1.0)")

	// Wait for the dispatcher to process the request
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Timed out waiting for dispatcher to drain queue after gate opened")
		default:
		}
		depth, _ := rdb.ZCard(ctx, gateReqQueue).Result()
		if depth == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Poll results via the producer until we find ours
	pollCtx, pollCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pollCancel()
	for {
		result, err := p.GetResult(pollCtx)
		if err != nil {
			t.Fatalf("Failed to get result for %s: %v", reqID, err)
		}
		if result.ID == reqID {
			t.Logf("Request %s completed after gate opened", reqID)
			return
		}
		t.Logf("Skipped stale result %s", result.ID)
	}
}

func testBatchAPIDispatchGate(t *testing.T, rdb *redis.Client) {
	ctx := context.Background()
	queueDepth, err := rdb.ZCard(ctx, gateReqQueue).Result()
	if err != nil {
		t.Fatalf("read initial dispatch queue: %v", err)
	}
	if queueDepth != 0 {
		t.Fatalf("gated queue is not clean before test: depth=%d", queueDepth)
	}

	previousBudget, err := rdb.Get(ctx, dispatchGateBudgetKey).Result()
	budgetExisted := err == nil
	if err != nil && err != redis.Nil {
		t.Fatalf("read prior dispatch gate budget: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
		defer cancel()

		if err := rdb.Set(cleanupCtx, dispatchGateBudgetKey, "1.0", 0).Err(); err != nil {
			t.Errorf("cleanup: open dispatch gate: %v", err)
			return
		}

		deadline := time.Now().Add(30 * time.Second)
		var depth int64
		for time.Now().Before(deadline) {
			var depthErr error
			depth, depthErr = rdb.ZCard(cleanupCtx, gateReqQueue).Result()
			if depthErr != nil {
				t.Errorf("cleanup: read dispatch queue: %v", depthErr)
				return
			}
			if depth == 0 {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if depth != 0 {
			t.Errorf("cleanup: gated queue did not drain after opening: depth=%d; leaving budget at 1.0", depth)
			return
		}

		if budgetExisted {
			if err := rdb.Set(cleanupCtx, dispatchGateBudgetKey, previousBudget, 0).Err(); err != nil {
				t.Errorf("cleanup: restore dispatch gate budget: %v", err)
			}
		} else if err := rdb.Del(cleanupCtx, dispatchGateBudgetKey).Err(); err != nil {
			t.Errorf("cleanup: remove dispatch gate budget: %v", err)
		}
	})

	if err := rdb.Set(ctx, dispatchGateBudgetKey, "0.0", 0).Err(); err != nil {
		t.Fatalf("close dispatch gate: %v", err)
	}
	t.Log("Gate closed (budget=0.0)")

	// The Redis gate polls every 500ms. Wait for it to observe the closed budget
	// before creating the batch so the request cannot race through the gate.
	time.Sleep(2 * time.Second)

	before := getSimStats(t, testSimService)
	if before.Running != 0 || before.Waiting != 0 {
		t.Fatalf("simulator is not idle before test: running=%d waiting=%d", before.Running, before.Waiting)
	}
	const customID = "batch-api-gate-1"
	jsonl := fmt.Sprintf(
		`{"custom_id":"%s","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"Hello composed gate"}]}}`,
		customID, gateModel)
	fileID := mustCreateFile(t, fmt.Sprintf("dispatcher-batch-api-gate-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		queueDepth, err = rdb.ZCard(ctx, gateReqQueue).Result()
		if err != nil {
			t.Fatalf("read dispatch queue: %v", err)
		}
		if queueDepth > 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if queueDepth == 0 {
		t.Fatalf("batch request did not appear in gated queue within 30s")
	}
	// Keep the gate closed across several Async poll intervals, then re-check the
	// queue and the engine's authoritative received counter.
	time.Sleep(2 * time.Second)
	queueDepth, err = rdb.ZCard(ctx, gateReqQueue).Result()
	if err != nil {
		t.Fatalf("re-read dispatch queue: %v", err)
	}
	if queueDepth != 1 {
		t.Fatalf("gated queue depth = %d, want 1 while budget is zero", queueDepth)
	}

	batch, err := newClient().Batches.Get(ctx, batchID)
	if err != nil {
		t.Fatalf("retrieve gated batch: %v", err)
	}
	if terminalBatchStatuses[batch.Status] {
		t.Fatalf("batch reached terminal status %q while dispatch gate was closed", batch.Status)
	}
	if batch.RequestCounts.Total != 1 {
		t.Fatalf("gated batch total requests = %d, want 1", batch.RequestCounts.Total)
	}
	if batch.RequestCounts.Completed != 0 || batch.RequestCounts.Failed != 0 {
		t.Fatalf("gated batch counts = completed:%d failed:%d, want 0 and 0",
			batch.RequestCounts.Completed, batch.RequestCounts.Failed)
	}

	gated := getSimStats(t, testSimService)
	if gated.RequestsReceived != before.RequestsReceived {
		t.Fatalf("inference received %d request(s) while gate was closed, want 0",
			gated.RequestsReceived-before.RequestsReceived)
	}
	t.Logf("Batch %s is %s with queue depth %d; inference received no request", batchID, batch.Status, queueDepth)

	if err := rdb.Set(ctx, dispatchGateBudgetKey, "1.0", 0).Err(); err != nil {
		t.Fatalf("open dispatch gate: %v", err)
	}
	t.Log("Gate opened (budget=1.0)")

	finalBatch, results := waitForBatchStatus(t, batchID, 2*time.Minute, openai.BatchStatusCompleted)
	if finalBatch.RequestCounts.Total != 1 {
		t.Errorf("total requests = %d, want 1", finalBatch.RequestCounts.Total)
	}
	if finalBatch.RequestCounts.Completed != 1 {
		t.Errorf("completed requests = %d, want 1", finalBatch.RequestCounts.Completed)
	}
	if finalBatch.RequestCounts.Failed != 0 {
		t.Errorf("failed requests = %d, want 0", finalBatch.RequestCounts.Failed)
	}
	if results == nil {
		t.Fatal("expected batch results")
	}
	if results.OutputLines != 1 || results.ErrorLines != 0 {
		t.Fatalf("result lines = output:%d error:%d, want 1 and 0", results.OutputLines, results.ErrorLines)
	}

	var output batchResultLine
	if err := json.Unmarshal([]byte(results.OutputBody), &output); err != nil {
		t.Fatalf("decode batch output: %v", err)
	}
	if output.CustomID != customID {
		t.Errorf("output custom_id = %q, want %q", output.CustomID, customID)
	}

	after := getSimStats(t, testSimService)
	if got := after.RequestsReceived - before.RequestsReceived; got != 1 {
		t.Errorf("inference requests received = %d, want exactly 1", got)
	}
	if got := after.RequestsCompleted - before.RequestsCompleted; got != 1 {
		t.Errorf("inference requests completed = %d, want exactly 1", got)
	}
	if got := after.RequestsFailed - before.RequestsFailed; got != 0 {
		t.Errorf("inference requests failed = %d, want 0", got)
	}
	if got := after.RequestsAborted - before.RequestsAborted; got != 0 {
		t.Errorf("inference requests aborted = %d, want 0", got)
	}
	if after.Running != 0 || after.Waiting != 0 {
		t.Errorf("simulator is not idle after test: running=%d waiting=%d", after.Running, after.Waiting)
	}
}

func testDispatcherEndpointScrapeGate(t *testing.T, rdb *redis.Client) {
	ctx := context.Background()

	p := newDispatcherProducer(t, rdb, scrapePool)

	// Saturate the sim — gate should close
	// (endpoint-scrape gate: vllm:num_requests_waiting / max_count_per_pod >= 1 → budget 0)
	release := saturateSim(t)
	t.Log("Sim saturated (gate should close)")

	// Give the scrape gate time to poll the new metric value
	time.Sleep(3 * time.Second)

	// Enqueue a request — it should stay in the queue (gate closed)
	reqID := fmt.Sprintf("scrape-gate-%s", testRunID)
	err := p.SubmitRequest(ctx, &asyncapi.RequestMessage{
		ID:       reqID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Minute).Unix(),
		Payload:  map[string]any{"model": testModel, "prompt": "Hello scrape gate", "max_tokens": 5},
		Endpoint: "/v1/completions",
	})
	if err != nil {
		t.Fatalf("Failed to submit request: %v", err)
	}
	t.Logf("Enqueued request %s while gate is closed", reqID)

	// Verify no result arrives (gate closed)
	time.Sleep(3 * time.Second)
	queueDepth, _ := rdb.ZCard(ctx, scrapeReqQueue).Result()
	if queueDepth == 0 {
		t.Fatal("Expected request in queue while gate is closed, but queue is empty")
	}
	t.Logf("Confirmed: request stuck in queue (depth=%d, gate closed)", queueDepth)

	// Clear saturation — gate should open
	release()
	t.Log("Sim idle (gate should open)")

	// Wait for the scrape gate to pick up the updated metrics and dispatcher to drain
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Timed out waiting for dispatcher to drain queue after gate opened")
		default:
		}
		depth, _ := rdb.ZCard(ctx, scrapeReqQueue).Result()
		if depth == 0 {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Poll result
	pollCtx, pollCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pollCancel()
	for {
		result, err := p.GetResult(pollCtx)
		if err != nil {
			t.Fatalf("Failed to get result for %s: %v", reqID, err)
		}
		if result.ID == reqID {
			t.Logf("Request %s completed after endpoint-scrape gate opened", reqID)
			return
		}
		t.Logf("Skipped stale result %s", result.ID)
	}
}

// saturateSim makes vllm:num_requests_waiting on the primary simulator real
// and large: it chokes the engine through the vllm-vcr control API (one
// running request, 30s decode step) and parks parkedRequests direct
// completions on it from the in-cluster curl pod, then waits until the
// simulator's /metrics reports at least gateWaitingThreshold waiting. The
// returned function (idempotent, also registered as cleanup) restores the
// engine, which lets the parked requests finish within a second, and waits
// for the waiting gauge to read zero.
func saturateSim(t *testing.T) func() {
	t.Helper()

	const parkedRequests = 10
	patchEngineConfig(t, testSimService, `{"max_num_seqs": 1, "time_to_first_token": 0, "inter_token_latency": 30000}`)

	ensureE2ECurlPod(t)
	url := fmt.Sprintf("http://%s.%s.svc.cluster.local:8000/v1/completions", testSimService, testNamespace)
	body := fmt.Sprintf(`{"model":%q,"prompt":"park","max_tokens":5}`, testModel)
	script := fmt.Sprintf(`for i in $(seq 1 %d); do curl -sS -o /dev/null -m 600 -X POST %s -H 'content-type: application/json' -d %s & done; wait`, parkedRequests, shQuote(url), shQuote(body))
	ctx, cancel := context.WithCancel(t.Context())
	cmd := exec.CommandContext(ctx, "kubectl", "exec", "-n", testNamespace, e2eCurlPod, "--", "sh", "-c", script)
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("failed to park requests on %s: %v", testSimService, err)
	}

	waitForSimWaiting(t, func(n int) bool { return n >= gateWaitingThreshold }, 60*time.Second)
	t.Logf("sim saturated: %d requests parked on a choked engine", parkedRequests)

	var once sync.Once
	release := func() {
		once.Do(func() {
			if err := tryPatchEngineConfig(t, testSimService, `{"max_num_seqs": 128, "time_to_first_token": 50, "inter_token_latency": 100}`); err != nil {
				t.Errorf("release engine: %v", err)
			}
			waitForSimWaiting(t, func(n int) bool { return n == 0 }, 60*time.Second)
			cancel()
			_ = cmd.Wait()
			t.Log("sim idle: engine released, parked requests drained")
		})
	}
	t.Cleanup(release)
	return release
}

// gateWaitingThreshold is the waiting-queue depth at which both dispatcher
// gates close: max_count_per_pod in helm-values-scrape.yaml and the
// denominator of the query in helm-values-prometheus.yaml.
const gateWaitingThreshold = 5

var simWaitingPattern = regexp.MustCompile(`(?m)^vllm:num_requests_waiting\{[^}]*\}\s+([0-9.e+-]+)$`)

// waitForSimWaiting polls the simulator's /metrics until
// vllm:num_requests_waiting satisfies ok.
func waitForSimWaiting(t *testing.T, ok func(int) bool, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	last := -1
	for time.Now().Before(deadline) {
		resp, err := http.Get(testSimURL + "/metrics")
		if err != nil {
			t.Fatalf("GET %s/metrics: %v", testSimURL, err)
		}
		raw, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			t.Fatalf("read %s/metrics: %v", testSimURL, readErr)
		}
		if match := simWaitingPattern.FindStringSubmatch(string(raw)); match != nil {
			v, parseErr := strconv.ParseFloat(match[1], 64)
			if parseErr != nil {
				t.Fatalf("parse vllm:num_requests_waiting %q: %v", match[1], parseErr)
			}
			last = int(v)
			if ok(last) {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("vllm:num_requests_waiting did not reach the wanted value within %v (last=%d)", timeout, last)
}

func testDispatcherPrometheusGate(t *testing.T, rdb *redis.Client) {
	ctx := context.Background()

	p := newDispatcherProducer(t, rdb, promPool)

	// Saturate the sim — gate should close
	// (query: 1 - clamp_max(vllm:num_requests_waiting / 5, 1) → 0 when waiting ≥ 5)
	release := saturateSim(t)
	t.Log("Sim saturated (gate should close)")

	// Give Prometheus time to scrape the new metric value
	time.Sleep(10 * time.Second)

	// Enqueue a request — it should stay in the queue (gate closed)
	reqID := fmt.Sprintf("prom-gate-%s", testRunID)
	err := p.SubmitRequest(ctx, &asyncapi.RequestMessage{
		ID:       reqID,
		Created:  time.Now().Unix(),
		Deadline: time.Now().Add(5 * time.Minute).Unix(),
		Payload:  map[string]any{"model": testModel, "prompt": "Hello prom gate", "max_tokens": 5},
		Endpoint: "/v1/completions",
	})
	if err != nil {
		t.Fatalf("Failed to submit request: %v", err)
	}
	t.Logf("Enqueued request %s while gate is closed", reqID)

	// Verify no result arrives (gate closed)
	time.Sleep(5 * time.Second)
	queueDepth, _ := rdb.ZCard(ctx, promReqQueue).Result()
	if queueDepth == 0 {
		t.Fatal("Expected request in queue while gate is closed, but queue is empty")
	}
	t.Logf("Confirmed: request stuck in queue (depth=%d, gate closed)", queueDepth)

	// Clear saturation — gate should open
	release()
	t.Log("Sim idle (gate should open)")

	// Wait for Prometheus to scrape the updated metric and dispatcher to react
	deadline := time.After(60 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Timed out waiting for dispatcher to drain queue after gate opened")
		default:
		}
		depth, _ := rdb.ZCard(ctx, promReqQueue).Result()
		if depth == 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}

	// Poll result
	pollCtx, pollCancel := context.WithTimeout(ctx, 15*time.Second)
	defer pollCancel()
	for {
		result, err := p.GetResult(pollCtx)
		if err != nil {
			t.Fatalf("Failed to get result for %s: %v", reqID, err)
		}
		if result.ID == reqID {
			t.Logf("Request %s completed after Prometheus gate opened", reqID)
			return
		}
		t.Logf("Skipped stale result %s", result.ID)
	}
}
