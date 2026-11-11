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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
)

func TestDispatcherOTelTraces(t *testing.T) {
	if !detectDispatcherDeployed(t) {
		t.Skip("skipping: dispatcher not deployed")
	}
	if os.Getenv("TEST_APISERVER_OBS_URL") == "" {
		probe := &http.Client{Timeout: 5 * time.Second}
		if resp, err := probe.Get(testApiserverObsURL + "/ready"); err == nil {
			resp.Body.Close()
		} else if isUnreachable(err) {
			t.Skipf("skipping: APIServer observability endpoint not reachable at default %s; set TEST_APISERVER_OBS_URL or port-forward it: %v", testApiserverObsURL, err)
		}
	}
	waitForReady(t, testApiserverObsURL, 30*time.Second)

	jaegerClient := &http.Client{Timeout: 5 * time.Second}
	checkResp, err := jaegerClient.Get(testJaegerURL + "/")
	if err != nil {
		t.Skipf("Jaeger not reachable at %s, skipping OTel trace verification: %v", testJaegerURL, err)
	}
	checkResp.Body.Close()

	t.Run("CrossServiceTracePropagation", func(t *testing.T) {
		testCrossServiceTracePropagation(t, jaegerClient)
	})
}

// testCrossServiceTracePropagation verifies that a batch processed through the
// async dispatcher produces a connected trace spanning both batch-gateway and
// llm-d-async. The batch-gateway injects trace context into
// RequestMessage.Metadata, the async processor extracts it, and both services
// export spans to the same Jaeger instance under the same trace ID.
func testCrossServiceTracePropagation(t *testing.T, jaegerClient *http.Client) {
	jsonl := fmt.Sprintf(
		`{"custom_id":"otel-xsvc-1","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":5,"messages":[{"role":"user","content":"trace propagation test"}]}}`,
		testModel)

	fileID := mustCreateFile(t, fmt.Sprintf("otel-xsvc-%s.jsonl", testRunID), jsonl)
	batchID := mustCreateBatch(t, fileID)
	t.Logf("Created batch %s for cross-service trace test", batchID)

	batch, _ := waitForBatchStatus(t, batchID, 120*time.Second, openai.BatchStatusCompleted)
	if batch.RequestCounts.Completed != 1 {
		t.Fatalf("Expected 1 completed request, got %d", batch.RequestCounts.Completed)
	}

	// Poll Jaeger for traces from batch-gateway that contain llm-d-async spans.
	// The batch-gateway creates a "process-batch" span, and llm-d-async
	// creates a "process-request" child span under the same trace ID.
	type jaegerSpan struct {
		OperationName string `json:"operationName"`
		ProcessID     string `json:"processID"`
	}
	type jaegerTrace struct {
		TraceID   string       `json:"traceID"`
		Spans     []jaegerSpan `json:"spans"`
		Processes map[string]struct {
			ServiceName string `json:"serviceName"`
		} `json:"processes"`
	}
	type jaegerResponse struct {
		Data []jaegerTrace `json:"data"`
	}

	var found *jaegerTrace
	deadline := time.After(30 * time.Second)
	for found == nil {
		select {
		case <-deadline:
			t.Fatal("Timed out waiting for cross-service trace in Jaeger")
		default:
		}

		resp, err := jaegerClient.Get(testJaegerURL + "/api/traces?service=llm-d-async&limit=20&lookback=5m")
		if err != nil {
			t.Logf("Jaeger query failed (retrying): %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var result jaegerResponse
		if err := json.Unmarshal(body, &result); err != nil {
			t.Logf("Failed to parse Jaeger response (retrying): %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		for i := range result.Data {
			trace := &result.Data[i]
			services := make(map[string]bool)
			for _, span := range trace.Spans {
				if proc, ok := trace.Processes[span.ProcessID]; ok {
					services[proc.ServiceName] = true
				}
			}
			if services["batch-gateway"] && services["llm-d-async"] {
				found = trace
				break
			}
		}

		if found == nil {
			time.Sleep(2 * time.Second)
		}
	}

	// Verify expected span operations exist in the trace
	spanOps := make(map[string]bool)
	for _, span := range found.Spans {
		spanOps[span.OperationName] = true
	}

	if !spanOps["process-request"] {
		t.Error("Expected 'process-request' span from llm-d-async in trace")
	}

	// Collect service names for logging
	services := make(map[string]bool)
	for _, proc := range found.Processes {
		services[proc.ServiceName] = true
	}

	t.Logf("Cross-service trace verified: traceID=%s, services=%v, spans=%v",
		found.TraceID, services, spanOps)
}
