//go:build integration

/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// Integration tests using llm-d-inference-sim mock server running in a container.
// Supports both Docker and Podman (mirrors the Makefile's CONTAINER_TOOL detection).
//
// Each test spawns its own mock server instance with specific configuration.
//
// Run tests with:
//   make test-integration
//   Or manually: go test -v -tags=integration ./test/integration/...
//
// findContainerTool is defined in util_test.go (shared with the GCS emulator
// provisioning helpers in gcs_client_test.go).

func startMockInferenceServer(containerTool string, port int, args ...string) error {
	baseArgs := []string{
		"compose", "-f", "./docker-compose.test.yml",
		"run", "-d", "--rm",
		"--publish", fmt.Sprintf("%d:8000", port),
		"--name", fmt.Sprintf("mock-server-test-%d", port),
		"llm-d-mock-server",
		"--port=8000",
		"--model=fake-model",
	}
	baseArgs = append(baseArgs, args...)

	cmd := exec.Command(containerTool, baseArgs...)
	cmd.Stdout = io.Discard
	cmd.Stderr = io.Discard

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to start mock server: %w", err)
	}

	serverURL := fmt.Sprintf("http://localhost:%d", port)
	for i := 0; i < 30; i++ {
		time.Sleep(200 * time.Millisecond)
		resp, err := http.Get(serverURL + "/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
	}

	return fmt.Errorf("mock server failed to become ready")
}

func stopMockInferenceServer(containerTool string, port int) {
	containerName := fmt.Sprintf("mock-server-test-%d", port)
	cmd := exec.Command(containerTool, "stop", containerName)
	_ = cmd.Run()
	time.Sleep(500 * time.Millisecond)
}

func TestHTTPClientIntegration(t *testing.T) {
	if os.Getenv("SKIP_INTEGRATION_TESTS") == "true" {
		t.Skip("Integration tests skipped")
	}

	containerTool := findContainerTool()
	if containerTool == "" {
		t.Skip("No container runtime (docker/podman) available, skipping")
	}
	if err := exec.Command(containerTool, "compose", "version").Run(); err != nil {
		t.Skipf("%s compose not functional, skipping: %v", containerTool, err)
	}

	t.Run("BasicInference", func(t *testing.T) { testHTTPClientBasicInference(t, containerTool) })
	t.Run("LatencySimulation", func(t *testing.T) { testHTTPClientLatencySimulation(t, containerTool) })
	t.Run("FailureInjection", func(t *testing.T) { testHTTPClientFailureInjection(t, containerTool) })
}

func testHTTPClientBasicInference(t *testing.T, containerTool string) {
	const testPort = 8200

	err := startMockInferenceServer(containerTool, testPort, "--mode=random")
	if err != nil {
		t.Skipf("Could not start mock server, skipping: %v", err)
	}
	t.Cleanup(func() { stopMockInferenceServer(containerTool, testPort) })

	logger := testr.NewWithInterface(t, testr.Options{})
	client, err := inference.NewInferenceClient(&inference.HTTPClientConfig{
		BaseURL: fmt.Sprintf("http://localhost:%d", testPort),
		Timeout: 10 * time.Second,
	}, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("should handle multiple sequential requests", func(t *testing.T) {
		for i := 0; i < 5; i++ {
			req := &inference.GenerateRequest{
				RequestID: fmt.Sprintf("sequential-test-%03d", i),
				Endpoint:  "/v1/chat/completions",
				Params: map[string]interface{}{
					"model":      "fake-model",
					"prompt":     "Test request",
					"max_tokens": 5,
				},
			}

			ctx := context.Background()
			resp, genErr := client.Generate(ctx, req)

			if genErr != nil {
				t.Errorf("request %d failed: %v", i, genErr)
			}
			if resp == nil {
				t.Fatalf("request %d returned nil response", i)
			}
		}
	})

	t.Run("should handle concurrent requests correctly", func(t *testing.T) {
		// Channel is typed *ClientError (the concrete return type of Generate)
		// rather than the error interface: assigning a nil concrete pointer
		// (*ClientError)(nil) into an interface produces a non-nil interface
		// value with a nil underlying pointer, so `inferr != nil` on the
		// receiving side would always be true. See #438.
		const numRequests = 10
		results := make(chan *inference.ClientError, numRequests)

		for i := 0; i < numRequests; i++ {
			go func(id int) {
				req := &inference.GenerateRequest{
					RequestID: fmt.Sprintf("concurrent-test-%03d", id),
					Endpoint:  "/v1/chat/completions",
					Params: map[string]interface{}{
						"model":      "fake-model",
						"prompt":     "Concurrent test",
						"max_tokens": 5,
					},
				}

				_, inferr := client.Generate(context.Background(), req)
				results <- inferr
			}(i)
		}

		for i := 0; i < numRequests; i++ {
			inferr := <-results
			if inferr != nil {
				t.Errorf("concurrent request %d failed: %v", i, inferr)
			}
		}
	})
}

func testHTTPClientLatencySimulation(t *testing.T, containerTool string) {
	const testPort = 8101

	err := startMockInferenceServer(containerTool, testPort,
		"--time-to-first-token=200ms",
		"--inter-token-latency=50ms",
	)
	if err != nil {
		t.Skipf("Could not start mock server, skipping: %v", err)
	}
	t.Cleanup(func() { stopMockInferenceServer(containerTool, testPort) })

	logger := testr.NewWithInterface(t, testr.Options{})
	client, err := inference.NewInferenceClient(&inference.HTTPClientConfig{
		BaseURL: fmt.Sprintf("http://localhost:%d", testPort),
		Timeout: 10 * time.Second,
	}, logger)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Run("should handle time-to-first-token latency", func(t *testing.T) {
		req := &inference.GenerateRequest{
			RequestID: "ttft-latency-001",
			Endpoint:  "/v1/chat/completions",
			Params: map[string]interface{}{
				"model":      "fake-model",
				"prompt":     "Test TTFT latency",
				"max_tokens": 5,
			},
		}

		start := time.Now()
		resp, genErr := client.Generate(context.Background(), req)
		duration := time.Since(start)

		if genErr != nil {
			t.Errorf("expected no error, got %v", genErr)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		if duration < 180*time.Millisecond {
			t.Errorf("expected duration >= 180ms, got %v", duration)
		}
		if duration >= 2*time.Second {
			t.Errorf("expected duration < 2s, got %v", duration)
		}
	})

	t.Run("should handle inter-token latency", func(t *testing.T) {
		req := &inference.GenerateRequest{
			RequestID: "inter-token-latency-001",
			Endpoint:  "/v1/chat/completions",
			Params: map[string]interface{}{
				"model":      "fake-model",
				"prompt":     "Test inter-token latency",
				"max_tokens": 10,
			},
		}

		start := time.Now()
		resp, genErr := client.Generate(context.Background(), req)
		duration := time.Since(start)

		if genErr != nil {
			t.Errorf("expected no error, got %v", genErr)
		}
		if resp == nil {
			t.Fatal("expected non-nil response")
		}
		// With 10 tokens, TTFT=200ms + ~10*50ms = ~700ms total
		if duration < 200*time.Millisecond {
			t.Errorf("expected duration >= 200ms, got %v", duration)
		}
	})
}

func testHTTPClientFailureInjection(t *testing.T, containerTool string) {
	// Specific error status code tests (429, 500, 401, 400, 404) are covered
	// in unit tests (TestErrorHandling, TestAdditionalHTTPStatusCodes,
	// TestRetryConditionLogic). This test focuses on real HTTP behavior with
	// randomized failures to test retry logic end-to-end.

	t.Run("Mixed Failure Rate (50%)", func(t *testing.T) {
		const testPort = 8108

		if err := startMockInferenceServer(containerTool, testPort,
			"--failure-injection-rate=50",
			"--failure-types=server_error",
		); err != nil {
			t.Skipf("Could not start mock server, skipping: %v", err)
		}
		t.Cleanup(func() { stopMockInferenceServer(containerTool, testPort) })

		logger := testr.NewWithInterface(t, testr.Options{})
		client, err := inference.NewInferenceClient(&inference.HTTPClientConfig{
			BaseURL:        fmt.Sprintf("http://localhost:%d", testPort),
			Timeout:        10 * time.Second,
			MaxRetries:     5,
			InitialBackoff: 50 * time.Millisecond,
		}, logger)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		req := &inference.GenerateRequest{
			RequestID: "mixed-failure-001",
			Endpoint:  "/v1/completions",
			Params: map[string]interface{}{
				"model":      "fake-model",
				"prompt":     "Test retry on partial failures",
				"max_tokens": 5,
			},
		}

		// With 50% failure rate and 5 retries, probability of all failing = 0.5^6 = 1.5%
		resp, inferr := client.Generate(context.Background(), req)

		if inferr == nil {
			if resp == nil {
				t.Error("expected non-nil response")
			}
		} else {
			if inferr.Category != httpclient.ErrCategoryServer {
				t.Errorf("got category %v, want %v", inferr.Category, httpclient.ErrCategoryServer)
			}
			if !inferr.IsRetryable() {
				t.Error("expected error to be retryable")
			}
		}
	})
}
