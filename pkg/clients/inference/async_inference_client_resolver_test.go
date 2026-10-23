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

package inference

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	asyncapi "github.com/llm-d/llm-d-async/api"
)

const testAsyncConsumerID = "processor-0"

func TestNewAsyncResolver(t *testing.T) {
	t.Run("creates per-model clients", func(t *testing.T) {
		mr := miniredis.RunT(t)

		cfg := AsyncClientConfig{
			RedisURL:   "redis://" + mr.Addr(),
			ConsumerID: testAsyncConsumerID,
			Models: map[string]AsyncModelPoolConfig{
				"model-a": {PoolName: "pool-a"},
				"model-b": {PoolName: "pool-b"},
			},
			ResultPollTimeout: time.Second,
		}

		r, err := NewAsyncResolver(cfg, testLogger(t))
		if err != nil {
			t.Fatalf("NewAsyncResolver: %v", err)
		}

		if got := r.SharedClientFor("model-a"); got == nil {
			t.Fatal("expected non-nil client for model-a")
		}
		if got := r.SharedClientFor("model-b"); got == nil {
			t.Fatal("expected non-nil client for model-b")
		}
		if got := r.SharedClientFor("unknown"); got != nil {
			t.Fatalf("expected nil for unknown model, got %v", got)
		}
	})

	t.Run("returns nil for unknown model", func(t *testing.T) {
		mr := miniredis.RunT(t)

		r, err := NewAsyncResolver(AsyncClientConfig{
			RedisURL:   "redis://" + mr.Addr(),
			ConsumerID: testAsyncConsumerID,
			Models: map[string]AsyncModelPoolConfig{
				"model-a": {PoolName: "pool-a"},
			},
			ResultPollTimeout: time.Second,
		}, testLogger(t))
		if err != nil {
			t.Fatalf("NewAsyncResolver: %v", err)
		}

		if got := r.SharedClientFor("unknown"); got != nil {
			t.Fatalf("expected nil for unknown model, got %v", got)
		}
	})

	t.Run("rejects duplicate pool mapping", func(t *testing.T) {
		mr := miniredis.RunT(t)

		_, err := NewAsyncResolver(AsyncClientConfig{
			RedisURL:   "redis://" + mr.Addr(),
			ConsumerID: testAsyncConsumerID,
			Models: map[string]AsyncModelPoolConfig{
				"model-a": {PoolName: "shared-pool"},
				"model-b": {PoolName: "shared-pool"},
			},
			ResultPollTimeout: time.Second,
		}, testLogger(t))
		if err == nil {
			t.Fatal("expected error for duplicate pool mapping")
		}
	})

	t.Run("invalid Redis URL returns error", func(t *testing.T) {
		_, err := NewAsyncResolver(AsyncClientConfig{
			RedisURL:   "not-a-url",
			ConsumerID: testAsyncConsumerID,
			Models: map[string]AsyncModelPoolConfig{
				"model-a": {PoolName: "pool-a"},
			},
			ResultPollTimeout: time.Second,
		}, testLogger(t))
		if err == nil {
			t.Fatal("expected error for invalid Redis URL")
		}
	})

	t.Run("rejects empty consumer ID", func(t *testing.T) {
		mr := miniredis.RunT(t)
		_, err := NewAsyncResolver(AsyncClientConfig{
			RedisURL: "redis://" + mr.Addr(),
			Models: map[string]AsyncModelPoolConfig{
				"model-a": {PoolName: "pool-a"},
			},
			ResultPollTimeout: time.Second,
		}, testLogger(t))
		if err == nil {
			t.Fatal("expected error for empty consumer ID")
		}
	})

	t.Run("close releases resources", func(t *testing.T) {
		mr := miniredis.RunT(t)

		r, err := NewAsyncResolver(AsyncClientConfig{
			RedisURL:   "redis://" + mr.Addr(),
			ConsumerID: testAsyncConsumerID,
			Models: map[string]AsyncModelPoolConfig{
				"model-a": {PoolName: "pool-a"},
			},
			ResultPollTimeout: time.Second,
		}, testLogger(t))
		if err != nil {
			t.Fatalf("NewAsyncResolver: %v", err)
		}

		if err := r.Close(); err != nil {
			t.Fatalf("Close() returned error: %v", err)
		}
	})

	t.Run("SharedClientFor reuses the same client", func(t *testing.T) {
		mr := miniredis.RunT(t)

		r, err := NewAsyncResolver(AsyncClientConfig{
			RedisURL:   "redis://" + mr.Addr(),
			ConsumerID: testAsyncConsumerID,
			Models: map[string]AsyncModelPoolConfig{
				"model-a": {PoolName: "pool-a"},
			},
			ResultPollTimeout: time.Second,
		}, testLogger(t))
		if err != nil {
			t.Fatalf("NewAsyncResolver: %v", err)
		}

		client1 := r.SharedClientFor("model-a")
		client2 := r.SharedClientFor("model-a")
		if client1 != client2 {
			t.Fatal("expected same client from SharedClientFor")
		}
	})

	t.Run("isolates results by consumer while sharing requests", func(t *testing.T) {
		mr := miniredis.RunT(t)
		const (
			requestQueue = "llm-d-async:requests:shared-pool"
			resultQueue  = "llm-d-async:results:shared-pool"
		)

		newResolver := func(consumerID string) *AsyncGatewayResolver {
			r, err := NewAsyncResolver(AsyncClientConfig{
				RedisURL:   "redis://" + mr.Addr(),
				ConsumerID: consumerID,
				Models: map[string]AsyncModelPoolConfig{
					"model-a": {
						PoolName:         "shared-pool",
						RequestQueueName: requestQueue,
						ResultQueueName:  resultQueue,
					},
				},
				ResultPollTimeout: time.Second,
			}, testLogger(t))
			if err != nil {
				t.Fatalf("NewAsyncResolver(%q): %v", consumerID, err)
			}
			t.Cleanup(func() { _ = r.Close() })
			return r
		}

		resolverA := newResolver("processor-a")
		resolverB := newResolver("processor-b")
		clientA := resolverA.SharedClientFor("model-a")
		clientB := resolverB.SharedClientFor("model-a")

		for _, submission := range []struct {
			client    AsyncInferenceClient
			requestID string
		}{
			{client: clientA, requestID: "request-a"},
			{client: clientB, requestID: "request-b"},
		} {
			if submitErr := submission.client.Submit(context.Background(), &GenerateRequest{
				RequestID: submission.requestID,
				Endpoint:  "/v1/chat/completions",
				Params:    map[string]any{"model": "model-a"},
			}); submitErr != nil {
				t.Fatalf("Submit(%q): %s", submission.requestID, submitErr.Message)
			}
		}

		members, err := mr.ZMembers(requestQueue)
		if err != nil {
			t.Fatalf("ZMembers(%q): %v", requestQueue, err)
		}
		if len(members) != 2 {
			t.Fatalf("shared request queue contains %d requests, want 2", len(members))
		}

		routes := make(map[string]string, len(members))
		for _, member := range members {
			var request asyncapi.InternalRequest
			if err := json.Unmarshal([]byte(member), &request); err != nil {
				t.Fatalf("unmarshal request: %v", err)
			}
			routes[request.PublicRequest.ReqID()] = request.ResultQueueName
		}

		queueA := resultQueue + ":processor-a"
		queueB := resultQueue + ":processor-b"
		if got := routes["request-a"]; got != queueA {
			t.Fatalf("request-a result queue = %q, want %q", got, queueA)
		}
		if got := routes["request-b"]; got != queueB {
			t.Fatalf("request-b result queue = %q, want %q", got, queueB)
		}

		pushResult := func(queue, requestID string) {
			payload, err := json.Marshal(asyncapi.ResultMessage{ID: requestID, Payload: `{"ok":true}`})
			if err != nil {
				t.Fatalf("marshal result: %v", err)
			}
			if _, err := mr.Lpush(queue, string(payload)); err != nil {
				t.Fatalf("LPUSH(%q): %v", queue, err)
			}
		}
		pushResult(queueB, "request-b")
		pushResult(queueA, "request-a")

		resultA, err := clientA.GetResult(context.Background())
		if err != nil {
			t.Fatalf("consumer A GetResult: %v", err)
		}
		if resultA.RequestID != "request-a" {
			t.Fatalf("consumer A received %q, want request-a", resultA.RequestID)
		}

		resultB, err := clientB.GetResult(context.Background())
		if err != nil {
			t.Fatalf("consumer B GetResult: %v", err)
		}
		if resultB.RequestID != "request-b" {
			t.Fatalf("consumer B received %q, want request-b", resultB.RequestID)
		}
	})
	t.Run("requires SQL URL for SQL transport", func(t *testing.T) {
		_, err := NewAsyncResolver(AsyncClientConfig{
			Transport:         AsyncTransportSQL,
			ConsumerID:        testAsyncConsumerID,
			ResultPollTimeout: time.Second,
			Models: map[string]AsyncModelPoolConfig{
				"model-a": {PoolName: "pool-a"},
			},
		}, testLogger(t))
		if err == nil {
			t.Fatal("expected an error when SQL URL is missing")
		}
	})
	t.Run("does not build unsafe SQL recovery clients", func(t *testing.T) {
		r := &AsyncGatewayResolver{
			transport:  AsyncTransportSQL,
			routeBases: map[string]string{"model-a": "results:model-a"},
		}

		_, err := r.RecoveryClientFor("model-a", "processor-1")
		if err == nil {
			t.Fatal("expected SQL recovery to be unsupported")
		}
		if !strings.Contains(err.Error(), "does not support cross-processor result recovery") {
			t.Fatalf("error = %q, want SQL recovery limitation", err)
		}
	})
}
