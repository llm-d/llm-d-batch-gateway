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
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/llm-d/llm-d-async/api"
	"github.com/llm-d/llm-d-async/producer"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/redis/go-redis/v9"
)

func TestAsyncSharedClient_Submit_roundtrip(t *testing.T) {
	t.Run("enqueues request and returns result via GetResult", func(t *testing.T) {
		mr := miniredis.RunT(t)
		poolName := "test-pool"
		reqQueue := asyncQueuePrefix + "requests:" + poolName
		resultQueue := asyncQueuePrefix + "results:" + poolName

		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer func() { _ = rdb.Close() }()

		p, err := producer.NewRedisSortedSetProducer(
			producer.RedisSortedSetConfig{
				RequestQueueName: asyncQueuePrefix + "requests:" + poolName,
				ResultQueueName:  asyncQueuePrefix + "results:" + poolName,
			},
			producer.WithRedisClient(rdb),
		)
		if err != nil {
			t.Fatalf("NewRedisSortedSetProducer: %v", err)
		}
		defer func() { _ = p.Close() }()

		client := newAsyncSharedClient(p, time.Second, testLogger(t))

		go func() {
			time.Sleep(50 * time.Millisecond)
			data, _ := json.Marshal(api.ResultMessage{
				ID:         "req-1",
				StatusCode: 200,
				Payload:    `{"choices":[{"text":"hello"}]}`,
			})
			if _, lpushErr := mr.Lpush(resultQueue, string(data)); lpushErr != nil {
				t.Errorf("Lpush: %v", lpushErr)
			}
		}()

		if submitErr := client.Submit(context.Background(), &GenerateRequest{
			RequestID: "req-1",
			Endpoint:  "/v1/completions",
			Params:    map[string]any{"model": "test-model", "prompt": "hello"},
		}); submitErr != nil {
			t.Fatalf("Submit() error: %s", submitErr.Message)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, getErr := client.GetResult(ctx)
		if getErr != nil {
			t.Fatalf("GetResult error: %v", getErr)
		}

		if resp.RequestID != "req-1" {
			t.Errorf("RequestID = %q, want %q", resp.RequestID, "req-1")
		}
		if resp.StatusCode != 200 {
			t.Errorf("StatusCode = %d, want 200", resp.StatusCode)
		}
		if resp.Response == nil {
			t.Fatal("expected non-nil Response")
		}

		members, zErr := rdb.ZRange(context.Background(), reqQueue, 0, -1).Result()
		if zErr != nil {
			t.Fatalf("ZRange: %v", zErr)
		}
		if len(members) != 1 {
			t.Fatalf("expected 1 member in request queue, got %d", len(members))
		}

		var envelope map[string]json.RawMessage
		if uErr := json.Unmarshal([]byte(members[0]), &envelope); uErr != nil {
			t.Fatalf("unmarshal enqueued request: %v", uErr)
		}
		var data api.RequestMessage
		if uErr := json.Unmarshal(envelope["data"], &data); uErr != nil {
			t.Fatalf("unmarshal data field: %v", uErr)
		}
		if data.ID != "req-1" {
			t.Errorf("enqueued request ID = %q, want %q", data.ID, "req-1")
		}
	})

	t.Run("propagates HTTP error status code from ResultMessage", func(t *testing.T) {
		mr := miniredis.RunT(t)
		poolName := "test-pool-403"
		resultQueue := asyncQueuePrefix + "results:" + poolName

		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer func() { _ = rdb.Close() }()

		p, err := producer.NewRedisSortedSetProducer(
			producer.RedisSortedSetConfig{
				RequestQueueName: asyncQueuePrefix + "requests:" + poolName,
				ResultQueueName:  resultQueue,
			},
			producer.WithRedisClient(rdb),
		)
		if err != nil {
			t.Fatalf("NewRedisSortedSetProducer: %v", err)
		}
		defer func() { _ = p.Close() }()

		client := newAsyncSharedClient(p, time.Second, testLogger(t))

		data, _ := json.Marshal(api.ResultMessage{
			ID:         "req-403",
			StatusCode: 403,
			Payload:    "",
		})
		if _, lpushErr := mr.Lpush(resultQueue, string(data)); lpushErr != nil {
			t.Fatalf("Lpush: %v", lpushErr)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, getErr := client.GetResult(ctx)
		if getErr != nil {
			t.Fatalf("GetResult error: %v", getErr)
		}
		if resp.RequestID != "req-403" {
			t.Errorf("RequestID = %q, want %q", resp.RequestID, "req-403")
		}
		if resp.StatusCode != 403 {
			t.Errorf("StatusCode = %d, want 403", resp.StatusCode)
		}
		if len(resp.Response) != 0 {
			t.Errorf("Response = %q, want empty", resp.Response)
		}
	})
}

// plainProducer is a producer without the durable result capability.
type plainProducer struct{}

func (plainProducer) SubmitRequest(context.Context, api.Request) error { return nil }
func (plainProducer) CancelRequests(context.Context, []string) error   { return nil }
func (plainProducer) GetResult(context.Context) (*api.ResultMessage, error) {
	return &api.ResultMessage{ID: "req-plain", StatusCode: 200, Payload: "{}"}, nil
}
func (plainProducer) Close() error { return nil }

// TestAsyncSharedClient_ReceiveResult_durable exercises the durable result
// delivery against the real sorted-set producer: results arrive as leased
// claims that stay recoverable until acknowledged, so a crash between the
// claim and the consumer's durable checkpoint redelivers the result instead
// of losing it.
func TestAsyncSharedClient_ReceiveResult_durable(t *testing.T) {
	newDurableEnv := func(t *testing.T) (*asyncSharedClient, *miniredis.Miniredis, *redis.Client, string) {
		t.Helper()
		mr := miniredis.RunT(t)
		poolName := "durable-pool"
		resultQueue := asyncQueuePrefix + "results:" + poolName
		rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		t.Cleanup(func() { _ = rdb.Close() })
		p, err := producer.NewRedisSortedSetProducer(
			producer.RedisSortedSetConfig{
				RequestQueueName: asyncQueuePrefix + "requests:" + poolName,
				ResultQueueName:  resultQueue,
			},
			producer.WithRedisClient(rdb),
		)
		if err != nil {
			t.Fatalf("NewRedisSortedSetProducer: %v", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		return newAsyncSharedClient(p, time.Second, testLogger(t)), mr, rdb, resultQueue
	}
	pushResult := func(t *testing.T, mr *miniredis.Miniredis, resultQueue, id string) {
		t.Helper()
		data, err := json.Marshal(api.ResultMessage{
			ID:         id,
			StatusCode: 200,
			Payload:    `{"choices":[{"text":"hello"}]}`,
		})
		if err != nil {
			t.Fatalf("marshal result: %v", err)
		}
		if _, lpushErr := mr.Lpush(resultQueue, string(data)); lpushErr != nil {
			t.Fatalf("Lpush: %v", lpushErr)
		}
	}

	t.Run("delivers under a lease and the ack releases it", func(t *testing.T) {
		client, mr, rdb, resultQueue := newDurableEnv(t)
		pushResult(t, mr, resultQueue, "req-1")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, ack, err := client.ReceiveResult(ctx)
		if err != nil {
			t.Fatalf("ReceiveResult: %v", err)
		}
		if resp.RequestID != "req-1" || resp.StatusCode != 200 || resp.Response == nil {
			t.Fatalf("response = %+v, want the mapped req-1 result", resp)
		}

		// The delivery is claimed, not consumed: the pending list is empty
		// but a recoverable copy sits in the claim state until the ack.
		if n, lErr := rdb.LLen(ctx, resultQueue).Result(); lErr != nil || n != 0 {
			t.Fatalf("pending after claim = %d (err %v), want 0", n, lErr)
		}
		if n, zErr := rdb.ZCard(ctx, resultQueue+":result-claims-idx").Result(); zErr != nil || n != 1 {
			t.Fatalf("claims after receive = %d (err %v), want 1 (recoverable until acked)", n, zErr)
		}

		// A second receive while the lease holds finds nothing.
		leasedCtx, leasedCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer leasedCancel()
		if _, _, err := client.ReceiveResult(leasedCtx); err == nil {
			t.Fatal("a leased delivery must not be handed out twice")
		}

		if err := ack(ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
		if n, zErr := rdb.ZCard(ctx, resultQueue+":result-claims-idx").Result(); zErr != nil || n != 0 {
			t.Fatalf("claims after ack = %d (err %v), want 0", n, zErr)
		}

		// An acknowledged result is not redelivered within the tombstone
		// window, even if a duplicate record is pushed for the same
		// request generation.
		pushResult(t, mr, resultQueue, "req-1")
		dupCtx, dupCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer dupCancel()
		if _, _, err := client.ReceiveResult(dupCtx); err == nil {
			t.Fatal("an acknowledged result must not be redelivered")
		}
	})

	t.Run("an unacked delivery is redelivered after its lease lapses", func(t *testing.T) {
		client, mr, rdb, resultQueue := newDurableEnv(t)
		pushResult(t, mr, resultQueue, "req-crash")

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		resp, ack, err := client.ReceiveResult(ctx)
		if err != nil {
			t.Fatalf("ReceiveResult: %v", err)
		}
		if resp.RequestID != "req-crash" {
			t.Fatalf("RequestID = %q, want req-crash", resp.RequestID)
		}
		_ = ack // the crash window: the consumer died before checkpointing

		// Backdate the claim's lease past its expiry, as lease lapse would.
		idxKey := resultQueue + ":result-claims-idx"
		members, err := rdb.ZRangeWithScores(ctx, idxKey, 0, -1).Result()
		if err != nil || len(members) != 1 {
			t.Fatalf("claims = %v (err %v), want the single unacked claim", members, err)
		}
		if _, err := rdb.ZAdd(ctx, idxKey, redis.Z{
			Member: members[0].Member,
			Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
		}).Result(); err != nil {
			t.Fatalf("backdate claim lease: %v", err)
		}

		// The next receive reclaims the expired lease and redelivers the
		// same result: the completed inference is not repeated.
		resp2, ack2, err := client.ReceiveResult(ctx)
		if err != nil {
			t.Fatalf("redelivery: %v", err)
		}
		if resp2.RequestID != "req-crash" || resp2.StatusCode != 200 || resp2.Response == nil {
			t.Fatalf("redelivered response = %+v, want the same req-crash result", resp2)
		}
		if err := ack2(ctx); err != nil {
			t.Fatalf("ack after redelivery: %v", err)
		}
	})

	t.Run("reports unavailability for producers without the capability", func(t *testing.T) {
		client := newAsyncSharedClient(plainProducer{}, time.Second, testLogger(t))
		if _, _, err := client.ReceiveResult(context.Background()); !errors.Is(err, ErrDurableResultsUnavailable) {
			t.Fatalf("ReceiveResult error = %v, want ErrDurableResultsUnavailable", err)
		}

		resp, err := client.GetResult(context.Background())
		if err != nil || resp.RequestID != "req-plain" {
			t.Fatalf("GetResult fallback = (%+v, %v), want the plain result", resp, err)
		}
	})
}

// TestAsyncResolverSQLPostgres_RoundTrip exercises the SQL transport with a
// real PostgreSQL store. The dispatcher side is represented by sqlqueue.Store:
// it claims the submitted request and atomically records the inference result
// on the processor's result route.
func TestAsyncResolverSQLPostgres_RoundTrip(t *testing.T) {
	sqlURL := os.Getenv("TEST_POSTGRES_URL")
	if sqlURL == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	poolName := "sql-roundtrip-pool-" + suffix
	requestQueue := "sql-roundtrip-request-" + suffix
	resultQueue := "sql-roundtrip-result-" + suffix
	consumerID := "sql-roundtrip-processor-" + suffix
	dispatcherID := "sql-roundtrip-dispatcher-" + suffix
	requestID := "sql-roundtrip-request-" + suffix

	resolver, err := NewAsyncResolver(AsyncClientConfig{
		Transport:         AsyncTransportSQL,
		SQLURL:            sqlURL,
		ConsumerID:        consumerID,
		ResultPollTimeout: time.Second,
		PollInterval:      10 * time.Millisecond,
		Models: map[string]AsyncModelPoolConfig{
			"model-a": {
				PoolName:         poolName,
				RequestQueueName: requestQueue,
				ResultQueueName:  resultQueue,
			},
		},
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewAsyncResolver SQL: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	store, err := sqlqueue.Open(ctx, sqlURL)
	if err != nil {
		t.Fatalf("sqlqueue.Open dispatcher store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	client := resolver.SharedClientFor("model-a")
	if client == nil {
		t.Fatal("expected SQL async client")
	}
	if submitErr := client.Submit(ctx, &GenerateRequest{
		RequestID: requestID,
		Endpoint:  "/v1/completions",
		Params:    map[string]any{"model": "model-a", "prompt": "hello"},
	}); submitErr != nil {
		t.Fatalf("Submit() error: %s", submitErr.Message)
	}

	now := time.Now()
	if err := store.EnsurePartitions(ctx, requestQueue); err != nil {
		t.Fatalf("EnsurePartitions: %v", err)
	}
	if _, err := store.AcquirePartitions(ctx, requestQueue, dispatcherID, now, time.Minute, sqlqueue.Partitions); err != nil {
		t.Fatalf("AcquirePartitions: %v", err)
	}

	dispatched, err := store.Dispatch(ctx, requestQueue, dispatcherID, now, 1)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if len(dispatched) != 1 || dispatched[0].ID != requestID {
		t.Fatalf("dispatched requests = %+v, want %q", dispatched, requestID)
	}

	resultPayload, err := json.Marshal(api.ResultMessage{
		ID:         requestID,
		StatusCode: 200,
		Payload:    `{"choices":[{"text":"hello"}]}`,
	})
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	acked, err := store.Ack(ctx, dispatcherID, []sqlqueue.Completion{{
		Key:       dispatched[0].Key(),
		Attempt:   dispatched[0].Attempt,
		Route:     resultQueue + ":" + consumerID,
		Payload:   string(resultPayload),
		ExpiresAt: time.Now().Add(time.Minute).Unix(),
	}})
	if err != nil {
		t.Fatalf("Ack: %v", err)
	}
	if len(acked) != 1 || !acked[0] {
		t.Fatalf("Ack result = %v, want [true]", acked)
	}

	result, getErr := client.GetResult(ctx)
	if getErr != nil {
		t.Fatalf("GetResult: %v", getErr)
	}
	if result.RequestID != requestID || result.StatusCode != 200 || result.Response == nil {
		t.Fatalf("result = %+v, want successful %q result", result, requestID)
	}
}

// TestAsyncResolver_RecoveryClientFor_crossProcessor is the transport-layer
// regression for cross-processor takeover (#742): processor A receives a
// result under a claim lease and dies before checkpointing it; the lease
// lapses; processor B — a different consumer identity that reclaimed the
// batch — recovers the completed result from A's per-processor route
// through RecoveryClientFor instead of re-running the inference.
func TestAsyncResolver_RecoveryClientFor_crossProcessor(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	newResolver := func(consumer string) *AsyncGatewayResolver {
		t.Helper()
		resolver, err := NewAsyncResolver(AsyncClientConfig{
			RedisURL:          "redis://" + mr.Addr(),
			ConsumerID:        consumer,
			ResultPollTimeout: 2 * time.Second,
			Models: map[string]AsyncModelPoolConfig{
				"m1": {PoolName: "xp-pool"},
			},
		}, testLogger(t))
		if err != nil {
			t.Fatalf("NewAsyncResolver(%s): %v", consumer, err)
		}
		t.Cleanup(func() { _ = resolver.Close() })
		return resolver
	}

	owner := newResolver("proc-a")
	recoverer := newResolver("proc-b")

	// proc-a submits the request; its result route carries the delivery.
	ownerClient := owner.SharedClientFor("m1")
	if submitErr := ownerClient.Submit(context.Background(), &GenerateRequest{
		RequestID: "req-x",
		Endpoint:  "/v1/completions",
		Params:    map[string]any{"model": "m1", "prompt": "hello"},
	}); submitErr != nil {
		t.Fatalf("Submit() error: %s", submitErr.Message)
	}

	ownerRoute := asyncQueuePrefix + "results:xp-pool:proc-a"
	data, err := json.Marshal(api.ResultMessage{
		ID:         "req-x",
		StatusCode: 200,
		Payload:    `{"choices":[{"text":"hello"}]}`,
	})
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if _, lpushErr := mr.Lpush(ownerRoute, string(data)); lpushErr != nil {
		t.Fatalf("Lpush: %v", lpushErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// proc-a claims the delivery, then dies before checkpointing: the ack
	// is never called.
	durableOwner, ok := ownerClient.(DurableResultClient)
	if !ok {
		t.Fatal("the shared async client must expose durable result delivery")
	}
	resp, ack, err := durableOwner.ReceiveResult(ctx)
	if err != nil {
		t.Fatalf("proc-a ReceiveResult: %v", err)
	}
	if resp.RequestID != "req-x" {
		t.Fatalf("proc-a received %q, want req-x", resp.RequestID)
	}
	_ = ack // the crash window

	// The claim lease lapses, as time passage would.
	idxKey := ownerRoute + ":result-claims-idx"
	members, err := rdb.ZRangeWithScores(ctx, idxKey, 0, -1).Result()
	if err != nil || len(members) != 1 {
		t.Fatalf("claims = %v (err %v), want the single unacked claim", members, err)
	}
	if _, err := rdb.ZAdd(ctx, idxKey, redis.Z{
		Member: members[0].Member,
		Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
	}).Result(); err != nil {
		t.Fatalf("backdate claim lease: %v", err)
	}

	// proc-b recovers the completed result from proc-a's route — no
	// re-submission, no re-inference.
	recoveryClient, err := recoverer.RecoveryClientFor("m1", "proc-a")
	if err != nil {
		t.Fatalf("RecoveryClientFor: %v", err)
	}
	recoveryDurable, ok := recoveryClient.(DurableResultClient)
	if !ok {
		t.Fatal("the recovery client must expose durable result delivery")
	}
	resp2, ack2, err := recoveryDurable.ReceiveResult(ctx)
	if err != nil {
		t.Fatalf("proc-b recovery ReceiveResult: %v", err)
	}
	if resp2.RequestID != "req-x" || resp2.StatusCode != 200 || resp2.Response == nil {
		t.Fatalf("recovered response = %+v, want the completed req-x result", resp2)
	}

	// The recovering owner checkpointed the result, so it releases the ack.
	if err := ack2(ctx); err != nil {
		t.Fatalf("recovery ack: %v", err)
	}
	if n, zErr := rdb.ZCard(ctx, idxKey).Result(); zErr != nil || n != 0 {
		t.Fatalf("claims after recovery ack = %d (err %v), want 0", n, zErr)
	}

	// The recovery route is drained: nothing is left stranded on proc-a.
	if n, lErr := rdb.LLen(ctx, ownerRoute).Result(); lErr != nil || n != 0 {
		t.Fatalf("pending after recovery = %d (err %v), want 0", n, lErr)
	}

	// A recovery client is receive-only garbage for unknown models.
	if _, err := recoverer.RecoveryClientFor("nope", "proc-a"); err == nil {
		t.Fatal("RecoveryClientFor for an unknown model must fail")
	}
}
