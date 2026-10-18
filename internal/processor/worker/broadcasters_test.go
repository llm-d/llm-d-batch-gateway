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

package worker

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-logr/logr"
	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/redis/go-redis/v9"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// TestBroadcasterRegistry_RecoveryRoutes pins the cross-processor recovery
// wiring: a job that resumed a batch from a previous submitter consumes
// that submitter's result route through a refcounted recovery broadcaster,
// a stranded delivery routes to the job's pending set, and the last job
// release stops the broadcaster.
func TestBroadcasterRegistry_RecoveryRoutes(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	resolver, err := inference.NewAsyncResolver(inference.AsyncClientConfig{
		RedisURL:          "redis://" + mr.Addr(),
		ConsumerID:        "proc-b",
		ResultPollTimeout: 2 * time.Second,
		Models: map[string]inference.AsyncModelPoolConfig{
			"m1": {PoolName: "rec-pool"},
		},
	}, logr.Discard())
	if err != nil {
		t.Fatalf("NewAsyncResolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	registry := newBroadcasterRegistry(resolver, logr.Discard(), nil)
	ctx, cancel := context.WithCancel(context.Background())
	registry.Run(ctx)
	t.Cleanup(func() {
		cancel()
		registry.Wait()
	})

	modelMap := &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1}

	// The recovering job subscribes through the group and pre-stores its
	// in-flight entry.
	group, release := registry.forModels(modelMap, []string{"proc-a"})
	pending := pipeline.NewPendingRequests(1)
	pending.Store(pipeline.RequestItem{RequestID: "req-stranded", CustomID: "c-1"})
	resultCh := make(chan pipeline.ResultItem, 1)
	group.Subscribe(resultCh, pending.Has)
	group.Start()

	registry.mu.Lock()
	refs := 0
	for _, rb := range registry.recovery {
		refs = rb.refs
	}
	registry.mu.Unlock()
	if refs != 1 {
		t.Fatalf("recovery broadcaster refs = %d, want 1", refs)
	}

	// A completed result for the entry sits on the previous submitter's
	// route; the recovery broadcaster claims it and routes it to the job.
	data, err := json.Marshal(asyncapi.ResultMessage{ID: "req-stranded", StatusCode: 200, Payload: `{"ok":true}`})
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	ownerRoute := "llm-d-async:results:rec-pool:proc-a"
	if _, err := rdb.LPush(context.Background(), ownerRoute, string(data)).Result(); err != nil {
		t.Fatalf("push stranded result: %v", err)
	}

	select {
	case result := <-resultCh:
		if result.RequestID != "req-stranded" {
			t.Fatalf("routed result = %q, want req-stranded", result.RequestID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stranded delivery was not recovered through the previous submitter's route")
	}

	// A second job recovering from the same submitter shares the broadcaster.
	_, release2 := registry.forModels(modelMap, []string{"proc-a"})
	registry.mu.Lock()
	refs = 0
	for _, rb := range registry.recovery {
		refs = rb.refs
	}
	registry.mu.Unlock()
	if refs != 2 {
		t.Fatalf("recovery broadcaster refs after second acquire = %d, want 2", refs)
	}

	release()
	release2()

	// The last release stops the recovery broadcaster.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		registry.mu.Lock()
		n := len(registry.recovery)
		registry.mu.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("recovery broadcaster was not stopped after the last release")
}

// stubEntryStore pins residueGate's verdicts without a database.
type stubEntryStore struct {
	db.ResumableEntryStore
	open bool
}

func (s *stubEntryStore) HasOpenEntryByRequest(context.Context, string) (bool, error) {
	return s.open, nil
}

// blockingEntryStore hangs until its context is cancelled, standing in for
// a database that never answers.
type blockingEntryStore struct {
	db.ResumableEntryStore
}

func (s *blockingEntryStore) HasOpenEntryByRequest(ctx context.Context, _ string) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

// TestResidueGate pins the durable residue check the broadcaster acks
// unrouted deliveries through: an open entry is not safe to acknowledge, a
// terminal one is, and a check that hangs fails closed within the bound
// instead of stalling the broadcaster's routing loop unboundedly.
func TestResidueGate(t *testing.T) {
	t.Run("open entry is not safe", func(t *testing.T) {
		gate := residueGate(&stubEntryStore{open: true}, residueCheckTimeout)
		safe, err := gate(context.Background(), "req-open")
		if err != nil || safe {
			t.Fatalf("gate(open entry) = %v (err %v), want false", safe, err)
		}
	})

	t.Run("terminal entry is safe", func(t *testing.T) {
		gate := residueGate(&stubEntryStore{open: false}, residueCheckTimeout)
		safe, err := gate(context.Background(), "req-terminal")
		if err != nil || !safe {
			t.Fatalf("gate(terminal entry) = %v (err %v), want true", safe, err)
		}
	})

	t.Run("hung check fails closed within the bound", func(t *testing.T) {
		const timeout = 50 * time.Millisecond
		gate := residueGate(&blockingEntryStore{}, timeout)

		start := time.Now()
		safe, err := gate(context.Background(), "req-hung")
		elapsed := time.Since(start)

		if err == nil {
			t.Fatal("hung check did not surface an error")
		}
		if safe {
			t.Fatal("hung check must fail closed, not acknowledge")
		}
		if elapsed > time.Second {
			t.Fatalf("hung check returned after %v, want within ~%v", elapsed, timeout)
		}
	})
}

// TestBroadcasterRegistry_RecoveryGatedOnSubscription pins the ordering
// contract: a recovery broadcaster must not claim a delivery already sitting
// on the previous submitter's route until the acquiring job has subscribed.
// A claim with no subscriber routes to nobody and is held unacked for the
// whole claim lease; gating consumption on Subscribe avoids that delay.
func TestBroadcasterRegistry_RecoveryGatedOnSubscription(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	resolver, err := inference.NewAsyncResolver(inference.AsyncClientConfig{
		RedisURL:          "redis://" + mr.Addr(),
		ConsumerID:        "proc-b",
		ResultPollTimeout: 2 * time.Second,
		Models: map[string]inference.AsyncModelPoolConfig{
			"m1": {PoolName: "gate-pool"},
		},
	}, logr.Discard())
	if err != nil {
		t.Fatalf("NewAsyncResolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	registry := newBroadcasterRegistry(resolver, logr.Discard(), func(context.Context, string) (bool, error) {
		return false, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	registry.Run(ctx)
	t.Cleanup(func() {
		cancel()
		registry.Wait()
	})

	// A completed result for the recovered entry is already on proc-a's
	// route before the job's group is even built.
	ownerRoute := "llm-d-async:results:gate-pool:proc-a"
	idxKey := ownerRoute + ":result-claims-idx"
	data, err := json.Marshal(asyncapi.ResultMessage{ID: "req-gated", StatusCode: 200, Payload: `{"ok":true}`})
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if _, err := rdb.LPush(context.Background(), ownerRoute, string(data)).Result(); err != nil {
		t.Fatalf("push result: %v", err)
	}

	modelMap := &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1}
	group, release := registry.forModels(modelMap, []string{"proc-a"})
	defer release()

	// Not yet subscribed or started: the broadcaster must leave the delivery
	// unclaimed, not consume it into a lease.
	time.Sleep(300 * time.Millisecond)
	if n, err := rdb.ZCard(context.Background(), idxKey).Result(); err != nil {
		t.Fatalf("read claims: %v", err)
	} else if n != 0 {
		t.Fatalf("delivery was claimed before the job subscribed (%d claims)", n)
	}

	// Subscribe first, then Start: the claim happens with the subscriber
	// present and routes immediately.
	pending := pipeline.NewPendingRequests(1)
	pending.Store(pipeline.RequestItem{RequestID: "req-gated", CustomID: "c-1"})
	resultCh := make(chan pipeline.ResultItem, 1)
	group.Subscribe(resultCh, pending.Has)
	group.Start()

	select {
	case result := <-resultCh:
		if result.RequestID != "req-gated" {
			t.Fatalf("routed result = %q, want req-gated", result.RequestID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("gated delivery was not routed after subscribe+start")
	}
}

// TestBroadcasterRegistry_MisClaimCompetingReplicas pins the documented
// cross-replica limitation and its bound: two replacement processors that
// recovered different batches from the same dead submitter both consume that
// submitter's route, so either can claim the other's result. The foreign
// claim is held unacked — the residue gate sees the still-open entry — under
// the short recovery lease, and once that lease lapses the delivery reaches
// its rightful owner instead of re-running inference.
func TestBroadcasterRegistry_MisClaimCompetingReplicas(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	newResolver := func(consumerID string) *inference.AsyncGatewayResolver {
		t.Helper()
		r, err := inference.NewAsyncResolver(inference.AsyncClientConfig{
			RedisURL:          "redis://" + mr.Addr(),
			ConsumerID:        consumerID,
			ResultPollTimeout: 2 * time.Second,
			Models: map[string]inference.AsyncModelPoolConfig{
				"m1": {PoolName: "comp-pool"},
			},
		}, logr.Discard())
		if err != nil {
			t.Fatalf("NewAsyncResolver(%s): %v", consumerID, err)
		}
		t.Cleanup(func() { _ = r.Close() })
		return r
	}
	openEntry := func(context.Context, string) (bool, error) { return false, nil }

	// Two replacement processors recovering different batches from the same
	// dead submitter proc-a: both consume proc-a's result route.
	regB := newBroadcasterRegistry(newResolver("proc-b"), logr.Discard(), openEntry)
	regC := newBroadcasterRegistry(newResolver("proc-c"), logr.Discard(), openEntry)
	ctxB, cancelB := context.WithCancel(context.Background())
	ctxC, cancelC := context.WithCancel(context.Background())
	regB.Run(ctxB)
	regC.Run(ctxC)
	t.Cleanup(func() {
		cancelB()
		cancelC()
		regB.Wait()
		regC.Wait()
	})

	ownerRoute := "llm-d-async:results:comp-pool:proc-a"
	idxKey := ownerRoute + ":result-claims-idx"
	data, err := json.Marshal(asyncapi.ResultMessage{ID: "req-owned", StatusCode: 200, Payload: `{"ok":true}`})
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if _, err := rdb.LPush(context.Background(), ownerRoute, string(data)).Result(); err != nil {
		t.Fatalf("push result: %v", err)
	}

	modelMap := &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1}

	// Processor B's job does not own req-owned: its recovery broadcaster
	// claims the delivery and, seeing an open entry, holds it unacked.
	groupB, releaseB := regB.forModels(modelMap, []string{"proc-a"})
	defer releaseB()
	pendingB := pipeline.NewPendingRequests(1)
	chB := make(chan pipeline.ResultItem, 1)
	groupB.Subscribe(chB, pendingB.Has)
	groupB.Start()

	var lease time.Duration
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		members, err := rdb.ZRangeWithScores(context.Background(), idxKey, 0, -1).Result()
		if err != nil {
			t.Fatalf("read claims: %v", err)
		}
		if len(members) == 1 {
			lease = time.Duration(members[0].Score-float64(time.Now().UnixMilli())) * time.Millisecond
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lease <= 0 {
		t.Fatal("the foreign processor did not hold the mis-claimed delivery as an unacked claim")
	}
	// The recovery claim lease is short (60s), not the 5m crash-detection
	// window: a mis-claim costs one brief bounce.
	if lease > 90*time.Second {
		t.Fatalf("recovery claim lease = %v, want the short recovery lease (<=90s)", lease)
	}

	// Processor C owns the request; its job subscribes and starts.
	groupC, releaseC := regC.forModels(modelMap, []string{"proc-a"})
	defer releaseC()
	pendingC := pipeline.NewPendingRequests(1)
	pendingC.Store(pipeline.RequestItem{RequestID: "req-owned", CustomID: "c-1"})
	chC := make(chan pipeline.ResultItem, 1)
	groupC.Subscribe(chC, pendingC.Has)

	// B's batch ends elsewhere; its stale claim lapses and the redelivery
	// reaches its rightful owner.
	cancelB()
	regB.Wait()
	if _, err := rdb.ZAdd(context.Background(), idxKey, redis.Z{
		Member: "req-owned",
		Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
	}).Result(); err != nil {
		t.Fatalf("backdate claim lease: %v", err)
	}
	groupC.Start()

	select {
	case result := <-chC:
		if result.RequestID != "req-owned" {
			t.Fatalf("routed result = %q, want req-owned", result.RequestID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the mis-claimed delivery did not reach its owner after the lease lapsed")
	}
}
