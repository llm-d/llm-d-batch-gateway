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

// TestBroadcasterRegistry_MisClaimConverges pins the cross-processor
// mis-claim path: a recovery broadcaster can claim a delivery whose entry
// another processor owns (two processors recovering from the same dead
// submitter split the route's claims). The foreign claim must be held
// unacked — the durable residue gate sees the open entry — under the short
// recovery lease, and the delivery must reach its owner after that lease
// lapses, instead of bouncing for a full crash-detection window.
func TestBroadcasterRegistry_MisClaimConverges(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	resolver, err := inference.NewAsyncResolver(inference.AsyncClientConfig{
		RedisURL:          "redis://" + mr.Addr(),
		ConsumerID:        "proc-b",
		ResultPollTimeout: 2 * time.Second,
		Models: map[string]inference.AsyncModelPoolConfig{
			"m1": {PoolName: "mis-pool"},
		},
	}, logr.Discard())
	if err != nil {
		t.Fatalf("NewAsyncResolver: %v", err)
	}
	t.Cleanup(func() { _ = resolver.Close() })

	// The residue gate reports every entry open: nothing may be discarded.
	registry := newBroadcasterRegistry(resolver, logr.Discard(), func(context.Context, string) (bool, error) {
		return false, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	registry.Run(ctx)
	t.Cleanup(func() {
		cancel()
		registry.Wait()
	})

	modelMap := &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1}
	ownerPending := pipeline.NewPendingRequests(1)
	ownerCh := make(chan pipeline.ResultItem, 1)

	// The wrong consumer phase: the recovery broadcaster runs with no owner
	// subscribed for the delivery (the owning job sits on another processor).
	group, release := registry.forModels(modelMap, []string{"proc-a"})
	defer release()

	ownerRoute := "llm-d-async:results:mis-pool:proc-a"
	data, err := json.Marshal(asyncapi.ResultMessage{ID: "req-foreign", StatusCode: 200, Payload: `{"ok":true}`})
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	if _, err := rdb.LPush(context.Background(), ownerRoute, string(data)).Result(); err != nil {
		t.Fatalf("push result: %v", err)
	}

	// The mis-claimed delivery is held unacked: the claim sits in the route's
	// expiry index under the short recovery lease.
	idxKey := ownerRoute + ":result-claims-idx"
	deadline := time.Now().Add(5 * time.Second)
	var lease time.Duration
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
		t.Fatal("the mis-claimed delivery was not held as an unacked claim")
	}
	// The recovery claim lease is short (60s), not the 5m crash-detection
	// window: a mis-claim costs one brief bounce.
	if lease > 90*time.Second {
		t.Fatalf("recovery claim lease = %v, want the short recovery lease (<=90s)", lease)
	}

	// The owning job appears (here: on this registry) and pre-stores the
	// request; the short lease lapses, and the redelivery routes to it.
	ownerPending.Store(pipeline.RequestItem{RequestID: "req-foreign", CustomID: "c-1"})
	group.Subscribe(ownerCh, ownerPending.Has)
	if _, err := rdb.ZAdd(context.Background(), idxKey, redis.Z{
		Member: "req-foreign",
		Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
	}).Result(); err != nil {
		t.Fatalf("backdate claim lease: %v", err)
	}

	select {
	case result := <-ownerCh:
		if result.RequestID != "req-foreign" {
			t.Fatalf("routed result = %q, want req-foreign", result.RequestID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the mis-claimed delivery did not reach its owner after the lease lapsed")
	}
}
