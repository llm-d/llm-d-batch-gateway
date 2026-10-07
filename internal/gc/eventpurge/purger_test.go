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

package eventpurge

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/gc/metrics"
)

// TestMain initializes the shared Prometheus metrics so runOnce can record
// them; the production binary does this in main before starting the purger.
func TestMain(m *testing.M) {
	if err := metrics.InitMetrics(); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

type fakePurgeClient struct {
	mu     sync.Mutex
	calls  int
	purged int64       // returned on each call
	err    error       // returned on each call
	onCall func(n int) // optional hook invoked on each call
}

func (f *fakePurgeClient) PurgeExpiredEvents(_ context.Context) (int64, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	purged, err := f.purged, f.err
	f.mu.Unlock()
	if f.onCall != nil {
		f.onCall(n)
	}
	return purged, err
}

func (f *fakePurgeClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestNew(t *testing.T) {
	t.Run("rejects nil client", func(t *testing.T) {
		if _, err := New(nil, time.Second); err == nil {
			t.Fatal("expected error for nil client")
		}
	})

	t.Run("rejects non-positive interval", func(t *testing.T) {
		client := &fakePurgeClient{}
		for _, interval := range []time.Duration{0, -time.Second} {
			if _, err := New(client, interval); err == nil {
				t.Fatalf("expected error for interval %v", interval)
			}
		}
	})

	t.Run("accepts valid arguments", func(t *testing.T) {
		if _, err := New(&fakePurgeClient{}, time.Second); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestPurgerRunLoop(t *testing.T) {
	t.Run("purges immediately on start and again on tick", func(t *testing.T) {
		client := &fakePurgeClient{purged: 1}
		purger, err := New(client, 20*time.Millisecond)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- purger.RunLoop(logr.NewContext(ctx, logr.Discard())) }()

		// The immediate purge happens before the first tick.
		deadline := time.Now().Add(2 * time.Second)
		for client.callCount() < 2 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		cancel()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("RunLoop = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("RunLoop did not return after cancel")
		}
		if got := client.callCount(); got < 2 {
			t.Fatalf("purge calls = %d, want >= 2 (immediate + at least one tick)", got)
		}
	})

	t.Run("purge failure is recorded and the loop continues", func(t *testing.T) {
		client := &fakePurgeClient{err: errors.New("boom")}
		purger, err := New(client, 20*time.Millisecond)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- purger.RunLoop(logr.NewContext(ctx, logr.Discard())) }()

		// The failed purge must not stop the loop.
		deadline := time.Now().Add(2 * time.Second)
		for client.callCount() < 2 && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		cancel()

		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("RunLoop = %v, want context.Canceled", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("RunLoop did not return after cancel")
		}
		if got := client.callCount(); got < 2 {
			t.Fatalf("purge calls = %d, want >= 2 (loop must continue past failures)", got)
		}
	})

	t.Run("returns context error on already-cancelled context", func(t *testing.T) {
		purger, err := New(&fakePurgeClient{}, time.Second)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := purger.RunLoop(logr.NewContext(ctx, logr.Discard())); !errors.Is(err, context.Canceled) {
			t.Fatalf("RunLoop on cancelled context = %v, want context.Canceled", err)
		}
	})
}
