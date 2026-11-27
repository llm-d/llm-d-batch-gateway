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
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

type fakePurgeClient struct {
	mu     sync.Mutex
	calls  int
	purged int64 // returned on each call
	err    error // returned on each call
}

func (f *fakePurgeClient) PurgeExpiredEvents(_ context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.purged, f.err
}

func (f *fakePurgeClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// runLoopUntilTwoCalls starts a purger over the client, waits until the
// client has been called at least twice (immediate purge plus one tick),
// cancels, and verifies RunLoop returns context.Canceled.
func runLoopUntilTwoCalls(t *testing.T, client *fakePurgeClient, onPurge func(purged int64, err error)) {
	t.Helper()

	purger, err := New(client, 20*time.Millisecond, onPurge)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- purger.RunLoop(logr.NewContext(ctx, logr.Discard())) }()

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
}

// purgeResult records one onPurge callback invocation.
type purgeResult struct {
	purged int64
	err    error
}

func TestNew(t *testing.T) {
	t.Run("rejects nil client", func(t *testing.T) {
		if _, err := New(nil, time.Second, nil); err == nil {
			t.Fatal("expected error for nil client")
		}
	})

	t.Run("rejects non-positive interval", func(t *testing.T) {
		client := &fakePurgeClient{}
		for _, interval := range []time.Duration{0, -time.Second} {
			if _, err := New(client, interval, nil); err == nil {
				t.Fatalf("expected error for interval %v", interval)
			}
		}
	})

	t.Run("accepts valid arguments", func(t *testing.T) {
		if _, err := New(&fakePurgeClient{}, time.Second, nil); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestPurgerRunLoop(t *testing.T) {
	t.Run("purges immediately on start and again on tick, reporting each result", func(t *testing.T) {
		client := &fakePurgeClient{purged: 1}

		var mu sync.Mutex
		var results []purgeResult
		runLoopUntilTwoCalls(t, client, func(purged int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			results = append(results, purgeResult{purged: purged, err: err})
		})

		mu.Lock()
		defer mu.Unlock()
		if len(results) < 2 {
			t.Fatalf("onPurge calls = %d, want >= 2 (immediate + at least one tick)", len(results))
		}
		for i, r := range results {
			if r.err != nil {
				t.Fatalf("onPurge result %d err = %v, want nil", i, r.err)
			}
			if r.purged != 1 {
				t.Fatalf("onPurge result %d purged = %d, want 1", i, r.purged)
			}
		}
	})

	t.Run("purge failure is reported and the loop continues", func(t *testing.T) {
		purgeErr := errors.New("boom")
		client := &fakePurgeClient{err: purgeErr}

		var mu sync.Mutex
		var results []purgeResult
		runLoopUntilTwoCalls(t, client, func(purged int64, err error) {
			mu.Lock()
			defer mu.Unlock()
			results = append(results, purgeResult{purged: purged, err: err})
		})

		mu.Lock()
		defer mu.Unlock()
		if len(results) < 2 {
			t.Fatalf("onPurge calls = %d, want >= 2 (loop must continue past failures)", len(results))
		}
		for i, r := range results {
			if !errors.Is(r.err, purgeErr) {
				t.Fatalf("onPurge result %d err = %v, want the purge error", i, r.err)
			}
		}
	})

	t.Run("nil onPurge callback is allowed", func(t *testing.T) {
		runLoopUntilTwoCalls(t, &fakePurgeClient{purged: 1}, nil)
	})

	t.Run("returns context error on already-cancelled context", func(t *testing.T) {
		purger, err := New(&fakePurgeClient{}, time.Second, nil)
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
