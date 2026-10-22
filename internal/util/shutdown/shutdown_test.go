package shutdown

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/go-logr/logr"
)

func TestCoordinatorRun(t *testing.T) {
	t.Run("runs phases in order", func(t *testing.T) {
		var got []string
		coordinator := New(logr.Discard(), time.Second)
		coordinator.Add(Phase{Name: "stop intake", Run: func(context.Context) error {
			got = append(got, "stop intake")
			return nil
		}})
		coordinator.Add(Phase{Name: "drain", Run: func(context.Context) error {
			got = append(got, "drain")
			return nil
		}})
		coordinator.Add(Phase{Name: "close clients", Run: func(context.Context) error {
			got = append(got, "close clients")
			return nil
		}})

		if err := coordinator.Run(context.Background()); err != nil {
			t.Fatalf("Run() error = %v", err)
		}
		want := []string{"stop intake", "drain", "close clients"}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("phase order = %v, want %v", got, want)
		}
	})

	t.Run("continues after phase error", func(t *testing.T) {
		wantErr := errors.New("drain failed")
		var got []string
		coordinator := New(logr.Discard(), time.Second)
		coordinator.Add(Phase{Name: "drain", Run: func(context.Context) error {
			got = append(got, "drain")
			return wantErr
		}})
		coordinator.Add(Phase{Name: "close clients", Run: func(context.Context) error {
			got = append(got, "close clients")
			return nil
		}})

		if err := coordinator.Run(context.Background()); !errors.Is(err, wantErr) {
			t.Fatalf("Run() error = %v, want %v", err, wantErr)
		}
		if want := []string{"drain", "close clients"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("phase order = %v, want %v", got, want)
		}
	})

	t.Run("honors overall budget", func(t *testing.T) {
		coordinator := New(logr.Discard(), 20*time.Millisecond)
		coordinator.Add(Phase{Name: "slow", Run: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}})
		coordinator.Add(Phase{Name: "after timeout", Run: func(ctx context.Context) error {
			if ctx.Err() == nil {
				t.Error("phase context error = nil, want deadline exceeded")
			}
			return nil
		}})

		if err := coordinator.Run(context.Background()); err == nil {
			t.Fatal("Run() error = nil, want timeout error")
		}
	})

	t.Run("rejects a non-positive budget", func(t *testing.T) {
		coordinator := New(logr.Discard(), 0)
		if err := coordinator.Run(context.Background()); err == nil {
			t.Fatal("Run() error = nil, want error for non-positive budget")
		}
	})
}

func TestGuard(t *testing.T) {
	t.Run("cleanup runs fn while armed", func(t *testing.T) {
		called := false
		g := NewGuard(logr.Discard(), "test", time.Second, func(context.Context) error {
			called = true
			return nil
		})
		g.Cleanup()
		if !called {
			t.Fatal("Cleanup() did not run fn while armed")
		}
	})

	t.Run("cleanup is a no-op after disarm", func(t *testing.T) {
		called := false
		g := NewGuard(logr.Discard(), "test", time.Second, func(context.Context) error {
			called = true
			return nil
		})
		g.Disarm()
		g.Cleanup()
		if called {
			t.Fatal("Cleanup() ran fn after Disarm()")
		}
	})

	t.Run("cleanup logs fn error", func(t *testing.T) {
		wantErr := errors.New("cleanup failed")
		g := NewGuard(logr.Discard(), "test", time.Second, func(context.Context) error {
			return wantErr
		})
		g.Cleanup() // must not panic; error is logged, not returned
	})
}

func TestWatchIntake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopped := make(chan struct{})
	WatchIntake(ctx, func() { close(stopped) })

	select {
	case <-stopped:
		t.Fatal("stop called before ctx was done")
	case <-time.After(20 * time.Millisecond):
	}

	cancel()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("stop not called after ctx was done")
	}
}
