// internal/util/shutdown/shutdown.go

package shutdown

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
)

// DefaultSlack is the recommended buffer added on top of the sum of a
// coordinator's phase timeouts when deriving its overall budget, so callers
// don't each invent their own padding.
const DefaultSlack = 10 * time.Second

// Phase is one ordered step in graceful shutdown. Timeout, if positive,
// bounds this phase with its own child context; zero or negative leaves it
// running under the Coordinator's overall budget deadline instead (unlike
// Guard, where timeout <= 0 means unbounded — see NewGuard).
type Phase struct {
	Name    string
	Timeout time.Duration
	Run     func(context.Context) error
}

// WatchIntake calls stop as soon as ctx is done, ahead of any drain that
// follows. It runs independently of a Coordinator: for workloads whose
// drain loop blocks the goroutine that will eventually call Coordinator.Run,
// waiting for the coordinator's own phases would stop intake too late.
func WatchIntake(ctx context.Context, stop func()) {
	go func() {
		<-ctx.Done()
		stop()
	}()
}

// Guard is a safety net for a resource a Coordinator will eventually take
// ownership of. If the caller returns before Disarm is called (e.g. a later
// initialization step fails), Cleanup runs fn once as a fallback so the
// resource isn't leaked; once Disarm has been called, Cleanup is a no-op.
type Guard struct {
	logger  logr.Logger
	name    string
	timeout time.Duration
	fn      func(context.Context) error
	armed   bool
}

// NewGuard creates an armed Guard for fn. Callers must `defer g.Cleanup()`
// right after creating it, then call Disarm once fn's resource has been
// registered as a Coordinator phase. Pass timeout <= 0 if fn ignores its
// context (e.g. it wraps a Close() method with no context parameter) — a
// timeout can't bound a call that never checks it, so Cleanup runs fn
// synchronously with no deadline instead of implying an unenforced one.
func NewGuard(logger logr.Logger, name string, timeout time.Duration, fn func(context.Context) error) *Guard {
	return &Guard{logger: logger, name: name, timeout: timeout, fn: fn, armed: true}
}

// Disarm marks the guarded resource as owned elsewhere, so Cleanup no
// longer runs fn.
func (g *Guard) Disarm() {
	g.armed = false
}

// Cleanup runs fn if the guard is still armed. Intended to be deferred.
func (g *Guard) Cleanup() {
	if !g.armed {
		return
	}

	if g.timeout <= 0 {
		if err := g.fn(context.Background()); err != nil {
			g.logger.Error(err, "safety-net cleanup failed", "phase", g.name)
		}
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), g.timeout)
	defer cancel()
	if err := g.fn(ctx); err != nil {
		g.logger.Error(err, "safety-net cleanup failed", "phase", g.name)
	}
}

// Coordinator runs shutdown phases within one overall budget.
type Coordinator struct {
	logger logr.Logger
	budget time.Duration
	phases []Phase
}

func New(logger logr.Logger, budget time.Duration) *Coordinator {
	return &Coordinator{logger: logger, budget: budget}
}

func (c *Coordinator) Add(phase Phase) {
	c.phases = append(c.phases, phase)
}

// Run executes every phase in registration order. A phase failure is recorded,
// but does not prevent later cleanup phases from running.
func (c *Coordinator) Run(parent context.Context) error {
	if c.budget <= 0 {
		return errors.New("shutdown budget must be positive")
	}

	ctx, cancel := context.WithTimeout(parent, c.budget)
	defer cancel()

	var shutdownErr error
	for _, phase := range c.phases {
		if phase.Run == nil {
			continue
		}

		phaseCtx := ctx
		phaseCancel := func() {}
		if phase.Timeout > 0 {
			phaseCtx, phaseCancel = context.WithTimeout(ctx, phase.Timeout)
		}

		c.logger.Info("running shutdown phase", "phase", phase.Name)
		err := phase.Run(phaseCtx)
		phaseCancel()
		if err != nil {
			shutdownErr = errors.Join(shutdownErr, fmt.Errorf("shutdown phase %q: %w", phase.Name, err))
		}
	}

	return shutdownErr
}
