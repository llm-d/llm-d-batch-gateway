package worker

import (
	"context"
	"sync"
	"time"

	"github.com/go-logr/logr"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// residueCheckTimeout bounds the durable residue check so a slow or hung
// database stalls the routing of at most one route for this long. A timeout
// is a failed check: the delivery stays unacknowledged for lease redelivery
// (fail-closed), which the transport already tolerates.
const residueCheckTimeout = 2 * time.Second

// residueGate adapts an entry store into the broadcaster's durable residue
// check: a delivery may be acknowledged only when no open entry can still
// account for it. The lookup runs in the broadcaster's receive loop, so it
// is bounded by residueCheckTimeout.
func residueGate(entries db.ResumableEntryStore, timeout time.Duration) func(context.Context, string) (bool, error) {
	return func(ctx context.Context, requestID string) (bool, error) {
		checkCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		open, err := entries.HasOpenEntryByRequest(checkCtx, requestID)
		if err != nil {
			return false, err
		}
		return !open, nil
	}
}

// broadcasterRegistry manages per-model ResultBroadcasters.
// Shared across jobs, lives on Processor.
type broadcasterRegistry struct {
	broadcasters map[string]*pipeline.ResultBroadcaster
	wg           sync.WaitGroup

	// residueSafe durably verifies an unrouted delivery before the
	// broadcaster acknowledges it; wired to the entry store.
	residueSafe func(ctx context.Context, requestID string) (bool, error)
	resolver    *inference.AsyncGatewayResolver
	logger      logr.Logger

	mu       sync.Mutex
	baseCtx  context.Context
	recovery map[recoveryRouteKey]*recoveryBroadcaster
}

// recoveryRouteKey identifies one foreign result route: the model whose
// pool carries the route and the processor identity that consumed it.
type recoveryRouteKey struct {
	model    string
	consumer string
}

// recoveryBroadcaster is a refcounted broadcaster over a previous
// submitter's result route. Jobs recovering a batch acquire it for the
// batch's models; the last release stops it.
type recoveryBroadcaster struct {
	b      *pipeline.ResultBroadcaster
	refs   int
	cancel context.CancelFunc
}

func newBroadcasterRegistry(resolver *inference.AsyncGatewayResolver, logger logr.Logger, residueSafe func(ctx context.Context, requestID string) (bool, error)) *broadcasterRegistry {
	broadcasters := make(map[string]*pipeline.ResultBroadcaster)
	for _, modelID := range resolver.Models() {
		client := resolver.SharedClientFor(modelID)
		if client == nil {
			continue
		}
		b := pipeline.NewResultBroadcaster(client, logger.WithValues("model", modelID))
		if residueSafe != nil {
			b = b.WithResidueSafetyCheck(residueSafe)
		}
		broadcasters[modelID] = b
	}
	return &broadcasterRegistry{
		broadcasters: broadcasters,
		residueSafe:  residueSafe,
		resolver:     resolver,
		logger:       logger,
		recovery:     make(map[recoveryRouteKey]*recoveryBroadcaster),
	}
}

func (r *broadcasterRegistry) Run(ctx context.Context) {
	r.mu.Lock()
	r.baseCtx = ctx
	r.mu.Unlock()

	for _, b := range r.broadcasters {
		r.wg.Add(1)
		go func() {
			defer r.wg.Done()
			b.Run(ctx)
		}()
	}
}

func (r *broadcasterRegistry) Wait() {
	r.wg.Wait()
}

// forModels returns the broadcaster group a job on the given models
// subscribes to: this processor's per-model broadcasters plus, for every
// previous submitter of an in-flight entry, a recovery broadcaster that
// consumes that submitter's result route. The returned release function
// drops the job's references; the last release stops the recovery
// broadcasters it no longer needs.
func (r *broadcasterRegistry) forModels(modelMap *modelMapFile, submitters []string) (*pipeline.BroadcasterGroup, func()) {
	group := make([]*pipeline.ResultBroadcaster, 0, len(modelMap.SafeToModel))
	for _, modelID := range modelMap.SafeToModel {
		if b, ok := r.broadcasters[modelID]; ok {
			group = append(group, b)
		}
	}

	var acquired []recoveryRouteKey
	for _, consumer := range submitters {
		if consumer == "" {
			continue
		}
		for _, modelID := range modelMap.SafeToModel {
			if b := r.acquireRecovery(modelID, consumer); b != nil {
				group = append(group, b)
				acquired = append(acquired, recoveryRouteKey{model: modelID, consumer: consumer})
			}
		}
	}

	once := sync.Once{}
	return pipeline.NewBroadcasterGroup(group), func() {
		once.Do(func() {
			for _, key := range acquired {
				r.releaseRecovery(key)
			}
		})
	}
}

// acquireRecovery returns the recovery broadcaster for a foreign result
// route, creating and starting it on first use. It returns nil when the
// route cannot be built (e.g. a test resolver without Redis) — recovery
// then degrades to re-submission after the recovery window.
func (r *broadcasterRegistry) acquireRecovery(modelID, consumerID string) *pipeline.ResultBroadcaster {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := recoveryRouteKey{model: modelID, consumer: consumerID}
	if rb, ok := r.recovery[key]; ok {
		rb.refs++
		return rb.b
	}
	if r.baseCtx == nil || r.baseCtx.Err() != nil {
		return nil
	}
	client, err := r.resolver.RecoveryClientFor(modelID, consumerID)
	if err != nil {
		r.logger.Info("Recovery route unavailable; in-flight entries fall back to re-submission",
			"model", modelID, "consumer", consumerID, "err", err)
		return nil
	}
	ctx, cancel := context.WithCancel(r.baseCtx)
	b := pipeline.NewResultBroadcaster(client, r.logger.WithValues("model", modelID, "recoveryFor", consumerID))
	if r.residueSafe != nil {
		b = b.WithResidueSafetyCheck(r.residueSafe)
	}
	rb := &recoveryBroadcaster{b: b, cancel: cancel}
	r.recovery[key] = rb
	rb.refs++
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		b.Run(ctx)
	}()
	r.logger.Info("Started recovery broadcaster for a previous submitter's result route",
		"model", modelID, "consumer", consumerID)
	return b
}

// releaseRecovery drops one reference; the last release stops the
// broadcaster. A re-acquire while the stopped loop is still draining
// creates a fresh broadcaster on the same route — claims are exclusive,
// so a brief overlap is harmless.
func (r *broadcasterRegistry) releaseRecovery(key recoveryRouteKey) {
	r.mu.Lock()
	defer r.mu.Unlock()

	rb, ok := r.recovery[key]
	if !ok || rb.refs <= 0 {
		return
	}
	rb.refs--
	if rb.refs > 0 {
		return
	}
	delete(r.recovery, key)
	rb.cancel()
	r.logger.Info("Stopped recovery broadcaster", "model", key.model, "consumer", key.consumer)
}
