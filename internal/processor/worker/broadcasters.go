package worker

import (
	"context"
	"sync"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// broadcasterRegistry manages per-model ResultBroadcasters.
// Shared across jobs, lives on Processor.
type broadcasterRegistry struct {
	broadcasters map[string]*pipeline.ResultBroadcaster
	wg           sync.WaitGroup
}

func newBroadcasterRegistry(resolver *inference.AsyncGatewayResolver, logger logr.Logger) *broadcasterRegistry {
	models := resolver.Models()
	broadcasters := make(map[string]*pipeline.ResultBroadcaster, len(models))
	for _, modelID := range models {
		client := resolver.SharedClientFor(modelID)
		if client == nil {
			continue
		}
		broadcasters[modelID] = pipeline.NewResultBroadcaster(client, logger.WithValues("model", modelID))
	}
	return &broadcasterRegistry{broadcasters: broadcasters}
}

func (r *broadcasterRegistry) Run(ctx context.Context) {
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

// forModelNames returns the broadcasters for an explicit set of models, used
// by sequential input where the model list comes from the manifest rather than
// from per-model plan files.
//
// Subscribing narrowly matters for isolation: a broadcaster delivers to its
// subscribers with a blocking send, so a job subscribed to a model it never
// uses can still stall delivery for every other job on that model.
//
// An unknown model simply has no broadcaster. Nothing is lost: it has no
// client either, so the dispatcher reports it as model_not_found locally.
// An empty list means the manifest did not record one, which must not be read
// as "subscribe to nothing" — that would leave the job waiting for results
// that are delivered elsewhere. Subscribe to everything instead: less
// isolation, but results are matched by request ID and PendingRequests drops
// any belonging to another job, so it is always correct.
func (r *broadcasterRegistry) forModelNames(models []string) *pipeline.BroadcasterGroup {
	if len(models) == 0 {
		return r.all()
	}
	result := make([]*pipeline.ResultBroadcaster, 0, len(models))
	for _, modelID := range models {
		if b, ok := r.broadcasters[modelID]; ok {
			result = append(result, b)
		}
	}
	return pipeline.NewBroadcasterGroup(result)
}

// all returns every broadcaster the processor runs.
func (r *broadcasterRegistry) all() *pipeline.BroadcasterGroup {
	result := make([]*pipeline.ResultBroadcaster, 0, len(r.broadcasters))
	for _, b := range r.broadcasters {
		result = append(result, b)
	}
	return pipeline.NewBroadcasterGroup(result)
}

func (r *broadcasterRegistry) forModels(modelMap *modelMapFile) *pipeline.BroadcasterGroup {
	var result []*pipeline.ResultBroadcaster
	for _, modelID := range modelMap.SafeToModel {
		if b, ok := r.broadcasters[modelID]; ok {
			result = append(result, b)
		}
	}
	return pipeline.NewBroadcasterGroup(result)
}
