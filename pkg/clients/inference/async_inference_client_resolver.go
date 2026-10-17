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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/producer"
	"github.com/redis/go-redis/v9"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/syncutil"
)

const asyncQueuePrefix = "llm-d-async:"

// AsyncModelPoolConfig holds the resolved pool and queue settings for one async model.
type AsyncModelPoolConfig struct {
	PoolName         string
	RequestQueueName string
	ResultQueueName  string
}

// AsyncClientConfig holds the resolved configuration for async dispatch.
type AsyncClientConfig struct {
	RedisURL          string
	ConsumerID        string // identity of this Batch Processor replica
	Models            map[string]AsyncModelPoolConfig
	ResultPollTimeout time.Duration // per-poll timeout in the result dispatcher loop
}

// AsyncGatewayResolver routes models to shared AsyncInferenceClient instances.
// Immutable after construction — safe for concurrent reads.
type AsyncGatewayResolver struct {
	pools           map[string]*asyncPool                          // model → pool
	sharedClients   *syncutil.MutexMap[string, *asyncSharedClient] // model → shared client
	closers         []io.Closer
	clientFactories map[string]func() AsyncInferenceClient // test-only override
	// routeBases maps each model to its result-route prefix (before the
	// consumer suffix); rdb and resultPollTimeout rebuild producers that
	// consume another processor's route for cross-processor recovery.
	routeBases        map[string]string
	rdb               redis.UniversalClient
	resultPollTimeout time.Duration
	logger            logr.Logger
}

// Models returns all configured model IDs.
func (r *AsyncGatewayResolver) Models() []string {
	if r.clientFactories != nil {
		models := make([]string, 0, len(r.clientFactories))
		for m := range r.clientFactories {
			models = append(models, m)
		}
		return models
	}
	models := make([]string, 0, len(r.pools))
	for m := range r.pools {
		models = append(models, m)
	}
	return models
}

// SharedClientFor returns a shared client for the given model.
// Reuses the same client across calls. Results for this Processor replica are
// isolated on its consumer-specific result queue.
func (r *AsyncGatewayResolver) SharedClientFor(modelID string) AsyncInferenceClient {
	if r.clientFactories != nil {
		if factory, ok := r.clientFactories[modelID]; ok {
			return factory()
		}
		return nil
	}
	if c, ok := r.sharedClients.Load(modelID); ok {
		return c
	}
	pool, ok := r.pools[modelID]
	if !ok {
		return nil
	}
	c := newAsyncSharedClient(pool.producer, pool.pollTimeout, r.logger.WithValues("model", modelID))
	actual, _ := r.sharedClients.LoadOrStore(modelID, c)
	return actual
}

// NewTestAsyncResolver creates a resolver backed by factory functions instead of
// real Redis connections. Each call to SharedClientFor invokes the corresponding factory.
func NewTestAsyncResolver(factories map[string]func() AsyncInferenceClient) *AsyncGatewayResolver {
	return &AsyncGatewayResolver{clientFactories: factories}
}

// Close releases resources held by the resolver (producers, Redis).
func (r *AsyncGatewayResolver) Close() error {
	var errs []error
	for _, c := range r.closers {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// NewAsyncResolver creates an AsyncGatewayResolver with one shared pool
// (producer) per model/pool pair.
func NewAsyncResolver(config AsyncClientConfig, logger logr.Logger) (*AsyncGatewayResolver, error) {
	if config.ConsumerID == "" {
		return nil, fmt.Errorf("consumerID must not be empty")
	}

	opts, err := redis.ParseURL(config.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse async inference Redis URL: %w", err)
	}
	rdb := redis.NewClient(opts)

	poolToModel := make(map[string]string, len(config.Models))
	for model, mcfg := range config.Models {
		if existing, ok := poolToModel[mcfg.PoolName]; ok {
			_ = rdb.Close()
			return nil, fmt.Errorf("models %q and %q both map to pool %q: each pool must have a single consumer", existing, model, mcfg.PoolName)
		}
		poolToModel[mcfg.PoolName] = model
	}

	if config.ResultPollTimeout <= 0 {
		_ = rdb.Close()
		return nil, fmt.Errorf("resultPollTimeout must be > 0")
	}

	pools := make(map[string]*asyncPool, len(config.Models))
	routeBases := make(map[string]string, len(config.Models))
	var closers []io.Closer

	for model, mcfg := range config.Models {
		reqQueue := mcfg.RequestQueueName
		resQueue := mcfg.ResultQueueName
		// Deprecated: derived queue names from pool name. Set explicit
		// request_queue_name / result_queue_name in async model config.
		if reqQueue == "" {
			reqQueue = asyncQueuePrefix + "requests:" + mcfg.PoolName
		}
		if resQueue == "" {
			resQueue = asyncQueuePrefix + "results:" + mcfg.PoolName
		}
		// Each Processor consumes from its own result queue so another healthy
		// replica cannot consume and discard its results.
		routeBases[model] = resQueue
		resQueue = resQueue + ":" + config.ConsumerID
		logger.Info(
			"Configured async inference queues",
			"model", model,
			"requestQueue", reqQueue,
			"resultQueue", resQueue,
		)

		p, err := producer.NewRedisSortedSetProducer(
			producer.RedisSortedSetConfig{
				RequestQueueName: reqQueue,
				ResultQueueName:  resQueue,
			},
			producer.WithRedisClient(rdb),
		)
		if err != nil {
			for _, c := range closers {
				_ = c.Close()
			}
			_ = rdb.Close()
			return nil, fmt.Errorf("failed to create producer for model %q (pool %s): %w", model, mcfg.PoolName, err)
		}

		pools[model] = &asyncPool{
			producer:    p,
			pollTimeout: config.ResultPollTimeout,
		}
		closers = append(closers, p)
	}

	closers = append(closers, rdb)

	return &AsyncGatewayResolver{
		pools:             pools,
		sharedClients:     syncutil.NewMutexMap[string, *asyncSharedClient](),
		closers:           closers,
		routeBases:        routeBases,
		rdb:               rdb,
		resultPollTimeout: config.ResultPollTimeout,
		logger:            logger,
	}, nil
}

// recoveryClaimLeaseTTL is the claim lease for producers consuming another
// processor's result route. A recovery claim only needs to cover the
// claim→route→checkpoint hop (seconds), unlike a route owner's lease, which
// bounds crash detection. Two processors may recover different batches from
// the same dead submitter and occasionally claim each other's deliveries —
// a foreign claim is held unacked (its entry is open) until the lease
// lapses, so a short lease turns that mis-claim into a brief bounce instead
// of a full crash-detection window.
const recoveryClaimLeaseTTL = 60 * time.Second

// RecoveryClientFor returns a client that consumes the per-processor result
// route of consumer — the route on which that processor's unacknowledged
// deliveries are stranded after a disruption. A processor recovering a batch
// reads the previous submitter's route through it so a completed result is
// checkpointed instead of re-inferred. The client is receive-only: requests
// are never submitted through it.
func (r *AsyncGatewayResolver) RecoveryClientFor(modelID, consumerID string) (AsyncInferenceClient, error) {
	if r.clientFactories != nil {
		return nil, fmt.Errorf("recovery clients require a Redis-backed resolver")
	}
	base, ok := r.routeBases[modelID]
	if !ok || r.rdb == nil {
		return nil, fmt.Errorf("recovery route for model %q is not configured", modelID)
	}
	p, err := producer.NewRedisSortedSetProducer(
		producer.RedisSortedSetConfig{ResultQueueName: base + ":" + consumerID},
		producer.WithRedisClient(r.rdb),
		producer.WithResultClaimLeaseTTL(recoveryClaimLeaseTTL),
	)
	if err != nil {
		return nil, fmt.Errorf("recovery producer for model %q consumer %q: %w", modelID, consumerID, err)
	}
	return newAsyncSharedClient(p, r.resultPollTimeout, r.logger.WithValues("model", modelID, "recoveryFor", consumerID)), nil
}
