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
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-async/producer"
	producersql "github.com/llm-d/llm-d-async/producer-sql"
	"github.com/llm-d/llm-d-async/producer-sql/sqlqueue"
	"github.com/redis/go-redis/v9"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/syncutil"
)

const (
	asyncQueuePrefix             = "llm-d-async:"
	AsyncTransportRedisSortedSet = "redis-sortedset"
	AsyncTransportSQL            = "sql"
)

// AsyncModelPoolConfig holds the resolved pool and queue settings for one async model.
type AsyncModelPoolConfig struct {
	PoolName         string
	RequestQueueName string
	ResultQueueName  string
}

// AsyncClientConfig holds the resolved configuration for async dispatch.
type AsyncClientConfig struct {
	Transport         string
	RedisURL          string
	SQLURL            string
	ConsumerID        string // identity of this Batch Processor replica
	Models            map[string]AsyncModelPoolConfig
	ResultPollTimeout time.Duration // per-poll timeout in the result dispatcher loop
	PollInterval      time.Duration // backend queue polling interval
}

// AsyncGatewayResolver routes models to shared AsyncInferenceClient instances.
// Immutable after construction — safe for concurrent reads.
type AsyncGatewayResolver struct {
	pools           map[string]*asyncPool                          // model → pool
	sharedClients   *syncutil.MutexMap[string, *asyncSharedClient] // model → shared client
	closers         []io.Closer
	clientFactories map[string]func() AsyncInferenceClient // test-only override
	// routeBases maps each model to its result-route prefix (before the
	// consumer suffix); Redis rebuilds producers for cross-processor recovery.
	routeBases        map[string]string
	transport         string
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
	transport := config.Transport
	if transport == "" {
		transport = AsyncTransportRedisSortedSet
	}
	if transport != AsyncTransportRedisSortedSet && transport != AsyncTransportSQL {
		return nil, fmt.Errorf("unsupported async transport %q", transport)
	}

	var rdb redis.UniversalClient
	if transport == AsyncTransportRedisSortedSet {
		opts, err := redis.ParseURL(config.RedisURL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse async inference Redis URL: %w", err)
		}
		rdb = redis.NewClient(opts)
	} else if config.SQLURL == "" {
		return nil, fmt.Errorf("async SQL transport requires a PostgreSQL URL")
	}

	poolToModel := make(map[string]string, len(config.Models))
	for model, mcfg := range config.Models {
		if existing, ok := poolToModel[mcfg.PoolName]; ok {
			if rdb != nil {
				_ = rdb.Close()
			}
			return nil, fmt.Errorf("models %q and %q both map to pool %q: each pool must have a single consumer", existing, model, mcfg.PoolName)
		}
		poolToModel[mcfg.PoolName] = model
	}

	if config.ResultPollTimeout <= 0 {
		if rdb != nil {
			_ = rdb.Close()
		}
		return nil, fmt.Errorf("resultPollTimeout must be > 0")
	}
	pollInterval := config.PollInterval
	if pollInterval <= 0 {
		pollInterval = time.Second
	}

	pools := make(map[string]*asyncPool, len(config.Models))
	routeBases := make(map[string]string, len(config.Models))
	var closers []io.Closer
	closeResources := func() {
		for _, c := range closers {
			_ = c.Close()
		}
		if rdb != nil {
			_ = rdb.Close()
		}
	}
	var sqlStore *sqlqueue.Store
	if transport == AsyncTransportSQL && len(config.Models) > 0 {
		var err error
		sqlStore, err = sqlqueue.Open(context.Background(), config.SQLURL)
		if err != nil {
			return nil, fmt.Errorf("failed to open async SQL store: %w", err)
		}
		closers = append(closers, sqlStore)
	}

	for model, mcfg := range config.Models {
		reqQueue := mcfg.RequestQueueName
		resQueue := mcfg.ResultQueueName
		// Deprecated: derived queue names from pool name. Set explicit
		// request_queue_name and result_queue_name in async model config.
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
			"transport", transport,
			"requestQueue", reqQueue,
			"resultQueue", resQueue,
		)

		var p producer.Producer
		var err error
		switch transport {
		case AsyncTransportRedisSortedSet:
			p, err = producer.NewRedisSortedSetProducer(
				producer.RedisSortedSetConfig{
					RequestQueueName: reqQueue,
					ResultQueueName:  resQueue,
				},
				producer.WithRedisClient(rdb),
			)
		case AsyncTransportSQL:
			p, err = producersql.New(
				context.Background(),
				producersql.Config{
					RequestQueueName: reqQueue,
					ResultQueueName:  resQueue,
					PollInterval:     pollInterval,
				},
				producersql.WithStore(sqlStore),
			)
		}
		if err != nil {
			closeResources()
			return nil, fmt.Errorf("failed to create %s producer for model %q (pool %s): %w", transport, model, mcfg.PoolName, err)
		}

		pools[model] = &asyncPool{
			producer:    p,
			pollTimeout: config.ResultPollTimeout,
		}
		closers = append(closers, p)
	}

	if rdb != nil {
		closers = append(closers, rdb)
	}

	return &AsyncGatewayResolver{
		pools:             pools,
		sharedClients:     syncutil.NewMutexMap[string, *asyncSharedClient](),
		closers:           closers,
		routeBases:        routeBases,
		transport:         transport,
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
//
// This bounds the cost of a mis-claim; it does not eliminate it. A result
// route is keyed by the foreign submitter (not by batch), so every processor
// recovering any of that submitter's batches consumes the whole route and
// claims are exclusive: a processor can hold a delivery that belongs to
// another processor's batch for up to this lease, and nothing guarantees the
// rightful owner wins the next claim (both poll the route). The design stays
// at-least-once-safe — an unowned delivery is never acknowledged while its
// entry is open, so it is redelivered rather than lost — but in the worst
// case it delays recovery into re-submission (replayed inference) or SLO
// expiry. Fully removing this needs per-batch routing or cross-replica
// forwarding of deliveries to their current batch owner.
const recoveryClaimLeaseTTL = 60 * time.Second

// RecoveryClientFor returns a Redis client that consumes the per-processor
// result route of consumer — the route on which that processor's
// unacknowledged deliveries are stranded after a disruption. A processor
// recovering a batch reads the previous submitter's route through it so a
// completed result is checkpointed instead of re-inferred. SQL does not
// support this operation because its current result read is destructive.
func (r *AsyncGatewayResolver) RecoveryClientFor(modelID, consumerID string) (AsyncInferenceClient, error) {
	if r.clientFactories != nil {
		return nil, fmt.Errorf("recovery clients require a real async transport")
	}
	base, ok := r.routeBases[modelID]
	if !ok {
		return nil, fmt.Errorf("recovery route for model %q is not configured", modelID)
	}

	var p producer.Producer
	var err error
	switch r.transport {
	case AsyncTransportRedisSortedSet:
		if r.rdb == nil {
			return nil, fmt.Errorf("Redis recovery route for model %q is not configured", modelID)
		}
		p, err = producer.NewRedisSortedSetProducer(
			producer.RedisSortedSetConfig{ResultQueueName: base + ":" + consumerID},
			producer.WithRedisClient(r.rdb),
			producer.WithResultClaimLeaseTTL(recoveryClaimLeaseTTL),
		)
	case AsyncTransportSQL:
		return nil, fmt.Errorf("SQL transport does not support cross-processor result recovery; resume by re-submission")
	default:
		return nil, fmt.Errorf("unsupported async transport %q", r.transport)
	}
	if err != nil {
		return nil, fmt.Errorf("recovery producer for model %q consumer %q: %w", modelID, consumerID, err)
	}
	return newAsyncSharedClient(p, r.resultPollTimeout, r.logger.WithValues("model", modelID, "recoveryFor", consumerID)), nil
}
