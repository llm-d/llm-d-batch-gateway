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

package postgresql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	eventChanBufSize   = 100
	channelEvents      = "batch_events"
	eventSweepInterval = 30 * time.Second

	// eventRescanInterval is how often the dispatcher re-drains every
	// subscribed job as a delivery backstop (see runEventDispatcher).
	eventRescanInterval = 30 * time.Second
)

var errEventChannelFull = errors.New("event channel full")

const ecSendEventSQL = `WITH ins AS (
	INSERT INTO batch_events (job_id, event_type, expires_at)
	VALUES ($1, $2, $3)
	RETURNING job_id
)
SELECT pg_notify('` + channelEvents + `', (SELECT job_id FROM ins))`

const ecDrainEventsSQL = `SELECT id, event_type FROM batch_events
WHERE job_id = $1 AND expires_at > EXTRACT(EPOCH FROM NOW())::BIGINT
ORDER BY id LIMIT $2`

const ecAckEventsSQL = `DELETE FROM batch_events WHERE job_id = $1 AND id = ANY($2)`

// ecPurgeExpiredEventsSQL deletes events that are past their TTL and were
// never consumed. The drain above only removes unexpired rows, so without
// this sweep the table would grow without bound. Uses
// idx_batch_events_expires_at.
const ecPurgeExpiredEventsSQL = `DELETE FROM batch_events
WHERE expires_at < EXTRACT(EPOCH FROM NOW())::BIGINT`

type eventSub struct {
	ch         chan api.BatchEvent
	closeOnce  sync.Once
	deliveryMu sync.Mutex
}

// PostgresBatchEventClient implements api.BatchEventChannelClient using PostgreSQL.
// The batch_events table is the durable source of truth (late-attach safe);
// PostgreSQL LISTEN/NOTIFY provides low-latency notification delivery without polling.
type PostgresBatchEventClient struct {
	pool         pgxPool
	listener     *pgListener
	logger       logr.Logger
	closeOnce    sync.Once
	producerOnly bool

	eventsMu     sync.Mutex
	eventSubs    map[string]*eventSub
	eventsCancel context.CancelFunc
	eventsDone   chan struct{}

	rescanInterval time.Duration
	rescanNow      chan struct{}
}

var _ api.BatchEventChannelClient = (*PostgresBatchEventClient)(nil)

func NewPostgresBatchEventClient(ctx context.Context, config *PostgreSQLConfig, logger logr.Logger) (*PostgresBatchEventClient, error) {
	pool, err := newEventPool(ctx, config)
	if err != nil {
		return nil, err
	}

	c := &PostgresBatchEventClient{
		pool:           pool,
		logger:         logger,
		eventSubs:      make(map[string]*eventSub),
		rescanInterval: eventRescanInterval,
		rescanNow:      make(chan struct{}, 1),
	}

	c.listener = newPGListener(pool, channelEvents, logger, c.onReconnect)
	c.startEventDispatcher()

	logger.V(logging.INFO).Info("NewPostgresBatchEventClient: client created successfully")
	return c, nil
}

func newEventPool(ctx context.Context, config *PostgreSQLConfig) (*pgxpool.Pool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config == nil {
		return nil, fmt.Errorf("postgresql config cannot be nil")
	}

	pool, err := newPool(ctx, config)
	if err != nil {
		return nil, err
	}

	if _, err := pool.Exec(ctx, batchSchemaSql); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to apply batch events schema: %w", err)
	}

	return pool, nil
}

func (c *PostgresBatchEventClient) Close() error {
	c.closeOnce.Do(func() {
		if c.eventsCancel != nil {
			c.eventsCancel()
			<-c.eventsDone
		}
		if c.listener != nil {
			_ = c.listener.close()
		}
		if c.pool != nil {
			c.pool.Close()
		}
	})
	return nil
}

// PostgresBatchEventGC sweeps expired events independently of event producers
// and consumers, sharing the GC process's existing batch database pool.
type PostgresBatchEventGC struct {
	pool   pgxPool
	logger logr.Logger
}

var _ api.BatchEventGC = (*PostgresBatchEventGC)(nil)

func NewPostgresBatchEventGC(db *PostgresBatchDBClient, logger logr.Logger) (*PostgresBatchEventGC, error) {
	if db == nil || db.pgCore == nil || db.pool == nil {
		return nil, fmt.Errorf("batch database client is required for event GC")
	}
	return &PostgresBatchEventGC{pool: db.pool, logger: logger}, nil
}

func (g *PostgresBatchEventGC) purgeExpired(ctx context.Context) (int64, error) {
	result, err := g.pool.Exec(ctx, ecPurgeExpiredEventsSQL)
	if err != nil {
		return 0, fmt.Errorf("purge expired events: %w", err)
	}
	return result.RowsAffected(), nil
}

// Run sweeps once on startup and then every eventSweepInterval until cancelled.
// The caller owns the batch database pool and must keep it open until Run exits.
func (g *PostgresBatchEventGC) Run(ctx context.Context) error {
	sweep := func() {
		purged, err := g.purgeExpired(ctx)
		if err != nil {
			if ctx.Err() == nil {
				g.logger.Error(err, "event GC: purge failed")
			}
		} else if purged > 0 {
			g.logger.Info("event GC: purged expired events", "purged", purged)
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	sweep()
	ticker := time.NewTicker(eventSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			sweep()
		}
	}
}

// NewPostgresBatchEventProducer creates an event client for the API server
// without a listener, dispatcher, or subscribers.
func NewPostgresBatchEventProducer(ctx context.Context, config *PostgreSQLConfig) (*PostgresBatchEventClient, error) {
	pool, err := newEventPool(ctx, config)
	if err != nil {
		return nil, err
	}
	return &PostgresBatchEventClient{pool: pool, producerOnly: true}, nil
}

func (c *PostgresBatchEventClient) ECProducerSendEvents(ctx context.Context, events []api.BatchEvent) (sentIDs []string, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("empty events")
	}
	for i := range events {
		if err = events[i].IsValid(); err != nil {
			return nil, err
		}
	}

	sentIDs = make([]string, 0, len(events))
	for i := range events {
		event := events[i]
		expiresAt := time.Now().Unix() + int64(event.TTL)
		if _, err = c.pool.Exec(ctx, ecSendEventSQL, event.ID, int(event.Type), expiresAt); err != nil {
			return sentIDs, fmt.Errorf("ECProducerSendEvents: %w", err)
		}
		sentIDs = append(sentIDs, event.ID)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("ECProducerSendEvents: succeeded", "nIDs", len(sentIDs))
	return sentIDs, nil
}

func (c *PostgresBatchEventClient) ECConsumerGetChannel(ctx context.Context, ID string) (*api.BatchEventsChan, error) {
	if c.producerOnly {
		return nil, fmt.Errorf("event producer does not support subscriptions")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if len(ID) == 0 {
		return nil, fmt.Errorf("ID is empty")
	}

	sub := &eventSub{
		ch: make(chan api.BatchEvent, eventChanBufSize),
	}

	c.eventsMu.Lock()
	if _, exists := c.eventSubs[ID]; exists {
		// Replace the previous subscriber. Its CloseFn is identity-guarded, so
		// it will close its own channel when its consumer exits; events drain
		// to the newest subscriber from here on.
		c.logger.V(logging.INFO).Info("event channel: replacing existing subscriber", "ID", ID)
	}
	c.eventSubs[ID] = sub
	c.eventsMu.Unlock()

	// Proactively drain existing events (late-attach safe)
	c.deliverJobEvents(ctx, ID)

	closeFn := func() {
		c.eventsMu.Lock()
		if cur, ok := c.eventSubs[ID]; ok && cur == sub {
			delete(c.eventSubs, ID)
		}
		c.eventsMu.Unlock()
		sub.closeOnce.Do(func() { close(sub.ch) })
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("ECConsumerGetChannel: succeeded", "ID", ID)
	return &api.BatchEventsChan{ID: ID, Events: sub.ch, CloseFn: closeFn}, nil
}

func (c *PostgresBatchEventClient) onReconnect() {
	// Nudge the dispatcher to drain subscribed jobs promptly (events may have
	// been missed while the listener was down). The listen loop only does a
	// non-blocking send; the drain runs in the dispatcher. A full nudge channel
	// means a drain is already queued or running — the periodic backstop tick
	// covers the rest.
	select {
	case c.rescanNow <- struct{}{}:
	default:
	}
}

func (c *PostgresBatchEventClient) startEventDispatcher() {
	dispCtx, cancel := context.WithCancel(context.Background())
	c.eventsCancel = cancel
	c.eventsDone = make(chan struct{})

	wake, unsubscribe := c.listener.subscribe()
	go c.runEventDispatcher(dispCtx, wake, unsubscribe)
}

func (c *PostgresBatchEventClient) runEventDispatcher(ctx context.Context, wake <-chan string, unsubscribe func()) {
	defer close(c.eventsDone)
	defer unsubscribe()

	// NOTIFY is a latency hint, not a delivery guarantee: a notification
	// dropped on a full wake channel (or missed across a listener flap)
	// must not lose the event. The row stays in batch_events, so periodically
	// re-drain every subscribed job to make delivery eventually correct.
	ticker := time.NewTicker(c.rescanInterval)
	defer ticker.Stop()
	c.logger.V(logging.INFO).Info("event dispatcher: start", "rescanInterval", c.rescanInterval.String())

	for {
		select {
		case <-ctx.Done():
			c.logger.V(logging.INFO).Info("event dispatcher: stop")
			return
		case payload, ok := <-wake:
			if !ok || ctx.Err() != nil {
				return
			}
			if payload == "" {
				c.deliverAllJobEvents(ctx)
			} else {
				c.deliverJobEvents(ctx, payload)
			}
		case <-ticker.C:
			c.deliverAllJobEvents(ctx)
		case <-c.rescanNow:
			c.deliverAllJobEvents(ctx)
		}
	}
}

func (c *PostgresBatchEventClient) deliverAllJobEvents(ctx context.Context) {
	c.eventsMu.Lock()
	jobIDs := make([]string, 0, len(c.eventSubs))
	for id := range c.eventSubs {
		jobIDs = append(jobIDs, id)
	}
	c.eventsMu.Unlock()

	for _, id := range jobIDs {
		c.deliverJobEvents(ctx, id)
	}
}

func (c *PostgresBatchEventClient) deliverJobEvents(ctx context.Context, jobID string) {
	c.eventsMu.Lock()
	sub, ok := c.eventSubs[jobID]
	c.eventsMu.Unlock()
	if !ok {
		return
	}
	sub.deliveryMu.Lock()
	defer sub.deliveryMu.Unlock()

	c.eventsMu.Lock()
	if c.eventSubs[jobID] != sub {
		c.eventsMu.Unlock()
		return
	}
	capacity := cap(sub.ch) - len(sub.ch)
	c.eventsMu.Unlock()
	if capacity == 0 {
		return
	}

	events, err := c.drainJobEvents(ctx, jobID, capacity)
	if err != nil {
		c.logger.Error(err, "event dispatcher: drain failed", "ID", jobID)
		return
	}
	if len(events) == 0 {
		return
	}

	c.eventsMu.Lock()
	if cur, present := c.eventSubs[jobID]; !present || cur != sub {
		c.eventsMu.Unlock()
		// The rows remain available for the next subscriber or rescan.
		return
	}
	accepted := make([]int64, 0, len(events))
deliveryLoop:
	for _, event := range events {
		select {
		case sub.ch <- api.BatchEvent{ID: jobID, Type: event.eventType}:
			accepted = append(accepted, event.id)
		default:
			c.logger.Error(errEventChannelFull, "event dispatcher: channel full, deferring event", "ID", jobID, "type", event.eventType)
			// Do not deliver later events ahead of this one.
			break deliveryLoop
		}
	}
	c.eventsMu.Unlock()

	// Selection and acknowledgement are separate from channel delivery, so a
	// failed acknowledgement (or concurrent consumer) may redeliver an event.
	// Only events enqueued to this subscriber may be removed from the table.
	if len(accepted) > 0 {
		if _, err := c.pool.Exec(ctx, ecAckEventsSQL, jobID, accepted); err != nil {
			c.logger.Error(err, "event dispatcher: acknowledge failed; events may be redelivered", "ID", jobID)
		}
	}
}

type pendingBatchEvent struct {
	id        int64
	eventType api.BatchEventType
}

func (c *PostgresBatchEventClient) drainJobEvents(ctx context.Context, jobID string, limit int) ([]pendingBatchEvent, error) {
	rows, err := c.pool.Query(ctx, ecDrainEventsSQL, jobID, limit)
	if err != nil {
		return nil, fmt.Errorf("drain events: %w", err)
	}
	defer rows.Close()

	var events []pendingBatchEvent
	for rows.Next() {
		var id int64
		var eventType int
		if err := rows.Scan(&id, &eventType); err != nil {
			return nil, fmt.Errorf("drain events scan: %w", err)
		}
		events = append(events, pendingBatchEvent{id: id, eventType: api.BatchEventType(eventType)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("drain events rows: %w", err)
	}

	return events, nil
}
