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

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	eventChannelBufferSize = 100
	batchEventsChannel     = "batch_events"
	eventSweepInterval     = 30 * time.Second
	eventRescanInterval    = 30 * time.Second
)

var errEventChannelFull = errors.New("event channel full")

const sendEventSQL = `WITH inserted AS (
	INSERT INTO batch_events (job_id, event_type, expires_at)
	VALUES ($1, $2, $3)
	RETURNING job_id
)
SELECT pg_notify('` + batchEventsChannel + `', (SELECT job_id FROM inserted))`

const drainEventsSQL = `WITH selected AS (
	SELECT id, event_type FROM batch_events
	WHERE job_id = $1 AND expires_at > EXTRACT(EPOCH FROM NOW())::BIGINT
	ORDER BY id
	FOR UPDATE SKIP LOCKED
), deleted AS (
	DELETE FROM batch_events events
	USING selected
	WHERE events.id = selected.id
	RETURNING selected.id, selected.event_type
)
SELECT event_type FROM deleted ORDER BY id`

const purgeExpiredEventsSQL = `DELETE FROM batch_events
WHERE expires_at < EXTRACT(EPOCH FROM NOW())::BIGINT`

type eventSubscription struct {
	ch        chan api.BatchEvent
	closeOnce sync.Once
}

// PostgresBatchEventClient implements api.BatchEventChannelClient using a
// durable PostgreSQL table. LISTEN/NOTIFY is a latency optimization; the
// table remains the source of truth for late subscribers and reconnects.
type PostgresBatchEventClient struct {
	pool      pgxPool
	listener  *pgListener
	logger    logr.Logger
	closeOnce sync.Once

	eventsMu     sync.Mutex
	eventSubs    map[string]*eventSubscription
	eventsCancel context.CancelFunc
	eventsDone   chan struct{}

	rescanInterval time.Duration
	rescanNow      chan struct{}

	sweepCancel context.CancelFunc
	sweepDone   chan struct{}
}

var _ api.BatchEventChannelClient = (*PostgresBatchEventClient)(nil)

// NewPostgresBatchEventClient creates a PostgreSQL-backed event channel
// client and starts the listener, periodic delivery backstop, and expiry sweep.
func NewPostgresBatchEventClient(ctx context.Context, config *PostgreSQLConfig, logger logr.Logger) (*PostgresBatchEventClient, error) {
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

	c := &PostgresBatchEventClient{
		pool:           pool,
		logger:         logger,
		eventSubs:      make(map[string]*eventSubscription),
		rescanInterval: eventRescanInterval,
		rescanNow:      make(chan struct{}, 1),
	}
	c.listener = newPGListener(pool, batchEventsChannel, logger, c.onReconnect)
	c.startEventDispatcher()
	c.startEventSweeper()

	logger.V(logging.INFO).Info("NewPostgresBatchEventClient: client created successfully")
	return c, nil
}

func (c *PostgresBatchEventClient) Close() error {
	c.closeOnce.Do(func() {
		if c.eventsCancel != nil {
			c.eventsCancel()
			<-c.eventsDone
		}
		if c.sweepCancel != nil {
			c.sweepCancel()
			<-c.sweepDone
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

func (c *PostgresBatchEventClient) ECProducerSendEvents(ctx context.Context, events []api.BatchEvent) ([]string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("empty events")
	}
	for i := range events {
		if err := events[i].IsValid(); err != nil {
			return nil, err
		}
	}

	sentIDs := make([]string, 0, len(events))
	for _, event := range events {
		expiresAt := time.Now().Unix() + int64(event.TTL)
		if _, err := c.pool.Exec(ctx, sendEventSQL, event.ID, int(event.Type), expiresAt); err != nil {
			return sentIDs, fmt.Errorf("ECProducerSendEvents: %w", err)
		}
		sentIDs = append(sentIDs, event.ID)
	}
	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("ECProducerSendEvents: succeeded", "nIDs", len(sentIDs))
	return sentIDs, nil
}

func (c *PostgresBatchEventClient) ECConsumerGetChannel(ctx context.Context, id string) (*api.BatchEventsChan, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == "" {
		return nil, fmt.Errorf("ID is empty")
	}

	sub := &eventSubscription{ch: make(chan api.BatchEvent, eventChannelBufferSize)}
	c.eventsMu.Lock()
	c.eventSubs[id] = sub
	c.eventsMu.Unlock()

	// Drain before returning so a cancellation sent before the worker attaches
	// remains observable.
	c.deliverJobEvents(ctx, id)

	closeFn := func() {
		c.eventsMu.Lock()
		if current, ok := c.eventSubs[id]; ok && current == sub {
			delete(c.eventSubs, id)
		}
		c.eventsMu.Unlock()
		sub.closeOnce.Do(func() { close(sub.ch) })
	}
	return &api.BatchEventsChan{ID: id, Events: sub.ch, CloseFn: closeFn}, nil
}

func (c *PostgresBatchEventClient) startEventSweeper() {
	ctx, cancel := context.WithCancel(context.Background())
	c.sweepCancel = cancel
	c.sweepDone = make(chan struct{})
	go func() {
		defer close(c.sweepDone)
		ticker := time.NewTicker(eventSweepInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if _, err := c.purgeExpiredEvents(ctx); err != nil {
					c.logger.V(logging.INFO).Info("event sweeper: purge failed", "err", err.Error())
				}
			}
		}
	}()
}

func (c *PostgresBatchEventClient) purgeExpiredEvents(ctx context.Context) (int64, error) {
	result, err := c.pool.Exec(ctx, purgeExpiredEventsSQL)
	if err != nil {
		return 0, fmt.Errorf("purge expired events: %w", err)
	}
	return result.RowsAffected(), nil
}

func (c *PostgresBatchEventClient) onReconnect() {
	select {
	case c.rescanNow <- struct{}{}:
	default:
	}
}

func (c *PostgresBatchEventClient) startEventDispatcher() {
	ctx, cancel := context.WithCancel(context.Background())
	c.eventsCancel = cancel
	c.eventsDone = make(chan struct{})
	wake, unsubscribe := c.listener.subscribe()
	go c.runEventDispatcher(ctx, wake, unsubscribe)
}

func (c *PostgresBatchEventClient) runEventDispatcher(ctx context.Context, wake <-chan string, unsubscribe func()) {
	defer close(c.eventsDone)
	defer unsubscribe()
	ticker := time.NewTicker(c.rescanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case id, ok := <-wake:
			if !ok || ctx.Err() != nil {
				return
			}
			if id == "" {
				c.deliverAllJobEvents(ctx)
			} else {
				c.deliverJobEvents(ctx, id)
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
	ids := make([]string, 0, len(c.eventSubs))
	for id := range c.eventSubs {
		ids = append(ids, id)
	}
	c.eventsMu.Unlock()
	for _, id := range ids {
		c.deliverJobEvents(ctx, id)
	}
}

func (c *PostgresBatchEventClient) deliverJobEvents(ctx context.Context, id string) {
	c.eventsMu.Lock()
	sub, ok := c.eventSubs[id]
	c.eventsMu.Unlock()
	if !ok {
		return
	}

	events, err := c.drainJobEvents(ctx, id)
	if err != nil {
		c.logger.Error(err, "event dispatcher: drain failed", "ID", id)
		return
	}

	c.eventsMu.Lock()
	defer c.eventsMu.Unlock()
	if current, ok := c.eventSubs[id]; !ok || current != sub {
		return
	}
	for _, event := range events {
		select {
		case sub.ch <- event:
		default:
			c.logger.Error(errEventChannelFull, "event dispatcher: dropping event", "ID", id, "type", event.Type)
		}
	}
}

func (c *PostgresBatchEventClient) drainJobEvents(ctx context.Context, id string) ([]api.BatchEvent, error) {
	rows, err := c.pool.Query(ctx, drainEventsSQL, id)
	if err != nil {
		return nil, fmt.Errorf("drain events: %w", err)
	}
	defer rows.Close()

	var events []api.BatchEvent
	for rows.Next() {
		var eventType int
		if err := rows.Scan(&eventType); err != nil {
			return nil, fmt.Errorf("drain events scan: %w", err)
		}
		events = append(events, api.BatchEvent{ID: id, Type: api.BatchEventType(eventType)})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("drain events rows: %w", err)
	}
	return events, nil
}
