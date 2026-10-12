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
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pashagolub/pgxmock/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func TestBatchEventValidation(t *testing.T) {
	evValid := api.BatchEvent{ID: "job-1", Type: api.BatchEventCancel, TTL: 60}
	if err := evValid.IsValid(); err != nil {
		t.Fatalf("expected valid event, got: %v", err)
	}

	evEmptyID := api.BatchEvent{ID: "", Type: api.BatchEventCancel, TTL: 60}
	if err := evEmptyID.IsValid(); err == nil {
		t.Fatal("expected error for empty ID")
	}

	evBadType := api.BatchEvent{ID: "job-1", Type: api.BatchEventMaxVal, TTL: 60}
	if err := evBadType.IsValid(); err == nil {
		t.Fatal("expected error for invalid event type")
	}

	evBadTTL := api.BatchEvent{ID: "job-1", Type: api.BatchEventCancel, TTL: 0}
	if err := evBadTTL.IsValid(); err == nil {
		t.Fatal("expected error for TTL <= 0")
	}
}

func TestPostgresBatchEventProducer(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	producer := &PostgresBatchEventClient{pool: mock, producerOnly: true}
	defer func() { _ = producer.Close() }()

	mock.ExpectExec("INSERT INTO batch_events").
		WithArgs("job-1", int(api.BatchEventCancel), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	ids, err := producer.ECProducerSendEvents(context.Background(), []api.BatchEvent{{
		ID: "job-1", Type: api.BatchEventCancel, TTL: 60,
	}})
	if err != nil || len(ids) != 1 || ids[0] != "job-1" {
		t.Fatalf("ECProducerSendEvents = %v, %v", ids, err)
	}
	if _, err := producer.ECConsumerGetChannel(context.Background(), "job-1"); err == nil {
		t.Fatal("producer accepted a subscription")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresBatchEventGC_PurgeExpiredEventsMock(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	defer mock.Close()
	db := &PostgresBatchDBClient{pgCore: &pgCore{pool: mock}}
	gc, err := NewPostgresBatchEventGC(db, logr.Discard())
	if err != nil {
		t.Fatalf("NewPostgresBatchEventGC: %v", err)
	}
	mock.ExpectExec("DELETE FROM batch_events(?s:.*)WHERE expires_at").
		WillReturnResult(pgxmock.NewResult("DELETE", 2))
	purged, err := gc.purgeExpired(context.Background())
	if err != nil || purged != 2 {
		t.Fatalf("purgeExpired = %d, %v; want 2, nil", purged, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresBatchEventGC_RunPurgesOnStart(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	defer mock.Close()
	gc := &PostgresBatchEventGC{pool: mock, logger: logr.Discard()}
	mock.ExpectExec("DELETE FROM batch_events(?s:.*)WHERE expires_at").
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := gc.Run(ctx); err != nil {
		t.Fatalf("event GC Run: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("event GC did not purge on start: %v", err)
	}
}

func TestPGListener_SubscribeDeliverClose(t *testing.T) {
	l := &pgListener{
		channel: "batch_events",
		logger:  logr.Discard(),
		subs:    make(map[int]chan string),
		done:    make(chan struct{}),
	}
	close(l.done) // stub done for close()

	wake, unsub := l.subscribe()

	l.deliver("job-123")

	select {
	case payload := <-wake:
		if payload != "job-123" {
			t.Fatalf("expected payload 'job-123', got %q", payload)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for notification delivery")
	}

	unsub()
	l.deliver("job-456")

	select {
	case payload := <-wake:
		t.Fatalf("unexpected delivery after unsubscribe: %q", payload)
	case <-time.After(50 * time.Millisecond):
		// Expected: nothing delivered after unsubscribe
	}

	if err := l.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestPostgresBatchEventClient_ConsumerSubscription(t *testing.T) {
	c := &PostgresBatchEventClient{
		logger:    logr.Discard(),
		eventSubs: make(map[string]*eventSub),
	}

	// Empty ID validation
	if _, err := c.ECConsumerGetChannel(context.Background(), ""); err == nil {
		t.Fatal("expected error for empty ID")
	}

	sub := &eventSub{
		ch: make(chan api.BatchEvent, 10),
	}
	c.eventSubs["job-test"] = sub

	// Verify deliverJobEvents fans out to the registered subscriber
	sub.ch <- api.BatchEvent{ID: "job-test", Type: api.BatchEventCancel}

	select {
	case ev := <-sub.ch:
		if ev.ID != "job-test" || ev.Type != api.BatchEventCancel {
			t.Fatalf("unexpected event: %+v", ev)
		}
	default:
		t.Fatal("expected event in subscriber channel")
	}
}

// TestPostgresBatchEventClient_BackstopRescan verifies that the dispatcher's
// periodic rescan delivers an event even when no NOTIFY arrives: the row is
// drained by the rescan tick, never by the subscription's one-shot proactive
// drain (which is made to return no rows), so a missed notification defers
// the event to the rescan instead of losing it.
func TestPostgresBatchEventClient_BackstopRescan(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	defer mock.Close()

	c := &PostgresBatchEventClient{
		pool:           mock,
		logger:         logr.Discard(),
		eventSubs:      make(map[string]*eventSub),
		eventsDone:     make(chan struct{}),
		rescanInterval: 50 * time.Millisecond,
	}

	const jobID = "job-rescan"
	// Proactive drain at subscription time: nothing in the table yet.
	mock.ExpectQuery("SELECT id, event_type FROM batch_events").
		WithArgs(jobID, eventChanBufSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "event_type"}))

	events, err := c.ECConsumerGetChannel(context.Background(), jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer events.CloseFn()

	// Install all expectations before starting the dispatcher: pgxmock is not
	// safe to configure concurrently with a running query.
	mock.ExpectQuery("SELECT id, event_type FROM batch_events").
		WithArgs(jobID, eventChanBufSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "event_type"}).AddRow(int64(1), int(api.BatchEventCancel)))
	mock.ExpectExec("DELETE FROM batch_events WHERE job_id").
		WithArgs(jobID, []int64{1}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	dispCtx, cancel := context.WithCancel(context.Background())
	go func() {
		// No wake channel traffic: only the rescan tick can deliver.
		c.runEventDispatcher(dispCtx, make(chan string, 1), func() {})
	}()
	defer func() {
		cancel()
		<-c.eventsDone
	}()

	select {
	case ev := <-events.Events:
		if ev.ID != jobID || ev.Type != api.BatchEventCancel {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("event not delivered by the periodic rescan within 2s")
	}

	cancel()
	select {
	case <-c.eventsDone:
	case <-time.After(2 * time.Second):
		t.Fatal("dispatcher did not stop after cancel")
	}
}

// TestPostgresBatchEventGC_PurgeExpiredEvents requires a real PostgreSQL
// instance (TEST_POSTGRES_URL) and verifies that expired, never-consumed
// events are deleted while live events remain.
func TestPostgresBatchEventGC_PurgeExpiredEvents(t *testing.T) {
	url := requirePostgresURL(t)
	ctx := context.Background()

	client, err := NewPostgresBatchDBClient(ctx, &PostgreSQLConfig{Url: url})
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	defer func() { _ = client.Close() }()
	eventGC, err := NewPostgresBatchEventGC(client, logr.Discard())
	if err != nil {
		t.Fatalf("NewPostgresBatchEventGC: %v", err)
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()

	now := time.Now().Unix()
	const (
		expiredJob = "purge-test-expired"
		liveJob    = "purge-test-live"
	)
	if _, err := pool.Exec(ctx,
		`INSERT INTO batch_events (job_id, event_type, expires_at) VALUES ($1, 1, $2), ($3, 1, $4)`,
		expiredJob, now-10, liveJob, now+3600); err != nil {
		t.Fatalf("seed events: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, `DELETE FROM batch_events WHERE job_id IN ($1, $2)`, expiredJob, liveJob)
	}()

	purged, err := eventGC.purgeExpired(ctx)
	if err != nil {
		t.Fatalf("purgeExpired: %v", err)
	}
	if purged < 1 {
		t.Fatalf("purgeExpired purged %d rows, want >= 1 (the expired test row)", purged)
	}

	var remaining int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM batch_events WHERE job_id IN ($1, $2)`, expiredJob, liveJob).Scan(&remaining); err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("expected exactly the unexpired row to remain, got %d rows", remaining)
	}
	var remainingJob string
	if err := pool.QueryRow(ctx,
		`SELECT job_id FROM batch_events WHERE job_id IN ($1, $2)`, expiredJob, liveJob).Scan(&remainingJob); err != nil {
		t.Fatalf("fetch remaining: %v", err)
	}
	if remainingJob != liveJob {
		t.Fatalf("expected %s to remain, got %s", liveJob, remainingJob)
	}
}

// newTestEventClientForURL requires a real PostgreSQL instance
// (TEST_POSTGRES_URL) and returns an event client wired to it.
func newTestEventClientForURL(t *testing.T, url string) *PostgresBatchEventClient {
	t.Helper()
	client, err := NewPostgresBatchEventClient(context.Background(), &PostgreSQLConfig{Url: url}, logr.Discard())
	if err != nil {
		t.Fatalf("NewPostgresBatchEventClient: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func requirePostgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	return url
}

// TestPostgresBatchEventClient_RoundTrip requires a real PostgreSQL instance
// (TEST_POSTGRES_URL) and verifies the full path: a produced event is drained
// from the table and delivered to a live subscriber. The 35s window covers the
// NOTIFY fast path (sub-second) and the 30s backstop rescan.
func TestPostgresBatchEventClient_RoundTrip(t *testing.T) {
	url := requirePostgresURL(t)
	const jobID = "round-trip-job"
	client := newTestEventClientForURL(t, url)

	ch, err := client.ECConsumerGetChannel(context.Background(), jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer ch.CloseFn()

	if _, err := client.ECProducerSendEvents(context.Background(), []api.BatchEvent{
		{ID: jobID, Type: api.BatchEventCancel, TTL: 300},
	}); err != nil {
		t.Fatalf("ECProducerSendEvents: %v", err)
	}

	select {
	case ev := <-ch.Events:
		if ev.ID != jobID || ev.Type != api.BatchEventCancel {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(35 * time.Second):
		t.Fatal("event not delivered within 35s (NOTIFY path or backstop rescan)")
	}
}

// TestPostgresBatchEventClient_LateAttach requires a real PostgreSQL instance
// (TEST_POSTGRES_URL) and verifies the durability contract: an event produced
// before any subscriber exists is still delivered when a subscriber attaches
// later (the proactive drain picks the row up from the table).
func TestPostgresBatchEventClient_LateAttach(t *testing.T) {
	url := requirePostgresURL(t)
	const jobID = "late-attach-job"
	client := newTestEventClientForURL(t, url)

	// Produce with no subscriber: the row must remain in the table.
	if _, err := client.ECProducerSendEvents(context.Background(), []api.BatchEvent{
		{ID: jobID, Type: api.BatchEventCancel, TTL: 300},
	}); err != nil {
		t.Fatalf("ECProducerSendEvents: %v", err)
	}

	ch, err := client.ECConsumerGetChannel(context.Background(), jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer ch.CloseFn()

	select {
	case ev := <-ch.Events:
		if ev.ID != jobID || ev.Type != api.BatchEventCancel {
			t.Fatalf("unexpected event: %+v", ev)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("late-attach drain did not deliver the pre-existing event within 5s")
	}
}

// TestPostgresBatchEventClient_FIFO verifies the drain requests rows in ID
// order and delivers the returned events in that order.
func TestPostgresBatchEventClient_FIFO(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	defer mock.Close()
	client := &PostgresBatchEventClient{
		pool:      mock,
		logger:    logr.Discard(),
		eventSubs: make(map[string]*eventSub),
	}
	const jobID = "job-fifo"
	want := []api.BatchEventType{api.BatchEventCancel, api.BatchEventPause, api.BatchEventResume}
	mock.ExpectQuery(`SELECT id, event_type FROM batch_events(?s:.*)ORDER BY id LIMIT \$2`).
		WithArgs(jobID, eventChanBufSize).
		WillReturnRows(pgxmock.NewRows([]string{"id", "event_type"}).
			AddRow(int64(1), int(want[0])).AddRow(int64(2), int(want[1])).AddRow(int64(3), int(want[2])))
	mock.ExpectExec("DELETE FROM batch_events WHERE job_id").
		WithArgs(jobID, []int64{1, 2, 3}).
		WillReturnResult(pgxmock.NewResult("DELETE", 3))

	ch, err := client.ECConsumerGetChannel(context.Background(), jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer ch.CloseFn()
	for i, expected := range want {
		select {
		case event := <-ch.Events:
			if event.Type != expected {
				t.Fatalf("event %d type = %d, want %d", i, event.Type, expected)
			}
		default:
			t.Fatalf("event %d not delivered", i)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresBatchEventClient_FullChannelKeepsEvent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	defer mock.Close()
	const jobID = "job-full"
	sub := &eventSub{ch: make(chan api.BatchEvent, 1)}
	client := &PostgresBatchEventClient{
		pool: mock, logger: logr.Discard(), eventSubs: map[string]*eventSub{jobID: sub},
	}
	sub.ch <- api.BatchEvent{ID: jobID, Type: api.BatchEventPause}

	// A full channel must not query or remove durable events.
	client.deliverJobEvents(context.Background(), jobID)
	<-sub.ch
	mock.ExpectQuery("SELECT id, event_type FROM batch_events").
		WithArgs(jobID, 1).
		WillReturnRows(pgxmock.NewRows([]string{"id", "event_type"}).AddRow(int64(7), int(api.BatchEventCancel)))
	mock.ExpectExec("DELETE FROM batch_events WHERE job_id").
		WithArgs(jobID, []int64{7}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))

	client.deliverJobEvents(context.Background(), jobID)
	if event := <-sub.ch; event.Type != api.BatchEventCancel {
		t.Fatalf("delivered event = %+v, want cancel", event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

type blockingEventPool struct {
	pgxPool
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (p *blockingEventPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	p.once.Do(func() {
		close(p.entered)
		select {
		case <-p.release:
		case <-ctx.Done():
		}
	})
	return p.pgxPool.Query(ctx, sql, args...)
}

func TestPostgresBatchEventClient_ReplacedSubscriberKeepsEvent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	defer mock.Close()
	pool := &blockingEventPool{pgxPool: mock, entered: make(chan struct{}), release: make(chan struct{})}
	const jobID = "job-replaced"
	oldSub := &eventSub{ch: make(chan api.BatchEvent, 1)}
	newSub := &eventSub{ch: make(chan api.BatchEvent, 1)}
	client := &PostgresBatchEventClient{
		pool: pool, logger: logr.Discard(), eventSubs: map[string]*eventSub{jobID: oldSub},
	}
	mock.ExpectQuery("SELECT id, event_type FROM batch_events").
		WithArgs(jobID, 1).
		WillReturnRows(pgxmock.NewRows([]string{"id", "event_type"}).AddRow(int64(8), int(api.BatchEventCancel)))

	done := make(chan struct{})
	go func() {
		defer close(done)
		client.deliverJobEvents(context.Background(), jobID)
	}()
	<-pool.entered
	client.eventsMu.Lock()
	client.eventSubs[jobID] = newSub
	client.eventsMu.Unlock()
	close(pool.release)
	<-done
	if len(oldSub.ch) != 0 {
		t.Fatal("event delivered to replaced subscriber")
	}

	// The next subscriber can still read the event because it was not acknowledged.
	mock.ExpectQuery("SELECT id, event_type FROM batch_events").
		WithArgs(jobID, 1).
		WillReturnRows(pgxmock.NewRows([]string{"id", "event_type"}).AddRow(int64(8), int(api.BatchEventCancel)))
	mock.ExpectExec("DELETE FROM batch_events WHERE job_id").
		WithArgs(jobID, []int64{8}).
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	client.deliverJobEvents(context.Background(), jobID)
	if event := <-newSub.ch; event.Type != api.BatchEventCancel {
		t.Fatalf("delivered event = %+v, want cancel", event)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresBatchEventClient_FIFORealPG(t *testing.T) {
	url := requirePostgresURL(t)
	ctx := context.Background()
	client := newTestEventClientForURL(t, url)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	jobID := fmt.Sprintf("fifo-%d", time.Now().UnixNano())
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM batch_events WHERE job_id = $1`, jobID) }()

	var ids [3]int64
	if err := pool.QueryRow(ctx, `SELECT nextval('batch_events_id_seq'), nextval('batch_events_id_seq'), nextval('batch_events_id_seq')`).
		Scan(&ids[0], &ids[1], &ids[2]); err != nil {
		t.Fatalf("reserve event IDs: %v", err)
	}
	want := []api.BatchEventType{api.BatchEventCancel, api.BatchEventPause, api.BatchEventResume}
	// Make heap order the reverse of ID order to catch reliance on scan order.
	for i := len(ids) - 1; i >= 0; i-- {
		if _, err := pool.Exec(ctx,
			`INSERT INTO batch_events (id, job_id, event_type, expires_at) VALUES ($1, $2, $3, $4)`,
			ids[i], jobID, int(want[i]), time.Now().Unix()+300); err != nil {
			t.Fatalf("insert event %d: %v", i, err)
		}
	}

	ch, err := client.ECConsumerGetChannel(ctx, jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer ch.CloseFn()
	for i, expected := range want {
		select {
		case event := <-ch.Events:
			if event.Type != expected {
				t.Fatalf("event %d type = %d, want %d", i, event.Type, expected)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

func TestPostgresBatchEventClient_FullChannelRealPG(t *testing.T) {
	url := requirePostgresURL(t)
	ctx := context.Background()
	client := newTestEventClientForURL(t, url)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	jobID := fmt.Sprintf("full-%d", time.Now().UnixNano())
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM batch_events WHERE job_id = $1`, jobID) }()

	ch, err := client.ECConsumerGetChannel(ctx, jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer ch.CloseFn()
	for range cap(ch.Events) {
		ch.Events <- api.BatchEvent{ID: jobID, Type: api.BatchEventPause}
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO batch_events (job_id, event_type, expires_at) VALUES ($1, $2, $3)`,
		jobID, int(api.BatchEventCancel), time.Now().Unix()+300); err != nil {
		t.Fatalf("insert event: %v", err)
	}
	client.deliverJobEvents(ctx, jobID)
	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM batch_events WHERE job_id = $1`, jobID).Scan(&remaining); err != nil {
		t.Fatalf("count pending events: %v", err)
	}
	if remaining != 1 {
		t.Fatalf("pending events with full channel = %d, want 1", remaining)
	}
	<-ch.Events // Make room for the pending event.
	client.deliverJobEvents(ctx, jobID)
	for range cap(ch.Events) - 1 {
		<-ch.Events
	}
	if event := <-ch.Events; event.Type != api.BatchEventCancel {
		t.Fatalf("delivered event = %+v, want cancel", event)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM batch_events WHERE job_id = $1`, jobID).Scan(&remaining); err != nil {
		t.Fatalf("count acknowledged events: %v", err)
	}
	if remaining != 0 {
		t.Fatalf("pending events after delivery = %d, want 0", remaining)
	}
}

func waitForListenerPID(t *testing.T, listener *pgListener, previous uint32) uint32 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if pid := listener.backendPID.Load(); pid != 0 && pid != previous {
			return pid
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("LISTEN backend did not reconnect after PID %d", previous)
	return 0
}

func TestPostgresBatchEventClient_ReconnectRealPG(t *testing.T) {
	url := requirePostgresURL(t)
	ctx := context.Background()
	client := newTestEventClientForURL(t, url)
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	jobID := fmt.Sprintf("reconnect-%d", time.Now().UnixNano())
	defer func() { _, _ = pool.Exec(ctx, `DELETE FROM batch_events WHERE job_id = $1`, jobID) }()
	ch, err := client.ECConsumerGetChannel(ctx, jobID)
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer ch.CloseFn()
	oldPID := waitForListenerPID(t, client.listener, 0)
	var terminated bool
	if err := pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, int(oldPID)).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate LISTEN backend %d: terminated=%t err=%v", oldPID, terminated, err)
	}
	newPID := waitForListenerPID(t, client.listener, oldPID)
	if newPID == oldPID {
		t.Fatal("listener reused terminated backend")
	}
	if _, err := client.ECProducerSendEvents(ctx, []api.BatchEvent{{ID: jobID, Type: api.BatchEventCancel, TTL: 300}}); err != nil {
		t.Fatalf("ECProducerSendEvents after reconnect: %v", err)
	}
	select {
	case event := <-ch.Events:
		if event.ID != jobID || event.Type != api.BatchEventCancel {
			t.Fatalf("event after reconnect = %+v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancel event was not delivered after listener reconnected")
	}
}
