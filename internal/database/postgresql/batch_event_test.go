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
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/pashagolub/pgxmock/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func newTestEventClient(t *testing.T) (*PostgresBatchEventClient, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	return &PostgresBatchEventClient{
		pool:           mock,
		logger:         logr.Discard(),
		eventSubs:      make(map[string]*eventSubscription),
		rescanInterval: time.Hour,
		rescanNow:      make(chan struct{}, 1),
	}, mock
}

func TestPostgresBatchEventClientProducer(t *testing.T) {
	client, mock := newTestEventClient(t)
	defer mock.Close()

	mock.ExpectExec("WITH inserted AS").
		WithArgs("job-1", int(api.BatchEventCancel), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	ids, err := client.ECProducerSendEvents(context.Background(), []api.BatchEvent{{
		ID: "job-1", Type: api.BatchEventCancel, TTL: 60,
	}})
	if err != nil {
		t.Fatalf("ECProducerSendEvents: %v", err)
	}
	if len(ids) != 1 || ids[0] != "job-1" {
		t.Fatalf("sent IDs = %v, want [job-1]", ids)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresBatchEventClientLateAttach(t *testing.T) {
	client, mock := newTestEventClient(t)
	defer mock.Close()

	mock.ExpectQuery("WITH selected AS").
		WithArgs("job-1").
		WillReturnRows(pgxmock.NewRows([]string{"event_type"}).AddRow(int(api.BatchEventCancel)))

	channel, err := client.ECConsumerGetChannel(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("ECConsumerGetChannel: %v", err)
	}
	defer channel.CloseFn()

	select {
	case event := <-channel.Events:
		if event.ID != "job-1" || event.Type != api.BatchEventCancel {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("late event was not delivered")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresBatchEventClientPurgeExpired(t *testing.T) {
	client, mock := newTestEventClient(t)
	defer mock.Close()
	mock.ExpectExec("DELETE FROM batch_events").
		WillReturnResult(pgxmock.NewResult("DELETE", 2))

	purged, err := client.purgeExpiredEvents(context.Background())
	if err != nil {
		t.Fatalf("purgeExpiredEvents: %v", err)
	}
	if purged != 2 {
		t.Fatalf("purged = %d, want 2", purged)
	}
}

func TestPGListenerSubscribeAndUnsubscribe(t *testing.T) {
	listener := &pgListener{
		logger: logr.Discard(),
		subs:   make(map[int]chan string),
		done:   make(chan struct{}),
	}
	close(listener.done)

	wake, unsubscribe := listener.subscribe()
	listener.deliver("job-1")
	if got := <-wake; got != "job-1" {
		t.Fatalf("notification = %q, want job-1", got)
	}
	unsubscribe()
	listener.deliver("job-2")
	select {
	case got := <-wake:
		t.Fatalf("unexpected notification after unsubscribe: %q", got)
	default:
	}
}
