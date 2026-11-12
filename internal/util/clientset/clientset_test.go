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

package clientset

import (
	"context"
	"fmt"
	neturl "net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	sharedcfg "github.com/llm-d/llm-d-batch-gateway/internal/shared/config"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

func requirePostgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	return url
}

func newClientsetForURL(t *testing.T, url string, component ucom.Component) *Clientset {
	t.Helper()
	cs, err := NewClientset(context.Background(), component, WithDB(sharedcfg.DBClientConfig{
		Type:          sharedcfg.DBTypePostgreSQL,
		PostgreSQLCfg: postgresql.PostgreSQLConfig{Url: url},
	}))
	if err != nil {
		t.Fatalf("NewClientset(%s): %v", component, err)
	}
	t.Cleanup(func() {
		// t.Errorf rather than t.Fatalf: a cleanup function may run after
		// the test body has finished, where FailNow is not allowed.
		if err := cs.Close(); err != nil {
			t.Errorf("Close(%s): %v", component, err)
		}
	})
	return cs
}

// withApplicationName returns connStr with the libpq application_name
// parameter set to name, so that connections opened with it are identifiable
// in pg_stat_activity. Both URL-style and keyword/value connection strings
// are supported.
func withApplicationName(t *testing.T, connStr, name string) string {
	t.Helper()
	if !strings.Contains(connStr, "://") {
		return connStr + " application_name=" + name
	}
	u, err := neturl.Parse(connStr)
	if err != nil {
		t.Fatalf("parse connection string: %v", err)
	}
	q := u.Query()
	q.Set("application_name", name)
	u.RawQuery = q.Encode()
	return u.String()
}

// requireTaggedConnectionVisible sanity-checks the pg_stat_activity
// counting used by the unsupported-component subtest: a pool opened with
// the tagged connection string must appear in the count while it is open,
// so a later count of zero cannot pass vacuously. The probe pool is closed
// via defer when this helper returns — it must not survive into the later
// count-to-zero polling.
func requireTaggedConnectionVisible(t *testing.T, ctx context.Context, taggedURL, appName string, count func() int) {
	t.Helper()
	probe, err := pgxpool.New(ctx, taggedURL)
	if err != nil {
		t.Fatalf("probe pool: %v", err)
	}
	defer probe.Close()
	if err := probe.Ping(ctx); err != nil {
		t.Fatalf("probe ping: %v", err)
	}
	if n := count(); n == 0 {
		t.Fatalf("no connection with application_name %q is visible; the pg_stat_activity check cannot work", appName)
	}
}

// TestNewClientset_PostgreSQL requires a real PostgreSQL instance
// (TEST_POSTGRES_URL) and verifies that NewClientset wires a different event
// client per component: a full channel client for the processor, a
// producer-only client for the apiserver, and a purge client for the GC.
// It also covers the event round trip from the apiserver's producer to the
// processor's consumer, the close path (database calls fail after Close),
// and the failure path for unsupported components (the pools built before
// the failure are closed).
func TestNewClientset_PostgreSQL(t *testing.T) {
	url := requirePostgresURL(t)
	ctx := context.Background()

	t.Run("processor", func(t *testing.T) {
		cs := newClientsetForURL(t, url, ucom.ComponentProcessor)

		if cs.BatchDB == nil || cs.FileDB == nil || cs.Queue == nil {
			t.Fatal("expected BatchDB, FileDB and Queue to be created for the processor")
		}
		if cs.BatchProgressDB == nil {
			t.Fatal("expected BatchProgressDB to be created for the processor")
		}
		// The processor updates progress through the same client it reads from,
		// so both interfaces must be backed by one shared object.
		batchDB, ok := cs.BatchDB.(*postgresql.PostgresBatchDBClient)
		if !ok {
			t.Fatalf("BatchDB = %T, want *postgresql.PostgresBatchDBClient", cs.BatchDB)
		}
		if progressDB, ok := cs.BatchProgressDB.(*postgresql.PostgresBatchDBClient); !ok || progressDB != batchDB {
			t.Fatalf("BatchProgressDB must be the same client as BatchDB, got %T", cs.BatchProgressDB)
		}

		ch, err := cs.Event.ECConsumerGetChannel(ctx, "wiring-test-processor")
		if err != nil {
			t.Fatalf("ECConsumerGetChannel: %v", err)
		}
		ch.CloseFn()
		if cs.EventPurge != nil {
			t.Fatal("expected EventPurge to be nil for the processor")
		}
	})

	t.Run("apiserver", func(t *testing.T) {
		cs := newClientsetForURL(t, url, ucom.ComponentApiserver)

		if cs.BatchProgressDB != nil {
			t.Fatal("expected BatchProgressDB to be nil for the apiserver")
		}
		if _, err := cs.Event.ECConsumerGetChannel(ctx, "wiring-test-apiserver"); err == nil {
			t.Fatal("producer accepted a subscription")
		}
		if cs.EventPurge != nil {
			t.Fatal("expected EventPurge to be nil for the apiserver")
		}
	})

	t.Run("apiserver to processor event round trip", func(t *testing.T) {
		// The producer built for the apiserver and the channel client built
		// for the processor are separate clients; verify that an event sent
		// by the former is delivered to the latter through PostgreSQL. The
		// job ID is unique per run: events are only deleted after delivery,
		// so a row left behind by a failed earlier run (within its TTL) must
		// not satisfy the receive assertion and mask a delivery regression.
		jobID := fmt.Sprintf("wiring-round-trip-job-%d", time.Now().UnixNano())
		processor := newClientsetForURL(t, url, ucom.ComponentProcessor)
		apiServer := newClientsetForURL(t, url, ucom.ComponentApiserver)

		ch, err := processor.Event.ECConsumerGetChannel(ctx, jobID)
		if err != nil {
			t.Fatalf("ECConsumerGetChannel: %v", err)
		}
		defer ch.CloseFn()

		sent, err := apiServer.Event.ECProducerSendEvents(ctx, []dbapi.BatchEvent{
			{ID: jobID, Type: dbapi.BatchEventPause, TTL: 300},
		})
		if err != nil {
			t.Fatalf("ECProducerSendEvents: %v", err)
		}
		if len(sent) != 1 || sent[0] != jobID {
			t.Fatalf("ECProducerSendEvents sent %v, want [%s]", sent, jobID)
		}

		// The 35s window covers the NOTIFY fast path (sub-second) and the
		// 30s backstop rescan.
		select {
		case ev := <-ch.Events:
			if ev.ID != jobID || ev.Type != dbapi.BatchEventPause {
				t.Fatalf("unexpected event: %+v", ev)
			}
		case <-time.After(35 * time.Second):
			t.Fatal("event not delivered within 35s (NOTIFY path or backstop rescan)")
		}
	})

	t.Run("gc", func(t *testing.T) {
		cs := newClientsetForURL(t, url, ucom.ComponentGC)

		if cs.Event != nil {
			t.Fatal("expected Event to be nil for the GC")
		}
		if cs.EventPurge == nil {
			t.Fatal("expected EventPurge to be created for the GC")
		}
		if _, err := cs.EventPurge.PurgeExpiredEvents(ctx); err != nil {
			t.Fatalf("PurgeExpiredEvents: %v", err)
		}
	})

	t.Run("close path rejects database calls", func(t *testing.T) {
		// The close path from #714: after Close the pools are gone, so any
		// BatchDB call must fail instead of silently reaching the database.
		// Every underlying client guards Close with closeOnce, so the
		// second Close from the t.Cleanup registered by newClientsetForURL
		// is safe and returns nil.
		cs := newClientsetForURL(t, url, ucom.ComponentProcessor)
		if err := cs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if _, _, _, err := cs.BatchDB.DBGet(ctx,
			&dbapi.BatchQuery{BaseQuery: dbapi.BaseQuery{IDs: []string{"wiring-close-test"}}},
			false, 0, 10); err == nil {
			t.Fatal("expected BatchDB.DBGet to fail after Close (pool closed)")
		}
	})

	t.Run("unsupported component", func(t *testing.T) {
		// NewClientset must fail and return a nil clientset. Its deferred
		// cleanup closes the clients built before the failure; verify that
		// their pools are really gone by tagging the connections with a
		// unique application_name and watching pg_stat_activity.
		const appName = "clientset-unsupported-component"
		tagged := withApplicationName(t, url, appName)

		observer, err := pgxpool.New(ctx, url)
		if err != nil {
			t.Fatalf("observer pool: %v", err)
		}
		defer observer.Close()
		countTagged := func() int {
			var n int
			if err := observer.QueryRow(ctx,
				`SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`,
				appName).Scan(&n); err != nil {
				t.Fatalf("count pg_stat_activity: %v", err)
			}
			return n
		}

		// Sanity check for the counting itself, so the zero count below
		// cannot pass vacuously.
		requireTaggedConnectionVisible(t, ctx, tagged, appName, countTagged)

		cs, err := NewClientset(ctx, "unsupported", WithDB(sharedcfg.DBClientConfig{
			Type:          sharedcfg.DBTypePostgreSQL,
			PostgreSQLCfg: postgresql.PostgreSQLConfig{Url: tagged},
		}))
		if err == nil {
			_ = cs.Close()
			t.Fatal("expected error for unsupported component")
		}
		if cs != nil {
			t.Fatalf("expected nil clientset on error, got %+v", cs)
		}

		// Backend rows can briefly outlive the client sockets, so poll
		// until every tagged connection is gone.
		deadline := time.Now().Add(10 * time.Second)
		for {
			n := countTagged()
			if n == 0 {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d connection(s) with application_name %q still open: pools built before the failure were not closed", n, appName)
			}
			time.Sleep(100 * time.Millisecond)
		}
	})
}
