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
	"os"
	"testing"

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
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

// TestNewClientset_PostgreSQL requires a real PostgreSQL instance
// (TEST_POSTGRES_URL) and verifies that NewClientset wires a different event
// client per component: a full channel client for the processor, a
// producer-only client for the apiserver, and a purge client for the GC.
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

	t.Run("unsupported component", func(t *testing.T) {
		// NewClientset must fail; the deferred cleanup in NewClientset closes
		// whatever was already constructed, so subsequent uses of the shared
		// pool (e.g. another call to Close in a cleanup function) are safe but
		// a BatchDB call now fails because the pool is closed.
		cs, err := NewClientset(ctx, "unsupported", WithDB(sharedcfg.DBClientConfig{
			Type:          sharedcfg.DBTypePostgreSQL,
			PostgreSQLCfg: postgresql.PostgreSQLConfig{Url: url},
		}))
		if err == nil {
			_ = cs.Close()
			t.Fatal("expected error for unsupported component")
		}
	})
}
