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

	"github.com/pashagolub/pgxmock/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func newTestResumableStore(t *testing.T) (*PostgresResumableStore, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create pgxmock pool: %v", err)
	}
	return &PostgresResumableStore{pgCore: &pgCore{pool: mock, desc: batchDescriptor{}}}, mock
}

func TestResumableStoreInputValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("activation rejects malformed requests", func(t *testing.T) {
		store, mock := newTestResumableStore(t)
		defer mock.Close()

		valid := &api.ResumableActivation{
			BatchID:     "batch-1",
			ProcessorID: testProcessorID,
			Epoch:       1,
			LeaseTTL:    time.Minute,
			Entries:     []api.ResumableActivationEntry{{RequestID: "r1", Ordinal: 0, CustomID: "c1", Status: "pending"}},
		}
		for name, mutate := range map[string]func(*api.ResumableActivation){
			"nil":             func(*api.ResumableActivation) {},
			"empty batch ID":  func(a *api.ResumableActivation) { a.BatchID = "" },
			"empty processor": func(a *api.ResumableActivation) { a.ProcessorID = "" },
			"zero lease TTL":  func(a *api.ResumableActivation) { a.LeaseTTL = 0 },
			"empty entry set": func(a *api.ResumableActivation) { a.Entries = nil },
		} {
			a := *valid
			mutate(&a)
			if err := store.ActivateResumableBatch(ctx, &a); err == nil {
				t.Errorf("%s: expected error", name)
			}
		}
		if err := store.ActivateResumableBatch(ctx, nil); err == nil {
			t.Error("nil activation: expected error")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("validation must reject before issuing SQL: %v", err)
		}
	})

	t.Run("renew rejects non-positive TTL", func(t *testing.T) {
		store, mock := newTestResumableStore(t)
		defer mock.Close()

		if err := store.RenewResumableBatchLease(ctx, "batch-1", "instance-0", 1, 0); err == nil {
			t.Fatal("expected error for zero TTL")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("validation must reject before issuing SQL: %v", err)
		}
	})

	t.Run("checkpoint rejects nil result", func(t *testing.T) {
		store, mock := newTestResumableStore(t)
		defer mock.Close()

		if _, err := store.CheckpointEntryResult(ctx, "batch-1", "r1", "instance-0", 1, nil); err == nil {
			t.Fatal("expected error for nil result")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("validation must reject before issuing SQL: %v", err)
		}
	})

	t.Run("list rejects non-positive limit", func(t *testing.T) {
		store, mock := newTestResumableStore(t)
		defer mock.Close()

		if _, err := store.ListBatchEntries(ctx, "batch-1", 0, 0); err == nil {
			t.Fatal("expected error for zero limit")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("validation must reject before issuing SQL: %v", err)
		}
	})
}
