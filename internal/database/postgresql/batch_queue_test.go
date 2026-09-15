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
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

const testProcessorID = "processor-0"

func newTestQueueClient(t *testing.T) (*PostgresBatchQueueClient, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create pgxmock pool: %v", err)
	}
	client := &PostgresBatchQueueClient{
		pgCore:      &pgCore{pool: mock, desc: batchDescriptor{}},
		processorID: testProcessorID,
	}
	return client, mock
}

func TestPQEnqueue(t *testing.T) {
	ctx := context.Background()

	t.Run("enqueues job and notifies", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		mock.ExpectExec("WITH re_enqueued AS").
			WithArgs("batch-1").
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))

		if err := client.PQEnqueue(ctx, &api.BatchJobPriority{ID: "batch-1"}); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns conflict when the job is not owned or is terminal", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		mock.ExpectExec("WITH re_enqueued AS").
			WithArgs("batch-1").
			WillReturnResult(pgxmock.NewResult("UPDATE", 0))

		if err := client.PQEnqueue(ctx, &api.BatchJobPriority{ID: "batch-1"}); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("expected ErrConflict for a no-op re-enqueue, got %v", err)
		}
	})

	t.Run("returns error for nil job priority", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		if err := client.PQEnqueue(ctx, nil); err == nil {
			t.Fatal("expected error for nil job priority")
		}
	})

	t.Run("returns error for empty ID", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		if err := client.PQEnqueue(ctx, &api.BatchJobPriority{}); err == nil {
			t.Fatal("expected error for empty ID")
		}
	})

	t.Run("returns error on failure", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		mock.ExpectExec("WITH re_enqueued AS").
			WithArgs("batch-1").
			WillReturnError(fmt.Errorf("connection refused"))

		if err := client.PQEnqueue(ctx, &api.BatchJobPriority{ID: "batch-1"}); err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestPQClaimOwned(t *testing.T) {
	ctx := context.Background()

	t.Run("returns owned jobs with bumped epoch and attempts", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		mock.ExpectQuery("UPDATE batch_items").
			WithArgs(testProcessorID).
			WillReturnRows(pgxmock.NewRows([]string{"id", "priority", "epoch", "recovery_attempts"}).
				AddRow("batch-1", int64(1234567890123456), int64(4), int64(2)))

		jobs, err := client.PQClaimOwned(ctx)
		if err != nil {
			t.Fatalf("PQClaimOwned: %v", err)
		}
		if len(jobs) != 1 {
			t.Fatalf("expected 1 job, got %d", len(jobs))
		}
		if jobs[0].ID != "batch-1" || jobs[0].Epoch != 4 || jobs[0].RecoveryAttempts != 2 {
			t.Errorf("unexpected job: %+v", jobs[0])
		}
		if !jobs[0].SLO.Equal(time.UnixMicro(1234567890123456)) {
			t.Errorf("SLO: got %v, want %v", jobs[0].SLO, time.UnixMicro(1234567890123456))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns error when processor ID is empty", func(t *testing.T) {
		mock, err := pgxmock.NewPool()
		if err != nil {
			t.Fatalf("failed to create pgxmock pool: %v", err)
		}
		defer mock.Close()
		client := &PostgresBatchQueueClient{pgCore: &pgCore{pool: mock, desc: batchDescriptor{}}}
		if _, err := client.PQClaimOwned(ctx); err == nil {
			t.Fatal("expected error for empty processor ID")
		}
	})
}

func TestPQDequeue(t *testing.T) {
	ctx := context.Background()

	t.Run("claims a job", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		slo := time.Now().Add(time.Hour)
		rows := pgxmock.NewRows([]string{"id", "priority", "epoch"}).
			AddRow("batch-1", slo.UnixMicro(), int64(1))

		mock.ExpectQuery("WITH claimed AS").
			WithArgs(1, testProcessorID).
			WillReturnRows(rows)

		result, err := client.PQDequeue(ctx, 0, 1)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(result) != 1 {
			t.Fatalf("expected 1 job, got %d", len(result))
		}
		if result[0].ID != "batch-1" {
			t.Errorf("expected ID batch-1, got %s", result[0].ID)
		}
		if result[0].SLO.UnixMicro() != slo.UnixMicro() {
			t.Errorf("expected SLO %v, got %v", slo.UnixMicro(), result[0].SLO.UnixMicro())
		}
		if result[0].Epoch != 1 {
			t.Errorf("expected Epoch 1, got %d", result[0].Epoch)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns nil when no jobs available", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		rows := pgxmock.NewRows([]string{"id", "priority", "epoch"})

		mock.ExpectQuery("WITH claimed AS").
			WithArgs(1, testProcessorID).
			WillReturnRows(rows)

		result, err := client.PQDequeue(ctx, 0, 1)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(result) != 0 {
			t.Fatalf("expected empty result, got %v", result)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("claims multiple jobs", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		slo1 := time.Now().Add(time.Hour)
		slo2 := time.Now().Add(2 * time.Hour)
		rows := pgxmock.NewRows([]string{"id", "priority", "epoch"}).
			AddRow("batch-1", slo1.UnixMicro(), int64(1)).
			AddRow("batch-2", slo2.UnixMicro(), int64(1))

		mock.ExpectQuery("WITH claimed AS").
			WithArgs(5, testProcessorID).
			WillReturnRows(rows)

		result, err := client.PQDequeue(ctx, 0, 5)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(result) != 2 {
			t.Fatalf("expected 2 jobs, got %d", len(result))
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		mock.ExpectQuery("WITH claimed AS").
			WithArgs(1, testProcessorID).
			WillReturnError(fmt.Errorf("connection refused"))

		_, err := client.PQDequeue(ctx, 0, 1)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestPQDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("cancels unclaimed job", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		mock.ExpectExec("WITH queued AS").
			WithArgs("batch-1", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))

		n, err := client.PQDelete(ctx, &api.BatchJobPriority{ID: "batch-1"})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if n != 1 {
			t.Errorf("expected 1 affected row, got %d", n)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns 0 for already claimed job", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		mock.ExpectExec("WITH queued AS").
			WithArgs("batch-1", pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("UPDATE", 0))

		n, err := client.PQDelete(ctx, &api.BatchJobPriority{ID: "batch-1"})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if n != 0 {
			t.Errorf("expected 0 affected rows, got %d", n)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns error for nil job priority", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		_, err := client.PQDelete(ctx, nil)
		if err == nil {
			t.Fatal("expected error for nil job priority")
		}
	})
}

func TestPQGetIDs(t *testing.T) {
	ctx := context.Background()

	t.Run("returns queued job IDs", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		rows := pgxmock.NewRows([]string{"id"}).
			AddRow("batch-1").
			AddRow("batch-2")

		mock.ExpectQuery("SELECT id FROM batch_items").
			WillReturnRows(rows)

		ids, err := client.PQGetIDs(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(ids) != 2 {
			t.Fatalf("expected 2 IDs, got %d", len(ids))
		}
		if !ids["batch-1"] || !ids["batch-2"] {
			t.Errorf("expected batch-1 and batch-2, got %v", ids)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns empty map when no jobs queued", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		rows := pgxmock.NewRows([]string{"id"})

		mock.ExpectQuery("SELECT id FROM batch_items").
			WillReturnRows(rows)

		ids, err := client.PQGetIDs(ctx)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(ids) != 0 {
			t.Errorf("expected empty map, got %v", ids)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns error on query failure", func(t *testing.T) {
		client, mock := newTestQueueClient(t)
		defer mock.Close()

		mock.ExpectQuery("SELECT id FROM batch_items").
			WillReturnError(fmt.Errorf("connection refused"))

		_, err := client.PQGetIDs(ctx)
		if err == nil {
			t.Fatal("expected error")
		}
	})
}
