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
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

const (
	testTable    = "batch_items"
	testTenantID = "tenant-1"
)

func newTestBatchClient(t *testing.T) (*PostgresBatchDBClient, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create pgxmock pool: %v", err)
	}

	client := &PostgresBatchDBClient{
		pgCore: &pgCore{pool: mock, desc: batchDescriptor{}},
	}

	return client, mock
}

func newTestBatchItem(id, tenantID string) *api.BatchItem {
	return &api.BatchItem{
		BaseIndexes: api.BaseIndexes{
			ID:       id,
			TenantID: tenantID,
			Expiry:   time.Now().Add(24 * time.Hour).Unix(),
			Tags:     api.Tags{"purpose": "batch"},
		},
		BaseContents: api.BaseContents{
			Spec:   []byte(`{"endpoint":"/v1/chat/completions"}`),
			Status: []byte(`{"status":"validating"}`),
		},
	}
}

func TestBatchStore(t *testing.T) {
	ctx := context.Background()

	t.Run("stores item successfully", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)

		mock.ExpectExec("INSERT INTO "+testTable).
			WithArgs("batch-1", testTenantID, pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		if err := client.DBStore(ctx, item); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns error for nil item", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		if err := client.DBStore(ctx, nil); err == nil {
			t.Fatal("expected error for nil item")
		}
	})
}

func TestBatchGet(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil for nil query", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		items, cursor, expectMore, err := client.DBGet(ctx, nil, true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if items != nil || cursor != 0 || expectMore {
			t.Fatal("expected empty result for nil query")
		}
	})

	t.Run("gets items by tenant ID with static", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		tags, _ := packTags(item.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus, colSpec}).
			AddRow(item.ID, item.TenantID, item.Expiry, &tags, "", int64(0), int64(0), int64(0), item.Status, item.Spec)

		// The SQL LIMIT is limit+1 because get() fetches an extra row to determine
		// if more results exist beyond the requested page.
		mock.ExpectQuery("SELECT .+ FROM "+testTable+" WHERE").
			WithArgs(testTenantID, 0, 11).
			WillReturnRows(rows)

		items, cursor, expectMore, err := client.DBGet(ctx,
			&api.BatchQuery{BaseQuery: api.BaseQuery{TenantID: testTenantID}},
			true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].ID != "batch-1" {
			t.Errorf("expected ID batch-1, got %s", items[0].ID)
		}
		if items[0].TenantID != testTenantID {
			t.Errorf("expected tenant %s, got %s", testTenantID, items[0].TenantID)
		}
		if cursor != 1 || expectMore {
			t.Errorf("unexpected pagination: cursor=%d, expectMore=%v", cursor, expectMore)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("gets items without static data", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		tags, _ := packTags(item.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus}).
			AddRow(item.ID, item.TenantID, item.Expiry, &tags, "", int64(0), int64(0), int64(0), item.Status)

		mock.ExpectQuery("SELECT .+ FROM "+testTable+" WHERE").
			WithArgs(testTenantID, 0, 11).
			WillReturnRows(rows)

		items, _, _, err := client.DBGet(ctx,
			&api.BatchQuery{BaseQuery: api.BaseQuery{TenantID: testTenantID}},
			false, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].Spec != nil {
			t.Error("expected nil spec when includeStatic=false")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("gets items by IDs", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item1 := newTestBatchItem("batch-1", testTenantID)
		item2 := newTestBatchItem("batch-2", testTenantID)
		tags1, _ := packTags(item1.Tags)
		tags2, _ := packTags(item2.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus, colSpec}).
			AddRow(item1.ID, item1.TenantID, item1.Expiry, &tags1, "", int64(0), int64(0), int64(0), item1.Status, item1.Spec).
			AddRow(item2.ID, item2.TenantID, item2.Expiry, &tags2, "", int64(0), int64(0), int64(0), item2.Status, item2.Spec)

		mock.ExpectQuery("SELECT .+ FROM "+testTable+" WHERE").
			WithArgs([]string{"batch-1", "batch-2"}, 0, 11).
			WillReturnRows(rows)

		items, _, _, err := client.DBGet(ctx,
			&api.BatchQuery{BaseQuery: api.BaseQuery{IDs: []string{"batch-1", "batch-2"}}},
			true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 2 {
			t.Fatalf("expected 2 items, got %d", len(items))
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("gets items by tag selectors with AND", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		item.Tags = api.Tags{"env": "prod", "team": "ml"}
		tags, _ := packTags(item.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus, colSpec}).
			AddRow(item.ID, item.TenantID, item.Expiry, &tags, "", int64(0), int64(0), int64(0), item.Status, item.Spec)

		mock.ExpectQuery("SELECT .+ FROM "+testTable+" WHERE").
			WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), 0, 11).
			WillReturnRows(rows)

		items, _, _, err := client.DBGet(ctx,
			&api.BatchQuery{BaseQuery: api.BaseQuery{
				TagSelectors:    api.Tags{"env": "prod", "team": "ml"},
				TagsLogicalCond: api.LogicalCondAnd,
			}},
			true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("gets non-terminal items", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		tags, _ := packTags(item.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus, colSpec}).
			AddRow(item.ID, item.TenantID, item.Expiry, &tags, "", int64(0), int64(0), int64(0), item.Status, item.Spec)

		mock.ExpectQuery("SELECT .+ FROM "+testTable+" WHERE .+NOT IN").
			WithArgs(0, 11).
			WillReturnRows(rows)

		items, _, _, err := client.DBGet(ctx,
			&api.BatchQuery{NonTerminal: true},
			true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("gets non-terminal items filtered by tenant", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		tags, _ := packTags(item.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus, colSpec}).
			AddRow(item.ID, item.TenantID, item.Expiry, &tags, "", int64(0), int64(0), int64(0), item.Status, item.Spec)

		mock.ExpectQuery("SELECT .+ FROM "+testTable+" WHERE .+NOT IN").
			WithArgs(testTenantID, 0, 11).
			WillReturnRows(rows)

		items, _, _, err := client.DBGet(ctx,
			&api.BatchQuery{
				BaseQuery:   api.BaseQuery{TenantID: testTenantID},
				NonTerminal: true,
			},
			true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("gets items by tag selectors with OR", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		tags, _ := packTags(item.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus, colSpec}).
			AddRow(item.ID, item.TenantID, item.Expiry, &tags, "", int64(0), int64(0), int64(0), item.Status, item.Spec)

		mock.ExpectQuery("SELECT .+ FROM "+testTable+" WHERE").
			WithArgs(pgxmock.AnyArg(), 0, 11).
			WillReturnRows(rows)

		items, _, _, err := client.DBGet(ctx,
			&api.BatchQuery{BaseQuery: api.BaseQuery{
				TagSelectors:    api.Tags{"purpose": "batch"},
				TagsLogicalCond: api.LogicalCondOr,
			}},
			true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestBatchUpdate(t *testing.T) {
	ctx := context.Background()

	t.Run("updates status and tags", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)

		mock.ExpectExec("UPDATE "+testTable+" SET").
			WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), "batch-1").
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))

		if err := client.DBUpdate(ctx, item, nil); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("CAS succeeds when status matches", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		expectedStatus := []byte(`{"status":"validating"}`)

		mock.ExpectExec("UPDATE "+testTable+" SET").
			WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), "batch-1", expectedStatus).
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))

		if err := client.DBUpdate(ctx, item, expectedStatus); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("CAS returns ErrConflict when status differs", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		expectedStatus := []byte(`{"status":"validating"}`)

		mock.ExpectExec("UPDATE "+testTable+" SET").
			WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), "batch-1", expectedStatus).
			WillReturnResult(pgxmock.NewResult("UPDATE", 0))

		err := client.DBUpdate(ctx, item, expectedStatus)
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("returns error for nil item", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		if err := client.DBUpdate(ctx, nil, nil); err == nil {
			t.Fatal("expected error for nil item")
		}
	})
}

func TestBatchDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes items and returns deleted IDs", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		rows := pgxmock.NewRows([]string{colID}).
			AddRow("batch-1").
			AddRow("batch-2")

		mock.ExpectQuery("DELETE FROM " + testTable + " WHERE").
			WithArgs([]string{"batch-1", "batch-2", "batch-nonexistent"}).
			WillReturnRows(rows)

		deletedIDs, err := client.DBDelete(ctx, []string{"batch-1", "batch-2", "batch-nonexistent"})
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(deletedIDs) != 2 {
			t.Fatalf("expected 2 deleted IDs, got %d", len(deletedIDs))
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}
