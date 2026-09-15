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

const (
	fileTestTable    = "file_items"
	fileTestTenantID = "tenant-1"
)

func newTestFileClient(t *testing.T) (*PostgresFileDBClient, pgxmock.PgxPoolIface) {
	t.Helper()

	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create pgxmock pool: %v", err)
	}

	client := &PostgresFileDBClient{
		pgCore: &pgCore{pool: mock, desc: fileTableDescriptor{}},
	}

	return client, mock
}

func newTestFileItem(id, tenantID string) *api.FileItem {
	return &api.FileItem{
		BaseIndexes: api.BaseIndexes{
			ID:       id,
			TenantID: tenantID,
			Expiry:   time.Now().Add(24 * time.Hour).Unix(),
			Tags:     api.Tags{"purpose": "batch"},
		},
		Purpose: "batch",
		BaseContents: api.BaseContents{
			Spec: []byte(`{"filename":"input.jsonl","bytes":1024}`),
		},
	}
}

func TestFileStore(t *testing.T) {
	ctx := context.Background()

	t.Run("stores file item successfully", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		item := newTestFileItem("file-1", fileTestTenantID)

		mock.ExpectExec("INSERT INTO "+fileTestTable).
			WithArgs("file-1", fileTestTenantID, pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))

		if err := client.DBStore(ctx, item); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns error for nil item", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		if err := client.DBStore(ctx, nil); err == nil {
			t.Fatal("expected error for nil item")
		}
	})
}

func TestFileGet(t *testing.T) {
	ctx := context.Background()

	t.Run("returns nil for nil query", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		items, cursor, expectMore, err := client.DBGet(ctx, nil, true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if items != nil || cursor != 0 || expectMore {
			t.Fatal("expected empty result for nil query")
		}
	})

	t.Run("gets file by tenant ID with purpose", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		item := newTestFileItem("file-1", fileTestTenantID)
		tags, _ := packTags(item.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colPurpose, colStatus, colSpec}).
			AddRow(item.ID, item.TenantID, item.Expiry, &tags, item.Purpose, item.Status, item.Spec)

		// The SQL LIMIT is limit+1 because get() fetches an extra row to determine
		// if more results exist beyond the requested page.
		mock.ExpectQuery("SELECT .+ FROM "+fileTestTable+" WHERE").
			WithArgs(fileTestTenantID, 0, 11).
			WillReturnRows(rows)

		items, _, _, err := client.DBGet(ctx,
			&api.FileQuery{BaseQuery: api.BaseQuery{TenantID: fileTestTenantID}},
			true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].Purpose != "batch" {
			t.Errorf("expected purpose batch, got %s", items[0].Purpose)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("filters by purpose", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		item := newTestFileItem("file-1", fileTestTenantID)
		tags, _ := packTags(item.Tags)

		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colPurpose, colStatus, colSpec}).
			AddRow(item.ID, item.TenantID, item.Expiry, &tags, item.Purpose, item.Status, item.Spec)

		mock.ExpectQuery("SELECT .+ FROM "+fileTestTable+" WHERE").
			WithArgs(fileTestTenantID, "batch", 0, 11).
			WillReturnRows(rows)

		items, _, _, err := client.DBGet(ctx,
			&api.FileQuery{
				BaseQuery: api.BaseQuery{TenantID: fileTestTenantID},
				Purpose:   "batch",
			},
			true, 0, 10)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected 1 item, got %d", len(items))
		}
		if items[0].Purpose != "batch" {
			t.Errorf("expected purpose batch, got %s", items[0].Purpose)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestFileUpdate(t *testing.T) {
	ctx := context.Background()

	t.Run("updates file tags", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		item := newTestFileItem("file-1", fileTestTenantID)

		mock.ExpectExec("UPDATE "+fileTestTable+" SET").
			WithArgs(pgxmock.AnyArg(), "file-1").
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))

		if err := client.DBUpdate(ctx, item, nil); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns error for nil item", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		if err := client.DBUpdate(ctx, nil, nil); err == nil {
			t.Fatal("expected error for nil item")
		}
	})

	t.Run("returns error for non-existent ID", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		item := newTestFileItem("nonexistent", fileTestTenantID)

		mock.ExpectExec("UPDATE "+fileTestTable+" SET").
			WithArgs(pgxmock.AnyArg(), "nonexistent").
			WillReturnResult(pgxmock.NewResult("UPDATE", 0))

		if err := client.DBUpdate(ctx, item, nil); err == nil {
			t.Fatal("expected error for non-existent ID")
		}

		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

func TestFileDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes files and returns deleted IDs", func(t *testing.T) {
		client, mock := newTestFileClient(t)
		defer mock.Close()

		rows := pgxmock.NewRows([]string{colID}).
			AddRow("file-1").
			AddRow("file-2")

		mock.ExpectQuery("DELETE FROM " + fileTestTable + " WHERE").
			WithArgs([]string{"file-1", "file-2"}).
			WillReturnRows(rows)

		deletedIDs, err := client.DBDelete(ctx, []string{"file-1", "file-2"})
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
