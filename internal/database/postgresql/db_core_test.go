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
	"testing"

	"github.com/pashagolub/pgxmock/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

// newTestCore creates a pgCore with a mock pool using the batch descriptor.
func newTestCore(t *testing.T) (*pgCore, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("failed to create pgxmock pool: %v", err)
	}
	return &pgCore{pool: mock, desc: batchDescriptor{}}, mock
}

// --- packTags / unpackTags ---

func TestPackTags(t *testing.T) {
	t.Run("nil tags returns empty JSON object", func(t *testing.T) {
		result, err := packTags(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "{}" {
			t.Errorf("expected \"{}\", got %q", result)
		}
	})

	t.Run("empty map returns empty JSON object", func(t *testing.T) {
		result, err := packTags(api.Tags{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != "{}" {
			t.Errorf("expected \"{}\", got %q", result)
		}
	})

	t.Run("single tag", func(t *testing.T) {
		result, err := packTags(api.Tags{"key": "value"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != `{"key":"value"}` {
			t.Errorf("unexpected result: %s", result)
		}
	})

	t.Run("round-trip", func(t *testing.T) {
		tags := api.Tags{"env": "prod", "team": "ml"}
		result, err := packTags(tags)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		unpacked, err := unpackTags(result)
		if err != nil {
			t.Fatalf("unpack error: %v", err)
		}
		if unpacked["env"] != "prod" || unpacked["team"] != "ml" {
			t.Errorf("round-trip failed: got %v, want %v", unpacked, tags)
		}
	})
}

func TestUnpackTags(t *testing.T) {
	t.Run("empty string returns nil", func(t *testing.T) {
		result, err := unpackTags("")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})

	t.Run("invalid JSON returns error", func(t *testing.T) {
		_, err := unpackTags("{invalid")
		if err == nil {
			t.Fatal("expected error for invalid JSON")
		}
	})
}

// --- store ---

func TestCoreStore_ValidationError(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	idx := &api.BaseIndexes{ID: "", TenantID: "t1"}
	contents := &api.BaseContents{Spec: []byte(`{}`)}

	if err := core.store(context.Background(), idx, contents, nil); err == nil {
		t.Fatal("expected validation error for empty ID")
	}
}

func TestCoreStore_DBFailure(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	idx := &api.BaseIndexes{ID: "id-1", TenantID: "t1"}
	contents := &api.BaseContents{Spec: []byte(`{}`), Status: []byte(`{}`)}

	mock.ExpectExec("INSERT INTO").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnError(fmt.Errorf("connection refused"))

	extras := map[string]any{colProcessorID: "", colPriority: int64(0), colEpoch: int64(0), colRecoveryAttempts: int64(0)}
	if err := core.store(context.Background(), idx, contents, extras); err == nil {
		t.Fatal("expected error on DB failure")
	}
}

func TestCoreStore_NilTags(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	idx := &api.BaseIndexes{ID: "id-1", TenantID: "t1", Tags: nil}
	contents := &api.BaseContents{Spec: []byte(`{}`), Status: []byte(`{}`)}

	mock.ExpectExec("INSERT INTO").
		WithArgs("id-1", "t1", pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg(), pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))

	extras := map[string]any{colProcessorID: "", colPriority: int64(0), colEpoch: int64(0), colRecoveryAttempts: int64(0)}
	if err := core.store(context.Background(), idx, contents, extras); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// --- get ---

func TestCoreGet_EmptyQuery(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	indexes, contents, _, cursor, expectMore, err := core.get(
		context.Background(), &api.BaseQuery{}, true, 0, 10, nil, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(indexes) != 0 || len(contents) != 0 {
		t.Error("expected empty results for empty query")
	}
	if cursor != 0 || expectMore {
		t.Errorf("unexpected pagination: cursor=%d, expectMore=%v", cursor, expectMore)
	}
}

func TestCoreGet_DBFailure(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	// The SQL LIMIT is limit+1 because get() fetches an extra row to determine
	// if more results exist beyond the requested page.
	mock.ExpectQuery("SELECT .+ FROM batch_items WHERE").
		WithArgs("t1", 0, 11).
		WillReturnError(fmt.Errorf("connection refused"))

	_, _, _, _, _, err := core.get(
		context.Background(), &api.BaseQuery{TenantID: "t1"}, true, 0, 10, nil, nil)
	if err == nil {
		t.Fatal("expected error on DB failure")
	}
}

func TestCoreGet_Expired(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	tags := `{"purpose":"batch"}`
	rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus, colSpec}).
		AddRow("id-1", "t1", int64(100), &tags, "", int64(0), int64(0), int64(0), []byte(`{}`), []byte(`{}`))

	mock.ExpectQuery("SELECT .+ FROM batch_items WHERE").
		WithArgs(0, 11).
		WillReturnRows(rows)

	indexes, _, _, _, _, err := core.get(
		context.Background(), &api.BaseQuery{Expired: true}, true, 0, 10, nil, nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(indexes) != 1 {
		t.Fatalf("expected 1 item, got %d", len(indexes))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCoreGet_Pagination(t *testing.T) {
	t.Run("expectMore true when extra row exists", func(t *testing.T) {
		core, mock := newTestCore(t)
		defer mock.Close()

		tags := `{"k":"v"}`
		// Return limit+1 rows (3) to indicate more results exist.
		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus}).
			AddRow("id-1", "t1", int64(0), &tags, "", int64(0), int64(0), int64(0), []byte(`{}`)).
			AddRow("id-2", "t1", int64(0), &tags, "", int64(0), int64(0), int64(0), []byte(`{}`)).
			AddRow("id-3", "t1", int64(0), &tags, "", int64(0), int64(0), int64(0), []byte(`{}`))

		// get() requests limit+1 rows from the DB.
		mock.ExpectQuery("SELECT .+ FROM batch_items WHERE").
			WithArgs("t1", 0, 3).
			WillReturnRows(rows)

		indexes, _, _, cursor, expectMore, err := core.get(
			context.Background(), &api.BaseQuery{TenantID: "t1"}, false, 0, 2, nil, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(indexes) != 2 {
			t.Errorf("expected 2 items (trimmed), got %d", len(indexes))
		}
		if cursor != 2 {
			t.Errorf("expected cursor 2, got %d", cursor)
		}
		if !expectMore {
			t.Error("expected expectMore=true when more rows exist beyond limit")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("expectMore false on last page", func(t *testing.T) {
		core, mock := newTestCore(t)
		defer mock.Close()

		tags := `{"k":"v"}`
		// Return exactly limit rows (2) — no extra row means no more results.
		rows := pgxmock.NewRows([]string{colID, colTenantID, colExpiry, colTags, colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colStatus}).
			AddRow("id-1", "t1", int64(0), &tags, "", int64(0), int64(0), int64(0), []byte(`{}`)).
			AddRow("id-2", "t1", int64(0), &tags, "", int64(0), int64(0), int64(0), []byte(`{}`))

		mock.ExpectQuery("SELECT .+ FROM batch_items WHERE").
			WithArgs("t1", 0, 3).
			WillReturnRows(rows)

		indexes, _, _, cursor, expectMore, err := core.get(
			context.Background(), &api.BaseQuery{TenantID: "t1"}, false, 0, 2, nil, nil)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if len(indexes) != 2 {
			t.Errorf("expected 2 items, got %d", len(indexes))
		}
		if cursor != 2 {
			t.Errorf("expected cursor 2, got %d", cursor)
		}
		if expectMore {
			t.Error("expected expectMore=false on last page")
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})
}

// --- update ---

func TestCoreUpdate_ValidationError(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	idx := &api.BaseIndexes{ID: "", TenantID: "t1"}
	contents := &api.BaseContents{Status: []byte(`{}`)}

	if err := core.update(context.Background(), idx, contents, nil, nil, nil); err == nil {
		t.Fatal("expected validation error for empty ID")
	}
}

func TestCoreUpdate_NonExistentID(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	idx := &api.BaseIndexes{ID: "missing", TenantID: "t1", Tags: api.Tags{"k": "v"}}
	contents := &api.BaseContents{Status: []byte(`{}`)}

	mock.ExpectExec("UPDATE batch_items SET").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), "missing").
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))

	if err := core.update(context.Background(), idx, contents, nil, nil, nil); err == nil {
		t.Fatal("expected error for non-existent ID")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCoreUpdate_NoFieldsToUpdate(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	idx := &api.BaseIndexes{ID: "id-1", TenantID: "t1", Tags: nil}
	contents := &api.BaseContents{}

	if err := core.update(context.Background(), idx, contents, nil, nil, nil); err != nil {
		t.Fatalf("expected nil for no-op update, got %v", err)
	}
}

func TestCoreUpdate_DBFailure(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	idx := &api.BaseIndexes{ID: "id-1", TenantID: "t1", Tags: api.Tags{"k": "v"}}
	contents := &api.BaseContents{Status: []byte(`{}`)}

	mock.ExpectExec("UPDATE batch_items SET").
		WithArgs(pgxmock.AnyArg(), pgxmock.AnyArg(), "id-1").
		WillReturnError(fmt.Errorf("connection refused"))

	if err := core.update(context.Background(), idx, contents, nil, nil, nil); err == nil {
		t.Fatal("expected error on DB failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// --- delete ---

func TestCoreDelete_EmptyIDs(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	ids, err := core.delete(context.Background(), []string{})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if ids != nil {
		t.Errorf("expected nil, got %v", ids)
	}
}

func TestCoreDelete_DBFailure(t *testing.T) {
	core, mock := newTestCore(t)
	defer mock.Close()

	mock.ExpectQuery("DELETE FROM batch_items WHERE").
		WithArgs([]string{"id-1"}).
		WillReturnError(fmt.Errorf("connection refused"))

	_, err := core.delete(context.Background(), []string{"id-1"})
	if err == nil {
		t.Fatal("expected error on DB failure")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// --- close ---

func TestClose(t *testing.T) {
	core, mock := newTestCore(t)
	mock.ExpectClose()

	if err := core.close(); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

// --- config ---

func TestConfigValidate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &PostgreSQLConfig{Url: "postgres://user:pass@localhost:5432/testdb?sslmode=disable"}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})

	t.Run("missing url", func(t *testing.T) {
		cfg := &PostgreSQLConfig{}
		if err := cfg.Validate(); err == nil {
			t.Fatal("expected error for missing url")
		}
	})
}
