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

package migrate

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLoad(t *testing.T) {
	migrations, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(migrations) == 0 {
		t.Fatal("no migrations embedded")
	}
	for i, m := range migrations {
		if m.Version != i+1 {
			t.Errorf("migration %d has version %d", i, m.Version)
		}
		if strings.TrimSpace(m.SQL) == "" {
			t.Errorf("migration %04d_%s is empty", m.Version, m.Name)
		}
	}

	latest, err := LatestVersion()
	if err != nil {
		t.Fatalf("LatestVersion: %v", err)
	}
	if latest != len(migrations) {
		t.Errorf("LatestVersion = %d, want %d", latest, len(migrations))
	}
}

func TestFileNamePattern(t *testing.T) {
	tests := []struct {
		name  string
		file  string
		match bool
	}{
		{name: "valid", file: "0001_initial_schema.sql", match: true},
		{name: "missing padding", file: "1_initial.sql", match: false},
		{name: "uppercase", file: "0002_AddColumn.sql", match: false},
		{name: "wrong extension", file: "0002_add_column.txt", match: false},
		{name: "no name", file: "0002_.sql", match: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := fileNamePattern.MatchString(tt.file); got != tt.match {
				t.Errorf("MatchString(%q) = %v, want %v", tt.file, got, tt.match)
			}
		})
	}
}

// The tests below run against a real PostgreSQL given by TEST_POSTGRESQL_URL.
// Each test works in its own schema so runs are isolated and leave nothing behind.

func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_POSTGRESQL_URL")
	if url == "" {
		t.Skip("TEST_POSTGRESQL_URL not set")
	}

	schema := fmt.Sprintf("migrate_test_%d", time.Now().UnixNano())
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema

	ctx := context.Background()
	admin, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
		_ = admin.Close(ctx)
	})

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("new pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}

func appliedVersions(t *testing.T, pool *pgxpool.Pool) []int {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT version FROM "+VersionTable+" ORDER BY version")
	if err != nil {
		t.Fatalf("query versions: %v", err)
	}
	versions, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		t.Fatalf("collect versions: %v", err)
	}
	return versions
}

func tableExists(t *testing.T, pool *pgxpool.Pool, table string) bool {
	t.Helper()
	rows, err := pool.Query(context.Background(), "SELECT to_regclass($1) IS NOT NULL", table)
	if err != nil {
		t.Fatalf("query to_regclass: %v", err)
	}
	exists, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[bool])
	if err != nil {
		t.Fatalf("collect to_regclass: %v", err)
	}
	return exists
}

func TestRun(t *testing.T) {
	ctx := context.Background()

	t.Run("fresh database", func(t *testing.T) {
		pool := newTestPool(t)
		if err := Check(ctx, pool); !errors.Is(err, ErrSchemaMissing) {
			t.Fatalf("Check before migrate: got %v, want ErrSchemaMissing", err)
		}

		applied, err := Run(ctx, pool)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		latest, _ := LatestVersion()
		if len(applied) != latest {
			t.Fatalf("applied %d migrations, want %d", len(applied), latest)
		}
		for _, table := range []string{"batch_items", "file_items"} {
			if !tableExists(t, pool, table) {
				t.Errorf("table %s missing after migrate", table)
			}
		}
		if err := Check(ctx, pool); err != nil {
			t.Fatalf("Check after migrate: %v", err)
		}
	})

	t.Run("second run is a no-op", func(t *testing.T) {
		pool := newTestPool(t)
		if _, err := Run(ctx, pool); err != nil {
			t.Fatalf("first Run: %v", err)
		}
		before := appliedVersions(t, pool)

		applied, err := Run(ctx, pool)
		if err != nil {
			t.Fatalf("second Run: %v", err)
		}
		if len(applied) != 0 {
			t.Fatalf("second Run applied %d migrations, want 0", len(applied))
		}
		after := appliedVersions(t, pool)
		if fmt.Sprint(before) != fmt.Sprint(after) {
			t.Fatalf("versions changed on no-op run: %v -> %v", before, after)
		}
	})

	t.Run("adopts tables created without a version table", func(t *testing.T) {
		pool := newTestPool(t)
		migrations, _ := Load()
		if _, err := pool.Exec(ctx, migrations[0].SQL); err != nil {
			t.Fatalf("pre-create legacy schema: %v", err)
		}
		if err := Check(ctx, pool); !errors.Is(err, ErrSchemaMissing) {
			t.Fatalf("Check on legacy schema: got %v, want ErrSchemaMissing", err)
		}

		if _, err := Run(ctx, pool); err != nil {
			t.Fatalf("Run on legacy schema: %v", err)
		}
		if got := appliedVersions(t, pool); got[0] != 1 {
			t.Fatalf("versions after adopting legacy schema: %v", got)
		}
		if err := Check(ctx, pool); err != nil {
			t.Fatalf("Check after adopting: %v", err)
		}
	})

	t.Run("concurrent runs apply each migration once", func(t *testing.T) {
		pool := newTestPool(t)
		const runners = 8

		var wg sync.WaitGroup
		results := make(chan int, runners)
		errs := make(chan error, runners)
		for range runners {
			wg.Add(1)
			go func() {
				defer wg.Done()
				applied, err := Run(ctx, pool)
				if err != nil {
					errs <- err
					return
				}
				results <- len(applied)
			}()
		}
		wg.Wait()
		close(results)
		close(errs)

		for err := range errs {
			t.Errorf("concurrent Run: %v", err)
		}
		total := 0
		for n := range results {
			total += n
		}
		latest, _ := LatestVersion()
		if total != latest {
			t.Fatalf("migrations applied across %d runners = %d, want %d", runners, total, latest)
		}
		if got := appliedVersions(t, pool); len(got) != latest {
			t.Fatalf("version rows = %v, want %d rows", got, latest)
		}
	})
}

func TestCheck(t *testing.T) {
	ctx := context.Background()

	t.Run("behind", func(t *testing.T) {
		pool := newTestPool(t)
		if _, err := Run(ctx, pool); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := pool.Exec(ctx, "DELETE FROM "+VersionTable); err != nil {
			t.Fatalf("delete versions: %v", err)
		}
		if err := Check(ctx, pool); !errors.Is(err, ErrSchemaBehind) {
			t.Fatalf("Check: got %v, want ErrSchemaBehind", err)
		}
	})

	t.Run("ahead is accepted", func(t *testing.T) {
		pool := newTestPool(t)
		if _, err := Run(ctx, pool); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if _, err := pool.Exec(ctx, insertVersion, 9999, "from_the_future"); err != nil {
			t.Fatalf("insert future version: %v", err)
		}
		if err := Check(ctx, pool); err != nil {
			t.Fatalf("Check: %v", err)
		}
	})
}
