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

// Package migrate owns the PostgreSQL schema. Migrations are embedded SQL
// files named NNNN_description.sql, applied in version order and recorded in
// the schema_migrations table. Run applies pending migrations; Check verifies
// a database is at the version the binary expects.
package migrate

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

const (
	// VersionTable records applied migrations.
	VersionTable = "schema_migrations"

	// lockKey is the pg_advisory_xact_lock key that serializes concurrent runs.
	lockKey int64 = 0x6c6c6d2d62617463

	createVersionTable = `CREATE TABLE IF NOT EXISTS ` + VersionTable + ` (
    version    INTEGER PRIMARY KEY,
    name       TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`
	selectCurrentVersion = `SELECT COALESCE(MAX(version), 0) FROM ` + VersionTable
	insertVersion        = `INSERT INTO ` + VersionTable + ` (version, name) VALUES ($1, $2)`

	sqlstateUndefinedTable = "42P01"
)

var (
	// ErrSchemaMissing means the database has never been migrated.
	ErrSchemaMissing = errors.New("schema not initialized: run the migrate subcommand")
	// ErrSchemaBehind means the database is at an older version than the binary expects.
	ErrSchemaBehind = errors.New("schema is behind: run the migrate subcommand")
)

var fileNamePattern = regexp.MustCompile(`^(\d{4})_([a-z0-9_]+)\.sql$`)

// Migration is one embedded SQL file.
type Migration struct {
	Version int
	Name    string
	SQL     string
}

// Beginner starts a transaction. *pgxpool.Pool satisfies it.
type Beginner interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

// Querier runs a query. *pgxpool.Pool and pgx.Tx satisfy it.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Load parses the embedded migrations, sorted by version. Versions must
// start at 1 and be contiguous.
func Load() ([]Migration, error) {
	entries, err := fs.ReadDir(migrationFiles, "migrations")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}

	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		m := fileNamePattern.FindStringSubmatch(entry.Name())
		if m == nil {
			return nil, fmt.Errorf("migration %q does not match NNNN_name.sql", entry.Name())
		}
		version, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", entry.Name(), err)
		}
		sql, err := fs.ReadFile(migrationFiles, "migrations/"+entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}
		migrations = append(migrations, Migration{Version: version, Name: m[2], SQL: string(sql)})
	}

	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	for i, m := range migrations {
		if m.Version != i+1 {
			return nil, fmt.Errorf("migration versions must be contiguous from 1, got %04d_%s at position %d", m.Version, m.Name, i+1)
		}
	}
	return migrations, nil
}

// LatestVersion is the version a binary built from this tree expects.
func LatestVersion() (int, error) {
	migrations, err := Load()
	if err != nil {
		return 0, err
	}
	return len(migrations), nil
}

// Run applies all pending migrations in a single transaction under an
// advisory lock, so concurrent callers serialize and at most one of them
// executes DDL. It returns the migrations it applied.
func Run(ctx context.Context, db Beginner) (applied []Migration, err error) {
	migrations, err := Load()
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback(ctx)
		}
	}()

	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
		return nil, fmt.Errorf("acquire migration lock: %w", err)
	}
	if _, err = tx.Exec(ctx, createVersionTable); err != nil {
		return nil, fmt.Errorf("create %s: %w", VersionTable, err)
	}

	current, err := currentVersion(ctx, tx)
	if err != nil {
		return nil, err
	}

	for _, m := range migrations {
		if m.Version <= current {
			continue
		}
		if _, err = tx.Exec(ctx, m.SQL); err != nil {
			return nil, fmt.Errorf("apply migration %04d_%s: %w", m.Version, m.Name, err)
		}
		if _, err = tx.Exec(ctx, insertVersion, m.Version, m.Name); err != nil {
			return nil, fmt.Errorf("record migration %04d_%s: %w", m.Version, m.Name, err)
		}
		applied = append(applied, m)
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return applied, nil
}

// Check verifies the database schema is at least the version this binary
// expects. A missing version table returns ErrSchemaMissing and an older
// version returns ErrSchemaBehind. A newer version is logged and accepted so
// that binaries can be rolled back after a migration.
func Check(ctx context.Context, db Querier) error {
	expected, err := LatestVersion()
	if err != nil {
		return err
	}

	current, err := currentVersion(ctx, db)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == sqlstateUndefinedTable {
			return ErrSchemaMissing
		}
		return err
	}

	switch {
	case current < expected:
		return fmt.Errorf("%w: database at version %d, binary expects %d", ErrSchemaBehind, current, expected)
	case current > expected:
		logr.FromContextOrDiscard(ctx).Info("database schema is newer than this binary expects",
			"databaseVersion", current, "expectedVersion", expected)
	}
	return nil
}

func currentVersion(ctx context.Context, db Querier) (int, error) {
	rows, err := db.Query(ctx, selectCurrentVersion)
	if err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	defer rows.Close()

	version, err := pgx.CollectExactlyOneRow(rows, pgx.RowTo[int])
	if err != nil {
		return 0, fmt.Errorf("read schema version: %w", err)
	}
	return version, nil
}
