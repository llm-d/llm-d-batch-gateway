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

// This file provides the PostgreSQL connection configuration and pool management.

package postgresql

import (
	"context"
	"fmt"
	"strings"

	"github.com/exaring/otelpgx"
	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// pgxPool is the subset of pgxpool.Pool used by the database clients.
// It is satisfied by *pgxpool.Pool and by pgxmock for testing.
type pgxPool interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Ping(ctx context.Context) error
	Close()
}

// PostgreSQLConfig holds the configuration for a PostgreSQL connection.
type PostgreSQLConfig struct {
	// Url is a PostgreSQL connection string (e.g., "postgres://[user]:[pass]@[host]:5432/[dbname]?sslmode=disable").
	Url string `yaml:"-"`
	// EnableTracing enables OpenTelemetry tracing for all PostgreSQL operations.
	EnableTracing bool
}

// Validate checks that the required configuration fields are set.
func (c *PostgreSQLConfig) Validate() error {
	if c.Url == "" {
		return fmt.Errorf("url is required")
	}
	return nil
}

// shortSpanName extracts "verb_table" from a SQL statement,
// e.g. "select_file_items" instead of the full query text.
func shortSpanName(sql string) string {
	tokens := strings.Fields(sql)
	if len(tokens) == 0 {
		return "db_query"
	}
	verb := strings.ToLower(tokens[0])
	// Find the table name based on SQL pattern:
	//   SELECT ... FROM <table>
	//   INSERT INTO <table>
	//   UPDATE <table> SET ...
	//   DELETE FROM <table>
	switch verb {
	case "select", "delete":
		if table := tokenAfter(tokens, "FROM"); table != "" {
			return verb + "_" + table
		}
	case "insert":
		if table := tokenAfter(tokens, "INTO"); table != "" {
			return verb + "_" + table
		}
	case "update":
		if len(tokens) > 1 {
			return verb + "_" + tokens[1]
		}
	}
	return verb
}

// tokenAfter returns the token immediately following keyword (case-insensitive).
func tokenAfter(tokens []string, keyword string) string {
	for i, t := range tokens {
		if strings.EqualFold(t, keyword) && i+1 < len(tokens) {
			return tokens[i+1]
		}
	}
	return ""
}

// newPool creates a new pgxpool.Pool from a PostgreSQLConfig.
func newPool(ctx context.Context, config *PostgreSQLConfig) (*pgxpool.Pool, error) {
	if config == nil {
		return nil, fmt.Errorf("config is nil")
	}
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	poolConfig, err := pgxpool.ParseConfig(config.Url)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	if poolConfig.MaxConns < 2 {
		logr.FromContextOrDiscard(ctx).Info("Increasing pool max connections to minimum required for LISTEN", "original", poolConfig.MaxConns, "new", 2)
		poolConfig.MaxConns = 2
	}
	if config.EnableTracing {
		poolConfig.ConnConfig.Tracer = otelpgx.NewTracer(
			otelpgx.WithTrimSQLInSpanName(),
			otelpgx.WithSpanNameFunc(shortSpanName),
		)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}
