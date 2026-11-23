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
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestShortSpanName(t *testing.T) {
	tests := []struct {
		sql  string
		want string
	}{
		{"SELECT id, tenant_id FROM batch_items WHERE tenant_id = $1 ORDER BY id OFFSET $2 LIMIT $3", "select_batch_items"},
		{"SELECT id, tenant_id, purpose FROM file_items WHERE id = ANY($1) AND tenant_id = $2 ORDER BY id OFFSET $3 LIMIT $4", "select_file_items"},
		{"INSERT INTO batch_items (id, tenant_id, expiry, tags, spec, status) VALUES ($1, $2, $3, $4, $5, $6)", "insert_batch_items"},
		{"INSERT INTO file_items (id, tenant_id) VALUES ($1, $2)", "insert_file_items"},
		{"UPDATE batch_items SET status = $1, tags = $2 WHERE id = $3", "update_batch_items"},
		{"UPDATE file_items SET status = $1 WHERE id = $2", "update_file_items"},
		{"DELETE FROM file_items WHERE id = ANY($1) RETURNING id", "delete_file_items"},
		{"DELETE FROM batch_items WHERE id = ANY($1) RETURNING id", "delete_batch_items"},
		{"", "db_query"},
		{"SELECT", "select"},
		{"COMMIT", "commit"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := shortSpanName(tt.sql); got != tt.want {
				t.Errorf("shortSpanName(%q) = %q, want %q", tt.sql, got, tt.want)
			}
		})
	}
}

func TestTokenAfter(t *testing.T) {
	tests := []struct {
		name    string
		tokens  []string
		keyword string
		want    string
	}{
		{"found", []string{"SELECT", "id", "FROM", "batch_items", "WHERE"}, "FROM", "batch_items"},
		{"case insensitive", []string{"delete", "from", "file_items"}, "FROM", "file_items"},
		{"not found", []string{"SELECT", "id"}, "FROM", ""},
		{"keyword at end", []string{"SELECT", "FROM"}, "FROM", ""},
		{"empty", []string{}, "FROM", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tokenAfter(tt.tokens, tt.keyword); got != tt.want {
				t.Errorf("tokenAfter(%v, %q) = %q, want %q", tt.tokens, tt.keyword, got, tt.want)
			}
		})
	}
}

func TestPoolConfigValidate(t *testing.T) {
	aboveInt32 := math.MaxInt32
	aboveInt32++
	tests := []struct {
		name           string
		maxConnections int
		wantErr        bool
	}{
		{name: "zero keeps the pgx default", maxConnections: 0},
		{name: "explicit value", maxConnections: 8},
		{name: "negative is rejected", maxConnections: -1, wantErr: true},
		{name: "above int32 is rejected", maxConnections: aboveInt32, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := PoolConfig{MaxConnections: tt.maxConnections}.Validate()
			if tt.wantErr != (err != nil) {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// TestNewPool runs against a real PostgreSQL instance (TEST_POSTGRES_URL).
func TestNewPool(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Which connection cap a pool gets for each pool.max_connections value.
	pgxDefault, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("pgxpool.ParseConfig: %v", err)
	}
	tests := []struct {
		name           string
		maxConnections int
		want           int32
	}{
		{name: "max_connections 0 leaves the size parsed from the URL", maxConnections: 0, want: pgxDefault.MaxConns},
		{name: "max_connections 3 caps the pool at 3", maxConnections: 3, want: 3},
		{name: "max_connections 1 is raised to the LISTEN minimum", maxConnections: 1, want: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool, err := newPool(ctx, &PostgreSQLConfig{Url: url, Pool: PoolConfig{MaxConnections: tt.maxConnections}})
			if err != nil {
				t.Fatalf("newPool: %v", err)
			}
			defer pool.Close()
			if got := pool.Config().MaxConns; got != tt.want {
				t.Fatalf("MaxConns = %d, want %d", got, tt.want)
			}
		})
	}

	// The cap seen from the server: 12 concurrent queries through a pool
	// capped at 3 never hold more than 3 connections in pg_stat_activity.
	t.Run("max_connections caps server connections", func(t *testing.T) {
		// Tag the pool's connections so pg_stat_activity can tell them apart
		// from the observer and from other test runs on the same database.
		applicationName := fmt.Sprintf("pool_max_connections_test_%d", time.Now().UnixNano())
		separator := "?"
		if strings.Contains(url, "?") {
			separator = "&"
		}
		pool, err := newPool(ctx, &PostgreSQLConfig{
			Url:  url + separator + "application_name=" + applicationName,
			Pool: PoolConfig{MaxConnections: 3},
		})
		if err != nil {
			t.Fatalf("newPool: %v", err)
		}
		defer pool.Close()

		observer, err := pgx.Connect(ctx, url)
		if err != nil {
			t.Fatalf("pgx.Connect: %v", err)
		}
		defer func() { _ = observer.Close(ctx) }()

		var wg sync.WaitGroup
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := pool.Exec(ctx, "SELECT pg_sleep(0.3)"); err != nil {
					t.Errorf("query through capped pool: %v", err)
				}
			}()
		}
		queriesDone := make(chan struct{})
		go func() {
			wg.Wait()
			close(queriesDone)
		}()

		peak := 0
		for running := true; running; {
			select {
			case <-queriesDone:
				running = false
			case <-time.After(20 * time.Millisecond):
			}
			var connections int
			if err := observer.QueryRow(ctx,
				"SELECT count(*) FROM pg_stat_activity WHERE application_name = $1", applicationName,
			).Scan(&connections); err != nil {
				<-queriesDone
				t.Fatalf("count pool connections: %v", err)
			}
			if connections > peak {
				peak = connections
			}
		}
		if peak != 3 {
			t.Fatalf("peak server connections = %d, want 3", peak)
		}
	})
}
