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

package worker

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// TestTransitionPersistentStatusPostgres runs the finalizing CAS against the
// jsonb status column: it must match a row the activation and progress
// writes changed with jsonb_set, and must not overwrite a row the API
// server's cancel moved to cancelling.
func TestTransitionPersistentStatusPostgres(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := testLoggerCtx(t)

	batchDB, err := postgresql.NewPostgresBatchDBClient(ctx, &postgresql.PostgreSQLConfig{Url: url})
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	const (
		jobID = "job-transition-pg"
		epoch = int64(3)
	)

	tests := []struct {
		name       string
		change     string // SQL applied to the row after the caller's copy was read
		wantStatus openai.BatchStatus
		wantErr    error
	}{
		{
			name:       "progress write in between",
			change:     `UPDATE batch_items SET status = jsonb_set(status, '{request_counts}', '{"total":2,"completed":2,"failed":0}') WHERE id = $1`,
			wantStatus: openai.BatchStatusFinalizing,
		},
		{
			name:       "cancel in between",
			change:     `UPDATE batch_items SET status = jsonb_set(status, '{status}', '"cancelling"') WHERE id = $1`,
			wantStatus: openai.BatchStatusCancelling,
			wantErr:    api.ErrConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := batchDB.DBDelete(ctx, []string{jobID}); err != nil {
				t.Fatalf("delete: %v", err)
			}
			job := &api.BatchItem{
				BaseIndexes:  api.BaseIndexes{ID: jobID, TenantID: "tenant-1"},
				BaseContents: api.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
				Epoch:        epoch,
			}
			if err := batchDB.DBStore(ctx, job); err != nil {
				t.Fatalf("seed: %v", err)
			}
			t.Cleanup(func() { _, _ = batchDB.DBDelete(context.Background(), []string{jobID}) })
			if _, err := pool.Exec(ctx, tt.change, jobID); err != nil {
				t.Fatalf("change row: %v", err)
			}

			got, err := NewStatusUpdater(batchDB).TransitionPersistentStatus(ctx, job,
				openai.BatchStatusInProgress, openai.BatchStatusFinalizing)
			if got != tt.wantStatus || (tt.wantErr == nil) != (err == nil) || (tt.wantErr != nil && !errors.Is(err, tt.wantErr)) {
				t.Fatalf("TransitionPersistentStatus = (%s, %v), want (%s, %v)", got, err, tt.wantStatus, tt.wantErr)
			}
			assertJobStatus(t, batchDB, jobID, tt.wantStatus)
		})
	}
}
