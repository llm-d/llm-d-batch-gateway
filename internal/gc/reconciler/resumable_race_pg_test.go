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

package reconciler

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

func TestResumableMarkerFencesStaleGCReadPostgres(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	const processorID = "dead-processor"

	cfg := &postgresql.PostgreSQLConfig{Url: url}
	batchDB, err := postgresql.NewPostgresBatchDBClient(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })
	queue, err := postgresql.NewPostgresBatchQueueClient(ctx, cfg, "gc-test")
	if err != nil {
		t.Fatalf("NewPostgresBatchQueueClient: %v", err)
	}
	t.Cleanup(func() { _ = queue.Close() })
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	defer pool.Close()
	r, err := NewReconciler(batchDB, queue, time.Minute, false, nil)
	if err != nil {
		t.Fatalf("NewReconciler: %v", err)
	}

	for _, tc := range []struct {
		name     string
		priority int64
	}{
		{name: "terminal transition", priority: time.Now().Add(-time.Hour).UnixMicro()},
		{name: "queue re-enqueue", priority: time.Now().Add(time.Hour).UnixMicro()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := pool.Exec(ctx, "TRUNCATE batch_items"); err != nil {
				t.Fatalf("truncate: %v", err)
			}
			status, _ := json.Marshal(openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})
			if err := batchDB.DBStore(ctx, &db.BatchItem{
				BaseIndexes:  db.BaseIndexes{ID: tc.name, TenantID: "tenant-1"},
				BaseContents: db.BaseContents{Status: status, Spec: []byte(`{}`)},
				ProcessorID:  processorID,
				Priority:     tc.priority,
				Epoch:        7,
			}); err != nil {
				t.Fatalf("seed: %v", err)
			}

			stale, _, _, err := batchDB.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{tc.name}}}, true, 0, 1)
			if err != nil || len(stale) != 1 {
				t.Fatalf("stale DBGet: items=%d err=%v", len(stale), err)
			}
			if _, err := pool.Exec(ctx, "UPDATE batch_items SET resumable = TRUE WHERE id = $1", tc.name); err != nil {
				t.Fatalf("enable resumable marker: %v", err)
			}

			result := &Result{}
			r.triageOrphan(ctx, stale[0], result)
			if result.Conflicts != 1 || result.Expired != 0 || result.ReEnqueued != 0 || result.Errors != 0 {
				t.Fatalf("stale GC mutation was not fenced: %+v", result)
			}

			current, _, _, err := batchDB.DBGet(ctx, &db.BatchQuery{BaseQuery: db.BaseQuery{IDs: []string{tc.name}}}, true, 0, 1)
			if err != nil || len(current) != 1 {
				t.Fatalf("current DBGet: items=%d err=%v", len(current), err)
			}
			var statusInfo openai.BatchStatusInfo
			if err := json.Unmarshal(current[0].Status, &statusInfo); err != nil {
				t.Fatalf("status: %v", err)
			}
			if !current[0].Resumable || current[0].Epoch != 7 || current[0].ProcessorID != processorID || statusInfo.Status != openai.BatchStatusInProgress {
				t.Fatalf("stale GC changed resumable row: %+v status=%s", current[0], statusInfo.Status)
			}
		})
	}
}
