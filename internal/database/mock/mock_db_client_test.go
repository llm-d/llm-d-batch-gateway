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

package mock

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func TestMockDBClient_DBUpdateProgress_EpochFence(t *testing.T) {
	ctx := context.Background()
	dbClient := NewMockDBClient[api.BatchItem, api.BatchQuery](
		func(b *api.BatchItem) string { return b.ID },
		func(q *api.BatchQuery) *api.BaseQuery { return &q.BaseQuery },
	)

	seed := &api.BatchItem{
		BaseIndexes:  api.BaseIndexes{ID: "job-1"},
		BaseContents: api.BaseContents{Status: []byte(`{"status":"in_progress"}`)},
		Epoch:        5,
	}
	if err := dbClient.DBStore(ctx, seed); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	getCounts := func(t *testing.T) map[string]any {
		t.Helper()
		items, _, _, err := dbClient.DBGet(ctx, &api.BatchQuery{BaseQuery: api.BaseQuery{IDs: []string{"job-1"}}}, true, 0, 1)
		if err != nil || len(items) != 1 {
			t.Fatalf("DBGet: err=%v len=%d", err, len(items))
		}
		var status map[string]any
		if err := json.Unmarshal(items[0].Status, &status); err != nil {
			t.Fatalf("unmarshal status: %v", err)
		}
		counts, _ := status["request_counts"].(map[string]any)
		return counts
	}

	// A stale-epoch write is fenced out: it surfaces ErrConflict and must not
	// touch the row.
	if err := dbClient.DBUpdateProgress(ctx, "job-1", 4, api.BatchRequestCounts{Total: 10, Completed: 1}); !errors.Is(err, api.ErrConflict) {
		t.Fatalf("stale-epoch DBUpdateProgress: expected ErrConflict, got %v", err)
	}
	if counts := getCounts(t); counts != nil {
		t.Fatalf("stale-epoch write must not update counts, got %v", counts)
	}

	// A write carrying the current epoch updates the counts.
	if err := dbClient.DBUpdateProgress(ctx, "job-1", 5, api.BatchRequestCounts{Total: 10, Completed: 7, Failed: 3}); err != nil {
		t.Fatalf("DBUpdateProgress: %v", err)
	}
	counts := getCounts(t)
	if counts == nil || counts["completed"] != float64(7) || counts["failed"] != float64(3) {
		t.Fatalf("expected completed=7 failed=3 after current-epoch write, got %v", counts)
	}
}
