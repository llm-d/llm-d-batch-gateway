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

package batch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

func storeTestBatches(t *testing.T, handler *BatchAPIHandler, n int) map[string]bool {
	t.Helper()
	ids := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		id := ucom.NewBatchID()
		batch := openai.Batch{
			ID: id,
			BatchSpec: openai.BatchSpec{
				Object:           "batch",
				InputFileID:      "file_in",
				Endpoint:         openai.EndpointChatCompletions,
				CompletionWindow: "24h",
				CreatedAt:        time.Now().UTC().Unix(),
			},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusValidating},
		}
		item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
		if err != nil {
			t.Fatalf("failed to convert batch: %v", err)
		}
		if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
			t.Fatalf("failed to store batch: %v", err)
		}
		ids[id] = true
	}
	return ids
}

func listBatchesPage(t *testing.T, handler *BatchAPIHandler, limit int, after string) (*httptest.ResponseRecorder, openai.ListBatchResponse) {
	t.Helper()
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if after != "" {
		q.Set("after", after)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/batches?"+q.Encode(), nil)
	rr := httptest.NewRecorder()
	handler.ListBatches(rr, req)

	var resp openai.ListBatchResponse
	if rr.Code == http.StatusOK {
		if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse list response: %v", err)
		}
	}
	return rr, resp
}

// TestListBatchesFollowsLastIDCursor pages through the batch list the way the
// OpenAI SDK does, passing the previous page's last_id as `after`.
func TestListBatchesFollowsLastIDCursor(t *testing.T) {
	const numBatches, pageSize = 5, 2

	handler := setupTestHandler()
	created := storeTestBatches(t, handler, numBatches)

	seen := make(map[string]int, numBatches)
	after := ""
	for page := 0; page < numBatches+1; page++ { // bounded so a bug cannot loop forever
		rr, resp := listBatchesPage(t, handler, pageSize, after)
		if rr.Code != http.StatusOK {
			t.Fatalf("page %d (after=%q): status %d: %s", page, after, rr.Code, rr.Body.String())
		}
		if len(resp.Data) > pageSize {
			t.Fatalf("page %d: got %d items, limit was %d", page, len(resp.Data), pageSize)
		}
		for _, b := range resp.Data {
			seen[b.ID]++
		}
		if !resp.HasMore {
			break
		}
		if resp.LastID == nil || *resp.LastID == "" {
			t.Fatalf("page %d: has_more=true but last_id is empty", page)
		}
		after = *resp.LastID
	}

	if len(seen) != numBatches {
		t.Errorf("expected %d distinct batches, got %d", numBatches, len(seen))
	}
	for id, n := range seen {
		if !created[id] {
			t.Errorf("unexpected batch id %q", id)
		}
		if n != 1 {
			t.Errorf("batch %q returned %d times, expected once", id, n)
		}
	}
}

// TestListBatchesIntegerAfterStillWorks keeps the legacy integer offset form working.
func TestListBatchesIntegerAfterStillWorks(t *testing.T) {
	handler := setupTestHandler()
	storeTestBatches(t, handler, 3)

	rr, resp := listBatchesPage(t, handler, 10, "2")
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	if len(resp.Data) != 1 {
		t.Errorf("after=2 over 3 batches: got %d items, want 1", len(resp.Data))
	}
}

// TestListBatchesRejectsMalformedAfter checks that values that are neither an
// integer nor a batch ID are rejected.
func TestListBatchesRejectsMalformedAfter(t *testing.T) {
	handler := setupTestHandler()
	for _, after := range []string{"abc", "-1", "file_123", "batch_"} {
		t.Run(after, func(t *testing.T) {
			if rr, _ := listBatchesPage(t, handler, 10, after); rr.Code != http.StatusBadRequest {
				t.Errorf("after=%q: status %d, want %d", after, rr.Code, http.StatusBadRequest)
			}
		})
	}
}

// TestListBatchesUnknownAfterID returns what sorts after an ID that does not exist.
func TestListBatchesUnknownAfterID(t *testing.T) {
	handler := setupTestHandler()
	storeTestBatches(t, handler, 3)

	// "batch_~" sorts after every UUID-based ID, so nothing is left.
	rr, resp := listBatchesPage(t, handler, 10, "batch_~")
	if rr.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rr.Code, rr.Body.String())
	}
	if len(resp.Data) != 0 || resp.HasMore {
		t.Errorf("got %d items has_more=%v, want empty last page", len(resp.Data), resp.HasMore)
	}
}
