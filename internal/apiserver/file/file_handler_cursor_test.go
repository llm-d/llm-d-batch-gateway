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

package file

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// TestListFilesFollowsLastIDCursor walks the file list the way the official
// OpenAI SDK does: request a page, then send the previous page's last_id as
// `after` until has_more is false. Every file must be returned exactly once.
func TestListFilesFollowsLastIDCursor(t *testing.T) {
	const numFiles = 5
	const pageSize = 2

	ctx := context.Background()
	handler := setupTestHandler(t)

	created := make(map[string]bool, numFiles)
	for i := 0; i < numFiles; i++ {
		f := createTestFile(t, handler, ctx, "f.jsonl", "batch",
			`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{}}`)
		created[f.ID] = true
	}

	seen := make(map[string]int, numFiles)
	after := ""
	for page := 0; page < numFiles+1; page++ { // bounded so a bug cannot loop forever
		q := url.Values{"limit": {strconv.Itoa(pageSize)}}
		if after != "" {
			q.Set("after", after)
		}
		req := httptest.NewRequest(http.MethodGet, "/v1/files?"+q.Encode(), nil).WithContext(ctx)
		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("page %d (after=%q): expected status %d, got %d: %s",
				page, after, http.StatusOK, w.Code, w.Body.String())
		}

		var resp openai.ListFilesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("page %d: failed to parse response: %v", page, err)
		}
		if len(resp.Data) > pageSize {
			t.Fatalf("page %d: got %d items, limit was %d", page, len(resp.Data), pageSize)
		}
		for _, f := range resp.Data {
			seen[f.ID]++
		}
		if !resp.HasMore {
			break
		}
		after = resp.LastID
		if after == "" {
			t.Fatalf("page %d: has_more=true but last_id is empty", page)
		}
	}

	if len(seen) != numFiles {
		t.Errorf("expected %d distinct files across pages, got %d", numFiles, len(seen))
	}
	for id, n := range seen {
		if !created[id] {
			t.Errorf("unexpected file id %q in listing", id)
		}
		if n != 1 {
			t.Errorf("file %q returned %d times, expected exactly once", id, n)
		}
	}
}

// TestListFilesOrderIsGlobalAcrossPages checks that the concatenation of all pages
// is sorted in the requested order, not only within each page.
func TestListFilesOrderIsGlobalAcrossPages(t *testing.T) {
	const numFiles = 7

	ctx := context.Background()
	handler := setupTestHandler(t)
	for i := 0; i < numFiles; i++ {
		createTestFile(t, handler, ctx, "f.jsonl", "batch",
			`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{}}`)
	}

	for _, order := range []string{"asc", "desc"} {
		t.Run(order, func(t *testing.T) {
			var ids []string
			after := ""
			for page := 0; page < numFiles+1; page++ {
				q := url.Values{"limit": {"3"}, "order": {order}}
				if after != "" {
					q.Set("after", after)
				}
				req := httptest.NewRequest(http.MethodGet, "/v1/files?"+q.Encode(), nil).WithContext(ctx)
				w := httptest.NewRecorder()
				handler.ListFiles(w, req)
				if w.Code != http.StatusOK {
					t.Fatalf("page %d: status %d: %s", page, w.Code, w.Body.String())
				}
				var resp openai.ListFilesResponse
				if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
					t.Fatalf("page %d: %v", page, err)
				}
				for _, f := range resp.Data {
					ids = append(ids, f.ID)
				}
				if !resp.HasMore {
					break
				}
				after = resp.LastID
			}

			if len(ids) != numFiles {
				t.Fatalf("got %d files, want %d", len(ids), numFiles)
			}
			sorted := sort.StringsAreSorted(ids)
			if order == "desc" {
				rev := append([]string(nil), ids...)
				sort.Sort(sort.Reverse(sort.StringSlice(rev)))
				sorted = reflect.DeepEqual(rev, ids)
			}
			if !sorted {
				t.Errorf("files are not globally ordered %s: %v", order, ids)
			}
		})
	}
}
