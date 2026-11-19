//go:build integration

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

package integration

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

func TestBatchLifecycle(t *testing.T) {
	ts := newTestServer(t)

	// Step 1: Upload a file
	fileContent := `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"test-model","messages":[{"role":"user","content":"Hello"}]}}` + "\n"
	resp := ts.uploadFile(t, "input.jsonl", "batch", fileContent)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("upload file: expected 200, got %d", resp.StatusCode)
	}
	fileObj := decodeJSON[openai.FileObject](t, resp)
	if fileObj.ID == "" {
		t.Fatal("upload file: expected non-empty file ID")
	}

	// Step 2: Create a batch
	createReq := openai.CreateBatchRequest{
		InputFileID:      fileObj.ID,
		Endpoint:         openai.EndpointChatCompletions,
		CompletionWindow: "24h",
		Metadata:         map[string]string{"env": "functional-test"},
	}
	resp = ts.doJSON(t, http.MethodPost, "/v1/batches", createReq)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("create batch: expected 200, got %d: %s", resp.StatusCode, body)
	}
	batch := decodeJSON[openai.Batch](t, resp)

	if batch.ID == "" {
		t.Fatal("create batch: expected non-empty batch ID")
	}
	if batch.Object != "batch" {
		t.Errorf("create batch: expected object=batch, got %q", batch.Object)
	}
	if batch.Status != openai.BatchStatusValidating {
		t.Errorf("create batch: expected status=validating, got %q", batch.Status)
	}
	if batch.InputFileID != fileObj.ID {
		t.Errorf("create batch: expected input_file_id=%q, got %q", fileObj.ID, batch.InputFileID)
	}
	if batch.Endpoint != openai.EndpointChatCompletions {
		t.Errorf("create batch: expected endpoint=%q, got %q", openai.EndpointChatCompletions, batch.Endpoint)
	}

	// Step 3: Retrieve the batch
	resp = ts.doRequest(t, http.MethodGet, "/v1/batches/"+batch.ID)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retrieve batch: expected 200, got %d", resp.StatusCode)
	}
	retrieved := decodeJSON[openai.Batch](t, resp)

	if retrieved.ID != batch.ID {
		t.Errorf("retrieve batch: expected ID=%q, got %q", batch.ID, retrieved.ID)
	}
	if retrieved.Status != openai.BatchStatusValidating {
		t.Errorf("retrieve batch: expected status=validating, got %q", retrieved.Status)
	}

	// Step 4: List batches — verify our batch appears
	resp = ts.doRequest(t, http.MethodGet, "/v1/batches?limit=100")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list batches: expected 200, got %d", resp.StatusCode)
	}
	listResp := decodeJSON[openai.ListBatchResponse](t, resp)

	found := false
	for _, b := range listResp.Data {
		if b.ID == batch.ID {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("list batches: created batch %q not found in list of %d batches", batch.ID, len(listResp.Data))
	}

	// Step 5: Cancel the batch — still queued (not yet processed), so directly cancelled
	resp = ts.doJSON(t, http.MethodPost, "/v1/batches/"+batch.ID+"/cancel", nil)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("cancel batch: expected 200, got %d: %s", resp.StatusCode, body)
	}
	cancelled := decodeJSON[openai.Batch](t, resp)
	if cancelled.Status != openai.BatchStatusCancelled {
		t.Errorf("cancel batch: expected status=cancelled, got %q", cancelled.Status)
	}
	if cancelled.CancelledAt == nil {
		t.Error("cancel batch: expected cancelled_at to be set")
	}

	// Step 6: Retrieve again — verify cancelled status persists
	resp = ts.doRequest(t, http.MethodGet, "/v1/batches/"+batch.ID)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retrieve after cancel: expected 200, got %d", resp.StatusCode)
	}
	afterCancel := decodeJSON[openai.Batch](t, resp)
	if afterCancel.Status != openai.BatchStatusCancelled {
		t.Errorf("retrieve after cancel: expected status=cancelled, got %q", afterCancel.Status)
	}
}

func TestBatchCreateValidation(t *testing.T) {
	ts := newTestServer(t)

	// Upload a valid file for the UnknownField case
	fileContent := `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"hi"}]}}` + "\n"
	resp := ts.uploadFile(t, "valid.jsonl", "batch", fileContent)
	validFile := decodeJSON[openai.FileObject](t, resp)

	tests := []struct {
		name    string
		body    string
		wantMsg string
	}{
		{
			name:    "FileNotFound",
			body:    `{"input_file_id":"file-nonexistent","endpoint":"/v1/chat/completions","completion_window":"24h"}`,
			wantMsg: "Input file with ID 'file-nonexistent' not found",
		},
		{
			name:    "UnknownField",
			body:    `{"input_file_id":"` + validFile.ID + `","endpoint":"/v1/chat/completions","completion_window":"24h","bogus":"val"}`,
			wantMsg: `json: unknown field "bogus"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/batches", bytes.NewReader([]byte(tc.body)))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set(testTenantHeader, testTenantID)

			resp, err := ts.Client.Do(req)
			if err != nil {
				t.Fatal(err)
			}

			if resp.StatusCode != http.StatusBadRequest {
				body := readBody(t, resp)
				t.Fatalf("expected 400, got %d: %s", resp.StatusCode, body)
			}

			errResp := decodeJSON[openai.ErrorResponse](t, resp)
			if errResp.Error.Message != tc.wantMsg {
				t.Errorf("expected error message %q, got %q", tc.wantMsg, errResp.Error.Message)
			}
		})
	}
}

func TestBatchMetadataRoundTrip(t *testing.T) {
	ts := newTestServer(t)

	fileContent := `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"hi"}]}}` + "\n"
	resp := ts.uploadFile(t, "input.jsonl", "batch", fileContent)
	fileObj := decodeJSON[openai.FileObject](t, resp)

	metadata := map[string]string{
		"env":     "test",
		"version": "1.2.3",
	}
	createReq := openai.CreateBatchRequest{
		InputFileID:      fileObj.ID,
		Endpoint:         openai.EndpointChatCompletions,
		CompletionWindow: "24h",
		Metadata:         metadata,
	}
	resp = ts.doJSON(t, http.MethodPost, "/v1/batches", createReq)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("create: expected 200, got %d: %s", resp.StatusCode, body)
	}
	batch := decodeJSON[openai.Batch](t, resp)

	// Retrieve and verify metadata round-trips
	resp = ts.doRequest(t, http.MethodGet, "/v1/batches/"+batch.ID)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("retrieve: expected 200, got %d", resp.StatusCode)
	}
	retrieved := decodeJSON[openai.Batch](t, resp)

	for k, want := range metadata {
		if got := retrieved.Metadata[k]; got != want {
			t.Errorf("metadata[%q]: expected %q, got %q", k, want, got)
		}
	}
}

func TestBatchNotFound(t *testing.T) {
	ts := newTestServer(t)

	t.Run("Retrieve", func(t *testing.T) {
		resp := ts.doRequest(t, http.MethodGet, "/v1/batches/batch-does-not-exist")
		if resp.StatusCode != http.StatusNotFound {
			body := readBody(t, resp)
			t.Fatalf("expected 404, got %d: %s", resp.StatusCode, body)
		}
		errResp := decodeJSON[openai.ErrorResponse](t, resp)
		if errResp.Error.Type != "not_found_error" {
			t.Errorf("expected error type not_found_error, got %q", errResp.Error.Type)
		}
	})

	t.Run("Cancel", func(t *testing.T) {
		resp := ts.doJSON(t, http.MethodPost, "/v1/batches/batch-does-not-exist/cancel", nil)
		if resp.StatusCode != http.StatusNotFound {
			body := readBody(t, resp)
			t.Fatalf("expected 404, got %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
	})
}

func TestBatchPaginationValidation(t *testing.T) {
	ts := newTestServer(t)
	fileContent := `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"hi"}]}}` + "\n"
	fileObj := decodeJSON[openai.FileObject](t, ts.uploadFile(t, "input.jsonl", "batch", fileContent))
	for range 2 {
		resp := ts.doJSON(t, http.MethodPost, "/v1/batches", openai.CreateBatchRequest{
			InputFileID:      fileObj.ID,
			Endpoint:         openai.EndpointChatCompletions,
			CompletionWindow: "24h",
		})
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("create batch: expected 200, got %d: %s", resp.StatusCode, readBody(t, resp))
		}
		decodeJSON[openai.Batch](t, resp)
	}

	for _, tc := range []struct {
		name  string
		query string
	}{
		{"LimitRawPlus", "limit=+5"},
		{"LimitLeadingSpace", "limit=%205"},
		{"LimitTrailingSpace", "limit=5%20"},
		{"LimitDecimal", "limit=1.5"},
		{"LimitExponent", "limit=1e2"},
		{"LimitTrailingText", "limit=1abc"},
		{"AfterRawPlus", "after=+0"},
		{"AfterLeadingSpace", "after=%200"},
		{"AfterTrailingSpace", "after=0%20"},
		{"AfterDecimal", "after=1.5"},
		{"AfterExponent", "after=1e2"},
		{"AfterTrailingText", "after=1abc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := ts.doRequest(t, http.MethodGet, "/v1/batches?"+tc.query)
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", resp.StatusCode, readBody(t, resp))
			}
			decodeJSON[openai.ErrorResponse](t, resp)
		})
	}

	for _, tc := range []struct {
		name        string
		query       string
		wantHasMore bool
	}{
		{"LimitEncodedPlus", "limit=%2B1", true},
		{"AfterEncodedPlus", "limit=1&after=%2B1", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := ts.doRequest(t, http.MethodGet, "/v1/batches?"+tc.query)
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", resp.StatusCode, readBody(t, resp))
			}
			page := decodeJSON[openai.ListBatchResponse](t, resp)
			if len(page.Data) != 1 || page.HasMore != tc.wantHasMore {
				t.Fatalf("expected one batch and has_more=%v, got %d batches and has_more=%v", tc.wantHasMore, len(page.Data), page.HasMore)
			}
			if tc.wantHasMore {
				resp = ts.doRequest(t, http.MethodGet, "/v1/batches?limit=1&after="+page.Data[0].ID)
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("cursor page: expected 200, got %d: %s", resp.StatusCode, readBody(t, resp))
				}
				next := decodeJSON[openai.ListBatchResponse](t, resp)
				if len(next.Data) != 1 || next.HasMore || next.Data[0].ID == page.Data[0].ID {
					t.Fatalf("cursor page: expected the remaining batch, got %+v", next)
				}
			}
		})
	}
}
