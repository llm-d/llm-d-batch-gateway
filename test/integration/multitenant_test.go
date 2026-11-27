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
	"net/http"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

func TestTenantIsolation(t *testing.T) {
	ts := newTestServer(t)

	tenantA := "tenant-alpha"
	tenantB := "tenant-beta"

	content := `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"hi"}]}}` + "\n"

	// Tenant A: upload a file
	resp := ts.uploadFileAs(t, "alpha.jsonl", "batch", content, tenantA)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("tenant-A upload: expected 200, got %d: %s", resp.StatusCode, body)
	}
	fileA := decodeJSON[openai.FileObject](t, resp)

	// Tenant A: create a batch
	createReq := openai.CreateBatchRequest{
		InputFileID:      fileA.ID,
		Endpoint:         openai.EndpointChatCompletions,
		CompletionWindow: "24h",
	}
	resp = ts.doJSONAs(t, http.MethodPost, "/v1/batches", createReq, tenantA)
	if resp.StatusCode != http.StatusOK {
		body := readBody(t, resp)
		t.Fatalf("tenant-A create batch: expected 200, got %d: %s", resp.StatusCode, body)
	}
	batchA := decodeJSON[openai.Batch](t, resp)

	// Tenant B: cannot see tenant A's file
	t.Run("FileIsolation", func(t *testing.T) {
		resp := ts.doRequestAs(t, http.MethodGet, "/v1/files/"+fileA.ID, tenantB)
		if resp.StatusCode != http.StatusNotFound {
			body := readBody(t, resp)
			t.Fatalf("tenant-B retrieve file: expected 404, got %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
	})

	// Tenant B: cannot see tenant A's batch
	t.Run("BatchIsolation", func(t *testing.T) {
		resp := ts.doRequestAs(t, http.MethodGet, "/v1/batches/"+batchA.ID, tenantB)
		if resp.StatusCode != http.StatusNotFound {
			body := readBody(t, resp)
			t.Fatalf("tenant-B retrieve batch: expected 404, got %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
	})

	// Tenant B: list files returns empty
	t.Run("FileListIsolation", func(t *testing.T) {
		resp := ts.doRequestAs(t, http.MethodGet, "/v1/files", tenantB)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("tenant-B list files: expected 200, got %d", resp.StatusCode)
		}
		listResp := decodeJSON[openai.ListFilesResponse](t, resp)
		if len(listResp.Data) != 0 {
			t.Errorf("tenant-B list files: expected 0 files, got %d", len(listResp.Data))
		}
	})

	// Tenant B: list batches returns empty
	t.Run("BatchListIsolation", func(t *testing.T) {
		resp := ts.doRequestAs(t, http.MethodGet, "/v1/batches?limit=100", tenantB)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("tenant-B list batches: expected 200, got %d", resp.StatusCode)
		}
		listResp := decodeJSON[openai.ListBatchResponse](t, resp)
		if len(listResp.Data) != 0 {
			t.Errorf("tenant-B list batches: expected 0 batches, got %d", len(listResp.Data))
		}
	})

	// Tenant B: cannot cancel tenant A's batch
	t.Run("CancelIsolation", func(t *testing.T) {
		resp := ts.doJSONAs(t, http.MethodPost, "/v1/batches/"+batchA.ID+"/cancel", nil, tenantB)
		if resp.StatusCode != http.StatusNotFound {
			body := readBody(t, resp)
			t.Fatalf("tenant-B cancel batch: expected 404, got %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
	})

	// Tenant B: cannot delete tenant A's file
	t.Run("DeleteIsolation", func(t *testing.T) {
		resp := ts.doRequestAs(t, http.MethodDelete, "/v1/files/"+fileA.ID, tenantB)
		if resp.StatusCode != http.StatusNotFound {
			body := readBody(t, resp)
			t.Fatalf("tenant-B delete file: expected 404, got %d: %s", resp.StatusCode, body)
		}
		resp.Body.Close()
	})
}

func TestMissingTenantHeader(t *testing.T) {
	ts := newTestServer(t)

	// Request without tenant header — should be rejected
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/files", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately NOT setting the tenant header

	resp, err := ts.Client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("list: expected 401, got %d", resp.StatusCode)
	}
	errResp := decodeJSON[openai.ErrorResponse](t, resp)

	if errResp.Error.Type != "authentication_error" {
		t.Errorf("expected error type %q, got %q", "authentication_error", errResp.Error.Type)
	}

	// CORS preflight carries no tenant header and is still answered
	req, err = http.NewRequest(http.MethodOptions, ts.URL+"/v1/files", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err = ts.Client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("preflight: expected 204, got %d", resp.StatusCode)
	}
}

func TestProxyToken(t *testing.T) {
	ts := newTestServer(t)

	tests := []struct {
		name       string
		method     string
		token      string // proxy token header value; empty sends no header
		wantStatus int
	}{
		{
			name:       "no proxy token",
			method:     http.MethodGet,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "wrong proxy token",
			method:     http.MethodGet,
			token:      "guess",
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "right proxy token",
			method:     http.MethodGet,
			token:      testProxyToken,
			wantStatus: http.StatusOK,
		},
		{
			name:       "CORS preflight without proxy token is answered",
			method:     http.MethodOptions,
			wantStatus: http.StatusNoContent,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, ts.URL+"/v1/files", nil)
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set(testTenantHeader, testTenantID)
			if tt.token != "" {
				req.Header.Set(common.DefaultProxyTokenHeader, tt.token)
			}

			// http.DefaultClient does not add the proxy token, unlike ts.Client.
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != tt.wantStatus {
				body := readBody(t, resp)
				t.Fatalf("expected %d, got %d: %s", tt.wantStatus, resp.StatusCode, body)
			}
			if tt.wantStatus != http.StatusUnauthorized {
				resp.Body.Close()
				return
			}
			errResp := decodeJSON[openai.ErrorResponse](t, resp)
			if errResp.Error.Type != "authentication_error" {
				t.Errorf("expected error type %q, got %q", "authentication_error", errResp.Error.Type)
			}
		})
	}
}
