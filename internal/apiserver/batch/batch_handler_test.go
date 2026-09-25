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

// The file contains unit tests for batch handler.
package batch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockapi "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
)

// failingEventClient wraps MockBatchEventChannelClient but returns an error on ECProducerSendEvents.
type failingEventClient struct {
	*mockapi.MockBatchEventChannelClient
	sendErr error
}

func (f *failingEventClient) ECProducerSendEvents(_ context.Context, _ []dbapi.BatchEvent) ([]string, error) {
	return nil, f.sendErr
}

func setupTestHandler() *BatchAPIHandler {
	return setupTestHandlerWithConfig(&common.ServerConfig{})
}

func setupTestHandlerWithConfig(config *common.ServerConfig) *BatchAPIHandler {
	clients := &clientset.Clientset{
		Inference: nil,
		File:      nil,
		BatchDB: mockapi.NewMockDBClient[dbapi.BatchItem, dbapi.BatchQuery](
			func(b *dbapi.BatchItem) string { return b.ID },
			func(q *dbapi.BatchQuery) *dbapi.BaseQuery { return &q.BaseQuery },
		),
		FileDB: mockapi.NewMockDBClient[dbapi.FileItem, dbapi.FileQuery](
			func(f *dbapi.FileItem) string { return f.ID },
			func(q *dbapi.FileQuery) *dbapi.BaseQuery { return &q.BaseQuery },
		),
		Queue: mockapi.NewMockBatchPriorityQueueClient(),
		Event: mockapi.NewMockBatchEventChannelClient(),
	}
	handler := NewBatchAPIHandler(config, clients)
	return handler
}

func TestBatchHandler(t *testing.T) {
	t.Run("CreateBatch", func(t *testing.T) {
		t.Run("Success", func(t *testing.T) {
			successCases := []struct {
				name               string
				fileID             string
				outputExpiresAfter *openai.OutputExpiresAfter
				wantTags           map[string]string
			}{
				{
					name:   "Basic",
					fileID: "file-abc123",
				},
				{
					name:   "WithExpires",
					fileID: "file-with-expiry",
					outputExpiresAfter: &openai.OutputExpiresAfter{
						Seconds: 86400,
						Anchor:  "created_at",
					},
					wantTags: map[string]string{
						batch_types.TagOutputExpiresAfterSeconds: "86400",
						batch_types.TagOutputExpiresAfterAnchor:  "created_at",
					},
				},
			}
			for _, tc := range successCases {
				t.Run(tc.name, func(t *testing.T) {
					handler := setupTestHandler()
					ctx := context.Background()

					if err := handler.clients.FileDB.DBStore(ctx, &dbapi.FileItem{
						BaseIndexes: dbapi.BaseIndexes{ID: tc.fileID, TenantID: common.DefaultTenantID},
						Purpose:     string(openai.FileObjectPurposeBatch),
					}); err != nil {
						t.Fatalf("Failed to store file: %v", err)
					}

					reqBody := openai.CreateBatchRequest{
						InputFileID:        tc.fileID,
						Endpoint:           openai.EndpointChatCompletions,
						CompletionWindow:   "24h",
						OutputExpiresAfter: tc.outputExpiresAfter,
					}
					body, err := json.Marshal(reqBody)
					if err != nil {
						t.Fatalf("Failed to marshal request body: %v", err)
					}
					req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					rr := httptest.NewRecorder()
					handler.CreateBatch(rr, req)

					if status := rr.Code; status != http.StatusOK {
						t.Fatalf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
					}
					t.Logf("Response Body: %s", rr.Body.String())

					var batch openai.Batch
					if err := json.NewDecoder(rr.Body).Decode(&batch); err != nil {
						t.Fatalf("Failed to decode response body: %v", err)
					}

					if batch.ID == "" {
						t.Error("Expected batch ID to be generated")
					}
					if batch.Object != "batch" {
						t.Errorf("Expected object to be 'batch', got %v", batch.Object)
					}
					if batch.Endpoint != openai.EndpointChatCompletions {
						t.Errorf("Expected endpoint to be '%s', got %v", openai.EndpointChatCompletions, batch.Endpoint)
					}
					if batch.InputFileID != tc.fileID {
						t.Errorf("Expected input_file_id to be %q, got %v", tc.fileID, batch.InputFileID)
					}
					if batch.CompletionWindow != "24h" {
						t.Errorf("Expected completion_window to be '24h', got %v", batch.CompletionWindow)
					}
					if batch.Status != openai.BatchStatusValidating {
						t.Errorf("Expected status to be '%s', got %v", openai.BatchStatusValidating, batch.Status)
					}
					if batch.RequestCounts.Total != 0 {
						t.Errorf("Expected request_counts.total to be 0, got %v", batch.RequestCounts.Total)
					}

					if len(tc.wantTags) > 0 {
						query := &dbapi.BatchQuery{
							BaseQuery: dbapi.BaseQuery{
								IDs:      []string{batch.ID},
								TenantID: common.DefaultTenantID,
							},
						}
						items, _, _, err := handler.clients.BatchDB.DBGet(ctx, query, true, 0, 1)
						if err != nil {
							t.Fatalf("Failed to retrieve batch from database: %v", err)
						}
						if len(items) == 0 {
							t.Fatal("Batch not found in database")
						}
						for k, want := range tc.wantTags {
							if got := items[0].Tags[k]; got != want {
								t.Errorf("Expected tag %q to be %q, got %q", k, want, got)
							}
						}
					}
				})
			}
		})

		t.Run("Negative", func(t *testing.T) {
			negativeCases := []struct {
				name        string
				fileID      string // if non-empty, pre-store this file before the request
				filePurpose string // purpose to set on the pre-stored file (default: "batch")
				reqBody     string // raw JSON request body
				wantMsg     string // if non-empty, verify error message
			}{
				{
					name:    "InvalidEndpoint",
					fileID:  "file-invalid-endpoint",
					reqBody: `{"input_file_id":"file-invalid-endpoint","endpoint":"/v1/invalid","completion_window":"24h"}`,
				},
				{
					name:    "MissingCompletionWindow",
					reqBody: `{"input_file_id":"file-abc123","endpoint":"/v1/chat/completions"}`,
				},
				{
					name:    "InvalidCompletionWindow",
					fileID:  "file-invalid-cw",
					reqBody: `{"input_file_id":"file-invalid-cw","endpoint":"/v1/chat/completions","completion_window":"abc"}`,
				},
				{
					name:    "MissingEndpoint",
					reqBody: `{"input_file_id":"file-abc123","completion_window":"24h"}`,
				},
				{
					name:    "UnknownField",
					reqBody: `{"input_file_id":"file-abc123","endpoint":"/v1/chat/completions","completion_window":"24h","invalid_field":"some_value"}`,
					wantMsg: `json: unknown field "invalid_field"`,
				},
				{
					name:    "FileNotFound",
					reqBody: `{"input_file_id":"file-nonexistent","endpoint":"/v1/chat/completions","completion_window":"24h"}`,
					wantMsg: "Input file with ID 'file-nonexistent' not found",
				},
				{
					name:        "WrongFilePurpose",
					fileID:      "file-wrong-purpose",
					filePurpose: "fine-tune",
					reqBody:     `{"input_file_id":"file-wrong-purpose","endpoint":"/v1/chat/completions","completion_window":"24h"}`,
					wantMsg:     "Input file 'file-wrong-purpose' has purpose 'fine-tune', but must have purpose 'batch'",
				},
			}
			for _, tc := range negativeCases {
				t.Run(tc.name, func(t *testing.T) {
					handler := setupTestHandler()

					if tc.fileID != "" {
						purpose := string(openai.FileObjectPurposeBatch)
						if tc.filePurpose != "" {
							purpose = tc.filePurpose
						}
						if err := handler.clients.FileDB.DBStore(context.Background(), &dbapi.FileItem{
							BaseIndexes: dbapi.BaseIndexes{ID: tc.fileID, TenantID: common.DefaultTenantID},
							Purpose:     purpose,
						}); err != nil {
							t.Fatalf("Failed to store file: %v", err)
						}
					}

					req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader([]byte(tc.reqBody)))
					req.Header.Set("Content-Type", "application/json")
					rr := httptest.NewRecorder()
					handler.CreateBatch(rr, req)

					if rr.Code != http.StatusBadRequest {
						t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, rr.Code, rr.Body.String())
					}

					if tc.wantMsg != "" {
						var errResp openai.ErrorResponse
						if err := json.NewDecoder(rr.Body).Decode(&errResp); err != nil {
							t.Fatalf("Failed to decode error response: %v", err)
						}
						if errResp.Error.Message != tc.wantMsg {
							t.Errorf("expected error message %q, got %q", tc.wantMsg, errResp.Error.Message)
						}
					}
				})
			}

			t.Run("OutputExpiresAfter", func(t *testing.T) {
				oeaCases := []struct {
					name    string
					fileID  string
					anchor  string
					seconds int64
				}{
					{"InvalidAnchor", "file-oea-anchor", "updated_at", 86400},
					{"SecondsTooSmall", "file-oea-small", "created_at", 100},
					{"SecondsTooLarge", "file-oea-large", "created_at", 9999999},
					{"SecondsNegative", "file-oea-neg", "created_at", -1},
				}
				for _, tc := range oeaCases {
					t.Run(tc.name, func(t *testing.T) {
						handler := setupTestHandler()

						if err := handler.clients.FileDB.DBStore(context.Background(), &dbapi.FileItem{
							BaseIndexes: dbapi.BaseIndexes{ID: tc.fileID, TenantID: common.DefaultTenantID},
							Purpose:     string(openai.FileObjectPurposeBatch),
						}); err != nil {
							t.Fatalf("Failed to store file: %v", err)
						}

						reqBody := openai.CreateBatchRequest{
							InputFileID:      tc.fileID,
							Endpoint:         openai.EndpointChatCompletions,
							CompletionWindow: "24h",
							OutputExpiresAfter: &openai.OutputExpiresAfter{
								Anchor:  tc.anchor,
								Seconds: tc.seconds,
							},
						}
						body, _ := json.Marshal(reqBody)
						req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
						req.Header.Set("Content-Type", "application/json")
						rr := httptest.NewRecorder()
						handler.CreateBatch(rr, req)

						if rr.Code != http.StatusBadRequest {
							t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, rr.Code, rr.Body.String())
						}
					})
				}
			})
		})

		t.Run("PassThroughHeaders", func(t *testing.T) {
			t.Run("SingleValue", func(t *testing.T) {
				handler := setupTestHandlerWithConfig(&common.ServerConfig{
					BatchAPI: common.BatchAPIConfig{
						PassThroughHeaders: []string{"X-Custom-Header"},
					},
				})

				fileItem := &dbapi.FileItem{
					BaseIndexes: dbapi.BaseIndexes{
						ID:       "file-pth-single",
						TenantID: common.DefaultTenantID,
					},
					Purpose: string(openai.FileObjectPurposeBatch),
				}
				ctx := context.Background()
				if err := handler.clients.FileDB.DBStore(ctx, fileItem); err != nil {
					t.Fatalf("Failed to store file: %v", err)
				}

				reqBody := openai.CreateBatchRequest{
					InputFileID:      "file-pth-single",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
				}
				body, err := json.Marshal(reqBody)
				if err != nil {
					t.Fatalf("Failed to marshal request body: %v", err)
				}
				req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Custom-Header", "custom-value")
				rr := httptest.NewRecorder()
				handler.CreateBatch(rr, req)

				if status := rr.Code; status != http.StatusOK {
					t.Fatalf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
				}

				var batch openai.Batch
				if err := json.NewDecoder(rr.Body).Decode(&batch); err != nil {
					t.Fatalf("Failed to decode response body: %v", err)
				}

				// Verify the pass-through header was stored as a tag
				query := &dbapi.BatchQuery{
					BaseQuery: dbapi.BaseQuery{
						IDs:      []string{batch.ID},
						TenantID: common.DefaultTenantID,
					},
				}
				items, _, _, err := handler.clients.BatchDB.DBGet(ctx, query, true, 0, 1)
				if err != nil {
					t.Fatalf("Failed to retrieve batch from database: %v", err)
				}
				if len(items) == 0 {
					t.Fatal("Batch not found in database")
				}

				if got := items[0].Tags["pth:X-Custom-Header"]; got != "custom-value" {
					t.Errorf("Expected tag pth:X-Custom-Header to be 'custom-value', got %q", got)
				}
			})

			// When multiple values exist for the same header (e.g. client-supplied
			// followed by Envoy ext_authz-injected), the handler must use the last
			// value because the auth service appends after any client-spoofed entry.
			t.Run("MultipleValuesUsesLast", func(t *testing.T) {
				handler := setupTestHandlerWithConfig(&common.ServerConfig{
					BatchAPI: common.BatchAPIConfig{
						PassThroughHeaders: []string{"X-Auth-User"},
					},
				})

				fileItem := &dbapi.FileItem{
					BaseIndexes: dbapi.BaseIndexes{
						ID:       "file-pth-multi",
						TenantID: common.DefaultTenantID,
					},
					Purpose: string(openai.FileObjectPurposeBatch),
				}
				ctx := context.Background()
				if err := handler.clients.FileDB.DBStore(ctx, fileItem); err != nil {
					t.Fatalf("Failed to store file: %v", err)
				}

				reqBody := openai.CreateBatchRequest{
					InputFileID:      "file-pth-multi",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
				}
				body, err := json.Marshal(reqBody)
				if err != nil {
					t.Fatalf("Failed to marshal request body: %v", err)
				}
				req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				// Simulate a spoofed client header followed by an auth-injected header.
				// Header.Add appends additional values for the same key.
				req.Header.Set("X-Auth-User", "spoofed-user")
				req.Header.Add("X-Auth-User", "real-user")
				rr := httptest.NewRecorder()
				handler.CreateBatch(rr, req)

				if status := rr.Code; status != http.StatusOK {
					t.Fatalf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
				}

				var batch openai.Batch
				if err := json.NewDecoder(rr.Body).Decode(&batch); err != nil {
					t.Fatalf("Failed to decode response body: %v", err)
				}

				query := &dbapi.BatchQuery{
					BaseQuery: dbapi.BaseQuery{
						IDs:      []string{batch.ID},
						TenantID: common.DefaultTenantID,
					},
				}
				items, _, _, err := handler.clients.BatchDB.DBGet(ctx, query, true, 0, 1)
				if err != nil {
					t.Fatalf("Failed to retrieve batch from database: %v", err)
				}
				if len(items) == 0 {
					t.Fatal("Batch not found in database")
				}

				// Must be "real-user" (the last/auth-injected value), not "spoofed-user"
				if got := items[0].Tags["pth:X-Auth-User"]; got != "real-user" {
					t.Errorf("Expected tag pth:X-Auth-User to be 'real-user', got %q", got)
				}
			})

			// When the auth service clears a spoofed header by appending an empty
			// entry, the empty last value must not produce a tag — otherwise
			// downstream consumers could misinterpret an empty string as valid.
			t.Run("EmptyLastValueSkipped", func(t *testing.T) {
				handler := setupTestHandlerWithConfig(&common.ServerConfig{
					BatchAPI: common.BatchAPIConfig{
						PassThroughHeaders: []string{"X-Auth-User"},
					},
				})

				fileItem := &dbapi.FileItem{
					BaseIndexes: dbapi.BaseIndexes{
						ID:       "file-pth-empty",
						TenantID: common.DefaultTenantID,
					},
					Purpose: string(openai.FileObjectPurposeBatch),
				}
				ctx := context.Background()
				if err := handler.clients.FileDB.DBStore(ctx, fileItem); err != nil {
					t.Fatalf("Failed to store file: %v", err)
				}

				reqBody := openai.CreateBatchRequest{
					InputFileID:      "file-pth-empty",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
				}
				body, err := json.Marshal(reqBody)
				if err != nil {
					t.Fatalf("Failed to marshal request body: %v", err)
				}
				req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				// Client sends a spoofed value, auth service clears it with an empty entry.
				req.Header.Set("X-Auth-User", "spoofed-user")
				req.Header.Add("X-Auth-User", "")
				rr := httptest.NewRecorder()
				handler.CreateBatch(rr, req)

				if status := rr.Code; status != http.StatusOK {
					t.Fatalf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
				}

				var batch openai.Batch
				if err := json.NewDecoder(rr.Body).Decode(&batch); err != nil {
					t.Fatalf("Failed to decode response body: %v", err)
				}

				query := &dbapi.BatchQuery{
					BaseQuery: dbapi.BaseQuery{
						IDs:      []string{batch.ID},
						TenantID: common.DefaultTenantID,
					},
				}
				items, _, _, err := handler.clients.BatchDB.DBGet(ctx, query, true, 0, 1)
				if err != nil {
					t.Fatalf("Failed to retrieve batch from database: %v", err)
				}
				if len(items) == 0 {
					t.Fatal("Batch not found in database")
				}

				// The spoofed value must NOT leak through; the empty last value
				// should cause the tag to be omitted entirely.
				if _, exists := items[0].Tags["pth:X-Auth-User"]; exists {
					t.Errorf("Expected no tag for empty last value, but pth:X-Auth-User was set to %q",
						items[0].Tags["pth:X-Auth-User"])
				}
			})

			t.Run("HeaderNotPresent", func(t *testing.T) {
				handler := setupTestHandlerWithConfig(&common.ServerConfig{
					BatchAPI: common.BatchAPIConfig{
						PassThroughHeaders: []string{"X-Missing-Header"},
					},
				})

				fileItem := &dbapi.FileItem{
					BaseIndexes: dbapi.BaseIndexes{
						ID:       "file-pth-missing",
						TenantID: common.DefaultTenantID,
					},
					Purpose: string(openai.FileObjectPurposeBatch),
				}
				ctx := context.Background()
				if err := handler.clients.FileDB.DBStore(ctx, fileItem); err != nil {
					t.Fatalf("Failed to store file: %v", err)
				}

				reqBody := openai.CreateBatchRequest{
					InputFileID:      "file-pth-missing",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
				}
				body, err := json.Marshal(reqBody)
				if err != nil {
					t.Fatalf("Failed to marshal request body: %v", err)
				}
				req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				// Do not set X-Missing-Header
				rr := httptest.NewRecorder()
				handler.CreateBatch(rr, req)

				if status := rr.Code; status != http.StatusOK {
					t.Fatalf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
				}

				var batch openai.Batch
				if err := json.NewDecoder(rr.Body).Decode(&batch); err != nil {
					t.Fatalf("Failed to decode response body: %v", err)
				}

				query := &dbapi.BatchQuery{
					BaseQuery: dbapi.BaseQuery{
						IDs:      []string{batch.ID},
						TenantID: common.DefaultTenantID,
					},
				}
				items, _, _, err := handler.clients.BatchDB.DBGet(ctx, query, true, 0, 1)
				if err != nil {
					t.Fatalf("Failed to retrieve batch from database: %v", err)
				}
				if len(items) == 0 {
					t.Fatal("Batch not found in database")
				}

				// Tag should not exist when the header is absent
				if _, exists := items[0].Tags["pth:X-Missing-Header"]; exists {
					t.Error("Expected no tag for absent header, but pth:X-Missing-Header was set")
				}
			})

			t.Run("MultipleConfiguredHeaders", func(t *testing.T) {
				handler := setupTestHandlerWithConfig(&common.ServerConfig{
					BatchAPI: common.BatchAPIConfig{
						PassThroughHeaders: []string{"X-Header-A", "X-Header-B"},
					},
				})

				fileItem := &dbapi.FileItem{
					BaseIndexes: dbapi.BaseIndexes{
						ID:       "file-pth-multiple",
						TenantID: common.DefaultTenantID,
					},
					Purpose: string(openai.FileObjectPurposeBatch),
				}
				ctx := context.Background()
				if err := handler.clients.FileDB.DBStore(ctx, fileItem); err != nil {
					t.Fatalf("Failed to store file: %v", err)
				}

				reqBody := openai.CreateBatchRequest{
					InputFileID:      "file-pth-multiple",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
				}
				body, err := json.Marshal(reqBody)
				if err != nil {
					t.Fatalf("Failed to marshal request body: %v", err)
				}
				req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(body))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-Header-A", "value-a")
				req.Header.Set("X-Header-B", "value-b")
				rr := httptest.NewRecorder()
				handler.CreateBatch(rr, req)

				if status := rr.Code; status != http.StatusOK {
					t.Fatalf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
				}

				var batch openai.Batch
				if err := json.NewDecoder(rr.Body).Decode(&batch); err != nil {
					t.Fatalf("Failed to decode response body: %v", err)
				}

				query := &dbapi.BatchQuery{
					BaseQuery: dbapi.BaseQuery{
						IDs:      []string{batch.ID},
						TenantID: common.DefaultTenantID,
					},
				}
				items, _, _, err := handler.clients.BatchDB.DBGet(ctx, query, true, 0, 1)
				if err != nil {
					t.Fatalf("Failed to retrieve batch from database: %v", err)
				}
				if len(items) == 0 {
					t.Fatal("Batch not found in database")
				}

				if got := items[0].Tags["pth:X-Header-A"]; got != "value-a" {
					t.Errorf("Expected tag pth:X-Header-A to be 'value-a', got %q", got)
				}
				if got := items[0].Tags["pth:X-Header-B"]; got != "value-b" {
					t.Errorf("Expected tag pth:X-Header-B to be 'value-b', got %q", got)
				}
			})
		})
	})

	t.Run("RetrieveBatch", func(t *testing.T) {
		handler := setupTestHandler()

		// create a batch first
		batchID := "batch-test-123"
		batch := openai.Batch{
			ID: batchID,
			BatchSpec: openai.BatchSpec{
				Object:           "batch",
				InputFileID:      "file-abc123",
				Endpoint:         openai.EndpointChatCompletions,
				CompletionWindow: "24h",
				CreatedAt:        time.Now().UTC().Unix(),
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusValidating,
				RequestCounts: openai.BatchRequestCounts{
					Total:     0,
					Completed: 0,
					Failed:    0,
				},
			},
		}
		item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
		if err != nil {
			t.Fatalf("Failed to convert batch to DB item: %v", err)
		}
		if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
			t.Fatalf("Failed to store item: %v", err)
		}

		// get batch
		req := httptest.NewRequest(http.MethodGet, "/v1/batches/"+batchID, nil)
		req.SetPathValue("batch_id", batchID)
		rr := httptest.NewRecorder()
		handler.RetrieveBatch(rr, req)

		// verify response
		if status := rr.Code; status != http.StatusOK {
			t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
		}
		t.Logf("Response Body: %s", rr.Body.String())

		var respBatch openai.Batch
		if err := json.NewDecoder(rr.Body).Decode(&respBatch); err != nil {
			t.Fatalf("Failed to decode response body: %v", err)
		}

		if respBatch.ID != batchID {
			t.Errorf("Expected batch ID to be %s, got %s", batchID, respBatch.ID)
		}
		if respBatch.Object != batch.Object {
			t.Errorf("Expected object to be %q, got %q", batch.Object, respBatch.Object)
		}
		if respBatch.Endpoint != batch.Endpoint {
			t.Errorf("Expected endpoint to be %q, got %q", batch.Endpoint, respBatch.Endpoint)
		}
		if respBatch.InputFileID != batch.InputFileID {
			t.Errorf("Expected input_file_id to be %q, got %q", batch.InputFileID, respBatch.InputFileID)
		}
		if respBatch.CompletionWindow != batch.CompletionWindow {
			t.Errorf("Expected completion_window to be %q, got %q", batch.CompletionWindow, respBatch.CompletionWindow)
		}
		if respBatch.CreatedAt != batch.CreatedAt {
			t.Errorf("Expected created_at to be %d, got %d", batch.CreatedAt, respBatch.CreatedAt)
		}
		if respBatch.Status != batch.Status {
			t.Errorf("Expected status to be %q, got %q", batch.Status, respBatch.Status)
		}
		if respBatch.RequestCounts != batch.RequestCounts {
			t.Errorf("Expected request_counts to be %+v, got %+v", batch.RequestCounts, respBatch.RequestCounts)
		}
	})

	t.Run("ListBatches", func(t *testing.T) {
		handler := setupTestHandler()

		createdAt := time.Now().UTC().Unix()
		// create two batches
		createdBatches := make([]openai.Batch, 2)
		for i := range 2 {
			batchID := fmt.Sprintf("batch-test-%d", i)
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      fmt.Sprintf("file-%d", i),
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        createdAt,
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status: openai.BatchStatusValidating,
					RequestCounts: openai.BatchRequestCounts{
						Total:     0,
						Completed: 0,
						Failed:    0,
					},
				},
			}
			createdBatches[i] = batch
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
			if err != nil {
				t.Fatalf("Failed to convert batch to DB item: %v", err)
			}
			if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
				t.Fatalf("Failed to store item: %v", err)
			}
		}

		// list batches
		req := httptest.NewRequest(http.MethodGet, "/v1/batches?limit=10", nil)
		rr := httptest.NewRecorder()
		handler.ListBatches(rr, req)

		// verify response
		if status := rr.Code; status != http.StatusOK {
			t.Fatalf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
		}
		t.Logf("Response Body: %s", rr.Body.String())

		var resp openai.ListBatchResponse
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("Failed to decode response body: %v", err)
		}

		if resp.Object != "list" {
			t.Errorf("Expected object to be 'list', got %v", resp.Object)
		}
		if len(resp.Data) != 2 {
			t.Fatalf("Expected 2 batches, got %d", len(resp.Data))
		}

		// Verify each batch's fields match what was created
		created := make(map[string]openai.Batch, len(createdBatches))
		for _, b := range createdBatches {
			created[b.ID] = b
		}
		for _, got := range resp.Data {
			want, ok := created[got.ID]
			if !ok {
				t.Errorf("Unexpected batch ID %q in list response", got.ID)
				continue
			}
			if got.Object != want.Object {
				t.Errorf("batch %s: expected object %q, got %q", got.ID, want.Object, got.Object)
			}
			if got.Endpoint != want.Endpoint {
				t.Errorf("batch %s: expected endpoint %q, got %q", got.ID, want.Endpoint, got.Endpoint)
			}
			if got.InputFileID != want.InputFileID {
				t.Errorf("batch %s: expected input_file_id %q, got %q", got.ID, want.InputFileID, got.InputFileID)
			}
			if got.CompletionWindow != want.CompletionWindow {
				t.Errorf("batch %s: expected completion_window %q, got %q", got.ID, want.CompletionWindow, got.CompletionWindow)
			}
			if got.CreatedAt != want.CreatedAt {
				t.Errorf("batch %s: expected created_at %d, got %d", got.ID, want.CreatedAt, got.CreatedAt)
			}
			if got.Status != want.Status {
				t.Errorf("batch %s: expected status %q, got %q", got.ID, want.Status, got.Status)
			}
			if got.RequestCounts != want.RequestCounts {
				t.Errorf("batch %s: expected request_counts %+v, got %+v", got.ID, want.RequestCounts, got.RequestCounts)
			}
		}

		// Verify pagination fields
		if resp.HasMore {
			t.Errorf("Expected has_more to be false, got %v", resp.HasMore)
		}
		if resp.FirstID == nil {
			t.Error("Expected first_id to be set")
		}
		if resp.LastID == nil {
			t.Error("Expected last_id to be set")
		}

		t.Run("Negative", func(t *testing.T) {
			negativeCases := []struct {
				name  string
				query string
			}{
				{"LimitZero", "limit=0"},
				{"LimitTooLarge", "limit=999"},
				{"InvalidLimit", "limit=abc"},
				{"AfterNegative", "after=-1"},
				{"InvalidAfter", "after=abc"},
			}
			for _, tc := range negativeCases {
				t.Run(tc.name, func(t *testing.T) {
					req := httptest.NewRequest(http.MethodGet, "/v1/batches?"+tc.query, nil)
					rr := httptest.NewRecorder()
					handler.ListBatches(rr, req)

					if rr.Code != http.StatusBadRequest {
						t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, rr.Code, rr.Body.String())
					}
				})
			}
		})
	})

	t.Run("CancelBatch", func(t *testing.T) {
		t.Run("CancelCompletedBatch", func(t *testing.T) {
			handler := setupTestHandler()

			batchID := "batch-test-cancel-completed"
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      "file-abc123",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status: openai.BatchStatusCompleted,
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
			if err != nil {
				t.Fatalf("Failed to convert batch to DB item: %v", err)
			}
			if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
				t.Fatalf("Failed to store item: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, rr.Code, rr.Body.String())
			}
		})

		t.Run("CancelAlreadyCancelledBatch", func(t *testing.T) {
			handler := setupTestHandler()

			batchID := "batch-test-cancel-already-cancelled"
			cancelledAt := time.Now().UTC().Unix()
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      "file-abc123",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status:      openai.BatchStatusCancelled,
					CancelledAt: &cancelledAt,
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
			if err != nil {
				t.Fatalf("Failed to convert batch to DB item: %v", err)
			}
			if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
				t.Fatalf("Failed to store item: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)

			if rr.Code != http.StatusOK {
				t.Errorf("expected status %d for already-cancelled batch, got %d: %s", http.StatusOK, rr.Code, rr.Body.String())
			}

			var respBatch openai.Batch
			if err := json.NewDecoder(rr.Body).Decode(&respBatch); err != nil {
				t.Fatalf("Failed to decode response body: %v", err)
			}
			if respBatch.Status != openai.BatchStatusCancelled {
				t.Errorf("expected status %q, got %q", openai.BatchStatusCancelled, respBatch.Status)
			}
		})

		t.Run("CancelInProgressBatch", func(t *testing.T) {
			handler := setupTestHandler()

			batchID := "batch-test-cancel"
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      "file-abc123",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status: openai.BatchStatusInProgress,
					RequestCounts: openai.BatchRequestCounts{
						Total:     10,
						Completed: 5,
						Failed:    0,
					},
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, nil)
			if err != nil {
				t.Fatalf("Failed to convert batch to DB item: %v", err)
			}
			if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
				t.Fatalf("Failed to store item: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)

			if status := rr.Code; status != http.StatusOK {
				t.Errorf("Handler returned wrong status code: got %v want %v", status, http.StatusOK)
			}
			t.Logf("Response Body: %s", rr.Body.String())

			var respBatch openai.Batch
			if err := json.NewDecoder(rr.Body).Decode(&respBatch); err != nil {
				t.Fatalf("Failed to decode response body: %v", err)
			}

			if respBatch.ID != batchID {
				t.Errorf("Expected batch ID to be %s, got %s", batchID, respBatch.ID)
			}
			if respBatch.Status != openai.BatchStatusCancelling {
				t.Errorf("Expected status to be '%s', got %s", openai.BatchStatusCancelling, respBatch.Status)
			}
			if respBatch.CancellingAt == nil {
				t.Error("Expected cancelling_at to be set")
			}
		})

		t.Run("CancelFinalizingBatch", func(t *testing.T) {
			handler := setupTestHandler()

			batchID := "batch-test-cancel-finalizing"
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      "file-abc123",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status: openai.BatchStatusFinalizing,
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
			if err != nil {
				t.Fatalf("Failed to convert batch to DB item: %v", err)
			}
			if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
				t.Fatalf("Failed to store item: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)

			if rr.Code != http.StatusBadRequest {
				t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, rr.Code, rr.Body.String())
			}
		})

		t.Run("CancelBatch_EventSendFailure", func(t *testing.T) {
			failingEvent := &failingEventClient{
				MockBatchEventChannelClient: mockapi.NewMockBatchEventChannelClient(),
				sendErr:                     fmt.Errorf("event broker unavailable"),
			}
			clients := &clientset.Clientset{
				BatchDB: mockapi.NewMockDBClient[dbapi.BatchItem, dbapi.BatchQuery](
					func(b *dbapi.BatchItem) string { return b.ID },
					func(q *dbapi.BatchQuery) *dbapi.BaseQuery { return &q.BaseQuery },
				),
				FileDB: mockapi.NewMockDBClient[dbapi.FileItem, dbapi.FileQuery](
					func(f *dbapi.FileItem) string { return f.ID },
					func(q *dbapi.FileQuery) *dbapi.BaseQuery { return &q.BaseQuery },
				),
				Queue: mockapi.NewMockBatchPriorityQueueClient(),
				Event: failingEvent,
			}
			handler := NewBatchAPIHandler(&common.ServerConfig{}, clients)

			batchID := "batch-test-cancel-event-fail"
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      "file-abc123",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status: openai.BatchStatusInProgress,
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, nil)
			if err != nil {
				t.Fatalf("Failed to convert batch to DB item: %v", err)
			}
			if err := clients.BatchDB.DBStore(context.Background(), item); err != nil {
				t.Fatalf("Failed to store item: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)

			if rr.Code != http.StatusInternalServerError {
				t.Errorf("expected status %d, got %d: %s", http.StatusInternalServerError, rr.Code, rr.Body.String())
			}
		})

		t.Run("CancelCancellingBatch_ResendsEvent", func(t *testing.T) {
			handler := setupTestHandler()

			batchID := "batch-test-cancel-retry"
			cancellingAt := int64(1700000000)
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      "file-abc123",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status:       openai.BatchStatusCancelling,
					CancellingAt: &cancellingAt,
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, nil)
			if err != nil {
				t.Fatalf("Failed to convert batch to database item: %v", err)
			}
			if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
				t.Fatalf("Failed to store item: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)

			if rr.Code != http.StatusOK {
				t.Fatalf("expected status %d, got %d: %s", http.StatusOK, rr.Code, rr.Body.String())
			}

			var respBatch openai.Batch
			if err := json.NewDecoder(rr.Body).Decode(&respBatch); err != nil {
				t.Fatalf("Failed to decode response body: %v", err)
			}
			if respBatch.Status != openai.BatchStatusCancelling {
				t.Errorf("Expected status %s, got %s", openai.BatchStatusCancelling, respBatch.Status)
			}
			if respBatch.CancellingAt == nil || *respBatch.CancellingAt != cancellingAt {
				t.Errorf("Expected cancelling_at unchanged (%d), got %v", cancellingAt, respBatch.CancellingAt)
			}
		})

		t.Run("CancelCancellingBatch_EventSendFailure", func(t *testing.T) {
			failingEvent := &failingEventClient{
				MockBatchEventChannelClient: mockapi.NewMockBatchEventChannelClient(),
				sendErr:                     fmt.Errorf("event broker unavailable"),
			}
			clients := &clientset.Clientset{
				BatchDB: mockapi.NewMockDBClient[dbapi.BatchItem, dbapi.BatchQuery](
					func(b *dbapi.BatchItem) string { return b.ID },
					func(q *dbapi.BatchQuery) *dbapi.BaseQuery { return &q.BaseQuery },
				),
				FileDB: mockapi.NewMockDBClient[dbapi.FileItem, dbapi.FileQuery](
					func(f *dbapi.FileItem) string { return f.ID },
					func(q *dbapi.FileQuery) *dbapi.BaseQuery { return &q.BaseQuery },
				),
				Queue: mockapi.NewMockBatchPriorityQueueClient(),
				Event: failingEvent,
			}
			handler := NewBatchAPIHandler(&common.ServerConfig{}, clients)

			batchID := "batch-test-cancel-retry-event-fail"
			cancellingAt := int64(1700000001)
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      "file-abc123",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status:       openai.BatchStatusCancelling,
					CancellingAt: &cancellingAt,
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, nil)
			if err != nil {
				t.Fatalf("Failed to convert batch to database item: %v", err)
			}
			if err := clients.BatchDB.DBStore(context.Background(), item); err != nil {
				t.Fatalf("Failed to store item: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)

			if rr.Code != http.StatusInternalServerError {
				t.Errorf("expected status %d, got %d: %s", http.StatusInternalServerError, rr.Code, rr.Body.String())
			}
		})
	})
}

// Benchmark tests for batch handler
func BenchmarkBatchHandler(b *testing.B) {
	handler := setupTestHandler()

	b.Run("CreateBatch", func(b *testing.B) {
		reqBody := openai.CreateBatchRequest{
			InputFileID:      "file-abc123",
			Endpoint:         openai.EndpointChatCompletions,
			CompletionWindow: "24h",
		}

		bodyBytes, _ := json.Marshal(reqBody)

		b.ResetTimer()
		for b.Loop() {
			req := httptest.NewRequest(http.MethodPost, "/v1/batches", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rr := httptest.NewRecorder()
			handler.CreateBatch(rr, req)
		}
	})

	b.Run("RetrieveBatch", func(b *testing.B) {
		// Setup: create a batch first
		batchID := "batch-benchmark-123"
		batch := openai.Batch{
			ID: batchID,
			BatchSpec: openai.BatchSpec{
				Object:           "batch",
				InputFileID:      "file-abc123",
				Endpoint:         openai.EndpointChatCompletions,
				CompletionWindow: "24h",
			},
			BatchStatusInfo: openai.BatchStatusInfo{
				Status: openai.BatchStatusValidating,
				RequestCounts: openai.BatchRequestCounts{
					Total:     0,
					Completed: 0,
					Failed:    0,
				},
			},
		}
		item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
		if err != nil {
			b.Fatalf("Failed to convert batch to DB item: %v", err)
		}
		if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
			b.Fatalf("Failed to store item: %v", err)
		}

		b.ResetTimer()
		for b.Loop() {
			req := httptest.NewRequest(http.MethodGet, "/v1/batches/"+batchID, nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.RetrieveBatch(rr, req)
		}
	})

	b.Run("ListBatches", func(b *testing.B) {
		// Setup: create multiple batches
		for i := range 10 {
			batchID := fmt.Sprintf("batch-benchmark-%d", i)
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      fmt.Sprintf("file-%d", i),
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status: openai.BatchStatusValidating,
					RequestCounts: openai.BatchRequestCounts{
						Total:     0,
						Completed: 0,
						Failed:    0,
					},
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
			if err != nil {
				b.Fatalf("Failed to convert batch to DB item: %v", err)
			}
			if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
				b.Fatalf("Failed to store item: %v", err)
			}
		}

		b.ResetTimer()
		for b.Loop() {
			req := httptest.NewRequest(http.MethodGet, "/v1/batches?limit=10", nil)
			rr := httptest.NewRecorder()
			handler.ListBatches(rr, req)
		}
	})

	b.Run("CancelBatch", func(b *testing.B) {
		b.ResetTimer()
		for i := range b.N {
			// Create a new batch for each iteration
			b.StopTimer()
			batchID := fmt.Sprintf("batch-benchmark-cancel-%d", i)
			batch := openai.Batch{
				ID: batchID,
				BatchSpec: openai.BatchSpec{
					Object:           "batch",
					InputFileID:      "file-abc123",
					Endpoint:         openai.EndpointChatCompletions,
					CompletionWindow: "24h",
					CreatedAt:        time.Now().UTC().Unix(),
				},
				BatchStatusInfo: openai.BatchStatusInfo{
					Status: openai.BatchStatusInProgress,
					RequestCounts: openai.BatchRequestCounts{
						Total:     10,
						Completed: 5,
						Failed:    0,
					},
				},
			}
			item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, map[string]string{})
			if err != nil {
				b.Fatalf("Failed to convert batch to DB item: %v", err)
			}
			if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
				b.Fatalf("Failed to store item: %v", err)
			}
			b.StartTimer()

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)
		}
	})
}
