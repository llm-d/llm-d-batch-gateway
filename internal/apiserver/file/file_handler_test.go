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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	dbmock "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	fsapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	fsclient "github.com/llm-d/llm-d-batch-gateway/internal/files_store/fs"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"k8s.io/klog/v2"
)

func TestFileHandler(t *testing.T) {
	// Setup logging once for all subtests
	klog.InitFlags(nil)

	t.Run("CreateFile", doTestCreateFile)
	t.Run("ListFiles", doTestListFiles)
	t.Run("RetrieveFile", doTestRetrieveFile)
	t.Run("DownloadFile", doTestDownloadFile)
	t.Run("DeleteFile", doTestDeleteFile)
}

// setupTestHandler creates a test handler with mocked dependencies
func setupTestHandler(t *testing.T) *FileAPIHandler {
	t.Helper()

	filesClient, err := fsclient.New(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create fs client: %v", err)
	}
	dbClient := dbmock.NewMockDBClient[dbapi.FileItem, dbapi.FileQuery](
		func(f *dbapi.FileItem) string { return f.ID },
		func(q *dbapi.FileQuery) *dbapi.BaseQuery { return &q.BaseQuery },
	)
	clients := &clientset.Clientset{
		Inference: nil,
		File:      filesClient,
		BatchDB:   nil,
		FileDB:    dbClient,
		Queue:     nil,
		Event:     nil,
		Status:    nil,
	}

	t.Cleanup(func() { _ = filesClient.Close() })

	config := &common.ServerConfig{
		FileAPI: common.FileAPIConfig{
			MaxSizeBytes:             common.DefaultMaxFileSizeBytes,
			MaxLineCount:             common.DefaultMaxFileLineCount,
			DefaultExpirationSeconds: 30 * 24 * 60 * 60, // 30 days (test value)
		},
	}

	handler := NewFileAPIHandler(config, clients)

	return handler
}

// createTestFile is a helper function that creates a test file and returns the created FileObject.
func createTestFile(t *testing.T, handler *FileAPIHandler, ctx context.Context, filename, purpose, content string) openai.FileObject {
	t.Helper()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	fileWriter, err := writer.CreateFormFile("file", filename)
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := io.WriteString(fileWriter, content); err != nil {
		t.Fatalf("failed to write file content: %v", err)
	}

	if err := writer.WriteField("purpose", purpose); err != nil {
		t.Fatalf("failed to write purpose field: %v", err)
	}

	if err := writer.Close(); err != nil {
		t.Fatalf("failed to close multipart writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/files", body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	req = req.WithContext(ctx)

	w := httptest.NewRecorder()
	handler.CreateFile(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("failed to create file %s: status %d, body: %s", filename, w.Code, w.Body.String())
	}

	var fileObj openai.FileObject
	if err := json.Unmarshal(w.Body.Bytes(), &fileObj); err != nil {
		t.Fatalf("failed to parse create file response: %v", err)
	}

	return fileObj
}

func doTestCreateFile(t *testing.T) {
	t.Run("Success", doTestCreateFileSuccess)
	t.Run("PurposeValidation", doTestCreateFilePurposeValidation)
	t.Run("ExpiresAfterValidation", doTestCreateFileExpiresAfter)
	t.Run("StoreValidationErrors", doTestCreateFileStoreValidationErrors)
}

// errStoreFileClient implements fsapi.BatchFilesClient and returns a fixed error from Store.
type errStoreFileClient struct {
	storeErr error
}

func (c *errStoreFileClient) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (*fsapi.BatchFileMetadata, error) {
	_, _ = io.Copy(io.Discard, reader)
	return nil, c.storeErr
}

func (c *errStoreFileClient) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *fsapi.BatchFileMetadata, error) {
	return nil, nil, errors.New("not implemented")
}

func (c *errStoreFileClient) Delete(ctx context.Context, fileName, folderName string) error {
	return nil
}

func (c *errStoreFileClient) Close() error {
	return nil
}

type errDeleteFileClient struct {
	fsapi.BatchFilesClient
	deleteErr error
}

func (c *errDeleteFileClient) Delete(ctx context.Context, fileName, folderName string) error {
	if c.deleteErr != nil {
		return c.deleteErr
	}
	return c.BatchFilesClient.Delete(ctx, fileName, folderName)
}

type failOnceDBClient struct {
	dbapi.FileDBClient
	failNext atomic.Bool
}

func (c *failOnceDBClient) DBDelete(ctx context.Context, ids []string) ([]string, error) {
	if c.failNext.CompareAndSwap(true, false) {
		return nil, errors.New("transient DB error")
	}
	return c.FileDBClient.DBDelete(ctx, ids)
}

func doTestCreateFileStoreValidationErrors(t *testing.T) {
	ctx := context.Background()
	dbClient := dbmock.NewMockDBClient[dbapi.FileItem, dbapi.FileQuery](
		func(f *dbapi.FileItem) string { return f.ID },
		func(q *dbapi.FileQuery) *dbapi.BaseQuery { return &q.BaseQuery },
	)
	// Keep the file below the size limit; Store() errors are injected by errStoreFileClient.
	config := &common.ServerConfig{
		FileAPI: common.FileAPIConfig{
			MaxSizeBytes:             common.DefaultMaxFileSizeBytes,
			MaxLineCount:             10,
			DefaultExpirationSeconds: 30 * 24 * 60 * 60,
		},
	}

	buildCreateRequest := func(t *testing.T) *http.Request {
		t.Helper()
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		fileWriter, err := writer.CreateFormFile("file", "test.jsonl")
		if err != nil {
			t.Fatalf("failed to create form file: %v", err)
		}
		if _, err := io.WriteString(fileWriter, `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{}}`+"\n"); err != nil {
			t.Fatalf("failed to write file content: %v", err)
		}
		if err := writer.WriteField("purpose", "batch"); err != nil {
			t.Fatalf("failed to write purpose field: %v", err)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close multipart writer: %v", err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/files", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		return req.WithContext(ctx)
	}

	cases := []struct {
		name        string
		storeErr    error
		wantStatus  int
		wantMessage string
	}{
		{
			name:        "file too large",
			storeErr:    fsapi.ErrFileTooLarge,
			wantStatus:  http.StatusBadRequest,
			wantMessage: fmt.Sprintf("File size exceeds the maximum allowed size of %d bytes", common.DefaultMaxFileSizeBytes),
		},
		{
			name:        "too many lines",
			storeErr:    fsapi.ErrTooManyLines,
			wantStatus:  http.StatusBadRequest,
			wantMessage: "File exceeds the maximum allowed line count of 10",
		},
		{
			name:        "file already exists",
			storeErr:    fsapi.ErrFileExists,
			wantStatus:  http.StatusConflict,
			wantMessage: "A file with this name already exists",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clients := &clientset.Clientset{
				File:    &errStoreFileClient{storeErr: tc.storeErr},
				FileDB:  dbClient,
				BatchDB: nil,
				Queue:   nil,
				Event:   nil,
				Status:  nil,
			}
			handler := NewFileAPIHandler(config, clients)
			req := buildCreateRequest(t)
			w := httptest.NewRecorder()
			handler.CreateFile(w, req)

			if w.Code != tc.wantStatus {
				t.Fatalf("expected status %d, got %d, body: %s", tc.wantStatus, w.Code, w.Body.String())
			}
			var errResp openai.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("failed to parse error response: %v", err)
			}
			if errResp.Error.Code != nil {
				t.Errorf("expected error code null (no explicit code), got %q", *errResp.Error.Code)
			}
			if errResp.Error.Message != tc.wantMessage {
				t.Errorf("expected message %q, got %q", tc.wantMessage, errResp.Error.Message)
			}
		})
	}
}

func doTestCreateFileSuccess(t *testing.T) {
	ctx := context.Background()
	handler := setupTestHandler(t)

	// Test file content - each line will get a newline appended by scanner during storage
	testLines := []string{
		`{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}`,
		`{"custom_id":"request-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"World"}]}}`,
	}
	fileContent := strings.Join(testLines, "\n") + "\n"

	// Calculate expected size after scanner processing (each line gets a newline appended)
	expectedBytes := int64(0)
	for _, line := range testLines {
		expectedBytes += int64(len(line)) + 1 // +1 for newline
	}

	// Create file using helper
	fileObj := createTestFile(t, handler, ctx, "test-batch.jsonl", "batch", fileContent)

	// Validate response fields
	if fileObj.Object != "file" {
		t.Errorf("expected object 'file', got '%s'", fileObj.Object)
	}
	if fileObj.Purpose != openai.FileObjectPurposeBatch {
		t.Errorf("expected purpose 'batch', got '%s'", fileObj.Purpose)
	}
	if fileObj.Status != openai.FileObjectStatusUploaded {
		t.Errorf("expected status 'uploaded', got '%s'", fileObj.Status)
	}
	if fileObj.Filename != "test-batch.jsonl" {
		t.Errorf("expected filename 'test-batch.jsonl', got '%s'", fileObj.Filename)
	}
	if !strings.HasPrefix(fileObj.ID, "file_") {
		t.Errorf("expected file ID to start with 'file_', got '%s'", fileObj.ID)
	}
	if fileObj.Bytes != expectedBytes {
		t.Errorf("expected bytes %d, got %d", expectedBytes, fileObj.Bytes)
	}
	if fileObj.CreatedAt <= 0 {
		t.Errorf("expected createdAt > 0, got %d", fileObj.CreatedAt)
	}
	if fileObj.ExpiresAt == nil || *fileObj.ExpiresAt <= 0 {
		t.Errorf("expected expiresAt > 0, got %v", fileObj.ExpiresAt)
	}

	// Verify file was stored in DB
	items, _, _, err := handler.clients.FileDB.DBGet(ctx, &dbapi.FileQuery{
		BaseQuery: dbapi.BaseQuery{IDs: []string{fileObj.ID}},
	}, true, 0, 10)
	if err != nil {
		t.Fatalf("failed to get file from DB: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item in DB, got %d", len(items))
	}
	if items[0].ID != fileObj.ID {
		t.Errorf("expected DB item ID '%s', got '%s'", fileObj.ID, items[0].ID)
	}

	// Verify file was actually uploaded to storage
	storageName := ucom.FileStorageName(fileObj.ID, fileObj.Filename)
	folderName, err := ucom.GetFolderNameByTenantID(common.DefaultTenantID)
	if err != nil {
		t.Fatalf("failed to get folder name from tenant ID: %v", err)
	}
	fileReader, fileMeta, err := handler.clients.File.Retrieve(ctx, storageName, folderName)
	if err != nil {
		t.Fatalf("failed to retrieve file from storage: %v", err)
	}
	defer fileReader.Close()

	// Read uploaded file content
	uploadedContent, err := io.ReadAll(fileReader)
	if err != nil {
		t.Fatalf("failed to read uploaded file: %v", err)
	}

	// Verify file size matches
	if fileMeta.Size != expectedBytes {
		t.Errorf("expected storage file size %d, got %d", expectedBytes, fileMeta.Size)
	}

	// Verify content matches (scanner adds newline to each line)
	expectedStoredContent := strings.Join(testLines, "\n") + "\n"
	if string(uploadedContent) != expectedStoredContent {
		t.Errorf("uploaded content doesn't match expected.\nExpected:\n%s\nGot:\n%s",
			expectedStoredContent, string(uploadedContent))
	}
}

func doTestCreateFilePurposeValidation(t *testing.T) {
	ctx := context.Background()
	handler := setupTestHandler(t)

	cases := []struct {
		name    string
		purpose string // empty string means omit the field
	}{
		{"MissingPurpose", ""},
		{"InvalidPurpose", "invalid"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := &bytes.Buffer{}
			writer := multipart.NewWriter(body)

			fileWriter, err := writer.CreateFormFile("file", tc.name+".jsonl")
			if err != nil {
				t.Fatalf("failed to create form file: %v", err)
			}
			if _, err := io.WriteString(fileWriter, `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{}}`); err != nil {
				t.Fatalf("failed to write file content: %v", err)
			}
			if tc.purpose != "" {
				if err := writer.WriteField("purpose", tc.purpose); err != nil {
					t.Fatalf("failed to write purpose field: %v", err)
				}
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("failed to close multipart writer: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/files", body)
			req.Header.Set("Content-Type", writer.FormDataContentType())
			req = req.WithContext(ctx)

			w := httptest.NewRecorder()
			handler.CreateFile(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("expected status %d, got %d, body: %s", http.StatusBadRequest, w.Code, w.Body.String())
			}
		})
	}
}

func doTestCreateFileExpiresAfter(t *testing.T) {
	ctx := context.Background()
	handler := setupTestHandler(t)
	fileContent := `{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}`

	buildRequest := func(name, anchor, seconds string) *http.Request {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		fileWriter, err := writer.CreateFormFile("file", name+".jsonl")
		if err != nil {
			t.Fatalf("failed to create form file: %v", err)
		}
		if _, err := io.WriteString(fileWriter, fileContent); err != nil {
			t.Fatalf("failed to write file content: %v", err)
		}
		if err := writer.WriteField("purpose", "batch"); err != nil {
			t.Fatalf("failed to write purpose field: %v", err)
		}
		if anchor != "" {
			if err := writer.WriteField("expires_after[anchor]", anchor); err != nil {
				t.Fatalf("failed to write anchor field: %v", err)
			}
		}
		if seconds != "" {
			if err := writer.WriteField("expires_after[seconds]", seconds); err != nil {
				t.Fatalf("failed to write seconds field: %v", err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("failed to close multipart writer: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/v1/files", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		return req.WithContext(ctx)
	}

	tests := []struct {
		name           string
		anchor         string
		seconds        string
		expectedStatus int
	}{
		{"valid min boundary", "created_at", "3600", http.StatusOK},
		{"valid max boundary", "created_at", "2592000", http.StatusOK},
		{"valid mid range", "created_at", "86400", http.StatusOK},
		{"too small", "created_at", "3599", http.StatusBadRequest},
		{"too large", "created_at", "2592001", http.StatusBadRequest},
		{"negative", "created_at", "-1", http.StatusBadRequest},
		{"zero", "created_at", "0", http.StatusBadRequest},
		{"anchor only", "created_at", "", http.StatusBadRequest},
		{"seconds only", "", "86400", http.StatusBadRequest},
		{"non-numeric seconds", "created_at", "abc", http.StatusBadRequest},
		{"invalid anchor", "updated_at", "86400", http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := buildRequest(tc.name, tc.anchor, tc.seconds)
			w := httptest.NewRecorder()
			handler.CreateFile(w, req)

			if w.Code != tc.expectedStatus {
				t.Errorf("expected status %d, got %d, body: %s", tc.expectedStatus, w.Code, w.Body.String())
			}

			if tc.expectedStatus == http.StatusOK {
				var fileObj openai.FileObject
				if err := json.Unmarshal(w.Body.Bytes(), &fileObj); err != nil {
					t.Fatalf("failed to parse response: %v", err)
				}
				if fileObj.ExpiresAt == nil || *fileObj.ExpiresAt <= fileObj.CreatedAt {
					t.Errorf("expected expiresAt > createdAt, got expiresAt=%v, createdAt=%d", fileObj.ExpiresAt, fileObj.CreatedAt)
				}
			}
		})
	}
}

func doTestListFiles(t *testing.T) {
	ctx := context.Background()
	handler := setupTestHandler(t)

	// Create multiple test files with different purposes
	testFiles := []struct {
		filename string
		purpose  string
		content  string
	}{
		{"batch-file-1.jsonl", "batch", `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{}}`},
		{"batch-file-2.jsonl", "batch", `{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{}}`},
		{"finetune-file.jsonl", "fine-tune", `{"prompt":"hello","completion":"world"}`},
	}

	createdFileIDs := []string{}

	// Create each test file using helper
	for _, tf := range testFiles {
		fileObj := createTestFile(t, handler, ctx, tf.filename, tf.purpose, tf.content)
		createdFileIDs = append(createdFileIDs, fileObj.ID)
	}

	// Test 1: List all files (no filter)
	t.Run("ListAll", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
			t.Logf("response body: %s", w.Body.String())
		}

		var listResp openai.ListFilesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
			t.Fatalf("failed to parse list response: %v", err)
		}

		if listResp.Object != "list" {
			t.Errorf("expected object 'list', got '%s'", listResp.Object)
		}

		if len(listResp.Data) != len(testFiles) {
			t.Errorf("expected %d files, got %d", len(testFiles), len(listResp.Data))
		}

		// Verify all created files are in the list
		for _, expectedID := range createdFileIDs {
			found := false
			for _, file := range listResp.Data {
				if file.ID == expectedID {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("expected file ID %s not found in list", expectedID)
			}
		}

		// Verify FirstID and LastID
		if len(listResp.Data) > 0 {
			if listResp.FirstID != listResp.Data[0].ID {
				t.Errorf("expected first_id '%s', got '%s'", listResp.Data[0].ID, listResp.FirstID)
			}
			if listResp.LastID != listResp.Data[len(listResp.Data)-1].ID {
				t.Errorf("expected last_id '%s', got '%s'", listResp.Data[len(listResp.Data)-1].ID, listResp.LastID)
			}
		}
	})

	// Test 2: Test limit parameter
	t.Run("LimitParameter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files?limit=1", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
		}

		var listResp openai.ListFilesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil {
			t.Fatalf("failed to parse list response: %v", err)
		}

		if len(listResp.Data) != 1 {
			t.Errorf("expected 1 file with limit=1, got %d", len(listResp.Data))
		}

		if listResp.HasMore != true {
			t.Errorf("expected has_more=true when limiting results, got %v", listResp.HasMore)
		}
	})

	// Test 4: Invalid purpose parameter
	t.Run("InvalidPurpose", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files?purpose=invalid", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for invalid purpose, got %d", http.StatusBadRequest, w.Code)
		}
	})

	// Test 5: Test after parameter (cursor-based pagination)
	t.Run("AfterParameter", func(t *testing.T) {
		// First request: get first 2 items
		req := httptest.NewRequest(http.MethodGet, "/v1/files?limit=2", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
		}

		var listResp1 openai.ListFilesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &listResp1); err != nil {
			t.Fatalf("failed to parse list response: %v", err)
		}

		if len(listResp1.Data) != 2 {
			t.Errorf("expected 2 files in first page, got %d", len(listResp1.Data))
		}

		if !listResp1.HasMore {
			t.Errorf("expected has_more=true for first page, got false")
		}

		// Second request: use after cursor to get remaining items
		req = httptest.NewRequest(http.MethodGet, "/v1/files?limit=2&after=2", nil)
		req = req.WithContext(ctx)

		w = httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
		}

		var listResp2 openai.ListFilesResponse
		if err := json.Unmarshal(w.Body.Bytes(), &listResp2); err != nil {
			t.Fatalf("failed to parse list response: %v", err)
		}

		// Should get remaining 1 file
		if len(listResp2.Data) != 1 {
			t.Errorf("expected 1 file in second page, got %d", len(listResp2.Data))
		}

		if listResp2.HasMore {
			t.Errorf("expected has_more=false for last page, got true")
		}

		// Verify no overlap between pages
		for _, file1 := range listResp1.Data {
			for _, file2 := range listResp2.Data {
				if file1.ID == file2.ID {
					t.Errorf("found duplicate file ID %s across pages", file1.ID)
				}
			}
		}
	})

	// Test 6: Invalid after parameter
	t.Run("InvalidAfter", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files?after=invalid", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for invalid after, got %d", http.StatusBadRequest, w.Code)
		}
	})

	// Test 7: limit=0 (below minimum)
	t.Run("LimitZero", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files?limit=0", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for limit=0, got %d", http.StatusBadRequest, w.Code)
		}
	})

	// Test 8: limit too large (above maximum)
	t.Run("LimitTooLarge", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files?limit=99999", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for limit=99999, got %d", http.StatusBadRequest, w.Code)
		}
	})

	// Test 9: Invalid limit (non-integer)
	t.Run("InvalidLimit", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files?limit=abc", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for invalid limit, got %d", http.StatusBadRequest, w.Code)
		}
	})

	// Test 9: Invalid order parameter
	t.Run("InvalidOrder", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files?order=invalid", nil)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.ListFiles(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for invalid order, got %d", http.StatusBadRequest, w.Code)
		}
	})
}

func doTestRetrieveFile(t *testing.T) {
	ctx := context.Background()
	handler := setupTestHandler(t)

	// Create a test file using helper
	testContent := `{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{}}`
	createdFile := createTestFile(t, handler, ctx, "test-retrieve.jsonl", "batch", testContent)

	// Test 1: Retrieve existing file
	t.Run("RetrieveExistingFile", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files/"+createdFile.ID, nil)
		req.SetPathValue(common.PathParamFileID, createdFile.ID)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.RetrieveFile(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
			t.Logf("response body: %s", w.Body.String())
		}

		var fileObj openai.FileObject
		if err := json.Unmarshal(w.Body.Bytes(), &fileObj); err != nil {
			t.Fatalf("failed to parse retrieve response: %v", err)
		}

		// Verify all fields match the created file
		if fileObj.ID != createdFile.ID {
			t.Errorf("expected ID '%s', got '%s'", createdFile.ID, fileObj.ID)
		}
		if fileObj.Filename != createdFile.Filename {
			t.Errorf("expected filename '%s', got '%s'", createdFile.Filename, fileObj.Filename)
		}
		if fileObj.Purpose != createdFile.Purpose {
			t.Errorf("expected purpose '%s', got '%s'", createdFile.Purpose, fileObj.Purpose)
		}
		if fileObj.Bytes != createdFile.Bytes {
			t.Errorf("expected bytes %d, got %d", createdFile.Bytes, fileObj.Bytes)
		}
		if fileObj.CreatedAt != createdFile.CreatedAt {
			t.Errorf("expected created_at %d, got %d", createdFile.CreatedAt, fileObj.CreatedAt)
		}
		if (fileObj.ExpiresAt == nil) != (createdFile.ExpiresAt == nil) || (fileObj.ExpiresAt != nil && *fileObj.ExpiresAt != *createdFile.ExpiresAt) {
			t.Errorf("expected expires_at %v, got %v", createdFile.ExpiresAt, fileObj.ExpiresAt)
		}
		if fileObj.Status != createdFile.Status {
			t.Errorf("expected status '%s', got '%s'", createdFile.Status, fileObj.Status)
		}
		if fileObj.Object != "file" {
			t.Errorf("expected object 'file', got '%s'", fileObj.Object)
		}
	})

	// Test 2: Retrieve non-existent file
	t.Run("RetrieveNonExistentFile", func(t *testing.T) {
		nonExistentID := "file_nonexistent"
		req := httptest.NewRequest(http.MethodGet, "/v1/files/"+nonExistentID, nil)
		req.SetPathValue(common.PathParamFileID, nonExistentID)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.RetrieveFile(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status %d for non-existent file, got %d", http.StatusNotFound, w.Code)
		}

		// Verify error message
		var errResp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &errResp); err != nil {
			t.Fatalf("failed to parse error response: %v", err)
		}

		if errObj, ok := errResp["error"].(map[string]interface{}); ok {
			if msg, ok := errObj["message"].(string); ok {
				expectedMsg := fmt.Sprintf("File with ID %s not found", nonExistentID)
				if msg != expectedMsg {
					t.Errorf("expected error message '%s', got '%s'", expectedMsg, msg)
				}
			}
		}
	})

	// Test 3: Missing file_id parameter
	t.Run("MissingFileID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files/", nil)
		// Don't set path value to simulate missing file_id
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.RetrieveFile(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for missing file_id, got %d", http.StatusBadRequest, w.Code)
		}
	})
}

func doTestDownloadFile(t *testing.T) {
	ctx := context.Background()
	handler := setupTestHandler(t)

	// Create a test file with specific content using helper
	testLines := []string{
		`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{}}`,
		`{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{}}`,
	}
	testContent := strings.Join(testLines, "\n") + "\n"
	createdFile := createTestFile(t, handler, ctx, "test-download.jsonl", "batch", testContent)

	// Test 1: Download existing file
	t.Run("DownloadExistingFile", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files/"+createdFile.ID+"/content", nil)
		req.SetPathValue(common.PathParamFileID, createdFile.ID)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.DownloadFile(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
			t.Logf("response body: %s", w.Body.String())
		}

		// Verify response headers
		contentType := w.Header().Get("Content-Type")
		if contentType != "application/octet-stream" {
			t.Errorf("expected Content-Type 'application/octet-stream', got '%s'", contentType)
		}

		contentDisposition := w.Header().Get("Content-Disposition")
		expectedDisposition := fmt.Sprintf("attachment; filename=%q", createdFile.Filename)
		if contentDisposition != expectedDisposition {
			t.Errorf("expected Content-Disposition '%s', got '%s'", expectedDisposition, contentDisposition)
		}

		contentLength := w.Header().Get("Content-Length")
		expectedLength := strconv.FormatInt(createdFile.Bytes, 10)
		if contentLength != expectedLength {
			t.Errorf("expected Content-Length '%s', got '%s'", expectedLength, contentLength)
		}

		// Verify downloaded content matches (scanner adds newline to each line)
		downloadedContent := w.Body.String()
		expectedContent := strings.Join(testLines, "\n") + "\n"
		if downloadedContent != expectedContent {
			t.Errorf("downloaded content doesn't match expected.\nExpected:\n%s\nGot:\n%s",
				expectedContent, downloadedContent)
		}

		// Verify content length
		if int64(len(downloadedContent)) != createdFile.Bytes {
			t.Errorf("expected downloaded size %d, got %d", createdFile.Bytes, len(downloadedContent))
		}
	})

	// Test 2: Download non-existent file
	t.Run("DownloadNonExistentFile", func(t *testing.T) {
		nonExistentID := "file_nonexistent"
		req := httptest.NewRequest(http.MethodGet, "/v1/files/"+nonExistentID+"/content", nil)
		req.SetPathValue(common.PathParamFileID, nonExistentID)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.DownloadFile(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status %d for non-existent file, got %d", http.StatusNotFound, w.Code)
		}
	})

	// Test 3: Missing file_id parameter
	t.Run("MissingFileID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/v1/files//content", nil)
		// Don't set path value to simulate missing file_id
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.DownloadFile(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for missing file_id, got %d", http.StatusBadRequest, w.Code)
		}
	})
}

func doTestDeleteFile(t *testing.T) {
	ctx := context.Background()
	handler := setupTestHandler(t)

	// Create a test file using helper
	testLines := []string{
		`{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}`,
		`{"custom_id":"request-2","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"World"}]}}`,
	}
	testContent := strings.Join(testLines, "\n")
	createdFile := createTestFile(t, handler, ctx, "test.jsonl", "batch", testContent)

	// Test 1: Delete existing file
	t.Run("DeleteExistingFile", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/files/"+createdFile.ID, nil)
		req.SetPathValue(common.PathParamFileID, createdFile.ID)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.DeleteFile(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
			t.Logf("response body: %s", w.Body.String())
		}

		// Verify response structure
		var deleteResp openai.FileDeleteResponse
		if err := json.Unmarshal(w.Body.Bytes(), &deleteResp); err != nil {
			t.Fatalf("failed to parse delete response: %v", err)
		}

		if deleteResp.ID != createdFile.ID {
			t.Errorf("expected ID %s, got %s", createdFile.ID, deleteResp.ID)
		}

		if deleteResp.Object != "file" {
			t.Errorf("expected object 'file', got '%s'", deleteResp.Object)
		}

		if deleteResp.Deleted != true {
			t.Errorf("expected deleted=true, got %v", deleteResp.Deleted)
		}

		// Verify file is actually deleted from database
		items, _, _, err := handler.clients.FileDB.DBGet(ctx, &dbapi.FileQuery{
			BaseQuery: dbapi.BaseQuery{IDs: []string{createdFile.ID}},
		}, true, 0, 1)
		if err != nil {
			t.Fatalf("failed to query database: %v", err)
		}

		if len(items) != 0 {
			t.Errorf("expected file to be deleted from database, but still exists")
		}

		// Verify physical file is deleted from storage
		storageName := ucom.FileStorageName(createdFile.ID, createdFile.Filename)
		folderName, err := ucom.GetFolderNameByTenantID(common.DefaultTenantID)
		if err != nil {
			t.Fatalf("failed to get folder name from tenant ID: %v", err)
		}
		_, _, err = handler.clients.File.Retrieve(ctx, storageName, folderName)
		if err == nil {
			t.Errorf("expected physical file to be deleted, but still exists")
		}
	})

	// Test 2: Delete non-existent file
	t.Run("DeleteNonExistentFile", func(t *testing.T) {
		nonExistentID := "file_nonexistent"
		req := httptest.NewRequest(http.MethodDelete, "/v1/files/"+nonExistentID, nil)
		req.SetPathValue(common.PathParamFileID, nonExistentID)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.DeleteFile(w, req)

		if w.Code != http.StatusNotFound {
			t.Errorf("expected status %d for non-existent file, got %d", http.StatusNotFound, w.Code)
		}
	})

	// Test 3: Missing file_id parameter
	t.Run("MissingFileID", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/v1/files/", nil)
		// Don't set path value to simulate missing file_id
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.DeleteFile(w, req)

		if w.Code != http.StatusBadRequest {
			t.Errorf("expected status %d for missing file_id, got %d", http.StatusBadRequest, w.Code)
		}
	})

	t.Run("DeletePhysicalFileErrorKeepsMetadata", func(t *testing.T) {
		handler := setupTestHandler(t)
		testContent := strings.Join([]string{
			`{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}`,
		}, "\n")
		createdFile := createTestFile(t, handler, ctx, "delete-error.jsonl", "batch", testContent)

		handler.clients.File = &errDeleteFileClient{
			BatchFilesClient: handler.clients.File,
			deleteErr:        errors.New("storage delete failed"),
		}

		req := httptest.NewRequest(http.MethodDelete, "/v1/files/"+createdFile.ID, nil)
		req.SetPathValue(common.PathParamFileID, createdFile.ID)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.DeleteFile(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected status %d, got %d, body: %s", http.StatusInternalServerError, w.Code, w.Body.String())
		}

		items, _, _, err := handler.clients.FileDB.DBGet(ctx, &dbapi.FileQuery{
			BaseQuery: dbapi.BaseQuery{IDs: []string{createdFile.ID}},
		}, true, 0, 1)
		if err != nil {
			t.Fatalf("failed to query database: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected file metadata to remain after storage delete failure, got %d items", len(items))
		}
	})

	t.Run("DeleteRetryAfterDBFailure", func(t *testing.T) {
		handler := setupTestHandler(t)
		testContent := strings.Join([]string{
			`{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}`,
		}, "\n")
		createdFile := createTestFile(t, handler, ctx, "delete-db-fail.jsonl", "batch", testContent)

		wrappedDB := &failOnceDBClient{FileDBClient: handler.clients.FileDB}
		wrappedDB.failNext.Store(true)
		handler.clients.FileDB = wrappedDB

		// First attempt: storage delete succeeds, DB delete fails → 500
		req := httptest.NewRequest(http.MethodDelete, "/v1/files/"+createdFile.ID, nil)
		req.SetPathValue(common.PathParamFileID, createdFile.ID)
		req = req.WithContext(ctx)
		w := httptest.NewRecorder()
		handler.DeleteFile(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("first attempt: expected %d, got %d", http.StatusInternalServerError, w.Code)
		}

		// Metadata should still exist after DB failure
		items, _, _, err := wrappedDB.DBGet(ctx, &dbapi.FileQuery{
			BaseQuery: dbapi.BaseQuery{IDs: []string{createdFile.ID}},
		}, true, 0, 1)
		if err != nil {
			t.Fatalf("failed to query database: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("expected metadata to survive DB failure, got %d items", len(items))
		}

		// Second attempt: the real fs client returns os.ErrNotExist because
		// the blob was already removed by the first attempt's successful
		// storage delete. The handler tolerates this and proceeds to DB delete.
		req2 := httptest.NewRequest(http.MethodDelete, "/v1/files/"+createdFile.ID, nil)
		req2.SetPathValue(common.PathParamFileID, createdFile.ID)
		req2 = req2.WithContext(ctx)
		w2 := httptest.NewRecorder()
		handler.DeleteFile(w2, req2)

		if w2.Code != http.StatusOK {
			t.Fatalf("retry: expected %d, got %d, body: %s", http.StatusOK, w2.Code, w2.Body.String())
		}

		// Both storage and metadata should be gone
		items2, _, _, err := wrappedDB.DBGet(ctx, &dbapi.FileQuery{
			BaseQuery: dbapi.BaseQuery{IDs: []string{createdFile.ID}},
		}, true, 0, 1)
		if err != nil {
			t.Fatalf("failed to query database: %v", err)
		}
		if len(items2) != 0 {
			t.Fatalf("expected metadata to be deleted on retry, got %d items", len(items2))
		}
	})

	t.Run("DeleteToleratesErrNotExist", func(t *testing.T) {
		handler := setupTestHandler(t)
		testContent := strings.Join([]string{
			`{"custom_id":"request-1","method":"POST","url":"/v1/chat/completions","body":{"model":"gpt-4","messages":[{"role":"user","content":"Hello"}]}}`,
		}, "\n")
		createdFile := createTestFile(t, handler, ctx, "delete-notexist.jsonl", "batch", testContent)

		handler.clients.File = &errDeleteFileClient{
			BatchFilesClient: handler.clients.File,
			deleteErr:        os.ErrNotExist,
		}

		req := httptest.NewRequest(http.MethodDelete, "/v1/files/"+createdFile.ID, nil)
		req.SetPathValue(common.PathParamFileID, createdFile.ID)
		req = req.WithContext(ctx)

		w := httptest.NewRecorder()
		handler.DeleteFile(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected status %d for ErrNotExist (idempotent delete), got %d, body: %s",
				http.StatusOK, w.Code, w.Body.String())
		}

		items, _, _, err := handler.clients.FileDB.DBGet(ctx, &dbapi.FileQuery{
			BaseQuery: dbapi.BaseQuery{IDs: []string{createdFile.ID}},
		}, true, 0, 1)
		if err != nil {
			t.Fatalf("failed to query database: %v", err)
		}
		if len(items) != 0 {
			t.Fatalf("expected metadata to be deleted after ErrNotExist, got %d items", len(items))
		}
	})
}

func TestCreateFileSizeBoundary(t *testing.T) {
	const limit = 1024
	for _, chunked := range []bool{false, true} {
		for _, size := range []int{limit - 1, limit, limit + 1} {
			t.Run(fmt.Sprintf("chunked=%t/size=%d", chunked, size), func(t *testing.T) {
				handler := setupTestHandler(t)
				handler.config.FileAPI.MaxSizeBytes = limit
				server := httptest.NewServer(http.HandlerFunc(handler.CreateFile))
				defer server.Close()

				var body bytes.Buffer
				writer := multipart.NewWriter(&body)
				part, err := writer.CreateFormFile("file", "input.jsonl")
				if err != nil {
					t.Fatal(err)
				}
				content := `{"input":"` + strings.Repeat("x", size-len("{\"input\":\"\"}\n")) + "\"}\n"
				if _, err := io.WriteString(part, content); err != nil {
					t.Fatal(err)
				}
				if err := writer.WriteField("purpose", "batch"); err != nil {
					t.Fatal(err)
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				req, err := http.NewRequest(http.MethodPost, server.URL+"/v1/files", &body)
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", writer.FormDataContentType())
				if chunked {
					req.ContentLength = -1
				}
				response, err := server.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				wantStatus := http.StatusOK
				if size > limit {
					wantStatus = http.StatusBadRequest
				}
				if response.StatusCode != wantStatus {
					data, _ := io.ReadAll(response.Body)
					t.Fatalf("status = %d, want %d: %s", response.StatusCode, wantStatus, data)
				}
				if wantStatus == http.StatusOK {
					var file openai.FileObject
					if err := json.NewDecoder(response.Body).Decode(&file); err != nil {
						t.Fatal(err)
					}
					if file.Bytes != int64(size) {
						t.Errorf("file bytes = %d, want %d", file.Bytes, size)
					}
				}
			})
		}
	}
}

type countedUploadReader struct {
	io.Reader
	bytesRead int64
}

func (r *countedUploadReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.bytesRead += int64(n)
	return n, err
}

func TestCreateFileBoundsRequestBody(t *testing.T) {
	const limit = 1024
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%t", chunked), func(t *testing.T) {
			handler := setupTestHandler(t)
			handler.config.FileAPI.MaxSizeBytes = limit
			var body bytes.Buffer
			writer := multipart.NewWriter(&body)
			part, err := writer.CreateFormFile("file", "large.jsonl")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := io.WriteString(part, strings.Repeat("x", 2*maxUploadFormOverhead)); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			reader := &countedUploadReader{Reader: &body}
			req := httptest.NewRequest(http.MethodPost, "/v1/files", reader)
			req.Header.Set("Content-Type", writer.FormDataContentType())
			req.ContentLength = int64(body.Len())
			if chunked {
				req.ContentLength = -1
			}
			response := httptest.NewRecorder()
			handler.CreateFile(response, req)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", response.Code, response.Body)
			}
			if !chunked && reader.bytesRead != 0 {
				t.Errorf("read %d bytes from oversized known-length request", reader.bytesRead)
			}
			if reader.bytesRead > limit+maxUploadFormOverhead+1 {
				t.Errorf("read %d bytes, exceeding request bound", reader.bytesRead)
			}
		})
	}
}
