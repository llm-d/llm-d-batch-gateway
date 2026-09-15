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
	"context"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/batch"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/file"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/middleware"
	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	dbmock "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	fsclient "github.com/llm-d/llm-d-batch-gateway/internal/files_store/fs"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
)

const (
	testTenantID     = "test-tenant"
	testTenantHeader = "X-Test-Tenant"
)

type testServer struct {
	URL    string
	Client *http.Client

	batchDB *dbmock.MockDBClient[dbapi.BatchItem, dbapi.BatchQuery]
	fileDB  *dbmock.MockDBClient[dbapi.FileItem, dbapi.FileQuery]
	queue   *dbmock.MockBatchPriorityQueueClient
	event   *dbmock.MockBatchEventChannelClient
}

func newTestServer(t *testing.T) *testServer {
	t.Helper()

	batchDB := dbmock.NewMockDBClient[dbapi.BatchItem, dbapi.BatchQuery](
		func(b *dbapi.BatchItem) string { return b.ID },
		func(q *dbapi.BatchQuery) *dbapi.BaseQuery { return &q.BaseQuery },
	)
	fileDB := dbmock.NewMockDBClient[dbapi.FileItem, dbapi.FileQuery](
		func(f *dbapi.FileItem) string { return f.ID },
		func(q *dbapi.FileQuery) *dbapi.BaseQuery { return &q.BaseQuery },
	)
	queue := dbmock.NewMockBatchPriorityQueueClient()
	event := dbmock.NewMockBatchEventChannelClient()

	// Mirror Postgres PQDelete: atomically transition cancelled jobs in the DB.
	queue.OnDelete = func(ctx context.Context, id string) error {
		items, _, _, err := batchDB.DBGet(ctx,
			&dbapi.BatchQuery{BaseQuery: dbapi.BaseQuery{IDs: []string{id}}},
			true, 0, 1)
		if err != nil || len(items) == 0 {
			return err
		}
		item := items[0]
		var statusInfo map[string]any
		if err := json.Unmarshal(item.Status, &statusInfo); err != nil {
			return err
		}
		statusInfo["status"] = "cancelled"
		statusInfo["cancelled_at"] = time.Now().UTC().Unix()
		newStatus, err := json.Marshal(statusInfo)
		if err != nil {
			return err
		}
		item.Status = newStatus
		return batchDB.DBUpdate(ctx, item, nil)
	}

	filesClient, err := fsclient.New(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create fs client: %v", err)
	}

	clients := &clientset.Clientset{
		File:    filesClient,
		BatchDB: batchDB,
		FileDB:  fileDB,
		Queue:   queue,
		Event:   event,
	}

	config := &common.ServerConfig{
		InputHeaders: map[string]string{
			common.InputHeaderKeyTenant: testTenantHeader,
		},
		FileAPI: common.FileAPIConfig{
			MaxSizeBytes:             common.DefaultMaxFileSizeBytes,
			MaxLineCount:             common.DefaultMaxFileLineCount,
			DefaultExpirationSeconds: 30 * 24 * 60 * 60,
		},
		BatchAPI: common.BatchAPIConfig{
			BatchEventTTLSeconds: 30 * 24 * 60 * 60,
		},
	}

	mux := http.NewServeMux()
	fileHandler := file.NewFileAPIHandler(config, clients)
	batchHandler := batch.NewBatchAPIHandler(config, clients)
	middlewares := []common.RouteMiddleware{
		middleware.Recovery,
		middleware.NewRequestMiddleware(config),
		middleware.SecurityHeaders,
	}
	for _, h := range []common.ApiHandler{fileHandler, batchHandler} {
		common.RegisterHandler(mux, h, middlewares...)
	}
	common.RegisterNotFoundHandler(mux, middlewares...)

	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		_ = filesClient.Close()
	})

	return &testServer{
		URL:     srv.URL,
		Client:  srv.Client(),
		batchDB: batchDB,
		fileDB:  fileDB,
		queue:   queue,
		event:   event,
	}
}

func (ts *testServer) doJSONAs(t *testing.T, method, path string, body any, tenantID string) *http.Response {
	t.Helper()

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("failed to marshal request body: %v", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	if err != nil {
		t.Fatalf("failed to create request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(testTenantHeader, tenantID)

	resp, err := ts.Client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	return resp
}

func (ts *testServer) doJSON(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()
	return ts.doJSONAs(t, method, path, body, testTenantID)
}

func (ts *testServer) doRequestAs(t *testing.T, method, path, tenantID string) *http.Response {
	t.Helper()
	return ts.doJSONAs(t, method, path, nil, tenantID)
}

func (ts *testServer) doRequest(t *testing.T, method, path string) *http.Response {
	t.Helper()
	return ts.doJSONAs(t, method, path, nil, testTenantID)
}

func (ts *testServer) uploadFileAs(t *testing.T, filename, purpose, content, tenantID string) *http.Response {
	t.Helper()

	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	fw, err := w.CreateFormFile("file", filename)
	if err != nil {
		t.Fatalf("failed to create form file: %v", err)
	}
	if _, err := io.WriteString(fw, content); err != nil {
		t.Fatalf("failed to write file content: %v", err)
	}

	if err := w.WriteField("purpose", purpose); err != nil {
		t.Fatalf("failed to write purpose field: %v", err)
	}
	w.Close()

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/v1/files", &buf)
	if err != nil {
		t.Fatalf("failed to create upload request: %v", err)
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	req.Header.Set(testTenantHeader, tenantID)

	resp, err := ts.Client.Do(req)
	if err != nil {
		t.Fatalf("upload request failed: %v", err)
	}
	return resp
}

func (ts *testServer) uploadFile(t *testing.T, filename, purpose, content string) *http.Response {
	t.Helper()
	return ts.uploadFileAs(t, filename, purpose, content, testTenantID)
}

func decodeJSON[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()

	var result T
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response (status %d): %v", resp.StatusCode, err)
	}
	return result
}

func readBody(t *testing.T, resp *http.Response) []byte {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read response body: %v", err)
	}
	return data
}
