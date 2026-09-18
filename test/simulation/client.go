//go:build simulation

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

package simulation

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

const (
	tenantHeader = "X-MaaS-Username"
	tenantID     = "sim-tenant"
)

// apiClient is a thin OpenAI-compatible client for driving scenarios. Errors
// are returned, not asserted, because several scenarios expect requests to
// fail mid-flight.
type apiClient struct {
	base string
	http *http.Client
}

func newAPIClient() *apiClient {
	return &apiClient{base: apiBase, http: newHTTPClient(30 * time.Second)}
}

// withTimeout returns a copy of the client with a different request timeout.
func (c *apiClient) withTimeout(d time.Duration) *apiClient {
	return &apiClient{base: c.base, http: newHTTPClient(d)}
}

// newHTTPClient builds a client for the active backend; the dev-deploy
// apiserver terminates TLS with a self-signed certificate.
func newHTTPClient(timeout time.Duration) *http.Client {
	client := &http.Client{Timeout: timeout}
	if strings.HasPrefix(apiBase, "https://") {
		client.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	}
	return client
}

func (c *apiClient) do(method, path string, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(method, c.base+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set(tenantHeader, tenantID)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	return c.http.Do(req)
}

func decodeInto[T any](resp *http.Response) (T, error) {
	var out T
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return out, err
	}
	if resp.StatusCode != http.StatusOK {
		return out, fmt.Errorf("status %d: %s", resp.StatusCode, data)
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return out, fmt.Errorf("decode response: %w (body: %s)", err, data)
	}
	return out, nil
}

// uploadFile uploads JSONL content with purpose=batch and returns the file ID.
func (c *apiClient) uploadFile(name, content string) (string, error) {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if err := mw.WriteField("purpose", "batch"); err != nil {
		return "", err
	}
	fw, err := mw.CreateFormFile("file", name)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(fw, strings.NewReader(content)); err != nil {
		return "", err
	}
	if err := mw.Close(); err != nil {
		return "", err
	}
	resp, err := c.do(http.MethodPost, "/v1/files", mw.FormDataContentType(), &buf)
	if err != nil {
		return "", err
	}
	file, err := decodeInto[openai.FileObject](resp)
	if err != nil {
		return "", err
	}
	return file.ID, nil
}

// createBatch submits a batch. The error is returned raw so scenarios can
// assert on mid-request failures (connection reset when a failpoint kills the
// apiserver).
func (c *apiClient) createBatch(inputFileID, completionWindow string) (openai.Batch, error) {
	payload := fmt.Sprintf(
		`{"input_file_id":%q,"endpoint":"/v1/chat/completions","completion_window":%q}`,
		inputFileID, completionWindow)
	resp, err := c.do(http.MethodPost, "/v1/batches", "application/json", strings.NewReader(payload))
	if err != nil {
		return openai.Batch{}, err
	}
	return decodeInto[openai.Batch](resp)
}

func (c *apiClient) getBatch(id string) (openai.Batch, error) {
	resp, err := c.do(http.MethodGet, "/v1/batches/"+id, "", nil)
	if err != nil {
		return openai.Batch{}, err
	}
	return decodeInto[openai.Batch](resp)
}

func (c *apiClient) listBatches() ([]openai.Batch, error) {
	resp, err := c.do(http.MethodGet, "/v1/batches", "", nil)
	if err != nil {
		return nil, err
	}
	page, err := decodeInto[openai.ListBatchResponse](resp)
	if err != nil {
		return nil, err
	}
	return page.Data, nil
}

func (c *apiClient) cancelBatch(id string) (openai.Batch, error) {
	resp, err := c.do(http.MethodPost, "/v1/batches/"+id+"/cancel", "", nil)
	if err != nil {
		return openai.Batch{}, err
	}
	return decodeInto[openai.Batch](resp)
}

func (c *apiClient) listFiles() ([]openai.FileObject, error) {
	resp, err := c.do(http.MethodGet, "/v1/files", "", nil)
	if err != nil {
		return nil, err
	}
	page, err := decodeInto[openai.ListFilesResponse](resp)
	if err != nil {
		return nil, err
	}
	return page.Data, nil
}
