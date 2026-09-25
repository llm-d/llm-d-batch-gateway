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
	"strings"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batchinput"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

func batchLine(customID, model, systemPrompt string) string {
	if systemPrompt == "" {
		return fmt.Sprintf(
			`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"hi"}]}}`,
			customID, model)
	}
	return fmt.Sprintf(
		`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"system","content":%q},{"role":"user","content":"hi"}]}}`,
		customID, model, systemPrompt)
}

// asMultipartFile turns content into the multipart.File that CreateFile works
// with, so tests exercise the same seekable-ReaderAt shape as a real upload.
func asMultipartFile(t *testing.T, content string) (multipart.File, int64) {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	fw, err := writer.CreateFormFile("file", "input.jsonl")
	if err != nil {
		t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := io.WriteString(fw, content); err != nil {
		t.Fatalf("write content: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	f, hdr, err := req.FormFile("file")
	if err != nil {
		t.Fatalf("FormFile: %v", err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f, hdr.Size
}

func mustPolicy(t *testing.T, id batchinput.PolicyID) batchinput.OrderPolicy {
	t.Helper()
	p, ok := batchinput.Lookup(id)
	if !ok {
		t.Fatalf("policy %q not registered", id)
	}
	return p
}

func TestPrepareBatchInput(t *testing.T) {
	t.Run("stores requests grouped by model and prefix", func(t *testing.T) {
		content := strings.Join([]string{
			batchLine("b-shared-1", "model-b", "common preamble"),
			batchLine("a-plain", "model-a", ""),
			batchLine("b-shared-2", "model-b", "common preamble"),
			batchLine("a-prompted", "model-a", "another preamble"),
		}, "\n") + "\n"

		src, size := asMultipartFile(t, content)
		prepared, err := prepareBatchInput(src, size, 1000, 1<<20, mustPolicy(t, batchinput.PolicyModelPrefixV1))
		if err != nil {
			t.Fatalf("prepareBatchInput: %v", err)
		}

		stored := readAll(t, prepared.reader)
		got := storedCustomIDs(t, stored)
		want := []string{"a-prompted", "a-plain", "b-shared-1", "b-shared-2"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("stored order = %v, want %v", got, want)
		}

		if prepared.meta.Policy != batchinput.PolicyModelPrefixV1 {
			t.Fatalf("Policy = %q", prepared.meta.Policy)
		}
		if prepared.meta.LineCount != 4 {
			t.Fatalf("LineCount = %d, want 4", prepared.meta.LineCount)
		}
		if prepared.meta.Invalid != nil {
			t.Fatalf("unexpected validation error: %v", prepared.meta.Invalid)
		}
		if strings.Join(prepared.meta.Models, ",") != "model-a,model-b" {
			t.Fatalf("Models = %v", prepared.meta.Models)
		}
	})

	t.Run("stores an invalid input verbatim and records why", func(t *testing.T) {
		content := strings.Join([]string{
			batchLine("a", "m1", ""),
			batchLine("a", "m1", ""), // duplicate custom_id
			batchLine("c", "m1", ""),
		}, "\n") + "\n"

		src, size := asMultipartFile(t, content)
		prepared, err := prepareBatchInput(src, size, 1000, 1<<20, mustPolicy(t, batchinput.PolicyModelPrefixV1))
		if err != nil {
			t.Fatalf("prepareBatchInput: %v", err)
		}

		if prepared.meta.Invalid == nil {
			t.Fatal("expected the duplicate custom_id to be recorded")
		}
		if prepared.meta.Invalid.Line != 2 {
			t.Fatalf("Invalid.Line = %d, want 2", prepared.meta.Invalid.Line)
		}
		if got := readAll(t, prepared.reader); got != content {
			t.Fatalf("an invalid input must be stored byte-for-byte\ngot:  %q\nwant: %q", got, content)
		}
		if prepared.meta.Policy != batchinput.PolicyOriginalV1 {
			t.Fatalf("a verbatim store must be recorded as original order, got %q", prepared.meta.Policy)
		}
	})

	t.Run("honours the configured policy", func(t *testing.T) {
		content := strings.Join([]string{
			batchLine("z", "model-z", ""),
			batchLine("a", "model-a", ""),
		}, "\n") + "\n"

		src, size := asMultipartFile(t, content)
		prepared, err := prepareBatchInput(src, size, 1000, 1<<20, mustPolicy(t, batchinput.PolicyOriginalV1))
		if err != nil {
			t.Fatalf("prepareBatchInput: %v", err)
		}
		if got := readAll(t, prepared.reader); got != content {
			t.Fatal("original-v1 must not reorder")
		}
		if prepared.meta.Policy != batchinput.PolicyOriginalV1 {
			t.Fatalf("Policy = %q, want the configured policy", prepared.meta.Policy)
		}
	})

	t.Run("enforces the line budget", func(t *testing.T) {
		content := strings.Join([]string{
			batchLine("a", "m1", ""), batchLine("b", "m1", ""), batchLine("c", "m1", ""),
		}, "\n") + "\n"
		src, size := asMultipartFile(t, content)
		_, err := prepareBatchInput(src, size, 2, 1<<20, mustPolicy(t, batchinput.PolicyModelPrefixV1))
		if !errors.Is(err, batchinput.ErrTooManyLines) {
			t.Fatalf("err = %v, want ErrTooManyLines", err)
		}
	})

	t.Run("allows the newline added to a reordered final line", func(t *testing.T) {
		// "zzz" sorts last, so the unterminated first line moves and needs a
		// newline. The client stayed within the limit, so we must not reject it.
		content := batchLine("a", "zzz", "") + "\n" + batchLine("b", "m1", "")
		src, size := asMultipartFile(t, content)

		prepared, err := prepareBatchInput(src, size, 1000, size, mustPolicy(t, batchinput.PolicyModelPrefixV1))
		if err != nil {
			t.Fatalf("prepareBatchInput: %v", err)
		}
		if prepared.sizeLimit <= size {
			t.Fatalf("sizeLimit = %d, want room for the added newline beyond %d", prepared.sizeLimit, size)
		}
		if got := int64(len(readAll(t, prepared.reader))); got != size+1 {
			t.Fatalf("stored %d bytes, want %d", got, size+1)
		}
	})

	t.Run("requires a policy", func(t *testing.T) {
		src, size := asMultipartFile(t, batchLine("a", "m1", "")+"\n")
		if _, err := prepareBatchInput(src, size, 1000, 1<<20, nil); err == nil {
			t.Fatal("expected a missing policy to be rejected")
		}
	})
}

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read prepared input: %v", err)
	}
	return string(data)
}

func storedCustomIDs(t *testing.T, content string) []string {
	t.Helper()
	var ids []string
	for _, l := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
		if l == "" {
			continue
		}
		meta, err := batchinput.ParseLine([]byte(l))
		if err != nil {
			t.Fatalf("stored line is not a valid request: %v (%q)", err, l)
		}
		ids = append(ids, meta.CustomID)
	}
	return ids
}

// TestCreateFileBatchInput covers the handler end of the upload path: what
// lands in object storage and what is recorded on the file row.
func TestCreateFileBatchInput(t *testing.T) {
	ctx := context.Background()

	readStored := func(t *testing.T, h *FileAPIHandler, obj openai.FileObject) string {
		t.Helper()
		folder, err := ucom.GetFolderNameByTenantID(common.DefaultTenantID)
		if err != nil {
			t.Fatalf("folder name: %v", err)
		}
		rc, _, err := h.clients.File.Retrieve(ctx, ucom.FileStorageName(obj.ID, obj.Filename), folder)
		if err != nil {
			t.Fatalf("retrieve stored object: %v", err)
		}
		defer rc.Close()
		data, err := io.ReadAll(rc)
		if err != nil {
			t.Fatalf("read stored object: %v", err)
		}
		return string(data)
	}

	fileMetadata := func(t *testing.T, h *FileAPIHandler, id string) *batchinput.Metadata {
		t.Helper()
		items, _, _, err := h.clients.FileDB.DBGet(ctx,
			&dbapi.FileQuery{BaseQuery: dbapi.BaseQuery{IDs: []string{id}}}, true, 0, 1)
		if err != nil {
			t.Fatalf("DBGet: %v", err)
		}
		if len(items) == 0 {
			t.Fatalf("file %s not found", id)
		}
		meta, err := batchinput.DecodeMetadata(items[0].Tags)
		if err != nil {
			t.Fatalf("DecodeMetadata: %v", err)
		}
		return meta
	}

	t.Run("batch upload is stored in dispatch order with metadata", func(t *testing.T) {
		h := setupTestHandler(t)
		content := strings.Join([]string{
			batchLine("second-model", "m2", ""),
			batchLine("first-model-a", "m1", "shared"),
			batchLine("first-model-b", "m1", "shared"),
		}, "\n") + "\n"

		obj := createTestFile(t, h, ctx, "input.jsonl", "batch", content)

		got := storedCustomIDs(t, readStored(t, h, obj))
		want := []string{"first-model-a", "first-model-b", "second-model"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("stored order = %v, want %v", got, want)
		}

		meta := fileMetadata(t, h, obj.ID)
		if meta == nil {
			t.Fatal("batch uploads must record input metadata")
		}
		if !meta.Readable() {
			t.Fatalf("metadata written by this binary must be readable: %+v", meta)
		}
		if meta.LineCount != 3 {
			t.Fatalf("LineCount = %d, want 3", meta.LineCount)
		}
		if meta.Bytes != obj.Bytes {
			t.Fatalf("metadata Bytes = %d but the file object reports %d", meta.Bytes, obj.Bytes)
		}
		if meta.Bytes != int64(len(readStored(t, h, obj))) {
			t.Fatal("metadata Bytes must describe the stored object")
		}
	})

	t.Run("reported size describes the stored object", func(t *testing.T) {
		h := setupTestHandler(t)
		content := strings.Join([]string{
			batchLine("a", "m1", ""), batchLine("b", "m1", ""),
		}, "\n") + "\n"
		obj := createTestFile(t, h, ctx, "input.jsonl", "batch", content)
		if obj.Bytes != int64(len(content)) {
			t.Fatalf("Bytes = %d, want %d", obj.Bytes, len(content))
		}
	})

	t.Run("an upload the ordering does not move is stored unchanged", func(t *testing.T) {
		// Same model, same prompt, no trailing newline: the policy has nothing
		// to reorder, so download must return the upload byte-for-byte and the
		// reported size must match it. Clients compare both.
		h := setupTestHandler(t)
		content := strings.Join([]string{
			batchLine("a", "m1", "shared"), batchLine("b", "m1", "shared"),
		}, "\n")

		obj := createTestFile(t, h, ctx, "input.jsonl", "batch", content)

		if got := readStored(t, h, obj); got != content {
			t.Fatalf("stored object differs from the upload\ngot:  %q\nwant: %q", got, content)
		}
		if obj.Bytes != int64(len(content)) {
			t.Fatalf("Bytes = %d, want %d", obj.Bytes, len(content))
		}
	})

	t.Run("invalid batch upload succeeds and records the failure", func(t *testing.T) {
		h := setupTestHandler(t)
		content := batchLine("dup", "m1", "") + "\n" + batchLine("dup", "m1", "") + "\n"

		obj := createTestFile(t, h, ctx, "input.jsonl", "batch", content)

		if got := readStored(t, h, obj); got != content {
			t.Fatal("an invalid batch input must be stored verbatim")
		}
		meta := fileMetadata(t, h, obj.ID)
		if meta == nil || meta.Invalid == nil {
			t.Fatalf("expected a recorded validation failure, got %+v", meta)
		}
		if !strings.Contains(meta.Invalid.Message, "duplicate custom_id") {
			t.Fatalf("unexpected message %q", meta.Invalid.Message)
		}
	})

	t.Run("non-batch uploads are untouched and carry no metadata", func(t *testing.T) {
		h := setupTestHandler(t)
		content := "not jsonl at all\njust text\n"

		obj := createTestFile(t, h, ctx, "notes.txt", "user_data", content)

		if got := readStored(t, h, obj); got != content {
			t.Fatalf("non-batch content must be stored verbatim, got %q", got)
		}
		if meta := fileMetadata(t, h, obj.ID); meta != nil {
			t.Fatalf("non-batch uploads must not record input metadata, got %+v", meta)
		}
	})

	t.Run("line limit is reported as a 400", func(t *testing.T) {
		h := setupTestHandler(t)
		h.config.FileAPI.MaxLineCount = 1
		content := batchLine("a", "m1", "") + "\n" + batchLine("b", "m1", "") + "\n"

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		fw, _ := writer.CreateFormFile("file", "input.jsonl")
		if _, err := io.WriteString(fw, content); err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteField("purpose", "batch"); err != nil {
			t.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		req := httptest.NewRequest(http.MethodPost, "/v1/files", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		w := httptest.NewRecorder()
		h.CreateFile(w, req.WithContext(ctx))

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400 (body: %s)", w.Code, w.Body.String())
		}
		var resp openai.ErrorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("parse error response: %v", err)
		}
		if !strings.Contains(resp.Error.Message, "line count") {
			t.Fatalf("unexpected error message %q", resp.Error.Message)
		}
	})
}
