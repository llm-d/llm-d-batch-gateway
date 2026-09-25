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

package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batchinput"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// countingFilesClient records how the input object was accessed, so tests can
// assert that the ordered path never downloads the whole file.
type countingFilesClient struct {
	filesapi.BatchFilesClient
	retrieves atomic.Int64
	ranges    atomic.Int64
}

func (c *countingFilesClient) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	c.retrieves.Add(1)
	return c.BatchFilesClient.Retrieve(ctx, fileName, folderName)
}

func (c *countingFilesClient) RetrieveRange(ctx context.Context, fileName, folderName string, offset, length int64) (io.ReadCloser, error) {
	c.ranges.Add(1)
	return c.BatchFilesClient.RetrieveRange(ctx, fileName, folderName, offset, length)
}

// orderedInputFixture is a processor with one stored batch input whose tags
// carry the metadata under test.
type orderedInputFixture struct {
	p       *Processor
	jobInfo *batch_types.JobInfo
	files   *countingFilesClient
	content string
}

func newOrderedInputFixture(t *testing.T, content string, meta *batchinput.Metadata, async bool) *orderedInputFixture {
	t.Helper()

	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	files := &countingFilesClient{BatchFilesClient: mockfiles.NewMockBatchFilesClient(t.TempDir())}
	fileDB := newMockFileDBClient()

	const (
		jobID       = "job-ordered"
		tenantID    = "tenant-1"
		inputFileID = "file-ordered-input"
		filename    = "input.jsonl"
	)

	ctx := context.Background()
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	if _, err := files.Store(ctx,
		ucom.FileStorageName(inputFileID, filename), folder, 0, 0, strings.NewReader(content)); err != nil {
		t.Fatalf("store input: %v", err)
	}

	tags := db.Tags{}
	if meta != nil {
		encoded, err := meta.Encode()
		if err != nil {
			t.Fatalf("encode metadata: %v", err)
		}
		tags[batchinput.TagKey] = encoded
	}
	if err := fileDB.DBStore(ctx, &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID, Tags: tags},
		BaseContents: db.BaseContents{Spec: mustJSON(t, &openai.FileObject{Filename: filename})},
	}); err != nil {
		t.Fatalf("store file record: %v", err)
	}

	clients := &clientset.Clientset{
		BatchDB:   newMockBatchDBClient(),
		FileDB:    fileDB,
		File:      files,
		Inference: inference.NewSingleClientResolver(&fakeInferenceClient{}),
	}
	p := mustNewProcessor(t, cfg, clients)
	if async {
		// The ordered path is wired for async dispatch only; a resolver with
		// no models is enough to select it.
		p.asyncInference = inference.NewTestAsyncResolver(map[string]func() inference.AsyncInferenceClient{})
	}

	return &orderedInputFixture{
		p:     p,
		files: files,
		jobInfo: &batch_types.JobInfo{
			JobID:    jobID,
			TenantID: tenantID,
			BatchJob: &openai.Batch{
				ID:        jobID,
				BatchSpec: openai.BatchSpec{InputFileID: inputFileID},
			},
		},
		content: content,
	}
}

func (f *orderedInputFixture) manifest(t *testing.T) *modelMapFile {
	t.Helper()
	root, err := f.p.jobRootDir(f.jobInfo.JobID, f.jobInfo.TenantID)
	if err != nil {
		t.Fatalf("jobRootDir: %v", err)
	}
	mm, err := readModelMap(root)
	if err != nil {
		t.Fatalf("readModelMap: %v", err)
	}
	return mm
}

func orderedContent(t *testing.T, requests ...batch_types.Request) string {
	t.Helper()
	var buf bytes.Buffer
	for _, r := range requests {
		buf.Write(mustJSON(t, r))
		buf.WriteByte('\n')
	}
	return buf.String()
}

func chatRequest(customID, model string) batch_types.Request {
	return batch_types.Request{
		CustomID: customID,
		Method:   "POST",
		URL:      "/v1/chat/completions",
		Body: map[string]any{
			"model":    model,
			"messages": []any{map[string]any{"role": "user", "content": "hi"}},
		},
	}
}

func currentMetadata(content string, models ...string) *batchinput.Metadata {
	return &batchinput.Metadata{
		Version:   batchinput.MetadataVersion,
		Policy:    batchinput.PolicyModelPrefixV1,
		LineCount: int64(strings.Count(content, "\n")),
		Bytes:     int64(len(content)),
		Models:    models,
	}
}

func TestPreProcessOrderedInput(t *testing.T) {
	ctx := testLoggerCtx(t)

	t.Run("skips the download entirely", func(t *testing.T) {
		content := orderedContent(t, chatRequest("a", "m1"), chatRequest("b", "m1"))
		f := newOrderedInputFixture(t, content, currentMetadata(content, "m1"), true)

		if err := f.p.preProcessJob(ctx, f.jobInfo); err != nil {
			t.Fatalf("preProcessJob: %v", err)
		}

		if got := f.files.retrieves.Load(); got != 0 {
			t.Fatalf("input was downloaded %d times; ordered ingestion must not read it", got)
		}
		if got := f.files.ranges.Load(); got != 0 {
			t.Fatalf("ordered ingestion issued %d ranged reads, want 0", got)
		}

		mm := f.manifest(t)
		if !mm.sequentialInput() {
			t.Fatalf("manifest must select the sequential source, got mode %q", mm.InputMode)
		}
		if mm.LineCount != 2 {
			t.Fatalf("LineCount = %d, want 2", mm.LineCount)
		}
		if mm.InputBytes != int64(len(content)) {
			t.Fatalf("InputBytes = %d, want %d", mm.InputBytes, len(content))
		}
		if mm.InputPolicy != batchinput.PolicyModelPrefixV1 {
			t.Fatalf("InputPolicy = %q", mm.InputPolicy)
		}

		// Execution appends to error.jsonl, so ingestion must have reset it.
		errPath, err := f.p.jobErrorFilePath(f.jobInfo.JobID, f.jobInfo.TenantID)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(errPath); err != nil {
			t.Fatalf("error file was not created: %v", err)
		}
	})

	t.Run("no plan files are built", func(t *testing.T) {
		content := orderedContent(t, chatRequest("a", "m1"))
		f := newOrderedInputFixture(t, content, currentMetadata(content, "m1"), true)

		if err := f.p.preProcessJob(ctx, f.jobInfo); err != nil {
			t.Fatalf("preProcessJob: %v", err)
		}

		plansDir, err := f.p.jobPlansDir(f.jobInfo.JobID, f.jobInfo.TenantID)
		if err != nil {
			t.Fatal(err)
		}
		if entries, err := os.ReadDir(plansDir); err == nil && len(entries) > 0 {
			t.Fatalf("ordered ingestion wrote %d plan files", len(entries))
		}
	})

	t.Run("stale error lines from a previous attempt are cleared", func(t *testing.T) {
		content := orderedContent(t, chatRequest("a", "m1"))
		f := newOrderedInputFixture(t, content, currentMetadata(content, "m1"), true)

		root, err := f.p.jobRootDir(f.jobInfo.JobID, f.jobInfo.TenantID)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(root, 0o700); err != nil {
			t.Fatal(err)
		}
		errPath := filepath.Join(root, errorFileName)
		if err := os.WriteFile(errPath, []byte(`{"stale":true}`+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}

		if err := f.p.preProcessJob(ctx, f.jobInfo); err != nil {
			t.Fatalf("preProcessJob: %v", err)
		}

		data, err := os.ReadFile(errPath)
		if err != nil {
			t.Fatal(err)
		}
		if len(data) != 0 {
			t.Fatalf("error file still holds %q from the previous attempt", data)
		}
	})

	t.Run("a recorded validation failure fails the batch without reading the input", func(t *testing.T) {
		content := orderedContent(t, chatRequest("dup", "m1"), chatRequest("dup", "m1"))
		meta := currentMetadata(content, "m1")
		meta.Invalid = &batchinput.ValidationError{Line: 2, Message: `duplicate custom_id "dup"`}
		f := newOrderedInputFixture(t, content, meta, true)

		err := f.p.preProcessJob(ctx, f.jobInfo)
		if err == nil {
			t.Fatal("expected the recorded validation failure to fail the batch")
		}
		if !strings.Contains(err.Error(), "duplicate custom_id") {
			t.Fatalf("error must explain the failure, got %v", err)
		}
		if got := f.files.retrieves.Load() + f.files.ranges.Load(); got != 0 {
			t.Fatalf("a known-invalid input must not be read, saw %d reads", got)
		}
	})
}

func TestPreProcessFallsBackWhenLayoutIsUnknown(t *testing.T) {
	ctx := testLoggerCtx(t)

	// A layout this build cannot reason about must never be guessed at: the
	// processor has to re-derive everything from the object itself.
	cases := map[string]*batchinput.Metadata{
		"no metadata at all": nil,
		"policy invented by a newer apiserver": {
			Version: batchinput.MetadataVersion,
			Policy:  "some-future-policy-v9",
			Bytes:   1,
		},
		"metadata encoded by a newer version": {
			Version: batchinput.MetadataVersion + 1,
			Policy:  batchinput.PolicyModelPrefixV1,
			Bytes:   1,
		},
	}

	for name, meta := range cases {
		t.Run(name, func(t *testing.T) {
			content := orderedContent(t, chatRequest("a", "m1"), chatRequest("b", "m1"))
			if meta != nil {
				meta.LineCount = 2
				meta.Bytes = int64(len(content))
			}
			f := newOrderedInputFixture(t, content, meta, true)

			if err := f.p.preProcessJob(ctx, f.jobInfo); err != nil {
				t.Fatalf("preProcessJob: %v", err)
			}

			if got := f.files.retrieves.Load(); got == 0 {
				t.Fatal("the fallback path must read the object to learn its layout")
			}

			mm := f.manifest(t)
			if mm.sequentialInput() {
				t.Fatal("an unknown layout must not be consumed as if it were ordered")
			}
			if mm.LineCount != 2 {
				t.Fatalf("LineCount = %d, want 2", mm.LineCount)
			}

			// Plan files carry the offsets the fallback path needs.
			plansDir, err := f.p.jobPlansDir(f.jobInfo.JobID, f.jobInfo.TenantID)
			if err != nil {
				t.Fatal(err)
			}
			entries, err := os.ReadDir(plansDir)
			if err != nil || len(entries) == 0 {
				t.Fatalf("fallback ingestion must build plan files (err=%v)", err)
			}
		})
	}
}

func TestPreProcessCorruptMetadataFallsBack(t *testing.T) {
	ctx := testLoggerCtx(t)
	content := orderedContent(t, chatRequest("a", "m1"))

	f := newOrderedInputFixture(t, content, nil, true)
	// Overwrite the tag with something undecodable.
	items, _, _, err := f.p.files.db.DBGet(ctx,
		&db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{f.jobInfo.BatchJob.InputFileID}}}, true, 0, 1)
	if err != nil || len(items) == 0 {
		t.Fatalf("load file record: %v", err)
	}
	items[0].Tags = db.Tags{batchinput.TagKey: "{ not json"}
	if err := f.p.files.db.DBStore(ctx, items[0]); err != nil {
		t.Fatalf("update file record: %v", err)
	}

	if err := f.p.preProcessJob(ctx, f.jobInfo); err != nil {
		t.Fatalf("corrupt metadata must fall back, not fail the job: %v", err)
	}
	if f.manifest(t).sequentialInput() {
		t.Fatal("corrupt metadata must not select the sequential source")
	}
}

func TestPreProcessSyncDispatchKeepsPlanFiles(t *testing.T) {
	ctx := testLoggerCtx(t)
	content := orderedContent(t, chatRequest("a", "m1"))

	// async=false: sync dispatch still needs the per-model plan metadata.
	f := newOrderedInputFixture(t, content, currentMetadata(content, "m1"), false)

	if err := f.p.preProcessJob(ctx, f.jobInfo); err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}
	if f.manifest(t).sequentialInput() {
		t.Fatal("sync dispatch must keep using plan files")
	}
}

// TestOrderedIngestionReadCost contrasts the two ingestion paths by the number
// of storage operations each one costs before dispatch begins.
func TestOrderedIngestionReadCost(t *testing.T) {
	ctx := testLoggerCtx(t)

	requests := make([]batch_types.Request, 0, 200)
	for i := range 200 {
		requests = append(requests, chatRequest(fmt.Sprintf("req-%d", i), "m1"))
	}
	content := orderedContent(t, requests...)

	ordered := newOrderedInputFixture(t, content, currentMetadata(content, "m1"), true)
	if err := ordered.p.preProcessJob(ctx, ordered.jobInfo); err != nil {
		t.Fatalf("ordered preProcessJob: %v", err)
	}

	fallback := newOrderedInputFixture(t, content, nil, true)
	if err := fallback.p.preProcessJob(ctx, fallback.jobInfo); err != nil {
		t.Fatalf("fallback preProcessJob: %v", err)
	}

	orderedReads := ordered.files.retrieves.Load() + ordered.files.ranges.Load()
	fallbackReads := fallback.files.retrieves.Load() + fallback.files.ranges.Load()
	if orderedReads != 0 {
		t.Fatalf("ordered ingestion cost %d storage reads, want 0", orderedReads)
	}
	if fallbackReads == 0 {
		t.Fatal("expected the fallback path to read the object")
	}
	t.Logf("ingestion storage reads: ordered=%d fallback=%d", orderedReads, fallbackReads)
}
