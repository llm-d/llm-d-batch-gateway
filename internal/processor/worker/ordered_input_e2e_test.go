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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/file"
	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batchinput"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// dispatchRecorder captures submission order across every model's queue, so a
// test can assert on the order requests were handed to inference.
type dispatchRecorder struct {
	mu        sync.Mutex
	submitted []string // custom ids, in submission order
}

func (r *dispatchRecorder) record(customID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.submitted = append(r.submitted, customID)
}

func (r *dispatchRecorder) order() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.submitted))
	copy(out, r.submitted)
	return out
}

// echoAsyncClient answers every submission immediately.
//
// Each model gets its own instance because a broadcaster consumes its model's
// result queue exclusively; sharing one queue between models would let one
// broadcaster swallow results destined for another, which is not how real
// per-model queues behave.
type echoAsyncClient struct {
	recorder *dispatchRecorder
	results  chan *inference.GenerateResponse
}

var _ inference.AsyncInferenceClient = (*echoAsyncClient)(nil)

func newEchoAsyncClient(recorder *dispatchRecorder) *echoAsyncClient {
	return &echoAsyncClient{
		recorder: recorder,
		results:  make(chan *inference.GenerateResponse, 4096),
	}
}

func (c *echoAsyncClient) Submit(_ context.Context, req *inference.GenerateRequest) *inference.ClientError {
	customID, _ := req.Params["custom_id"].(string)
	c.recorder.record(customID)

	body, _ := json.Marshal(map[string]any{
		"id":     req.RequestID,
		"object": "chat.completion",
		"usage":  map[string]any{"prompt_tokens": 1, "completion_tokens": 1},
	})
	c.results <- &inference.GenerateResponse{
		RequestID:  req.RequestID,
		Response:   body,
		StatusCode: 200,
	}
	return nil
}

func (c *echoAsyncClient) GetResult(ctx context.Context) (*inference.GenerateResponse, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-c.results:
		return r, nil
	}
}

func (c *echoAsyncClient) Cancel(context.Context, []string) error { return nil }
func (c *echoAsyncClient) Close() error                           { return nil }

// orderedRoundTrip wires the real file upload handler and a real Processor
// over one shared storage and file database, so a batch input travels the
// whole way from HTTP upload to async dispatch.
type orderedRoundTrip struct {
	t        *testing.T
	handler  *file.FileAPIHandler
	p        *Processor
	storage  *countingFilesClient
	dispatch *dispatchRecorder
}

func newOrderedRoundTrip(t *testing.T, policy batchinput.PolicyID) *orderedRoundTrip {
	t.Helper()

	storage := &countingFilesClient{BatchFilesClient: mockfiles.NewMockBatchFilesClient(t.TempDir())}
	fileDB := newMockFileDBClient()

	apiCfg := &common.ServerConfig{
		FileAPI: common.FileAPIConfig{
			MaxSizeBytes:             common.DefaultMaxFileSizeBytes,
			MaxLineCount:             common.DefaultMaxFileLineCount,
			DefaultExpirationSeconds: 30 * 24 * 60 * 60,
			BatchInputOrderPolicy:    string(policy),
		},
	}
	handler := file.NewFileAPIHandler(apiCfg, &clientset.Clientset{File: storage, FileDB: fileDB})

	procCfg := config.NewConfig()
	procCfg.WorkDir = t.TempDir()

	recorder := &dispatchRecorder{}
	perModel := map[string]*echoAsyncClient{
		"m1": newEchoAsyncClient(recorder),
		"m2": newEchoAsyncClient(recorder),
	}
	factories := make(map[string]func() inference.AsyncInferenceClient, len(perModel))
	for model, client := range perModel {
		factories[model] = func() inference.AsyncInferenceClient { return client }
	}
	asyncResolver := inference.NewTestAsyncResolver(factories)

	p := mustNewProcessor(t, procCfg, &clientset.Clientset{
		BatchDB: newMockBatchDBClient(),
		FileDB:  fileDB,
		File:    storage,
		Status:  mockdb.NewMockBatchStatusClient(),
	})
	p.asyncInference = asyncResolver
	p.broadcasters = newBroadcasterRegistry(asyncResolver, testLogger(t))

	return &orderedRoundTrip{t: t, handler: handler, p: p, storage: storage, dispatch: recorder}
}

// upload posts content through the real file API and returns the file record.
func (rt *orderedRoundTrip) upload(content string) openai.FileObject {
	rt.t.Helper()

	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	fw, err := writer.CreateFormFile("file", "input.jsonl")
	if err != nil {
		rt.t.Fatalf("CreateFormFile: %v", err)
	}
	if _, err := io.WriteString(fw, content); err != nil {
		rt.t.Fatalf("write upload: %v", err)
	}
	if err := writer.WriteField("purpose", "batch"); err != nil {
		rt.t.Fatalf("write purpose: %v", err)
	}
	if err := writer.Close(); err != nil {
		rt.t.Fatalf("close writer: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/files", &body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	w := httptest.NewRecorder()
	rt.handler.CreateFile(w, req)

	if w.Code != http.StatusOK {
		rt.t.Fatalf("upload failed: status %d body %s", w.Code, w.Body.String())
	}
	var obj openai.FileObject
	if err := json.Unmarshal(w.Body.Bytes(), &obj); err != nil {
		rt.t.Fatalf("parse upload response: %v", err)
	}
	return obj
}

// run drives a batch over the uploaded file through ingestion and execution.
func (rt *orderedRoundTrip) run(fileID string) (*openai.BatchRequestCounts, error) {
	rt.t.Helper()

	jobInfo := &batch_types.JobInfo{
		JobID:    "job-round-trip",
		TenantID: common.DefaultTenantID,
		BatchJob: &openai.Batch{
			ID:        "job-round-trip",
			BatchSpec: openai.BatchSpec{InputFileID: fileID},
		},
	}
	dbJob := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{ID: jobInfo.JobID, TenantID: jobInfo.TenantID, Tags: db.Tags{}},
	}
	if err := rt.p.batchDB.DBStore(context.Background(), dbJob); err != nil {
		rt.t.Fatalf("seed batch row: %v", err)
	}

	ctx, cancel := context.WithCancel(testLoggerCtx(rt.t))
	defer cancel()

	broadcastCtx, stopBroadcast := context.WithCancel(context.Background())
	rt.p.broadcasters.Run(broadcastCtx)
	defer func() {
		stopBroadcast()
		rt.p.broadcasters.Wait()
	}()

	if err := rt.p.preProcessJob(ctx, jobInfo); err != nil {
		return nil, fmt.Errorf("pre-process: %w", err)
	}

	return rt.p.executeJobAsync(ctx, &jobExecutionParams{
		jobInfo: jobInfo,
		jobItem: dbJob,
		updater: rt.p.updater,
	})
}

// outputCustomIDs reads the job's output file.
func (rt *orderedRoundTrip) outputCustomIDs() []string {
	rt.t.Helper()
	path, err := rt.p.jobOutputFilePath("job-round-trip", common.DefaultTenantID)
	if err != nil {
		rt.t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		rt.t.Fatalf("open output: %v", err)
	}
	defer f.Close()

	var ids []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var line struct {
			CustomID string `json:"custom_id"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &line); err != nil {
			rt.t.Fatalf("parse output line: %v", err)
		}
		ids = append(ids, line.CustomID)
	}
	if err := scanner.Err(); err != nil {
		rt.t.Fatalf("scan output: %v", err)
	}
	return ids
}

// TestOrderedInputRoundTrip walks a batch input from an HTTP upload all the
// way to async dispatch, over one shared object store, and checks the two
// things this whole change is for: dispatch follows the order chosen at upload
// time, and the input is read in a handful of requests rather than one per
// line.
func TestOrderedInputRoundTrip(t *testing.T) {
	const requestsPerGroup = 40

	// Interleave models and system prompts so that upload order is nothing
	// like the grouping the policy should produce. The custom id carries its
	// group so dispatch order can be collapsed back into runs.
	var content strings.Builder
	var uploaded []string
	seq := 0
	for range requestsPerGroup {
		for _, spec := range []struct {
			model  string
			prompt string
		}{
			{"m2", "prompt-beta"},
			{"m1", ""},
			{"m1", "prompt-alpha"},
			{"m2", "prompt-beta"},
		} {
			id := fmt.Sprintf("%s/%s#%d", spec.model, spec.prompt, seq)
			seq++
			uploaded = append(uploaded, id)
			content.WriteString(promptRequestLine(t, id, spec.model, spec.prompt))
		}
	}

	rt := newOrderedRoundTrip(t, batchinput.PolicyModelPrefixV1)
	obj := rt.upload(content.String())

	counts, err := rt.run(obj.ID)
	if err != nil {
		t.Fatalf("run batch: %v", err)
	}

	total := int64(len(uploaded))
	if counts.Total != total || counts.Completed != total || counts.Failed != 0 {
		t.Fatalf("counts = %+v, want %d completed", counts, total)
	}

	// Every uploaded request must come back exactly once.
	got := rt.outputCustomIDs()
	if len(got) != len(uploaded) {
		t.Fatalf("output has %d lines, want %d", len(got), len(uploaded))
	}
	seen := map[string]int{}
	for _, id := range got {
		seen[id]++
	}
	for _, id := range uploaded {
		if seen[id] != 1 {
			t.Fatalf("custom_id %q appears %d times in the output", id, seen[id])
		}
	}

	// Dispatch must follow the upload-time grouping: one contiguous run per
	// (model, system prompt) pair rather than the interleaving that was
	// uploaded.
	order := rt.dispatch.order()
	if len(order) != len(uploaded) {
		t.Fatalf("dispatched %d requests, want %d", len(order), len(uploaded))
	}
	groups := groupRuns(order)
	if len(groups) != 3 {
		t.Fatalf("dispatch produced %d contiguous groups, want 3 (m1/alpha, m1/none, m2/beta): %v",
			len(groups), groups)
	}
	for _, g := range groups {
		if g.count != requestsPerGroup && g.count != requestsPerGroup*2 {
			t.Fatalf("group %q has %d requests, expected a whole group", g.key, g.count)
		}
	}

	// Reading the input must not scale with the request count.
	reads := rt.storage.ranges.Load()
	if reads > 4 {
		t.Fatalf("input cost %d ranged reads for %d requests", reads, len(uploaded))
	}
	if downloads := rt.storage.retrieves.Load(); downloads != 0 {
		t.Fatalf("input was downloaded %d times; ordered ingestion must not need it", downloads)
	}
	t.Logf("%d requests dispatched using %d ranged reads and %d downloads",
		len(uploaded), reads, rt.storage.retrieves.Load())
}

// TestOrderedInputRoundTripUnknownPolicy is the version-skew case: an object
// stored by an API server running a policy this processor has never heard of
// must still execute correctly, via the fallback path.
func TestOrderedInputRoundTripUnknownPolicy(t *testing.T) {
	var content strings.Builder
	var uploaded []string
	for i := range 10 {
		id := fmt.Sprintf("req-%d", i)
		uploaded = append(uploaded, id)
		content.WriteString(promptRequestLine(t, id, "m1", "shared"))
	}

	rt := newOrderedRoundTrip(t, batchinput.PolicyModelPrefixV1)
	obj := rt.upload(content.String())

	// Rewrite the recorded policy to one this build does not know, leaving the
	// stored bytes untouched.
	ctx := context.Background()
	items, _, _, err := rt.p.files.db.DBGet(ctx,
		&db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{obj.ID}}}, true, 0, 1)
	if err != nil || len(items) == 0 {
		t.Fatalf("load file record: %v", err)
	}
	meta, err := batchinput.DecodeMetadata(items[0].Tags)
	if err != nil {
		t.Fatalf("decode metadata: %v", err)
	}
	meta.Policy = "policy-from-a-newer-apiserver-v2"
	encoded, err := meta.Encode()
	if err != nil {
		t.Fatalf("encode metadata: %v", err)
	}
	items[0].Tags[batchinput.TagKey] = encoded
	if err := rt.p.files.db.DBStore(ctx, items[0]); err != nil {
		t.Fatalf("update file record: %v", err)
	}

	counts, err := rt.run(obj.ID)
	if err != nil {
		t.Fatalf("run batch under an unknown policy: %v", err)
	}
	if counts.Completed != int64(len(uploaded)) {
		t.Fatalf("counts = %+v, want %d completed", counts, len(uploaded))
	}

	got := rt.outputCustomIDs()
	if len(got) != len(uploaded) {
		t.Fatalf("output has %d lines, want %d", len(got), len(uploaded))
	}

	// The fallback path reads the object to rebuild plans, which is exactly
	// the cost this change avoids when the layout is understood.
	if rt.storage.retrieves.Load() == 0 {
		t.Fatal("the fallback path must read the object to learn its layout")
	}
	t.Logf("unknown policy fell back to %d downloads and %d ranged reads",
		rt.storage.retrieves.Load(), rt.storage.ranges.Load())
}

// TestOrderedInputRoundTripInvalidUpload checks the agreed division of labour:
// a malformed batch body is accepted at upload and reported when the batch
// runs, not at upload time.
func TestOrderedInputRoundTripInvalidUpload(t *testing.T) {
	content := promptRequestLine(t, "dup", "m1", "") + promptRequestLine(t, "dup", "m1", "")

	rt := newOrderedRoundTrip(t, batchinput.PolicyModelPrefixV1)
	obj := rt.upload(content) // must succeed

	_, err := rt.run(obj.ID)
	if err == nil {
		t.Fatal("a batch over an invalid input must fail")
	}
	if !strings.Contains(err.Error(), "duplicate custom_id") {
		t.Fatalf("failure should explain the invalid input, got %v", err)
	}
}

type run struct {
	key   string
	count int
}

// groupRuns collapses a dispatch order into contiguous runs sharing the same
// "model/prompt" prefix, which is how a custom id is built in these tests.
func groupRuns(order []string) []run {
	var runs []run
	for _, id := range order {
		key := id[:strings.LastIndex(id, "#")]
		if len(runs) > 0 && runs[len(runs)-1].key == key {
			runs[len(runs)-1].count++
			continue
		}
		runs = append(runs, run{key: key, count: 1})
	}
	return runs
}

func promptRequestLine(t *testing.T, customID, model, systemPrompt string) string {
	t.Helper()
	messages := []any{}
	if systemPrompt != "" {
		messages = append(messages, map[string]any{"role": "system", "content": systemPrompt})
	}
	messages = append(messages, map[string]any{"role": "user", "content": "hello"})

	line, err := json.Marshal(map[string]any{
		"custom_id": customID,
		"method":    "POST",
		"url":       "/v1/chat/completions",
		"body": map[string]any{
			"model":     model,
			"messages":  messages,
			"custom_id": customID, // echoed back by the fake client
		},
	})
	if err != nil {
		t.Fatalf("marshal request line: %v", err)
	}
	return string(line) + "\n"
}
