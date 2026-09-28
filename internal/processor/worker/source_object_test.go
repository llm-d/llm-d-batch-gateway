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
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batchinput"
)

// drainSource runs Produce and collects everything it emits.
func drainSource(t *testing.T, s *ObjectSource) ([]pipeline.RequestItem, error) {
	t.Helper()
	out := make(chan pipeline.RequestItem, 4096)
	err := s.Produce(context.Background(), out)

	var items []pipeline.RequestItem
	for item := range out {
		items = append(items, item)
	}
	return items, err
}

func objectSourceOver(content string, storage *fakeRangeStorage, cfg *config.ProcessorConfig) *ObjectSource {
	if cfg == nil {
		cfg = config.NewConfig()
	}
	return NewObjectSource(ObjectSourceConfig{
		Storage:       storage,
		InputRef:      &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
		Size:          int64(len(content)),
		LineCount:     int64(strings.Count(content, "\n")),
		Policy:        batchinput.PolicyModelPrefixV1,
		Cfg:           cfg,
		Logger:        logr.Discard(),
		ChunkSize:     64,
		PrefetchBytes: 256,
	})
}

func requestJSONL(t *testing.T, ids ...string) string {
	t.Helper()
	var b strings.Builder
	for _, id := range ids {
		fmt.Fprintf(&b,
			`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"hello there"}]}}`, id)
		b.WriteByte('\n')
	}
	return b.String()
}

func TestObjectSourceProduce(t *testing.T) {
	t.Run("emits every request in stored order", func(t *testing.T) {
		ids := make([]string, 0, 50)
		for i := range 50 {
			ids = append(ids, "req-"+strconv.Itoa(i))
		}
		content := requestJSONL(t, ids...)
		storage := newFakeRangeStorage(content)

		items, err := drainSource(t, objectSourceOver(content, storage, nil))
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if len(items) != len(ids) {
			t.Fatalf("emitted %d items, want %d", len(items), len(ids))
		}
		for i, item := range items {
			if item.CustomID != ids[i] {
				t.Fatalf("item %d has custom_id %q, want %q; stored order was not preserved",
					i, item.CustomID, ids[i])
			}
		}
	})

	t.Run("reassembles requests that straddle chunk boundaries", func(t *testing.T) {
		content := requestJSONL(t, "a", "b", "c", "d", "e")
		storage := newFakeRangeStorage(content)

		// A chunk far smaller than one line forces every request to span
		// several ranged reads.
		source := NewObjectSource(ObjectSourceConfig{
			Storage:       storage,
			InputRef:      &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
			Size:          int64(len(content)),
			LineCount:     5,
			Cfg:           config.NewConfig(),
			Logger:        logr.Discard(),
			ChunkSize:     7,
			PrefetchBytes: 21,
		})

		items, err := drainSource(t, source)
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		got := make([]string, len(items))
		for i, item := range items {
			got[i] = item.CustomID
		}
		if strings.Join(got, ",") != "a,b,c,d,e" {
			t.Fatalf("custom_ids = %v; lines spanning chunks were mangled", got)
		}
		if len(storage.recorded()) < 5 {
			t.Fatal("expected the small chunk size to produce many ranged reads")
		}
	})

	t.Run("costs a handful of reads rather than one per request", func(t *testing.T) {
		ids := make([]string, 0, 200)
		for i := range 200 {
			ids = append(ids, "req-"+strconv.Itoa(i))
		}
		content := requestJSONL(t, ids...)
		storage := newFakeRangeStorage(content)

		source := NewObjectSource(ObjectSourceConfig{
			Storage:       storage,
			InputRef:      &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
			Size:          int64(len(content)),
			LineCount:     int64(len(ids)),
			Cfg:           config.NewConfig(),
			Logger:        logr.Discard(),
			ChunkSize:     8 << 20,
			PrefetchBytes: 32 << 20,
		})

		items, err := drainSource(t, source)
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if len(items) != len(ids) {
			t.Fatalf("emitted %d items, want %d", len(items), len(ids))
		}
		if reads := len(storage.recorded()); reads != 1 {
			t.Fatalf("%d ranged reads for %d requests, want 1", reads, len(ids))
		}
	})

	t.Run("resolves the model from the line and applies routing headers", func(t *testing.T) {
		content := requestJSONL(t, "a")
		storage := newFakeRangeStorage(content)

		cfg := config.NewConfig()
		cfg.DispatchMode = config.DispatchModeSync
		cfg.RouteKeyMethod = config.RouteKeyMethodTenant
		cfg.SendFairnessHeader = true
		cfg.ModelGateways = map[string]config.ModelGatewayConfig{
			"tenant-x/m1": {URL: "http://gw:8000", InferenceObjective: "batch-low"},
		}

		source := NewObjectSource(ObjectSourceConfig{
			Storage:            storage,
			InputRef:           &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
			Size:               int64(len(content)),
			LineCount:          1,
			Cfg:                cfg,
			TenantID:           "tenant-x",
			SLODeadline:        time.Now().Add(time.Minute),
			PassThroughHeaders: map[string]string{"x-keep": "me"},
			Logger:             logr.Discard(),
		})

		items, err := drainSource(t, source)
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if len(items) != 1 {
			t.Fatalf("emitted %d items, want 1", len(items))
		}
		item := items[0]
		if item.ModelID != "tenant-x/m1" {
			t.Fatalf("ModelID = %q, want the tenant-scoped route key", item.ModelID)
		}
		if item.Endpoint != "/v1/chat/completions" {
			t.Fatalf("Endpoint = %q", item.Endpoint)
		}
		if item.Body["model"] != "m1" {
			t.Fatalf("the forwarded body must keep the bare model name, got %v", item.Body["model"])
		}
		if item.Headers["x-keep"] != "me" {
			t.Fatal("pass-through headers were dropped")
		}
		if item.Headers[inferenceObjectiveHeader] != "batch-low" {
			t.Fatalf("objective header = %q", item.Headers[inferenceObjectiveHeader])
		}
		if item.Headers[fairnessIDHeader] != "tenant-x" {
			t.Fatalf("fairness header = %q", item.Headers[fairnessIDHeader])
		}
		if _, ok := item.Headers[sloTTFTMSHeader]; !ok {
			t.Fatal("SLO header missing")
		}
	})

	t.Run("a corrupted line becomes a parse error, not a job failure", func(t *testing.T) {
		content := requestJSONL(t, "a") + "{ this is not json\n" + requestJSONL(t, "c")
		storage := newFakeRangeStorage(content)

		items, err := drainSource(t, objectSourceOver(content, storage, nil))
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if len(items) != 3 {
			t.Fatalf("emitted %d items, want 3 so that every line is accounted for", len(items))
		}
		if items[1].ParseError == nil {
			t.Fatal("the corrupted line must carry a parse error")
		}
		if items[0].ParseError != nil || items[2].ParseError != nil {
			t.Fatal("valid neighbours must still dispatch")
		}
	})

	t.Run("a line count mismatch is reported", func(t *testing.T) {
		content := requestJSONL(t, "a", "b")
		storage := newFakeRangeStorage(content)

		source := NewObjectSource(ObjectSourceConfig{
			Storage:   storage,
			InputRef:  &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
			Size:      int64(len(content)),
			LineCount: 5, // metadata disagrees with the object
			Cfg:       config.NewConfig(),
			Logger:    logr.Discard(),
		})

		_, err := drainSource(t, source)
		if err == nil {
			t.Fatal("an object that does not match its recorded line count must not pass silently")
		}
		if !strings.Contains(err.Error(), "expected 5") {
			t.Fatalf("unexpected error %v", err)
		}
	})

	t.Run("a storage failure fails the job", func(t *testing.T) {
		content := requestJSONL(t, "a", "b", "c", "d", "e", "f", "g", "h")
		storage := newFakeRangeStorage(content)
		storage.failAtOffset = 64
		storage.failErr = fmt.Errorf("range read rejected")

		_, err := drainSource(t, objectSourceOver(content, storage, nil))
		if err == nil {
			t.Fatal("expected the storage failure to surface")
		}
		if !strings.Contains(err.Error(), errRequestInputRead.Error()) {
			t.Fatalf("error should be classified as an input read failure, got %v", err)
		}
	})

	t.Run("an unterminated final line is still emitted", func(t *testing.T) {
		content := requestJSONL(t, "a") + strings.TrimSuffix(requestJSONL(t, "b"), "\n")
		storage := newFakeRangeStorage(content)

		source := NewObjectSource(ObjectSourceConfig{
			Storage:   storage,
			InputRef:  &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
			Size:      int64(len(content)),
			LineCount: 2,
			Cfg:       config.NewConfig(),
			Logger:    logr.Discard(),
		})

		items, err := drainSource(t, source)
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if len(items) != 2 || items[1].CustomID != "b" {
			t.Fatalf("unterminated final line was lost: %+v", items)
		}
	})

	t.Run("an empty object produces nothing", func(t *testing.T) {
		storage := newFakeRangeStorage("")
		source := NewObjectSource(ObjectSourceConfig{
			Storage:  storage,
			InputRef: &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
			Size:     0,
			Cfg:      config.NewConfig(),
			Logger:   logr.Discard(),
		})
		items, err := drainSource(t, source)
		if err != nil {
			t.Fatalf("Produce: %v", err)
		}
		if len(items) != 0 {
			t.Fatalf("emitted %d items from an empty object", len(items))
		}
	})

	t.Run("requires a storage reference", func(t *testing.T) {
		source := NewObjectSource(ObjectSourceConfig{Cfg: config.NewConfig(), Logger: logr.Discard()})
		if _, err := drainSource(t, source); err == nil {
			t.Fatal("expected a missing input reference to be rejected")
		}
	})
}

// TestObjectSourceEnumeratesAfterCancellation pins the accounting rule that
// the drain path depends on: cancelling a batch must not stop the source from
// naming every request, or the error file would come up short.
func TestObjectSourceEnumeratesAfterCancellation(t *testing.T) {
	content := requestJSONL(t, "a", "b", "c", "d", "e")
	storage := newFakeRangeStorage(content)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled before Produce runs

	source := objectSourceOver(content, storage, nil)
	out := make(chan pipeline.RequestItem, 16)
	if err := source.Produce(ctx, out); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	seen := map[string]bool{}
	for item := range out {
		seen[item.CustomID] = true
	}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		if !seen[id] {
			t.Fatalf("custom_id %q was dropped; cancelled batches would report fewer lines than requests", id)
		}
	}
}

// TestObjectSourceSurvivesSlowDispatch is the regression guard for coupling a
// wall-clock deadline to enumeration.
//
// Prefetch is bounded, so ranged reads are driven by how fast the dispatcher
// consumes. A deadline covering the whole read would therefore really be a
// deadline on submitting the entire batch, and a slow inference queue would
// trip it and silently truncate the request set.
func TestObjectSourceSurvivesSlowDispatch(t *testing.T) {
	ids := make([]string, 0, 40)
	for i := range 40 {
		ids = append(ids, "req-"+strconv.Itoa(i))
	}
	content := requestJSONL(t, ids...)
	storage := newFakeRangeStorage(content)

	// Chunks far smaller than the object, so reads must keep being issued as
	// the consumer drains.
	source := NewObjectSource(ObjectSourceConfig{
		Storage:       storage,
		InputRef:      &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
		Size:          int64(len(content)),
		LineCount:     int64(len(ids)),
		Cfg:           config.NewConfig(),
		Logger:        logr.Discard(),
		ChunkSize:     64,
		PrefetchBytes: 128,
	})

	out := make(chan pipeline.RequestItem)
	errCh := make(chan error, 1)
	go func() { errCh <- source.Produce(context.Background(), out) }()

	// Consume slowly enough that the read spans far longer than any single
	// storage operation.
	var got []string
	for item := range out {
		got = append(got, item.CustomID)
		time.Sleep(2 * time.Millisecond)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Produce: %v", err)
	}

	if len(got) != len(ids) {
		t.Fatalf("emitted %d of %d requests; a slow consumer must not truncate the input",
			len(got), len(ids))
	}
	for i, id := range ids {
		if got[i] != id {
			t.Fatalf("item %d = %q, want %q", i, got[i], id)
		}
	}
}

// TestObjectSourceReportsBareModelName guards against leaking the internal
// routing key into the customer-visible error file.
func TestObjectSourceReportsBareModelName(t *testing.T) {
	content := requestJSONL(t, "a")
	storage := newFakeRangeStorage(content)

	cfg := config.NewConfig()
	cfg.RouteKeyMethod = config.RouteKeyMethodTenant

	source := NewObjectSource(ObjectSourceConfig{
		Storage:   storage,
		InputRef:  &inputFileRef{storageName: "input.jsonl", folderName: "tenant"},
		Size:      int64(len(content)),
		LineCount: 1,
		Cfg:       cfg,
		TenantID:  "secret-tenant",
		Logger:    logr.Discard(),
	})

	items, err := drainSource(t, source)
	if err != nil {
		t.Fatalf("Produce: %v", err)
	}
	item := items[0]

	if item.ModelID != "secret-tenant/m1" {
		t.Fatalf("ModelID = %q, want the tenant-scoped route key for routing", item.ModelID)
	}
	if item.ModelName != "m1" {
		t.Fatalf("ModelName = %q, want the bare model the client sent", item.ModelName)
	}

	// The message that reaches the error file must not carry the tenant.
	msg := item.ModelNotFound().Error.Message
	if strings.Contains(msg, "secret-tenant") {
		t.Fatalf("model_not_found message leaks the tenant-scoped routing key: %q", msg)
	}
	if !strings.Contains(msg, `"m1"`) {
		t.Fatalf("model_not_found message should name the client's model, got %q", msg)
	}
}
