package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"

	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

func TestMergeHeaders(t *testing.T) {
	t.Run("no SLO no objective no fairness leaves headers unchanged", func(t *testing.T) {
		s := &PlanFileSource{
			cfg:    config.NewConfig(),
			logger: logr.Discard(),
		}
		got := s.mergeHeaders(nil, "m1")
		if _, ok := got[sloTTFTMSHeader]; ok {
			t.Fatalf("unexpected %s without SLO", sloTTFTMSHeader)
		}
		if _, ok := got[inferenceObjectiveHeader]; ok {
			t.Fatal("unexpected inference objective header")
		}
		if _, ok := got[fairnessIDHeader]; ok {
			t.Fatal("unexpected fairness header")
		}
	})

	t.Run("SLO deadline remaining milliseconds", func(t *testing.T) {
		want := 5*time.Second + 100*time.Millisecond
		s := &PlanFileSource{
			cfg:         config.NewConfig(),
			sloDeadline: time.Now().Add(want),
			logger:      logr.Discard(),
		}
		h := s.mergeHeaders(nil, "m1")
		got, err := strconv.ParseInt(h[sloTTFTMSHeader], 10, 64)
		if err != nil {
			t.Fatalf("parse header: %v", err)
		}
		const slackMs int64 = 150
		hi := want.Milliseconds()
		lo := hi - slackMs
		if got < lo || got > hi {
			t.Fatalf("x-slo-ttft-ms = %d, want in [%d, %d]", got, lo, hi)
		}
	})

	t.Run("SLO deadline in the past omits header", func(t *testing.T) {
		s := &PlanFileSource{
			cfg:         config.NewConfig(),
			sloDeadline: time.Now().Add(-time.Second),
			logger:      logr.Discard(),
		}
		h := s.mergeHeaders(nil, "m1")
		if _, ok := h[sloTTFTMSHeader]; ok {
			t.Fatalf("unexpected %s with expired deadline", sloTTFTMSHeader)
		}
	})

	t.Run("preserves existing headers", func(t *testing.T) {
		s := &PlanFileSource{
			cfg:         config.NewConfig(),
			sloDeadline: time.Now().Add(time.Minute),
			logger:      logr.Discard(),
		}
		h := s.mergeHeaders(map[string]string{"a": "b"}, "m1")
		if h["a"] != "b" {
			t.Fatal("lost existing header")
		}
		if _, ok := h[sloTTFTMSHeader]; !ok {
			t.Fatal("SLO header missing")
		}
	})

	t.Run("per-model inference objective set", func(t *testing.T) {
		cfg := config.NewConfig()
		cfg.ModelGateways = map[string]config.ModelGatewayConfig{
			"m1": {
				URL:                "http://gw:8000",
				InferenceObjective: "batch-sheddable-a",
			},
		}
		s := &PlanFileSource{
			cfg:    cfg,
			logger: logr.Discard(),
		}
		h := s.mergeHeaders(nil, "m1")
		if h[inferenceObjectiveHeader] != "batch-sheddable-a" {
			t.Fatalf("objective = %q, want %q", h[inferenceObjectiveHeader], "batch-sheddable-a")
		}
	})

	t.Run("no per-model objective omits header", func(t *testing.T) {
		cfg := config.NewConfig()
		cfg.ModelGateways = map[string]config.ModelGatewayConfig{
			"m1": {URL: "http://gw:8000"},
		}
		s := &PlanFileSource{
			cfg:    cfg,
			logger: logr.Discard(),
		}
		h := s.mergeHeaders(nil, "m1")
		if _, ok := h[inferenceObjectiveHeader]; ok {
			t.Fatal("objective header should not be set when empty")
		}
	})

	t.Run("fairness header sent when enabled and tenantID non-empty", func(t *testing.T) {
		cfg := config.NewConfig()
		cfg.SendFairnessHeader = true
		s := &PlanFileSource{
			cfg:      cfg,
			tenantID: "tenant-x",
			logger:   logr.Discard(),
		}
		h := s.mergeHeaders(nil, "m1")
		if h[fairnessIDHeader] != "tenant-x" {
			t.Fatalf("fairness header = %q, want %q", h[fairnessIDHeader], "tenant-x")
		}
	})

	t.Run("fairness header not sent when disabled", func(t *testing.T) {
		cfg := config.NewConfig()
		cfg.SendFairnessHeader = false
		s := &PlanFileSource{
			cfg:      cfg,
			tenantID: "tenant-x",
			logger:   logr.Discard(),
		}
		h := s.mergeHeaders(nil, "m1")
		if _, ok := h[fairnessIDHeader]; ok {
			t.Fatal("fairness header should not be set when SendFairnessHeader=false")
		}
	})

	t.Run("fairness header not sent when tenantID empty", func(t *testing.T) {
		cfg := config.NewConfig()
		cfg.SendFairnessHeader = true
		s := &PlanFileSource{
			cfg:    cfg,
			logger: logr.Discard(),
		}
		h := s.mergeHeaders(nil, "m1")
		if _, ok := h[fairnessIDHeader]; ok {
			t.Fatal("fairness header should not be set when tenantID is empty")
		}
	})

	t.Run("fairness header honors pass-through value when present", func(t *testing.T) {
		cfg := config.NewConfig()
		cfg.SendFairnessHeader = true
		s := &PlanFileSource{
			cfg:      cfg,
			tenantID: "real-tenant",
			logger:   logr.Discard(),
		}
		passThrough := map[string]string{
			fairnessIDHeader: "stale-value",
			"x-custom":       "keep-me",
		}
		h := s.mergeHeaders(passThrough, "m1")
		if h[fairnessIDHeader] != "stale-value" {
			t.Fatalf("fairness header = %q, want pass-through %q", h[fairnessIDHeader], "stale-value")
		}
		if h["x-custom"] != "keep-me" {
			t.Fatal("non-conflicting pass-through header was lost")
		}
	})

	t.Run("all three headers together", func(t *testing.T) {
		cfg := config.NewConfig()
		cfg.SendFairnessHeader = true
		cfg.ModelGateways = map[string]config.ModelGatewayConfig{
			"m1": {
				URL:                "http://gw:8000",
				InferenceObjective: "batch-low-priority",
			},
		}
		s := &PlanFileSource{
			cfg:         cfg,
			sloDeadline: time.Now().Add(10 * time.Second),
			tenantID:    "tenant-xyz",
			logger:      logr.Discard(),
		}
		h := s.mergeHeaders(nil, "m1")
		if _, ok := h[sloTTFTMSHeader]; !ok {
			t.Fatal("SLO header missing")
		}
		if h[inferenceObjectiveHeader] != "batch-low-priority" {
			t.Fatalf("objective = %q, want %q", h[inferenceObjectiveHeader], "batch-low-priority")
		}
		if h[fairnessIDHeader] != "tenant-xyz" {
			t.Fatalf("fairness = %q, want %q", h[fairnessIDHeader], "tenant-xyz")
		}
	})
}

func TestReadPlanEntries(t *testing.T) {
	t.Run("reads entries correctly", func(t *testing.T) {
		dir := t.TempDir()
		plansDir := filepath.Join(dir, "plans")
		want := []planEntry{
			{Offset: 0, Length: 100, PrefixHash: 42},
			{Offset: 100, Length: 200, PrefixHash: 99},
		}
		writePlanFile(t, plansDir, "test", want)

		got, err := readPlanEntries(filepath.Join(plansDir, "test.plan"))
		if err != nil {
			t.Fatalf("readPlanEntries: %v", err)
		}
		if len(got) != len(want) {
			t.Fatalf("len = %d, want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("entry %d: got %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("empty file", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "empty.plan")
		if err := os.WriteFile(path, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		got, err := readPlanEntries(path)
		if err != nil {
			t.Fatalf("readPlanEntries: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("expected empty, got %d entries", len(got))
		}
	})

	t.Run("invalid size", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bad.plan")
		if err := os.WriteFile(path, []byte("abc"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := readPlanEntries(path)
		if err == nil {
			t.Fatal("expected error for invalid file size")
		}
	})

	t.Run("missing file", func(t *testing.T) {
		_, err := readPlanEntries("/nonexistent/path.plan")
		if err == nil {
			t.Fatal("expected error for missing file")
		}
	})
}

func TestPlanFileSource_Produce(t *testing.T) {
	dir := t.TempDir()

	requests := []batch_types.Request{
		{CustomID: "c-1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "hello"}},
		{CustomID: "c-2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "world"}},
	}

	inputPath := filepath.Join(dir, "input.jsonl")
	var entries []planEntry
	f, err := os.Create(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range requests {
		data, _ := json.Marshal(req)
		data = append(data, '\n')
		offset, _ := f.Seek(0, 1)
		entries = append(entries, planEntry{
			Offset: offset,
			Length: uint32(len(data)),
		})
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()

	plansDir := filepath.Join(dir, "plans")
	writePlanFile(t, plansDir, "m1", entries)

	inputFile, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer inputFile.Close()

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile: inputFile,
		PlansDir:  plansDir,
		ModelMap:  &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 2},
		Resolver:  resolver,
		Cfg:       config.NewConfig(),
		Logger:    logr.Discard(),
	})

	out := make(chan pipeline.RequestItem, 10)
	if err := source.Produce(context.Background(), out); err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	var items []pipeline.RequestItem
	for item := range out {
		items = append(items, item)
	}

	if len(items) != 2 {
		t.Fatalf("produced %d items, want 2", len(items))
	}
	if items[0].CustomID != "c-1" {
		t.Errorf("item 0 CustomID = %q, want %q", items[0].CustomID, "c-1")
	}
	if items[1].CustomID != "c-2" {
		t.Errorf("item 1 CustomID = %q, want %q", items[1].CustomID, "c-2")
	}
	if items[0].ModelID != "m1" {
		t.Errorf("item 0 ModelID = %q, want %q", items[0].ModelID, "m1")
	}
	if items[0].RequestID == "" {
		t.Error("expected non-empty RequestID")
	}
}

func TestPlanFileSource_Produce_TenantScopedLookup(t *testing.T) {
	dir := t.TempDir()

	requests := []batch_types.Request{
		{CustomID: "c-1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "hello"}},
	}

	inputPath := filepath.Join(dir, "input.jsonl")
	var entries []planEntry
	f, err := os.Create(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range requests {
		data, _ := json.Marshal(req)
		data = append(data, '\n')
		offset, _ := f.Seek(0, 1)
		entries = append(entries, planEntry{
			Offset: offset,
			Length: uint32(len(data)),
		})
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()

	plansDir := filepath.Join(dir, "plans")
	writePlanFile(t, plansDir, "m1", entries)

	inputFile, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer inputFile.Close()

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	cfg := config.NewConfig()
	cfg.RouteKeyMethod = config.RouteKeyMethodTenant
	cfg.ModelGateways = map[string]config.ModelGatewayConfig{
		"inferset-a/m1": {
			URL:                "http://gw-a:8000",
			InferenceObjective: "inferset-a-batch",
		},
	}

	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile: inputFile,
		PlansDir:  plansDir,
		ModelMap:  &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1},
		Resolver:  resolver,
		Cfg:       cfg,
		TenantID:  "inferset-a",
		Logger:    logr.Discard(),
	})

	out := make(chan pipeline.RequestItem, 10)
	if err := source.Produce(context.Background(), out); err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	var items []pipeline.RequestItem
	for item := range out {
		items = append(items, item)
	}

	if len(items) != 1 {
		t.Fatalf("produced %d items, want 1", len(items))
	}
	if items[0].ModelID != "inferset-a/m1" {
		t.Errorf("item 0 ModelID = %q, want tenant-scoped %q", items[0].ModelID, "inferset-a/m1")
	}
	// The body is forwarded verbatim: the runtime still sees the raw model name.
	if items[0].Body["model"] != "m1" {
		t.Errorf("item 0 body model = %v, want raw %q", items[0].Body["model"], "m1")
	}
	if items[0].Headers[inferenceObjectiveHeader] != "inferset-a-batch" {
		t.Errorf("objective header = %q, want %q", items[0].Headers[inferenceObjectiveHeader], "inferset-a-batch")
	}

	// With the default config (route_key_method empty), the same tenant must
	// not affect the lookup key — guards the backward-compatible behavior.
	cfgOff := config.NewConfig()
	inputFile2, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer inputFile2.Close()
	sourceOff := NewPlanFileSource(PlanFileSourceConfig{
		InputFile: inputFile2,
		PlansDir:  plansDir,
		ModelMap:  &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1},
		Resolver:  resolver,
		Cfg:       cfgOff,
		TenantID:  "inferset-a",
		Logger:    logr.Discard(),
	})
	outOff := make(chan pipeline.RequestItem, 10)
	if err := sourceOff.Produce(context.Background(), outOff); err != nil {
		t.Fatalf("Produce (route_key_method empty) error: %v", err)
	}
	for item := range outOff {
		if item.ModelID != "m1" {
			t.Errorf("route_key_method empty: ModelID = %q, want bare %q", item.ModelID, "m1")
		}
	}
}

func TestPlanFileSource_Produce_MultipleModels(t *testing.T) {
	dir := t.TempDir()

	requests := []batch_types.Request{
		{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1"}},
		{CustomID: "b", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m2"}},
		{CustomID: "c", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1"}},
	}

	inputPath := filepath.Join(dir, "input.jsonl")
	f, err := os.Create(inputPath)
	if err != nil {
		t.Fatal(err)
	}

	type lineInfo struct {
		offset int64
		length uint32
		model  string
	}
	var lines []lineInfo
	for _, req := range requests {
		data, _ := json.Marshal(req)
		data = append(data, '\n')
		offset, _ := f.Seek(0, 1)
		model := req.Body["model"].(string)
		lines = append(lines, lineInfo{offset: offset, length: uint32(len(data)), model: model})
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()

	plansDir := filepath.Join(dir, "plans")

	var m1Entries, m2Entries []planEntry
	for _, l := range lines {
		entry := planEntry{Offset: l.offset, Length: l.length}
		switch l.model {
		case "m1":
			m1Entries = append(m1Entries, entry)
		case "m2":
			m2Entries = append(m2Entries, entry)
		}
	}
	writePlanFile(t, plansDir, "m1", m1Entries)
	writePlanFile(t, plansDir, "m2", m2Entries)

	inputFile, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer inputFile.Close()

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile: inputFile,
		PlansDir:  plansDir,
		ModelMap: &modelMapFile{
			SafeToModel: map[string]string{"m1": "m1", "m2": "m2"},
			LineCount:   3,
		},
		Resolver: resolver,
		Cfg:      config.NewConfig(),
		Logger:   logr.Discard(),
	})

	out := make(chan pipeline.RequestItem, 10)
	if err := source.Produce(context.Background(), out); err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	var items []pipeline.RequestItem
	for item := range out {
		items = append(items, item)
	}

	if len(items) != 3 {
		t.Fatalf("produced %d items, want 3", len(items))
	}

	seenCustomIDs := make(map[string]bool)
	for _, item := range items {
		seenCustomIDs[item.CustomID] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !seenCustomIDs[want] {
			t.Errorf("missing custom_id %q", want)
		}
	}
}

func TestPlanFileSource_Produce_MalformedLine(t *testing.T) {
	dir := t.TempDir()

	// One valid line, one malformed line
	validReq := `{"custom_id":"c-1","method":"POST","url":"/v1/chat/completions","body":{"model":"m1"}}` + "\n"
	badLine := `{not valid json}` + "\n"

	inputPath := filepath.Join(dir, "input.jsonl")
	f, err := os.Create(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	var entries []planEntry

	// Write valid line
	offset, _ := f.Seek(0, 1)
	if _, err := f.WriteString(validReq); err != nil {
		t.Fatal(err)
	}
	entries = append(entries, planEntry{Offset: offset, Length: uint32(len(validReq))})

	// Write malformed line
	offset, _ = f.Seek(0, 1)
	if _, err := f.WriteString(badLine); err != nil {
		t.Fatal(err)
	}
	entries = append(entries, planEntry{Offset: offset, Length: uint32(len(badLine))})
	f.Close()

	plansDir := filepath.Join(dir, "plans")
	writePlanFile(t, plansDir, "m1", entries)

	inputFile, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer inputFile.Close()

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile: inputFile,
		PlansDir:  plansDir,
		ModelMap:  &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 2},
		Resolver:  resolver,
		Cfg:       config.NewConfig(),
		Logger:    logr.Discard(),
	})

	out := make(chan pipeline.RequestItem, 10)
	if err := source.Produce(context.Background(), out); err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	var items []pipeline.RequestItem
	for item := range out {
		items = append(items, item)
	}

	// Both lines must be produced — malformed one with ParseError set
	if len(items) != 2 {
		t.Fatalf("produced %d items, want 2 (valid + parse error)", len(items))
	}

	var valid, parseErr int
	for _, item := range items {
		if item.ParseError != nil {
			parseErr++
			if item.ParseError.Code != "parse_error" {
				t.Errorf("ParseError.Code = %q, want %q", item.ParseError.Code, "parse_error")
			}
			if item.RequestID == "" {
				t.Error("parse error item should have a RequestID")
			}
		} else {
			valid++
			if item.CustomID != "c-1" {
				t.Errorf("valid item CustomID = %q, want %q", item.CustomID, "c-1")
			}
		}
	}
	if valid != 1 || parseErr != 1 {
		t.Fatalf("valid=%d parseErr=%d, want valid=1 parseErr=1", valid, parseErr)
	}
}

func TestPlanFileSource_Produce_BadPlanFile(t *testing.T) {
	dir := t.TempDir()

	inputPath := filepath.Join(dir, "input.jsonl")
	if err := os.WriteFile(inputPath, []byte(`{"custom_id":"c-1"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	inputFile, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer inputFile.Close()

	plansDir := filepath.Join(dir, "plans")
	if err := os.MkdirAll(plansDir, 0o755); err != nil {
		t.Fatal(err)
	}

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile: inputFile,
		PlansDir:  plansDir,
		ModelMap:  &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1},
		Resolver:  resolver,
		Cfg:       config.NewConfig(),
		Logger:    logr.Discard(),
	})

	out := make(chan pipeline.RequestItem, 10)
	err = source.Produce(context.Background(), out)
	if err == nil {
		t.Fatal("expected error for missing plan file")
	}
}

// TestPlanFileSource_Produce_CancellationProducesAllEntries verifies that when
// the context is cancelled mid-produce, ALL plan entries still reach the output
// channel with their original custom_id so the dispatcher can drain them as
// cancelled without losing OpenAI request identity. If entries are dropped or
// get synthetic custom_ids, completed + failed may match total while clients
// cannot match error-file rows to their input.
func TestPlanFileSource_Produce_CancellationProducesAllEntries(t *testing.T) {
	const totalRequests = 10
	dir := t.TempDir()

	var requests []batch_types.Request
	for i := range totalRequests {
		requests = append(requests, batch_types.Request{
			CustomID: fmt.Sprintf("c-%d", i),
			Method:   "POST",
			URL:      "/v1/chat/completions",
			Body:     map[string]any{"model": "m1", "prompt": fmt.Sprintf("q%d", i)},
		})
	}

	inputPath := filepath.Join(dir, "input.jsonl")
	var entries []planEntry
	f, err := os.Create(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range requests {
		data, _ := json.Marshal(req)
		data = append(data, '\n')
		offset, _ := f.Seek(0, 1)
		entries = append(entries, planEntry{Offset: offset, Length: uint32(len(data))})
		if _, err := f.Write(data); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()

	plansDir := filepath.Join(dir, "plans")
	writePlanFile(t, plansDir, "m1", entries)

	inputFile, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	defer inputFile.Close()

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	// Cancel before Produce; identity must still come from the input lines.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile: inputFile,
		PlansDir:  plansDir,
		ModelMap:  &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: int64(totalRequests)},
		Resolver:  resolver,
		Cfg:       config.NewConfig(),
		Logger:    logr.Discard(),
	})

	out := make(chan pipeline.RequestItem, totalRequests+1)
	_ = source.Produce(ctx, out)

	seen := make(map[string]bool, totalRequests)
	for item := range out {
		if item.CustomID == item.RequestID {
			t.Errorf("custom_id %q equals request_id: cancel path must keep original input custom_id", item.CustomID)
		}
		if seen[item.CustomID] {
			t.Errorf("duplicate custom_id %q", item.CustomID)
		}
		seen[item.CustomID] = true
	}

	if len(seen) != totalRequests {
		t.Fatalf("produced %d unique custom_ids, want %d", len(seen), totalRequests)
	}
	for i := range totalRequests {
		want := fmt.Sprintf("c-%d", i)
		if !seen[want] {
			t.Errorf("missing custom_id %q after cancelled Produce", want)
		}
	}
}

func TestPlanFileSource_Produce_StorageRangedReads(t *testing.T) {
	dir := t.TempDir()

	requests := []batch_types.Request{
		{CustomID: "c-1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "hello"}},
		{CustomID: "c-2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "world"}},
	}

	var (
		rawInput bytes.Buffer
		entries  []planEntry
	)
	for _, req := range requests {
		data, _ := json.Marshal(req)
		data = append(data, '\n')
		offset := int64(rawInput.Len())
		entries = append(entries, planEntry{
			Offset: offset,
			Length: uint32(len(data)),
		})
		rawInput.Write(data)
	}

	plansDir := filepath.Join(dir, "plans")
	writePlanFile(t, plansDir, "m1", entries)

	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())
	storageName := "test-input-file.jsonl"
	folderName := "tenant_tenant-1"
	if _, err := filesClient.Store(context.Background(), storageName, folderName, 0, 0, bytes.NewReader(rawInput.Bytes())); err != nil {
		t.Fatalf("Store input: %v", err)
	}

	inputRef := &inputFileRef{
		storageName: storageName,
		folderName:  folderName,
	}

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	source := NewPlanFileSource(PlanFileSourceConfig{
		Storage:  filesClient,
		InputRef: inputRef,
		PlansDir: plansDir,
		ModelMap: &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 2},
		Resolver: resolver,
		Cfg:      config.NewConfig(),
		Logger:   logr.Discard(),
	})

	out := make(chan pipeline.RequestItem, 10)
	if err := source.Produce(context.Background(), out); err != nil {
		t.Fatalf("Produce error: %v", err)
	}

	var items []pipeline.RequestItem
	for item := range out {
		items = append(items, item)
	}

	if len(items) != 2 {
		t.Fatalf("produced %d items, want 2", len(items))
	}
	if items[0].CustomID != "c-1" {
		t.Errorf("item 0 CustomID = %q, want %q", items[0].CustomID, "c-1")
	}
	if items[1].CustomID != "c-2" {
		t.Errorf("item 1 CustomID = %q, want %q", items[1].CustomID, "c-2")
	}
	if items[0].ModelID != "m1" {
		t.Errorf("item 0 ModelID = %q, want %q", items[0].ModelID, "m1")
	}
	if items[0].RequestID == "" {
		t.Error("expected non-empty RequestID")
	}
}

func TestPlanFileSource_Produce_StorageRangedReads_Error(t *testing.T) {
	dir := t.TempDir()

	entries := []planEntry{
		{Offset: 0, Length: 50},
	}
	plansDir := filepath.Join(dir, "plans")
	writePlanFile(t, plansDir, "m1", entries)

	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())
	inputRef := &inputFileRef{
		storageName: "nonexistent.jsonl",
		folderName:  "tenant_tenant-1",
	}

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	source := NewPlanFileSource(PlanFileSourceConfig{
		Storage:  filesClient,
		InputRef: inputRef,
		PlansDir: plansDir,
		ModelMap: &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1},
		Resolver: resolver,
		Cfg:      config.NewConfig(),
		Logger:   logr.Discard(),
	})

	out := make(chan pipeline.RequestItem, 10)
	err := source.Produce(context.Background(), out)
	if err == nil {
		t.Fatal("expected error from non-existent storage file, got nil")
	}
	if !errors.Is(err, errRequestInputRead) {
		t.Fatalf("expected errRequestInputRead, got %v", err)
	}
}

// TestPlanFileSource_Produce_StorageRangedReads_CancelledContextStillProduces
// verifies that a cancelled context does not stop the storage-backed source
// from emitting every entry with its original custom_id. The dispatcher drains
// submitted-but-undispatched requests as batch_expired/batch_cancelled, which
// requires that identity — it only comes from reading the input line. Aborting
// the read on cancellation would drop entries and break batch accounting.
func TestPlanFileSource_Produce_StorageRangedReads_CancelledContextStillProduces(t *testing.T) {
	dir := t.TempDir()

	requests := []batch_types.Request{
		{CustomID: "c-1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "hello"}},
	}

	var rawInput bytes.Buffer
	data, _ := json.Marshal(requests[0])
	data = append(data, '\n')
	rawInput.Write(data)

	entries := []planEntry{
		{Offset: 0, Length: uint32(len(data))},
	}
	plansDir := filepath.Join(dir, "plans")
	writePlanFile(t, plansDir, "m1", entries)

	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())
	storageName := "test-input-file.jsonl"
	folderName := "tenant_tenant-1"
	if _, err := filesClient.Store(context.Background(), storageName, folderName, 0, 0, bytes.NewReader(rawInput.Bytes())); err != nil {
		t.Fatalf("Store input: %v", err)
	}

	inputRef := &inputFileRef{
		storageName: storageName,
		folderName:  folderName,
	}

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	source := NewPlanFileSource(PlanFileSourceConfig{
		Storage:  filesClient,
		InputRef: inputRef,
		PlansDir: plansDir,
		ModelMap: &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1},
		Resolver: resolver,
		Cfg:      config.NewConfig(),
		Logger:   logr.Discard(),
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	out := make(chan pipeline.RequestItem, 10)
	if err := source.Produce(ctx, out); err != nil {
		t.Fatalf("Produce with cancelled context must still enumerate for the drain path, got: %v", err)
	}

	var items []pipeline.RequestItem
	for item := range out {
		items = append(items, item)
	}
	if len(items) != 1 {
		t.Fatalf("produced %d items, want 1 (drain needs the custom_id even after cancel)", len(items))
	}
	if items[0].CustomID != "c-1" {
		t.Errorf("item 0 CustomID = %q, want %q", items[0].CustomID, "c-1")
	}
}
