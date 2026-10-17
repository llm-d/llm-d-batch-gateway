package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"

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
		cfg.DispatchMode = config.DispatchModeSync
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
		cfg.DispatchMode = config.DispatchModeSync
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
		cfg.DispatchMode = config.DispatchModeSync
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
	cfg.DispatchMode = config.DispatchModeSync
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
	cfgOff.DispatchMode = config.DispatchModeSync
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

// writeThreeRequestPlan writes an input file and plan with three requests
// and returns the open input file plus the stable request IDs.
func writeThreeRequestPlan(t *testing.T, batchID string) (*os.File, string, []string) {
	t.Helper()
	dir := t.TempDir()
	requests := []batch_types.Request{
		{CustomID: "c-1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "a"}},
		{CustomID: "c-2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "b"}},
		{CustomID: "c-3", Method: "POST", URL: "/v1/chat/completions", Body: map[string]any{"model": "m1", "prompt": "c"}},
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
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	writePlanFile(t, filepath.Join(dir, "plans"), "m1", entries)
	inputFile, err := os.Open(inputPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inputFile.Close() })
	ids := []string{
		stableBatchRequestID(batchID, "c-1"),
		stableBatchRequestID(batchID, "c-2"),
		stableBatchRequestID(batchID, "c-3"),
	}
	return inputFile, dir, ids
}

// TestPlanFileSource_Produce_RecoversInFlightEntries pins the takeover
// contract: an in-flight entry is pre-stored into the pending set
// immediately — so its stranded delivery routes to this job — while its
// re-submission waits out the recovery window, and only entries with no
// delivery by then fall back to submission.
func TestPlanFileSource_Produce_RecoversInFlightEntries(t *testing.T) {
	const batchID = "batch-recover"
	inputFile, dir, ids := writeThreeRequestPlan(t, batchID)

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	preStored := make(chan pipeline.RequestItem, 1)
	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile:      inputFile,
		PlansDir:       filepath.Join(dir, "plans"),
		ModelMap:       &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 3},
		Resolver:       resolver,
		Cfg:            config.NewConfig(),
		BatchID:        batchID,
		Recover:        map[string]struct{}{ids[2]: {}},
		RecoveryWindow: 400 * time.Millisecond,
		PreStore:       func(item pipeline.RequestItem) { preStored <- item },
		Logger:         logr.Discard(),
	})

	// The recovering entry is pre-stored before Produce — and before the
	// recovery broadcasters start claiming — so its delivery routes to this
	// job from the start.
	if err := source.PreStoreRecovering(); err != nil {
		t.Fatalf("PreStoreRecovering: %v", err)
	}
	select {
	case item := <-preStored:
		if item.CustomID != "c-3" || item.RequestID != ids[2] {
			t.Fatalf("pre-stored = %q/%q, want c-3/%s", item.CustomID, item.RequestID, ids[2])
		}
	default:
		t.Fatal("recovering entry was not pre-stored")
	}

	out := make(chan pipeline.RequestItem)
	done := make(chan error, 1)
	go func() { done <- source.Produce(context.Background(), out) }()

	// The two fresh entries stream immediately.
	for i := range 2 {
		select {
		case item := <-out:
			if item.CustomID != fmt.Sprintf("c-%d", i+1) {
				t.Fatalf("fresh item %d = %q", i, item.CustomID)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("fresh entry %d was not produced promptly", i)
		}
	}

	// The recovering entry is not submitted before the window elapses.
	select {
	case item := <-out:
		t.Fatalf("recovering entry %q submitted before the recovery window elapsed", item.CustomID)
	case <-time.After(100 * time.Millisecond):
	}

	// After the window the unrecovered entry falls back to submission.
	select {
	case item := <-out:
		if item.CustomID != "c-3" {
			t.Fatalf("deferred item = %q, want c-3", item.CustomID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("recovering entry was not submitted after the recovery window")
	}
	if err := <-done; err != nil {
		t.Fatalf("Produce: %v", err)
	}
}

// TestPlanFileSource_Produce_RecoveryWindowSLOCap verifies a batch whose
// SLO budget is shorter than the recovery window submits the held entries
// when the SLO caps the wait, not after the full window.
func TestPlanFileSource_Produce_RecoveryWindowSLOCap(t *testing.T) {
	const batchID = "batch-slo"
	inputFile, dir, ids := writeThreeRequestPlan(t, batchID)

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile:      inputFile,
		PlansDir:       filepath.Join(dir, "plans"),
		ModelMap:       &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 3},
		Resolver:       resolver,
		Cfg:            config.NewConfig(),
		BatchID:        batchID,
		SLODeadline:    time.Now().Add(150 * time.Millisecond),
		Recover:        map[string]struct{}{ids[2]: {}},
		RecoveryWindow: 30 * time.Second,
		Logger:         logr.Discard(),
	})

	out := make(chan pipeline.RequestItem)
	done := make(chan error, 1)
	go func() { done <- source.Produce(context.Background(), out) }()

	deadline := time.Now().Add(2 * time.Second)
	for range 3 {
		select {
		case <-out:
		case <-time.After(time.Until(deadline)):
			t.Fatal("held entries were not submitted within the SLO-capped window")
		}
	}
	if err := <-done; err != nil {
		t.Fatalf("Produce: %v", err)
	}
}

// TestPlanFileSource_Produce_RecoveryWindowEarlyExit pins the happy-path
// cost model: when the stranded delivery of every held entry is received and
// checkpointed during the window (its pending-set entry resolves), the
// source exits the window early instead of idling out the full duration —
// the fence would drop the held items as terminal duplicates anyway.
func TestPlanFileSource_Produce_RecoveryWindowEarlyExit(t *testing.T) {
	const batchID = "batch-early-exit"
	inputFile, dir, ids := writeThreeRequestPlan(t, batchID)

	client := &mockInferenceClient{}
	resolver := inference.NewSingleClientResolver(client)
	defer func() { _ = resolver.Close() }()

	pending := pipeline.NewPendingRequests(3)
	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile:      inputFile,
		PlansDir:       filepath.Join(dir, "plans"),
		ModelMap:       &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 3},
		Resolver:       resolver,
		Cfg:            config.NewConfig(),
		BatchID:        batchID,
		Recover:        map[string]struct{}{ids[2]: {}},
		RecoveryWindow: 30 * time.Second,
		PreStore:       pending.Store,
		StillPending:   pending.Has,
		Logger:         logr.Discard(),
	})
	if err := source.PreStoreRecovering(); err != nil {
		t.Fatalf("PreStoreRecovering: %v", err)
	}
	if !pending.Has(ids[2]) {
		t.Fatal("recovering entry not pre-stored into the pending set")
	}

	out := make(chan pipeline.RequestItem)
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- source.Produce(context.Background(), out) }()

	// The two fresh entries stream immediately.
	for range 2 {
		select {
		case <-out:
		case <-time.After(2 * time.Second):
			t.Fatal("fresh entries were not produced promptly")
		}
	}

	// The recovering entry is pre-stored; its stranded delivery arrives and
	// is checkpointed, resolving it out of the pending set.
	select {
	case item := <-out:
		t.Fatalf("recovering entry %q submitted before recovery concluded", item.CustomID)
	case <-time.After(100 * time.Millisecond):
	}
	if !pending.Resolve(&pipeline.ResultItem{RequestID: ids[2]}) {
		t.Fatal("expected the pre-stored request to resolve")
	}

	// The window exits early: the held item is emitted well within the
	// configured 30s window (the fence will drop it as a terminal
	// duplicate downstream).
	select {
	case item := <-out:
		if item.CustomID != "c-3" {
			t.Fatalf("held item = %q, want c-3", item.CustomID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("recovery window did not exit early after every held entry resolved")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("early exit took %v, want well under the 30s window", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatalf("Produce: %v", err)
	}
}
