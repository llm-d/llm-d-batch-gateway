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
	"encoding/json"
	"hash/fnv"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

func TestGroupFirstUserMessageBoundaries(t *testing.T) {
	cases := []struct{ name, messages, selected string }{
		{"first string", `[{"role":"user","content":"passage"},{"role":"user","content":"question"}]`, "passage"},
		{"system wins", `[{"role":"user","content":"passage"},{"role":"system","content":"system"}]`, "system"},
		{"system text parts win", `[{"role":"user","content":"passage"},{"role":"system","content":[{"type":"text","text":"system"}]}]`, "system"},
		{"assistant first", `[{"role":"assistant","content":"history"},{"role":"user","content":"passage"}]`, ""},
		{"empty system first", `[{"role":"system","content":""},{"role":"user","content":"passage"}]`, ""},
		{"multimodal user", `[{"role":"user","content":[{"type":"text","text":"passage"},{"type":"image_url","image_url":{"url":"x"}}]}]`, ""},
		{"user text parts", `[{"role":"user","content":[{"type":"text","text":"passage"}]}]`, ""},
		{"empty user", `[{"role":"user","content":""}]`, ""},
		{"missing user content", `[{"role":"user"}]`, ""},
		{"user followed by empty system", `[{"role":"user","content":"passage"},{"role":"system","content":""}]`, "passage"},
		{"first nonempty system wins", `[{"role":"user","content":"passage"},{"role":"system","content":""},{"role":"system","content":"system"}]`, "system"},
		{"null user", `[{"role":"user","content":null}]`, ""},
		{"other user content", `[{"role":"user","content":{"future":true}}]`, ""},
		{"no messages", `[]`, ""},
	}
	for _, enabled := range []bool{false, true} {
		for _, tc := range cases {
			t.Run(tc.name+map[bool]string{false: "/disabled", true: "/enabled"}[enabled], func(t *testing.T) {
				line := []byte(`{"custom_id":"r","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":` + tc.messages + `}}`)
				meta, err := extractAndValidateLine(line, openai.EndpointAllowlist{}, "/v1/chat/completions", enabled)
				if err != nil {
					t.Fatal(err)
				}
				want := NoPrefixHash
				if tc.selected != "" && (enabled || tc.name == "system wins" || tc.name == "system text parts win" || tc.name == "first nonempty system wins") {
					h := fnv.New32a()
					h.Write([]byte(tc.selected))
					want = h.Sum32()
				}
				if meta.PrefixHash != want {
					t.Fatalf("hash = %d, want %d", meta.PrefixHash, want)
				}
			})
		}
	}
}

func TestGroupFirstUserMessageConsumer(t *testing.T) {
	cases := []struct {
		name, yaml      string
		enabled, system bool
	}{
		{name: "omitted"},
		{name: "explicit false", yaml: "group_first_user_message: false\n"},
		{name: "enabled", yaml: "group_first_user_message: true\n", enabled: true},
		{name: "system precedence", yaml: "group_first_user_message: true\n", enabled: true, system: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testLoggerCtx(t)
			cfg := config.NewConfig()
			configPath := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(configPath, []byte("route_key_method: tenant\nsend_fairness_header: true\n"+tc.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			if err := cfg.LoadFromYAML(configPath); err != nil {
				t.Fatal(err)
			}
			if cfg.GroupFirstUserMessage != tc.enabled {
				t.Fatalf("loaded setting = %v, want %v", cfg.GroupFirstUserMessage, tc.enabled)
			}
			cfg.WorkDir = t.TempDir()
			tenant, jobID, fileID := "tenant__prefix", "job-prefix", "file-prefix"
			route := tenant + "/model-a"
			cfg.ModelGateways = map[string]config.ModelGatewayConfig{route: {InferenceObjective: "owned-objective"}}
			resolver, err := inference.NewPerModelResolver(map[string]inference.GatewayClientConfig{route: {URL: "http://unused.invalid"}}, testLogger(t))
			if err != nil {
				t.Fatal(err)
			}
			defer resolver.Close()
			files := mockfiles.NewMockBatchFilesClient(t.TempDir())
			fileDB := newMockFileDBClient()
			ids := []string{"a1", "b1", "a2", "b2"}
			first := []string{"passage A", "passage B", "passage A", "passage B"}
			systems := []string{"system X", "system Y", "system Y", "system X"}
			bodies := map[string]map[string]any{}
			selected := map[string]string{}
			var input bytes.Buffer
			for i, id := range ids {
				messages := []map[string]any{{"role": "user", "content": first[i]}, {"role": "user", "content": id}}
				if tc.system {
					messages = append(messages, map[string]any{"role": "system", "content": systems[i]})
				}
				body := map[string]any{"model": "model-a", "messages": messages, "temperature": 0, "chat_template_kwargs": map[string]bool{"enable_thinking": false}}
				line, err := json.Marshal(map[string]any{"custom_id": id, "method": "POST", "url": "/v1/chat/completions", "body": body})
				if err != nil {
					t.Fatal(err)
				}
				input.Write(line)
				input.WriteByte('\n')
				var original struct {
					Body map[string]any `json:"body"`
				}
				if err := json.Unmarshal(line, &original); err != nil {
					t.Fatal(err)
				}
				bodies[id] = original.Body
				selected[id] = first[i]
				if tc.system {
					selected[id] = systems[i]
				}
			}
			folder, err := ucom.GetFolderNameByTenantID(tenant)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := files.Store(ctx, ucom.FileStorageName(fileID, "input.jsonl"), folder, 0, 0, bytes.NewReader(input.Bytes())); err != nil {
				t.Fatal(err)
			}
			if err := fileDB.DBStore(ctx, &db.FileItem{BaseIndexes: db.BaseIndexes{ID: fileID, TenantID: tenant}, BaseContents: db.BaseContents{Spec: mustJSON(t, &openai.FileObject{Filename: "input.jsonl"})}}); err != nil {
				t.Fatal(err)
			}
			p := mustNewProcessor(t, cfg, &clientset.Clientset{BatchDB: newMockBatchDBClient(), FileDB: fileDB, File: files, Inference: resolver})
			manifest, err := p.preProcessJob(ctx, &batch_types.JobInfo{JobID: jobID, TenantID: tenant, BatchJob: &openai.Batch{ID: jobID, BatchSpec: openai.BatchSpec{InputFileID: fileID, Endpoint: openai.EndpointChatCompletions}}})
			if err != nil {
				t.Fatal(err)
			}
			if len(manifest.Entries) != len(ids) {
				t.Fatalf("manifest count = %d", len(manifest.Entries))
			}
			for i, entry := range manifest.Entries {
				if entry.CustomID != ids[i] || entry.Ordinal != int64(i) || entry.RequestID != stableBatchRequestID(jobID, ids[i]) || entry.Rejected {
					t.Fatalf("manifest changed: %+v", entry)
				}
			}
			root, err := p.jobRootDir(jobID, tenant)
			if err != nil {
				t.Fatal(err)
			}
			mm, err := readModelMap(root)
			if err != nil {
				t.Fatal(err)
			}
			inputPath, err := p.jobInputFilePath(jobID, tenant)
			if err != nil {
				t.Fatal(err)
			}
			local, err := os.ReadFile(inputPath)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(local, input.Bytes()) {
				t.Fatal("input bytes changed")
			}
			entries, err := readPlanEntries(filepath.Join(root, "plans", mm.ModelToSafe["model-a"]+".plan"))
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != len(ids) {
				t.Fatalf("plan count = %d", len(entries))
			}
			for _, entry := range entries {
				var request struct {
					CustomID string `json:"custom_id"`
				}
				if err := json.Unmarshal(local[entry.Offset:entry.Offset+int64(entry.Length)], &request); err != nil {
					t.Fatal(err)
				}
				want := NoPrefixHash
				if tc.enabled || tc.system {
					h := fnv.New32a()
					h.Write([]byte(selected[request.CustomID]))
					want = h.Sum32()
				}
				if entry.PrefixHash != want {
					t.Fatalf("plan hash = %d, want %d for %s", entry.PrefixHash, want, request.CustomID)
				}
			}
			if tc.enabled && (entries[0].PrefixHash != entries[1].PrefixHash || entries[2].PrefixHash != entries[3].PrefixHash || entries[0].PrefixHash == entries[2].PrefixHash) {
				t.Fatal("selected prefixes not grouped")
			}
			for _, terminal := range []bool{false, true} {
				f, err := os.Open(inputPath)
				if err != nil {
					t.Fatal(err)
				}
				source := NewPlanFileSource(PlanFileSourceConfig{InputFile: f, PlansDir: filepath.Join(root, "plans"), ModelMap: mm, Resolver: resolver, Cfg: cfg, TenantID: tenant, BatchID: jobID, Logger: testLogger(t), Skip: func(id string) bool { return terminal && id == stableBatchRequestID(jobID, "a2") }})
				out := make(chan pipeline.RequestItem, len(ids))
				if err := source.Produce(ctx, out); err != nil {
					f.Close()
					t.Fatal(err)
				}
				if err := f.Close(); err != nil {
					t.Fatal(err)
				}
				seen := map[string]bool{}
				for item := range out {
					if seen[item.CustomID] || bodies[item.CustomID] == nil || !reflect.DeepEqual(item.Body, bodies[item.CustomID]) || item.RequestID != stableBatchRequestID(jobID, item.CustomID) || item.ModelID != route || item.Endpoint != "/v1/chat/completions" || item.Headers[inferenceObjectiveHeader] != "owned-objective" || item.Headers[fairnessIDHeader] != tenant {
						t.Fatalf("request changed: %+v", item)
					}
					seen[item.CustomID] = true
				}
				want := len(ids)
				if terminal {
					want--
					if seen["a2"] {
						t.Fatal("terminal checkpoint emitted")
					}
				}
				if len(seen) != want {
					t.Fatalf("produced %d, want %d", len(seen), want)
				}
			}
		})
	}
}

func TestGroupFirstUserMessagePreservesValidation(t *testing.T) {
	for _, line := range []string{
		`{"custom_id":"r","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"passage"},{"role":"system","content":42}]}}`,
		`{"custom_id":"r","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"passage"}],"stream":true}}`,
		`{"custom_id":"r","method":"POST","url":"/v1/chat/completions","body":`,
	} {
		if _, err := extractAndValidateLine([]byte(line), openai.EndpointAllowlist{}, "/v1/chat/completions", true); err == nil {
			t.Fatalf("grouping bypassed request validation: %s", line)
		}
	}
}
