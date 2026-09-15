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
	"encoding/json"
	"hash/fnv"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
)

func TestContentPartsPreprocessingAndForwarding(t *testing.T) {
	cases := []struct{ name, content, text string }{
		{"string", `"hello world"`, "hello world"},
		{"text parts", `[{"type":"text","text":"hello "},{"type":"text","text":"world"}]`, "hello world"},
		{"empty string", `""`, ""},
		{"empty array", `[]`, ""},
		{"null", `null`, ""},
		{"non-text part", `[{"type":"image_url","image_url":{"url":"https://example.com/image.png"}}]`, ""},
		{"mixed parts", `[{"type":"text","text":"hello world"},{"type":"image_url","image_url":{"url":"https://example.com/image.png"}}]`, "hello world"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, role := range []string{"system", "user", "assistant"} {
				t.Run(role, func(t *testing.T) {
					line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"text-model","messages":[{"role":"` + role + `","content":` + tc.content + `}]}}` + "\n")
					meta, err := extractAndValidateLine(line)
					checkContentError(t, err)
					expectedHash := NoPrefixHash
					if role == "system" && tc.text != "" {
						h := fnv.New32a()
						_, err = h.Write([]byte(tc.text))
						checkContentError(t, err)
						expectedHash = h.Sum32()
					}
					if expectedHash != meta.PrefixHash {
						t.Errorf("prefix hash = %d, want %d", meta.PrefixHash, expectedHash)
					}
					root := t.TempDir()
					acc := newPlanAccumulator(root)
					mapping := map[string]string{}
					accumulatePlanEntry(acc, meta.ModelID, mapping, map[string]int{}, 0, uint32(len(line)), meta.PrefixHash)
					checkContentError(t, finalizePlanFiles(acc, mapping))
					checkContentError(t, writeModelMappings(root, mapping, 1, 0))
					mm, err := readModelMap(root)
					checkContentError(t, err)
					path := filepath.Join(root, "input.jsonl")
					checkContentError(t, os.WriteFile(path, line, 0600))
					f, err := os.Open(path)
					checkContentError(t, err)
					defer f.Close()
					src := NewPlanFileSource(PlanFileSourceConfig{InputFile: f, PlansDir: filepath.Join(root, "plans"), ModelMap: mm, Cfg: config.NewConfig(), Logger: logr.Discard()})
					ch := make(chan pipeline.RequestItem, 2)
					checkContentError(t, src.Produce(context.Background(), ch))
					item, ok := <-ch
					if !ok {
						t.Fatal("missing request")
					}
					var original struct {
						Body map[string]any `json:"body"`
					}
					checkContentError(t, json.Unmarshal(line, &original))
					if !reflect.DeepEqual(original.Body, item.Body) {
						t.Errorf("body changed: got %#v, want %#v", item.Body, original.Body)
					}
					_, ok = <-ch
					if ok {
						t.Fatal("unexpected extra request")
					}
				})
			}
		})
	}
}

func checkContentError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestContentPartsRejectInvalidSystemContent(t *testing.T) {
	for _, content := range []string{`42`, `true`, `{}`, `[42]`, `[{"type":"text","text":42}]`} {
		t.Run(content, func(t *testing.T) {
			line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"system","content":` + content + `}]}}`)
			if _, err := extractAndValidateLine(line); err == nil {
				t.Fatal("accepted invalid message content")
			}
		})
	}
}

func BenchmarkContentPreprocessing(b *testing.B) {
	for _, tc := range []struct{ name, content string }{
		{"string", `"` + strings.Repeat("user prompt ", 100) + `"`},
		{"text_parts", `[` + strings.TrimSuffix(strings.Repeat(`{"type":"text","text":"user prompt "},`, 100), ",") + `]`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"system","content":"system prompt"},{"role":"user","content":` + tc.content + `}]}}`)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := extractAndValidateLine(line); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestContentPartsSkipNonSystemDecoding(t *testing.T) {
	// Metadata extraction leaves non-system content to the inference endpoint.
	line := []byte(`{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":{"future_content_type":true}},{"role":"system"},{"role":"system","content":null},{"role":"system","content":[]},{"role":"system","content":[{"type":"text","text":"hello"}]},{"role":"system","content":42}]}}`)
	meta, err := extractAndValidateLine(line)
	checkContentError(t, err)
	h := fnv.New32a()
	_, err = h.Write([]byte("hello"))
	checkContentError(t, err)
	if meta.PrefixHash != h.Sum32() {
		t.Errorf("prefix hash = %d, want %d", meta.PrefixHash, h.Sum32())
	}
}
