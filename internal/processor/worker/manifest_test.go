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
	"strings"
	"testing"
)

func TestBuildBatchManifestStableIdentity(t *testing.T) {
	input := strings.Join([]string{
		`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":"model-a"}}`,
		`{"custom_id":"b","method":"POST","url":"/v1/chat/completions","body":{"model":"model-b"}}`,
	}, "\n") + "\n"

	first, err := buildBatchManifest("batch-1", strings.NewReader(input))
	if err != nil {
		t.Fatalf("buildBatchManifest: %v", err)
	}
	second, err := buildBatchManifest("batch-1", strings.NewReader(input))
	if err != nil {
		t.Fatalf("buildBatchManifest again: %v", err)
	}
	if len(first.Entries) != 2 {
		t.Fatalf("entries=%d, want 2", len(first.Entries))
	}
	if first.Entries[0].RequestID != second.Entries[0].RequestID {
		t.Fatalf("request identity changed: %q != %q", first.Entries[0].RequestID, second.Entries[0].RequestID)
	}
	if first.Entries[0].RequestID == first.Entries[1].RequestID {
		t.Fatal("distinct custom IDs produced the same request ID")
	}
	if first.Entries[0].ModelID != "model-a" || first.Entries[1].Ordinal != 1 {
		t.Fatalf("unexpected entries: %#v", first.Entries)
	}
}

func TestBuildBatchManifestRejectsDuplicateCustomID(t *testing.T) {
	input := `{"custom_id":"dup","body":{"model":"m"}}` + "\n" +
		`{"custom_id":"dup","body":{"model":"m"}}` + "\n"
	if _, err := buildBatchManifest("batch-1", strings.NewReader(input)); err == nil {
		t.Fatal("expected duplicate custom_id error")
	}
}
