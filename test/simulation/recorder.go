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
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// recorder writes a JSONL event stream for one scenario run under
// test/simulation/artifacts/<scenario>-<start>/events.jsonl. The stream
// interleaves harness actions (failpoints armed, services restarted), API
// interactions, and observed status transitions, so a run can be replayed
// into visualizations. Component-side detail lives in the OTLP traces the
// stack ships to Tempo; this file is the external observer's view.
type recorder struct {
	mu   sync.Mutex
	file *os.File
	enc  *json.Encoder
	path string
}

type recordedEvent struct {
	At     time.Time      `json:"at"`
	Kind   string         `json:"kind"`
	Fields map[string]any `json:"fields,omitempty"`
}

func newRecorder(t *testing.T) *recorder {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	dir := filepath.Join(filepath.Dir(thisFile), "artifacts",
		fmt.Sprintf("%s-%d", t.Name(), time.Now().Unix()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create artifacts dir: %v", err)
	}
	path := filepath.Join(dir, "events.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create event log: %v", err)
	}
	r := &recorder{file: file, enc: json.NewEncoder(file), path: path}
	t.Cleanup(func() {
		if err := file.Close(); err != nil {
			t.Logf("close event log: %v", err)
		}
		t.Logf("scenario event log: %s (traces: http://127.0.0.1:13200)", path)
	})
	return r
}

// event appends one entry to the stream. Failures are reported once via the
// test log rather than failing the scenario; the recording is a byproduct,
// not the subject under test.
func (r *recorder) event(kind string, fields map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.enc.Encode(recordedEvent{At: time.Now().UTC(), Kind: kind, Fields: fields}); err != nil {
		fmt.Fprintf(os.Stderr, "recorder: %v\n", err)
	}
}
