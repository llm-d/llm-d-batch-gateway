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

// Package simulation runs the consistency simulation harness: real stores,
// real gateway binaries with armed failpoints, and invariant checks over the
// observable state. See docs/design/consistency-harness.md.
package simulation

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const readyTimeout = 5 * time.Minute

// apiBase is the apiserver base URL; set by the active backend.
var apiBase = "http://127.0.0.1:18080"

// harness is the scenario-facing API over the active stack backend.
type harness struct {
	t   *testing.T
	b   stackBackend
	rec *recorder
}

func newHarness(t *testing.T, env map[string]string) *harness {
	t.Helper()
	if env == nil {
		env = map[string]string{}
	}
	rec := newRecorder(t)
	b := selectBackend(t)
	apiBase = b.apiURL()
	h := &harness{t: t, b: b, rec: rec}

	rec.event("stack-up", map[string]any{"env": env, "backend": backendName()})
	start := time.Now().Add(-time.Minute)
	b.ensureUp(env)
	h.waitAPIReady()
	rec.event("stack-ready", nil)

	// Registered after the backend's own teardown cleanup, so these run
	// first (LIFO) while the stack is still up.
	t.Cleanup(func() {
		b.harvest(filepath.Join(filepath.Dir(rec.path), "traces"), start)
		if t.Failed() {
			t.Logf("=== component logs ===\n%s", b.dumpLogs())
		}
	})
	return h
}

// setEnv changes an abstract knob (e.g. failpoint arming); effective at the
// next restart of the component it affects.
func (h *harness) setEnv(key, value string) {
	h.rec.event("env-change", map[string]any{"key": key, "value": value})
	h.b.applyEnv(key, value)
}

func (h *harness) restart(service string) {
	h.t.Helper()
	h.rec.event("service-restart", map[string]any{"service": service})
	h.b.restart(service)
	if service == "apiserver" {
		h.waitAPIReady()
	}
}

func (h *harness) stop(service string) {
	h.t.Helper()
	h.rec.event("service-stop", map[string]any{"service": service})
	h.b.stop(service)
}

// kill SIGKILLs the service's container or pod, simulating an OOM kill or
// node eviction; local state is gone when the replacement starts.
func (h *harness) kill(service string) {
	h.t.Helper()
	h.rec.event("service-kill", map[string]any{"service": service})
	h.b.kill(service)
}

func (h *harness) waitAPIReady() {
	h.t.Helper()
	deadline := time.Now().Add(readyTimeout)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, apiBase+"/v1/batches", nil)
		if err != nil {
			h.t.Fatalf("build readiness request: %v", err)
		}
		req.Header.Set(tenantHeader, tenantID)
		resp, err := newHTTPClient(5 * time.Second).Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	h.t.Fatalf("apiserver did not become ready within %s", readyTimeout)
}

// simModel is the model id every simulation request targets. It must match
// what the backend's inference layer serves: the vcr frontend's MODEL in
// compose, the vllm-sim model name on kind.
func simModel() string {
	if m := os.Getenv("SIM_MODEL"); m != "" {
		return m
	}
	if backendName() == "kind" {
		return "sim-model"
	}
	return "Qwen/Qwen2.5-0.5B-Instruct"
}

// inputJSONL builds an n-line batch input where each request generates
// exactly maxTokens tokens under the backend's latency model. The vcr engine
// emits random token ids, so without ignore_eos a sampled EOS ends
// generation after a few tokens and requests finish near-instantly.
func inputJSONL(n, maxTokens int) string {
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b,
			`{"custom_id":"sim-%d","method":"POST","url":"/v1/chat/completions","body":{"model":"%s","max_tokens":%d,"min_tokens":%d,"ignore_eos":true,"messages":[{"role":"user","content":"simulation request %d"}]}}`+"\n",
			i, simModel(), maxTokens, maxTokens, i)
	}
	return b.String()
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
