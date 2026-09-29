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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// TestDuplicateExecution holds a dequeued job past two reconciler cycles
// before it starts running. Before dequeue and ownership were one statement,
// the held job was invisible (absent from the queue, still validating, no
// in-flight entry), the reconciler re-enqueued it, and it executed twice.
//
// Invariant (single execution): requests served by the engine for a batch
// must not exceed its line count.
func TestDuplicateExecution(t *testing.T) {
	const scenario = "duplicate_execution"
	const lines = 4
	// The window outlasts two reconciler cycles while the dequeue is held.
	window := 2*params().ReconcilerInterval + 5*time.Second
	h := newHarness(t, map[string]string{
		"PROCESSOR_FAILPOINTS": fmt.Sprintf("processor/after-dequeue=sleep(%d)", window.Milliseconds()),
	})
	baseline := h.inferenceWitness()
	client := newAPIClient()

	// The duplicate is dequeued right after the first launch and held for a
	// second window; the dequeue gate accepts validating and in_progress, so
	// the first execution must still be running when the duplicate wakes.
	// ~27s generations outlast the window with margin.
	fileID, err := client.uploadFile("duplicate-execution.jsonl", inputJSONL(lines, 900))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	ctx, cancelObs := context.WithCancel(context.Background())
	defer cancelObs()
	tl := observe(ctx, client, batch.ID, h.rec)

	// Dequeue, hold the window, run; then wait out a second window in case
	// a duplicate was launched.
	if _, ok := waitForStatus(client, batch.ID, 2*window+3*time.Minute,
		openai.BatchStatusCompleted, openai.BatchStatusFailed, openai.BatchStatusExpired); !ok {
		t.Fatal("batch never terminalized")
	}

	// Wait out the duplicate's window plus its execution.
	served := 0
	deadline := time.Now().Add(window + 2*time.Minute)
	for time.Now().Before(deadline) {
		served = h.inferenceWitness() - baseline
		if served > lines {
			break
		}
		time.Sleep(2 * time.Second)
	}
	h.rec.event("witness", map[string]any{"served": served, "lines": lines})
	time.Sleep(1 * time.Second)
	cancelObs()

	var msgs []string
	for _, v := range tl.checkTransitions() {
		msgs = append(msgs, v.String())
	}
	detail := fmt.Sprintf("engine served %d requests for a %d-line batch; sequence %v; transition violations %v",
		served, lines, tl.statuses(), msgs)
	judge(t, scenario, served > lines, detail)
}
