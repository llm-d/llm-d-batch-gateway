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

// TestF1bEnqueueFalseFailure reproduces finding F1b: PQEnqueue lands in Redis
// but the response is lost, so the apiserver times out, runs the compensating
// DBDelete, and returns 500. The processor dequeues the phantom entry long
// before the slow timeout fires, fetches the still-present row, and starts
// executing; the compensation then deletes the batch row out from under a
// running job. The client was told the create failed, yet inference ran and
// billed, and the batch has vanished from the API.
//
// Violated invariant (API honesty): a create that returned 5xx must never
// produce a batch that executes.
func TestF1bEnqueueFalseFailure(t *testing.T) {
	const scenario = "F1b_enqueue_false_failure"
	h := newHarness(t, nil)
	baseline := h.inferenceWitness()
	tox := h.toxics()
	client := newAPIClient().withTimeout(90 * time.Second)

	fileID, err := client.uploadFile("f1b.jsonl", inputJSONL(4, 300))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}

	// The enqueue command flows upstream and executes; the reply never comes
	// back, so the redis client times out, retries (ZADD NX is idempotent),
	// and finally surfaces an error to the create handler.
	h.blackholeResponses(tox, proxyAPIServerRedis)
	if _, err := client.createBatch(fileID, "24h"); err == nil {
		t.Fatal("batch create succeeded; expected the blackholed enqueue response to fail it")
	}
	h.rec.event("create-failed", map[string]any{"file": fileID})
	h.healToxics(tox)

	// The worker's heartbeat notices the deleted row within seconds and
	// aborts; give in-flight generations time to stop reaching the engine.
	time.Sleep(15 * time.Second)

	batches, err := client.listBatches()
	if err != nil {
		t.Fatalf("list batches: %v", err)
	}
	served := h.inferenceWitness() - baseline
	h.rec.event("witness", map[string]any{"served": served, "rows": len(batches)})
	detail := fmt.Sprintf("engine served %d requests for a create that returned 5xx; %d batch rows remain",
		served, len(batches))
	judge(t, scenario, served > 0, detail)
}

// TestF1cCreateCompensationPartition reproduces finding F1c: the enqueue
// fails AND the compensating DBDelete fails, because the apiserver is
// partitioned from both stores after the row was stored. A sleep failpoint
// holds the DBStore->PQEnqueue window open while the partition is applied.
// The client gets 500 and the row survives unqueued, so the reconciler later
// re-enqueues it as an orphaned validating job and it runs, same class of
// violation as F1a but reachable by network faults alone once the create
// becomes transactional.
//
// Violated invariant (API honesty): a create that returned 5xx must never
// produce a batch that executes.
func TestF1cCreateCompensationPartition(t *testing.T) {
	const scenario = "F1c_create_compensation_partition"
	h := newHarness(t, map[string]string{
		"APISERVER_FAILPOINTS": "apiserver/after-batch-dbstore=sleep(6000)",
	})
	tox := h.toxics()
	client := newAPIClient().withTimeout(90 * time.Second)

	fileID, err := client.uploadFile("f1c.jsonl", inputJSONL(2, 10))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}

	createErr := make(chan error, 1)
	go func() {
		_, err := client.createBatch(fileID, "24h")
		createErr <- err
	}()

	// The row is stored within the first moments of the request; partition
	// the apiserver from both stores while the failpoint sleeps, so the
	// enqueue and then the compensating delete both fail fast.
	time.Sleep(1500 * time.Millisecond)
	h.resetConnections(tox, proxyAPIServerRedis)
	h.resetConnections(tox, proxyAPIServerPostgres)

	if err := <-createErr; err == nil {
		t.Fatal("batch create succeeded; expected the partition to fail the enqueue")
	}
	h.rec.event("create-failed", map[string]any{"file": fileID})
	h.healToxics(tox)

	batches, err := client.listBatches()
	if err != nil {
		t.Fatalf("list batches: %v", err)
	}
	if len(batches) == 0 {
		judge(t, scenario, false, "no batch row survived the failed create")
		return
	}
	if len(batches) > 1 {
		t.Fatalf("expected at most one batch, found %d", len(batches))
	}
	batchID := batches[0].ID
	ctx, cancelObs := context.WithCancel(context.Background())
	defer cancelObs()
	observe(ctx, client, batchID, h.rec)

	final, ran := waitForStatus(client, batchID, 2*time.Minute,
		openai.BatchStatusInProgress, openai.BatchStatusFinalizing, openai.BatchStatusCompleted)
	detail := fmt.Sprintf("batch %s reached status %s after a failed create", batchID, final.Status)
	judge(t, scenario, ran, detail)
}

// TestF4bDuplicateExecution reproduces the F4 double-execution race: the
// window between PQDequeue and InFlightSet leaves a dequeued job invisible,
// still validating in the DB, absent from the queue, with no in-flight entry.
// A sleep failpoint holds that window open past the reconciler's staleness
// threshold; the reconciler re-enqueues the "orphaned" validating job, the
// polling loop dequeues the duplicate, and the job executes twice. The
// in_progress and terminal writes CAS nothing, so both executions run to
// completion and every inference request is paid for twice.
//
// Violated invariant (single execution): requests served by the engine for a
// batch must not exceed its line count.
func TestF4bDuplicateExecution(t *testing.T) {
	const scenario = "F4b_duplicate_execution"
	const lines = 4
	// The window must outlast the reconciler's staleness threshold plus one
	// cycle so the re-enqueue happens while the first dequeue is held.
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
	fileID, err := client.uploadFile("f4b.jsonl", inputJSONL(lines, 900))
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

	// First execution: dequeue, hold the window, run. The duplicate launches
	// mid-run; once the first terminalizes, the duplicate's heartbeat sees
	// the terminal status and aborts, but its requests have already reached
	// the engine.
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
