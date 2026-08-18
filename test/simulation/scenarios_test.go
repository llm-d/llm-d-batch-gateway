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
	"strings"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// TestF1aCreateCrashBeforeEnqueue reproduces finding F1a from the consistency
// review: batch create is DBStore then PQEnqueue with no atomicity. The
// apiserver is killed between the two writes, so the client receives an
// error, yet the batch row exists. The orphan reconciler later re-enqueues
// the "orphaned" validating job and the batch runs to completion.
//
// Violated invariant (API honesty): a create that failed with a server error
// must never produce a batch that executes and bills.
func TestF1aCreateCrashBeforeEnqueue(t *testing.T) {
	const scenario = "F1a_create_crash_before_enqueue"
	h := newHarness(t, map[string]string{
		"APISERVER_FAILPOINTS": "apiserver/after-batch-dbstore=exit",
	})
	client := newAPIClient()

	fileID, err := client.uploadFile("f1a.jsonl", inputJSONL(2, 10))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}

	// The failpoint kills the apiserver after the DB insert; the request must
	// fail from the client's perspective.
	if _, err := client.createBatch(fileID, "24h"); err == nil {
		t.Fatal("batch create succeeded; expected the armed failpoint to kill the apiserver mid-request")
	}
	h.rec.event("create-failed", map[string]any{"file": fileID})

	// Bring the apiserver back unarmed, as a crashed pod would be replaced.
	h.setEnv("APISERVER_FAILPOINTS", "")
	h.restart("apiserver")

	batches, err := client.listBatches()
	if err != nil {
		t.Fatalf("list batches: %v", err)
	}

	// With the current architecture the row exists despite the failed create.
	// With a transactional create it must not.
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

	// If the row exists, the reconciler (5s cycles) will treat it as an
	// orphaned validating job, re-enqueue it, and the processor will run it.
	final, ran := waitForStatus(client, batchID, 2*time.Minute,
		openai.BatchStatusInProgress, openai.BatchStatusFinalizing, openai.BatchStatusCompleted)
	detail := fmt.Sprintf("batch %s reached status %s after a failed create", batchID, final.Status)
	judge(t, scenario, ran, detail)
}

// TestF3TerminalOverwrite reproduces finding F3: processor terminal status
// writes pass no expected status (status_updater.go DBUpdate with nil CAS),
// while the reconciler CASes. With the processor's heartbeat effectively
// disabled (stale-heartbeat config, simulating sustained Redis
// unreachability) the reconciler declares the live job orphaned and CASes it
// to failed; the processor then blindly overwrites the terminal failed with
// completed. A sleep failpoint before the terminal write holds the window
// open deterministically.
//
// Violated invariant (terminal immutability): no batch may leave a terminal
// state.
func TestF3TerminalOverwrite(t *testing.T) {
	const scenario = "F3_terminal_overwrite"
	// The sleep must outlast the reconciler's staleness threshold plus one
	// cycle so the reconciler acts while the terminal write is held open.
	sleep := 2*params().ReconcilerInterval + 10*time.Second
	h := newHarness(t, map[string]string{
		"PROCESSOR_FAILPOINTS": fmt.Sprintf("processor/before-terminal-write=sleep(%d)", sleep.Milliseconds()),
		"PROCESSOR_CONFIG":     "processor-stale-heartbeat.yaml",
	})
	client := newAPIClient()

	fileID, err := client.uploadFile("f3.jsonl", inputJSONL(2, 10))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := observe(ctx, client, batch.ID, h.rec)

	// Expected sequence today: the job executes, the processor sleeps 20s
	// before its completed write, the reconciler CASes the stale-heartbeat
	// job to failed during the sleep, and the processor then overwrites it.
	final, _ := waitForStatus(client, batch.ID, sleep+4*params().ReconcilerInterval+30*time.Second, openai.BatchStatusCompleted)

	// Give the observer a final polling cycle past the last transition.
	time.Sleep(1 * time.Second)
	cancel()

	violations := tl.checkTransitions()
	var terminal []string
	for _, v := range violations {
		if v.kind == "terminal-transition" {
			terminal = append(terminal, v.String())
		}
	}
	detail := fmt.Sprintf("observed sequence %v (final %s)", tl.statuses(), final.Status)
	if len(terminal) > 0 {
		detail = strings.Join(terminal, "; ") + "; " + detail
	}
	judge(t, scenario, len(terminal) > 0, detail)
}

// TestF4aWorkerCrashStrandsJob reproduces the pod-replacement orphan window:
// the processor keeps all execution state on container-local disk, so a
// SIGKILL mid-execution strands the job with no owner and no recoverable
// workdir. The replacement processor's startup recovery finds nothing; the
// reconciler eventually CASes the job to failed with no output or error file,
// discarding every inference result the job had already paid for.
//
// Violated invariant (work conservation): a job interrupted by worker loss
// must eventually complete or preserve its partial results, not terminalize
// with all completed work discarded.
func TestF4aWorkerCrashStrandsJob(t *testing.T) {
	const scenario = "F4a_worker_crash_strands_job"
	h := newHarness(t, nil)
	client := newAPIClient()

	// max_tokens 300 at ~30ms/token keeps each request in flight ~9s, so the
	// kill lands mid-execution deterministically.
	fileID, err := client.uploadFile("f4a.jsonl", inputJSONL(4, 300))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := observe(ctx, client, batch.ID, h.rec)

	if _, ok := waitForStatus(client, batch.ID, 60*time.Second, openai.BatchStatusInProgress); !ok {
		t.Fatal("batch never reached in_progress")
	}
	time.Sleep(1500 * time.Millisecond)
	h.kill("processor")
	// Recreate the processor: its tmpfs workdir starts empty, so this is a
	// replaced pod, not a container restart with surviving emptyDir.
	h.restart("processor")

	final, terminal := waitForStatus(client, batch.ID, 2*time.Minute,
		openai.BatchStatusFailed, openai.BatchStatusCompleted, openai.BatchStatusExpired)
	if !terminal {
		t.Fatalf("batch never terminalized after worker loss; last status %s", final.Status)
	}
	time.Sleep(1 * time.Second)
	cancel()

	stranded := final.Status == openai.BatchStatusFailed &&
		final.OutputFileID == nil && final.ErrorFileID == nil
	detail := fmt.Sprintf("observed sequence %v, final %s, output_file=%v error_file=%v",
		tl.statuses(), final.Status, final.OutputFileID != nil, final.ErrorFileID != nil)
	judge(t, scenario, stranded, detail)
}

// TestF2aCancelReverted reproduces finding F2a: cancelling a queued batch is
// PQDelete then DBUpdate with no atomicity. The apiserver dies between the
// two, leaving the batch out of the queue but still validating in the DB.
// The reconciler sees an unqueued validating job, re-enqueues it, and the
// batch the user tried to cancel runs to completion.
//
// Violated invariant (cancel effectiveness): after a cancel attempt removes
// a batch from the queue, that batch must never execute.
func TestF2aCancelReverted(t *testing.T) {
	const scenario = "F2a_cancel_reverted"
	h := newHarness(t, map[string]string{
		"APISERVER_FAILPOINTS": "apiserver/after-cancel-pqdelete=exit",
	})
	client := newAPIClient()

	// Park the batch in the queue: with the processor stopped nothing
	// dequeues it, so the cancel takes the queued-batch path.
	h.stop("processor")
	fileID, err := client.uploadFile("f2a.jsonl", inputJSONL(2, 10))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := observe(ctx, client, batch.ID, h.rec)

	if _, err := client.cancelBatch(batch.ID); err == nil {
		t.Fatal("cancel succeeded; expected the armed failpoint to kill the apiserver after PQDelete")
	}
	h.rec.event("cancel-failed", map[string]any{"batch": batch.ID})

	h.setEnv("APISERVER_FAILPOINTS", "")
	h.restart("apiserver")
	h.restart("processor")

	final, ran := waitForStatus(client, batch.ID, 2*time.Minute,
		openai.BatchStatusInProgress, openai.BatchStatusFinalizing, openai.BatchStatusCompleted)
	time.Sleep(1 * time.Second)
	cancel()
	detail := fmt.Sprintf("observed sequence %v, final %s after a cancel removed it from the queue",
		tl.statuses(), final.Status)
	judge(t, scenario, ran, detail)
}

// TestF2bCancelEventLost reproduces finding F2b: for an in-flight batch the
// apiserver writes cancelling to the DB and then sends the cancel event to
// Redis as a separate step. The apiserver dies between the two; the worker
// never learns of the cancel, finishes the job, and its blind write moves
// the batch cancelling -> completed.
//
// Violated invariant (legal transitions): cancelling may only lead to
// cancelled or failed.
func TestF2bCancelEventLost(t *testing.T) {
	const scenario = "F2b_cancel_event_lost"
	h := newHarness(t, map[string]string{
		"APISERVER_FAILPOINTS": "apiserver/after-cancel-dbupdate=exit",
	})
	client := newAPIClient()

	// Long generations keep the batch in_progress while the cancel lands.
	fileID, err := client.uploadFile("f2b.jsonl", inputJSONL(4, 300))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := observe(ctx, client, batch.ID, h.rec)

	if _, ok := waitForStatus(client, batch.ID, 60*time.Second, openai.BatchStatusInProgress); !ok {
		t.Fatal("batch never reached in_progress")
	}
	time.Sleep(1 * time.Second)
	if _, err := client.cancelBatch(batch.ID); err == nil {
		t.Fatal("cancel succeeded; expected the armed failpoint to kill the apiserver after the DB write")
	}
	h.rec.event("cancel-failed", map[string]any{"batch": batch.ID})

	h.setEnv("APISERVER_FAILPOINTS", "")
	h.restart("apiserver")

	final, _ := waitForStatus(client, batch.ID, 2*time.Minute,
		openai.BatchStatusCompleted, openai.BatchStatusCancelled, openai.BatchStatusFailed)
	time.Sleep(1 * time.Second)
	cancel()

	reproduced := false
	var msgs []string
	for _, v := range tl.checkTransitions() {
		msgs = append(msgs, v.String())
		reproduced = true
	}
	detail := fmt.Sprintf("observed sequence %v, final %s", tl.statuses(), final.Status)
	if len(msgs) > 0 {
		detail = strings.Join(msgs, "; ") + "; " + detail
	}
	judge(t, scenario, reproduced, detail)
}

// TestF5OrphanedBlob reproduces finding F5: finalization uploads the blob to
// S3 and then inserts the file record as a separate write, and batch-gc only
// walks DB records. A crash between the two leaves a blob no sweep will ever
// reclaim.
//
// Violated invariant (referential, blob -> record): every object in the
// bucket older than the grace period has a file record.
func TestF5OrphanedBlob(t *testing.T) {
	const scenario = "F5_orphaned_blob"
	h := newHarness(t, map[string]string{
		"PROCESSOR_FAILPOINTS": "processor/after-blob-store=exit",
	})
	client := newAPIClient()

	fileID, err := client.uploadFile("f5.jsonl", inputJSONL(2, 10))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := observe(ctx, client, batch.ID, h.rec)

	// The processor dies inside finalization; the reconciler terminalizes
	// the orphaned finalizing job.
	final, terminal := waitForStatus(client, batch.ID, 2*time.Minute,
		openai.BatchStatusFailed, openai.BatchStatusExpired)
	if !terminal {
		t.Fatalf("batch never terminalized; last status %s", final.Status)
	}
	time.Sleep(1 * time.Second)
	cancel()

	orphans, err := orphanedBlobs(client)
	if err != nil {
		t.Fatalf("check for orphaned blobs: %v", err)
	}
	h.rec.event("orphan-check", map[string]any{"orphans": orphans})
	detail := fmt.Sprintf("observed sequence %v, final %s, orphaned blobs %v",
		tl.statuses(), final.Status, orphans)
	judge(t, scenario, len(orphans) > 0, detail)
}

// TestF6FinalizationStrand reproduces finding F6: the processor crashes after
// the output blob and its file record are durably written but before the
// completed status write. The reconciler CASes the finalizing orphan to
// failed, so the batch record never gets output_file_id even though the
// results are fully stored and reachable by ID.
//
// Violated invariant (results reachability): durably stored results must be
// linked from the batch that produced them.
func TestF6FinalizationStrand(t *testing.T) {
	const scenario = "F6_finalization_strand"
	// The second failpoint keeps the replacement container from completing
	// the job via startup recovery (on Kubernetes an exit only restarts the
	// container and emptyDir survives), modeling pod replacement / crash
	// looping until the reconciler terminalizes the orphan.
	h := newHarness(t, map[string]string{
		"PROCESSOR_FAILPOINTS": "processor/after-file-records=exit;processor/recovery-found-job=exit",
	})
	client := newAPIClient()

	fileID, err := client.uploadFile("f6.jsonl", inputJSONL(2, 10))
	if err != nil {
		t.Fatalf("upload input file: %v", err)
	}
	batch, err := client.createBatch(fileID, "24h")
	if err != nil {
		t.Fatalf("create batch: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tl := observe(ctx, client, batch.ID, h.rec)

	final, terminal := waitForStatus(client, batch.ID, 2*time.Minute,
		openai.BatchStatusFailed, openai.BatchStatusExpired, openai.BatchStatusCompleted)
	if !terminal {
		t.Fatalf("batch never terminalized; last status %s", final.Status)
	}
	time.Sleep(1 * time.Second)
	cancel()

	files, err := client.listFiles()
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	// The input file plus at least one result-file record exist; the batch
	// links to none of them.
	stranded := final.Status == openai.BatchStatusFailed &&
		final.OutputFileID == nil && len(files) > 1
	detail := fmt.Sprintf("observed sequence %v, final %s, output_file linked=%v, file records=%d",
		tl.statuses(), final.Status, final.OutputFileID != nil, len(files))
	judge(t, scenario, stranded, detail)
}
