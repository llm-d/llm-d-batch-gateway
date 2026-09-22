//go:build simulation

package simulation

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// TestZombieOwnerBurnsInference: a processor is frozen mid-job, a replacement
// with the same identity takes the job over and finishes it, and the original
// is then thawed. Its writes are fenced by the epoch, but nothing tells it to
// stop, so it keeps sending the rest of its requests to the engine.
//
// Invariant (bounded waste): once ownership has moved, the previous owner
// stops sending inference requests within a small slack.
func TestZombieOwnerBurnsInference(t *testing.T) {
	const scenario = "zombie_owner_burns_inference"
	// More lines than the per-endpoint concurrency, so the frozen owner still
	// has requests it has not sent when it is thawed.
	const lines = 24
	const slack = 2
	h := newHarness(t, nil)
	baseline := h.inferenceWitness()
	client := newAPIClient()
	fileID, err := client.uploadFile("zombie-owner.jsonl", inputJSONL(lines, 300))
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
	time.Sleep(3 * time.Second)
	startedBefore := h.inferenceWitness() - baseline
	h.rec.event("witness", map[string]any{"phase": "before-pause", "started": startedBefore})

	h.pause("processor")
	// Same identity as the frozen owner: it claims the job, bumps the epoch,
	// and runs it to completion.
	h.restart("processor-zombie")

	final, terminal := waitForStatus(client, batch.ID, 6*time.Minute,
		openai.BatchStatusCompleted, openai.BatchStatusFailed, openai.BatchStatusExpired)
	if !terminal {
		t.Fatalf("batch never terminalized after takeover; last status %s", final.Status)
	}
	replacementServed := h.inferenceWitness() - baseline - startedBefore
	h.rec.event("witness", map[string]any{"phase": "after-takeover", "replacement": replacementServed})

	h.unpause("processor")
	// Let the thawed owner do whatever it is going to do: stop, or send the
	// rest of its requests.
	served := 0
	stable := 0
	for deadline := time.Now().Add(4 * time.Minute); time.Now().Before(deadline) && stable < 6; {
		now := h.inferenceWitness() - baseline
		if now == served {
			stable++
		} else {
			stable = 0
		}
		served = now
		time.Sleep(5 * time.Second)
	}
	zombieAfter := served - startedBefore - replacementServed
	h.rec.event("witness", map[string]any{"phase": "after-thaw", "zombie": zombieAfter})
	time.Sleep(1 * time.Second)
	cancel()

	var msgs []string
	for _, v := range tl.checkTransitions() {
		msgs = append(msgs, v.String())
	}
	reproduced := zombieAfter > slack || len(msgs) > 0
	detail := fmt.Sprintf("requests started: %d before freeze, %d by the replacement, %d by the thawed owner (slack %d); final %s; sequence %v",
		startedBefore, replacementServed, zombieAfter, slack, final.Status, tl.statuses())
	if len(msgs) > 0 {
		detail = strings.Join(msgs, "; ") + "; " + detail
	}
	judge(t, scenario, reproduced, detail)
}

// TestReplicasExecuteOnce: three processors compete for a burst of batches.
// Dequeue and ownership are one statement, so every batch must execute on
// exactly one replica.
//
// Invariant (single execution): requests served by the engine equal the
// total line count, and every batch's counts add up.
func TestReplicasExecuteOnce(t *testing.T) {
	const scenario = "replicas_execute_once"
	const batches = 12
	const lines = 4
	h := newHarness(t, nil)
	baseline := h.inferenceWitness()
	h.restart("processor-1")
	h.restart("processor-2")
	client := newAPIClient()

	ids := make([]string, 0, batches)
	for i := 0; i < batches; i++ {
		fileID, err := client.uploadFile(fmt.Sprintf("replicas-%02d.jsonl", i), inputJSONL(lines, 10))
		if err != nil {
			t.Fatalf("upload input file %d: %v", i, err)
		}
		batch, err := client.createBatch(fileID, "24h")
		if err != nil {
			t.Fatalf("create batch %d: %v", i, err)
		}
		ids = append(ids, batch.ID)
	}

	var problems []string
	for _, id := range ids {
		final, terminal := waitForStatus(client, id, 5*time.Minute,
			openai.BatchStatusCompleted, openai.BatchStatusFailed, openai.BatchStatusExpired)
		if !terminal {
			t.Fatalf("batch %s never terminalized; last status %s", id, final.Status)
		}
		c := final.RequestCounts
		if final.Status != openai.BatchStatusCompleted || c.Total != lines || c.Completed+c.Failed != c.Total {
			problems = append(problems, fmt.Sprintf("%s: %s total=%d completed=%d failed=%d", id, final.Status, c.Total, c.Completed, c.Failed))
		}
	}
	// Give any straggling duplicate time to reach the engine.
	time.Sleep(10 * time.Second)
	served := h.inferenceWitness() - baseline
	h.rec.event("witness", map[string]any{"served": served, "expected": batches * lines})

	reproduced := served > batches*lines || len(problems) > 0
	detail := fmt.Sprintf("engine served %d requests for %d batches x %d lines", served, batches, lines)
	if len(problems) > 0 {
		detail += "; " + strings.Join(problems, "; ")
	}
	judge(t, scenario, reproduced, detail)
}
