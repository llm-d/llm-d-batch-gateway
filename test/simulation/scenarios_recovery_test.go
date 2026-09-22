//go:build simulation

package simulation

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// TestRecoveryCrashLoop: the processor dies right after uploading the output
// blob, and dies the same way every time it recovers that job. A job that
// crashes its owner on every recovery must still reach a terminal state, and
// the owner must come back healthy instead of restarting forever.
//
// Invariant (bounded recovery): a batch whose owner crashes during recovery
// terminalizes within a bounded window, after which the processor is healthy.
func TestRecoveryCrashLoop(t *testing.T) {
	const scenario = "recovery_crash_loop"
	h := newHarness(t, map[string]string{
		"PROCESSOR_FAILPOINTS": "processor/after-blob-store=exit",
	})
	if !h.restartsOnExit() {
		t.Skip("needs a backend that restarts a crashed processor on its own")
	}
	client := newAPIClient()
	fileID, err := client.uploadFile("recovery-crash-loop.jsonl", inputJSONL(2, 10))
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

	// Each restart claims the same job and crashes at the same point; with a
	// recovery budget of 3 the fourth claim fails the job. Container restart
	// backoff on kind adds roughly 10+20+40s on top of the runs themselves.
	final, terminal := waitForStatus(client, batch.ID, 5*time.Minute,
		openai.BatchStatusFailed, openai.BatchStatusExpired, openai.BatchStatusCompleted)
	h.rec.event("terminal-wait", map[string]any{"terminal": terminal, "status": final.Status})

	healthy := false
	if terminal {
		// Disarm so the next restart is clean, then expect the owner to settle.
		h.setEnv("PROCESSOR_FAILPOINTS", "")
		healthy = h.waitHealthy("processor", 3*time.Minute)
	}
	time.Sleep(1 * time.Second)
	cancel()

	detail := fmt.Sprintf("observed sequence %v, final %s, terminal=%v, processor healthy=%v",
		tl.statuses(), final.Status, terminal, healthy)
	judge(t, scenario, !terminal || !healthy, detail)
}
