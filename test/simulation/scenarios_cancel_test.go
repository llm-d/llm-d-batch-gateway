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

// TestCancelRacingCompletion: the apiserver reads an in-progress batch, then
// stalls before writing cancelling while the processor completes the job.
// The stale cancel write must not move the batch out of its terminal state.
//
// Invariant (terminal immutability): no batch may leave a terminal state.
func TestCancelRacingCompletion(t *testing.T) {
	const scenario = "cancel_racing_completion"
	hold := 20 * time.Second
	h := newHarness(t, map[string]string{
		"APISERVER_FAILPOINTS": fmt.Sprintf("apiserver/before-cancel-dbupdate=sleep(%d)", hold.Milliseconds()),
	})
	client := newAPIClient()
	fileID, err := client.uploadFile("cancel-racing-completion.jsonl", inputJSONL(2, 10))
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

	// The handler reads in_progress, then sleeps at the failpoint while the
	// job finishes underneath it.
	cancelDone := make(chan error, 1)
	go func() {
		_, err := client.withTimeout(hold + 30*time.Second).cancelBatch(batch.ID)
		cancelDone <- err
	}()

	final, terminal := waitForStatus(client, batch.ID, hold+60*time.Second,
		openai.BatchStatusCompleted, openai.BatchStatusFailed, openai.BatchStatusCancelled)
	if !terminal {
		t.Fatalf("batch never terminalized; last status %s", final.Status)
	}
	select {
	case err := <-cancelDone:
		h.rec.event("cancel-returned", map[string]any{"error": fmt.Sprint(err)})
	case <-time.After(hold + 40*time.Second):
		t.Fatal("cancel request never returned")
	}
	time.Sleep(2 * time.Second)
	cancel()

	after, err := client.getBatch(batch.ID)
	if err != nil {
		t.Fatalf("get batch after cancel: %v", err)
	}
	var msgs []string
	for _, v := range tl.checkTransitions() {
		if v.kind == "terminal-transition" {
			msgs = append(msgs, v.String())
		}
	}
	reproduced := len(msgs) > 0 || !after.Status.IsTerminal()
	detail := fmt.Sprintf("observed sequence %v, final %s, after cancel %s", tl.statuses(), final.Status, after.Status)
	if len(msgs) > 0 {
		detail = strings.Join(msgs, "; ") + "; " + detail
	}
	judge(t, scenario, reproduced, detail)
}
