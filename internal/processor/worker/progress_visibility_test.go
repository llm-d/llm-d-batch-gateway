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
	"fmt"
	"sync"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// progressCall is one successful DBUpdateProgress invocation.
type progressCall struct {
	at     time.Time
	epoch  int64
	counts openai.BatchRequestCounts
}

// progressRecorder wraps a BatchProgressDBClient and records successful
// DBUpdateProgress calls, so a test can assert what was pushed and when.
type progressRecorder struct {
	db.BatchProgressDBClient
	mu    sync.Mutex
	calls []progressCall
}

func (r *progressRecorder) DBUpdateProgress(ctx context.Context, id string, epoch int64, countsJSON []byte) error {
	if err := r.BatchProgressDBClient.DBUpdateProgress(ctx, id, epoch, countsJSON); err != nil {
		return err
	}
	var counts openai.BatchRequestCounts
	if err := json.Unmarshal(countsJSON, &counts); err != nil {
		return fmt.Errorf("unmarshal progress counts: %w", err)
	}
	r.mu.Lock()
	r.calls = append(r.calls, progressCall{at: time.Now(), epoch: epoch, counts: counts})
	r.mu.Unlock()
	return nil
}

func (r *progressRecorder) snapshot() []progressCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]progressCall(nil), r.calls...)
}

// TestExecuteJob_PushesProgressCountsMidRun is the deterministic replacement
// for the removed ProgressPolling e2e test (INFERENG-11391). That e2e test
// raced the deployment's progress interval against simulator response
// lengths, neither of which it controlled. Here the interval is processor
// config the test owns and the batch is held mid-execution by a blocked
// inference call: a progress write with the first completion's counts must
// reach the status store through the throttled, epoch-fenced
// UpdateProgressCounts path while execution is still running.
func TestExecuteJob_PushesProgressCountsMidRun(t *testing.T) {
	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()
	cfg.ProgressUpdateInterval = 50 * time.Millisecond

	// Two models dispatched through separate per-model goroutines: m1 answers
	// immediately, m2 blocks until the test releases it, holding the batch
	// mid-execution.
	releaseM2 := make(chan struct{})
	m2Entered := make(chan struct{})
	var releaseOnce, enteredOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseM2) }) }
	t.Cleanup(release)

	mock := &mockInferenceClient{generateFn: func(_ context.Context, req *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
		if model, _ := req.Params["model"].(string); model == "m2" {
			enteredOnce.Do(func() { close(m2Entered) })
			<-releaseM2
		}
		return &inference.GenerateResponse{
			RequestID: req.RequestID,
			Response:  []byte(`{"choices":[{"message":{"content":"hello"}}]}`),
		}, nil
	}}

	requests := []batch_types.Request{
		{CustomID: "r1", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
		{CustomID: "r2", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m2"}},
	}
	env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1", "m2": "m2"})

	// Seed the row the epoch-fenced progress writes target, and record every
	// successful push through the recorder-wrapped updater.
	seedDBJob(t, env.dbClient, jobInfo.JobID)
	recorder := &progressRecorder{BatchProgressDBClient: env.dbClient}

	type execResult struct {
		counts *openai.BatchRequestCounts
		err    error
	}
	done := make(chan execResult, 1)
	go func() {
		counts, err := env.p.executeJob(testLoggerCtx(t), &jobExecutionParams{
			updater: NewStatusUpdater(recorder),
			jobInfo: jobInfo,
		})
		done <- execResult{counts: counts, err: err}
	}()

	// Wait until m2 is blocked mid-execution.
	select {
	case <-m2Entered:
	case <-time.After(30 * time.Second):
		t.Fatal("m2 request was never dispatched")
	}

	// While m2 is still blocked, the tracker must persist m1's completion:
	// a progress write with completed=1 lands before execution finishes.
	var midRun *progressCall
	deadline := time.Now().Add(10 * time.Second)
	for midRun == nil && time.Now().Before(deadline) {
		for _, c := range recorder.snapshot() {
			if c.counts.Completed == 1 && c.counts.Total == 2 {
				cc := c
				midRun = &cc
				break
			}
		}
		if midRun == nil {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if midRun == nil {
		t.Fatal("no mid-run progress write with completed=1 while the batch was still executing")
	}

	// By construction the mid-run write predates execution completion: m2 is
	// still blocked here, so executeJob cannot have returned.
	select {
	case res := <-done:
		t.Fatalf("executeJob returned before m2 was released: %+v", res)
	default:
	}

	// Release m2 and let the batch finish.
	release()
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("executeJob error: %v", res.err)
		}
		if res.counts == nil || res.counts.Completed != 2 || res.counts.Total != 2 {
			t.Fatalf("final counts = %+v, want total=2 completed=2", res.counts)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("executeJob did not return after release")
	}
}
