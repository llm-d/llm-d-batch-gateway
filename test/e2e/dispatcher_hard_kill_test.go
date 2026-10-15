// Copyright 2026 The llm-d Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	asyncapi "github.com/llm-d/llm-d-async/api"
	"github.com/openai/openai-go/v3"
	"github.com/redis/go-redis/v9"
)

const (
	durableAsyncImage  = "ghcr.io/llm-d/llm-d-async:v1.0.0@sha256:62a3e78320b0423c3c2ed327e6b870f786c3daf48f81a34f9e0332c80905663c"
	durableAsyncDigest = "sha256:62a3e78320b0423c3c2ed327e6b870f786c3daf48f81a34f9e0332c80905663c"
)

type asyncPod struct {
	Name    string
	UID     string
	Image   string
	ImageID string
	Ready   bool
}

type asyncClaimSnapshot struct {
	Owners       map[string]string
	ResultQueues map[string]string
}

// testBatchAPIHardKillRecovery is the composed Kubernetes complement to
// llm-d Async PR #412's component tests. It submits through the public Files
// and Batch APIs, waits until Async has durably moved every request into Redis
// claim state, force-deletes the owning pod, proves the replacement acquired
// new fenced ownership tokens for the same request generations, and validates
// Batch Gateway's terminal counts and externally visible JSONL records.
func testBatchAPIHardKillRecovery(t *testing.T, rdb *redis.Client) {
	ctx := context.Background()
	const requestCount = 2
	var batchID string
	claimKeys := []string{
		gateReqQueue + ":claimed",
		gateReqQueue + ":claim-owners",
		gateReqQueue + ":claims-idx",
	}

	assertRecoveryQueueClean(t, rdb, append([]string{gateReqQueue}, claimKeys...)...)
	assertRecoveryResultQueuesClean(t, rdb)

	previousBudget, err := rdb.Get(ctx, dispatchGateBudgetKey).Result()
	budgetExisted := err == nil
	if err != nil && err != redis.Nil {
		t.Fatalf("read prior dispatch gate budget: %v", err)
	}

	engineReleased := false
	releaseEngine := func() error {
		if engineReleased {
			return nil
		}
		if err := tryPatchEngineConfig(t, testSimService, `{"max_num_seqs":128,"time_to_first_token":50,"inter_token_latency":100}`); err != nil {
			return err
		}
		engineReleased = true
		return nil
	}
	t.Cleanup(func() {
		if err := rdb.Set(context.Background(), dispatchGateBudgetKey, "1.0", 0).Err(); err != nil {
			t.Errorf("cleanup: open dispatch gate: %v", err)
		}
		if err := releaseEngine(); err != nil {
			t.Errorf("cleanup: release recovery engine: %v", err)
		}
		if _, err := waitForReplacementAsyncPod(t, "", 2*time.Minute); err != nil {
			t.Errorf("cleanup: wait for Async pod: %v", err)
		}
		if batchID != "" {
			if err := waitForRecoveryBatchTerminal(batchID, 2*time.Minute); err != nil {
				t.Errorf("cleanup: %v", err)
				return
			}
		}
		if err := waitForRecoveryStateDrainedWithError(rdb, 30*time.Second); err != nil {
			t.Errorf("cleanup: %v; leaving dispatch gate open", err)
			return
		}
		if budgetExisted {
			if err := rdb.Set(context.Background(), dispatchGateBudgetKey, previousBudget, 0).Err(); err != nil {
				t.Errorf("cleanup: restore dispatch gate budget: %v", err)
			}
		} else if err := rdb.Del(context.Background(), dispatchGateBudgetKey).Err(); err != nil {
			t.Errorf("cleanup: remove dispatch gate budget: %v", err)
		}
	})

	if err := rdb.Set(ctx, dispatchGateBudgetKey, "0.0", 0).Err(); err != nil {
		t.Fatalf("close dispatch gate: %v", err)
	}
	// Keep claimed work in flight long enough to observe pod replacement and
	// lease takeover. This changes only the simulator, not Async's lease model.
	patchEngineConfig(t, testSimService, `{"max_num_seqs":1,"time_to_first_token":30000,"inter_token_latency":100}`)
	time.Sleep(2 * time.Second) // redis gate poll interval is 500ms

	customIDs := []string{
		fmt.Sprintf("hard-kill-a-%s", testRunID),
		fmt.Sprintf("hard-kill-b-%s", testRunID),
	}
	lines := make([]string, 0, requestCount)
	for _, customID := range customIDs {
		lines = append(lines, fmt.Sprintf(
			`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"max_tokens":5,"messages":[{"role":"user","content":"hard kill recovery"}]}}`,
			customID, gateModel))
	}
	fileID := mustCreateFile(t, fmt.Sprintf("dispatcher-hard-kill-%s.jsonl", testRunID), strings.Join(lines, "\n"))
	batchID = mustCreateBatch(t, fileID)

	waitForRedisCount(t, 30*time.Second, func() (int64, error) {
		return rdb.ZCard(ctx, gateReqQueue).Result()
	}, requestCount, "pending recovery requests")

	var batch *openai.Batch
	countsDeadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(countsDeadline) {
		batch, err = newClient().Batches.Get(ctx, batchID)
		if err != nil {
			t.Fatalf("retrieve recovery batch: %v", err)
		}
		if terminalBatchStatuses[batch.Status] {
			t.Fatalf("batch reached terminal status %q before dispatch", batch.Status)
		}
		if batch.RequestCounts.Total == requestCount {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if batch.RequestCounts.Total != requestCount {
		t.Fatalf("batch before dispatch: status=%s total=%d, want non-terminal with total=%d",
			batch.Status, batch.RequestCounts.Total, requestCount)
	}

	if err := rdb.Set(ctx, dispatchGateBudgetKey, "1.0", 0).Err(); err != nil {
		t.Fatalf("open dispatch gate: %v", err)
	}
	initialClaims := waitForClaims(t, rdb, requestCount, nil, 30*time.Second)
	originalPod, err := readyAsyncPod(t)
	if err != nil {
		t.Fatal(err)
	}
	assertDurableAsyncImage(t, originalPod)

	t.Logf("hard-deleting Async pod %s only after %d durable claims were observed", originalPod.Name, len(initialClaims.Owners))
	out, err := exec.Command("kubectl", "delete", "pod", originalPod.Name,
		"--namespace", testNamespace,
		"--grace-period=0", "--force", "--wait=true").CombinedOutput()
	if err != nil {
		t.Fatalf("force-delete Async pod %s: %v\n%s", originalPod.Name, err, out)
	}

	replacementPod, err := waitForReplacementAsyncPod(t, originalPod.UID, 2*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	assertDurableAsyncImage(t, replacementPod)
	reclaimed := waitForClaims(t, rdb, requestCount, initialClaims.Owners, 30*time.Second)
	if !sameStringMap(initialClaims.ResultQueues, reclaimed.ResultQueues) {
		t.Fatalf("result queue routing changed across redelivery: before=%v after=%v",
			initialClaims.ResultQueues, reclaimed.ResultQueues)
	}
	for claimID, previousOwner := range initialClaims.Owners {
		t.Logf("claim takeover: generation=%q previous_owner=%s replacement_owner=%s",
			claimID, previousOwner, reclaimed.Owners[claimID])
	}
	t.Logf("replacement pod %s acquired new fenced owners for all %d request generations", replacementPod.Name, requestCount)

	if err := releaseEngine(); err != nil {
		t.Fatalf("release recovery engine: %v", err)
	}
	finalBatch, results := waitForBatchStatus(t, batchID, 3*time.Minute, openai.BatchStatusCompleted)
	if finalBatch.RequestCounts.Total != requestCount ||
		finalBatch.RequestCounts.Completed != requestCount ||
		finalBatch.RequestCounts.Failed != 0 {
		t.Errorf("terminal counts = total:%d completed:%d failed:%d, want %d/%d/0",
			finalBatch.RequestCounts.Total,
			finalBatch.RequestCounts.Completed,
			finalBatch.RequestCounts.Failed,
			requestCount, requestCount)
	}
	if results == nil {
		t.Fatal("expected terminal output file")
	}
	if finalBatch.OutputFileID == "" || finalBatch.ErrorFileID != "" {
		t.Fatalf("terminal files = output:%q error:%q, want non-empty output and empty error",
			finalBatch.OutputFileID, finalBatch.ErrorFileID)
	}
	if results.OutputLines != requestCount || results.ErrorLines != 0 {
		t.Fatalf("terminal file lines = output:%d error:%d, want %d/0",
			results.OutputLines, results.ErrorLines, requestCount)
	}
	assertUniqueRecoveryRecords(t, results.OutputBody, customIDs)
	t.Logf("terminal files: output=%s error=%s", finalBatch.OutputFileID, finalBatch.ErrorFileID)

	waitForRecoveryStateDrained(t, rdb, 30*time.Second)
	t.Log("Async pending, claim, owner, expiry-index, and result queue depths drained to zero")
	t.Logf("batch %s completed after hard-kill redelivery with %d unique external records", batchID, requestCount)
}

func readyAsyncPod(t *testing.T) (asyncPod, error) {
	t.Helper()
	pods, err := listAsyncPods(t)
	if err != nil {
		return asyncPod{}, err
	}
	if len(pods) != 1 {
		return asyncPod{}, fmt.Errorf("hard-kill recovery requires exactly one Async pod to prove the claimed work's owner; found %d", len(pods))
	}
	if !pods[0].Ready {
		return asyncPod{}, fmt.Errorf("sole Async pod %s is not ready", pods[0].Name)
	}
	return pods[0], nil
}

func listAsyncPods(t *testing.T) ([]asyncPod, error) {
	t.Helper()
	release := getEnvOrDefault("TEST_DISPATCHER_RELEASE", "dispatcher")
	out, err := exec.Command("kubectl", "get", "pods",
		"--namespace", testNamespace,
		"--selector", fmt.Sprintf("app.kubernetes.io/instance=%s,app.kubernetes.io/name=llm-d-async", release),
		"-o", "json").CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("list Async pods: %w\n%s", err, out)
	}

	var list struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
				UID  string `json:"uid"`
			} `json:"metadata"`
			Spec struct {
				Containers []struct {
					Name  string `json:"name"`
					Image string `json:"image"`
				} `json:"containers"`
			} `json:"spec"`
			Status struct {
				Phase      string `json:"phase"`
				Conditions []struct {
					Type   string `json:"type"`
					Status string `json:"status"`
				} `json:"conditions"`
				ContainerStatuses []struct {
					Name    string `json:"name"`
					ImageID string `json:"imageID"`
				} `json:"containerStatuses"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, fmt.Errorf("decode Async pod list: %w", err)
	}

	pods := make([]asyncPod, 0, len(list.Items))
	for _, item := range list.Items {
		pod := asyncPod{Name: item.Metadata.Name, UID: item.Metadata.UID}
		for _, condition := range item.Status.Conditions {
			if condition.Type == "Ready" && condition.Status == "True" {
				pod.Ready = item.Status.Phase == "Running"
				break
			}
		}
		for _, container := range item.Spec.Containers {
			if container.Name == "llm-d-async" {
				pod.Image = container.Image
				break
			}
		}
		for _, status := range item.Status.ContainerStatuses {
			if status.Name == "llm-d-async" {
				pod.ImageID = status.ImageID
				break
			}
		}
		pods = append(pods, pod)
	}
	return pods, nil
}

func findReadyAsyncPod(t *testing.T, excludedUID string) (asyncPod, error) {
	t.Helper()
	pods, err := listAsyncPods(t)
	if err != nil {
		return asyncPod{}, err
	}
	for _, pod := range pods {
		if pod.UID != excludedUID && pod.Ready {
			return pod, nil
		}
	}
	return asyncPod{}, fmt.Errorf("no ready replacement Async pod found (excluded UID %q)", excludedUID)
}

func waitForReplacementAsyncPod(t *testing.T, excludedUID string, timeout time.Duration) (asyncPod, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		pod, err := findReadyAsyncPod(t, excludedUID)
		if err == nil {
			return pod, nil
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	return asyncPod{}, fmt.Errorf("Async replacement was not ready within %v: %w", timeout, lastErr)
}

func assertDurableAsyncImage(t *testing.T, pod asyncPod) {
	t.Helper()
	if pod.Image != durableAsyncImage {
		t.Fatalf("Async pod %s image = %q, want exact artifact %q", pod.Name, pod.Image, durableAsyncImage)
	}
	if !strings.HasSuffix(pod.ImageID, "@"+durableAsyncDigest) {
		t.Fatalf("Async pod %s runtime imageID = %q, want digest %q", pod.Name, pod.ImageID, durableAsyncDigest)
	}
	t.Logf("verified Async pod %s image=%s imageID=%s", pod.Name, pod.Image, pod.ImageID)
}

func waitForClaims(t *testing.T, rdb *redis.Client, want int, previousOwners map[string]string, timeout time.Duration) asyncClaimSnapshot {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var lastOwners map[string]string
	for time.Now().Before(deadline) {
		owners, err := rdb.HGetAll(ctx, gateReqQueue+":claim-owners").Result()
		if err != nil {
			t.Fatalf("read claim owners: %v", err)
		}
		lastOwners = owners
		if len(owners) != want || !ownersReplaced(previousOwners, owners) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		claimed, err := rdb.HGetAll(ctx, gateReqQueue+":claimed").Result()
		if err != nil {
			t.Fatalf("read claimed payloads: %v", err)
		}
		indexDepth, err := rdb.ZCard(ctx, gateReqQueue+":claims-idx").Result()
		if err != nil {
			t.Fatalf("read claim expiry index: %v", err)
		}
		pendingDepth, err := rdb.ZCard(ctx, gateReqQueue).Result()
		if err != nil {
			t.Fatalf("read pending queue: %v", err)
		}
		if len(claimed) != want || indexDepth != int64(want) || pendingDepth != 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		resultQueues := make(map[string]string, want)
		for claimID, raw := range claimed {
			owner, ok := owners[claimID]
			if !ok || owner == "" {
				t.Fatalf("claimed request %q has no owner token", claimID)
			}
			var request asyncapi.InternalRequest
			if err := json.Unmarshal([]byte(raw), &request); err != nil {
				t.Fatalf("decode claimed request %q: %v", claimID, err)
			}
			if request.PublicRequest == nil || request.PublicRequest.ReqID() == "" || request.RequestToken == "" {
				t.Fatalf("claimed request %q lacks stable ID or RequestToken", claimID)
			}
			wantClaimID := request.PublicRequest.ReqID() + "\x00" + request.RequestToken
			if claimID != wantClaimID {
				t.Fatalf("claim ID %q does not match request generation %q", claimID, wantClaimID)
			}
			if request.ResultQueueName == "" {
				t.Fatalf("claimed request %q lacks result queue routing", claimID)
			}
			resultQueues[claimID] = request.ResultQueueName
			t.Logf("durable claim: request_id=%s request_token=%s owner=%s result_queue=%s",
				request.PublicRequest.ReqID(), request.RequestToken, owner, request.ResultQueueName)
		}
		return asyncClaimSnapshot{Owners: owners, ResultQueues: resultQueues}
	}
	t.Fatalf("claims did not reach wanted state within %v: owners=%v", timeout, lastOwners)
	return asyncClaimSnapshot{}
}

func ownersReplaced(previous, current map[string]string) bool {
	if previous == nil {
		return true
	}
	if len(previous) != len(current) {
		return false
	}
	for claimID, oldOwner := range previous {
		newOwner, ok := current[claimID]
		if !ok || newOwner == "" || newOwner == oldOwner {
			return false
		}
	}
	return true
}

func assertUniqueRecoveryRecords(t *testing.T, body string, wantCustomIDs []string) {
	t.Helper()
	gotCustomIDs := make([]string, 0, len(wantCustomIDs))
	seenRecordIDs := make(map[string]bool, len(wantCustomIDs))
	for _, raw := range strings.Split(strings.TrimSpace(body), "\n") {
		var record batchResultLine
		if err := json.Unmarshal([]byte(raw), &record); err != nil {
			t.Fatalf("decode recovery output record: %v\n%s", err, raw)
		}
		if record.ID == "" {
			t.Fatal("recovery output contains empty record ID")
		}
		if seenRecordIDs[record.ID] {
			t.Errorf("duplicate external record ID %q", record.ID)
		}
		seenRecordIDs[record.ID] = true
		gotCustomIDs = append(gotCustomIDs, record.CustomID)
	}
	assertSliceEqual(t, wantCustomIDs, gotCustomIDs)
}

func waitForRecoveryStateDrained(t *testing.T, rdb *redis.Client, timeout time.Duration) {
	t.Helper()
	if err := waitForRecoveryStateDrainedWithError(rdb, timeout); err != nil {
		t.Fatal(err)
	}
}

func assertRecoveryQueueClean(t *testing.T, rdb *redis.Client, keys ...string) {
	t.Helper()
	ctx := context.Background()
	for _, key := range keys {
		typeName, err := rdb.Type(ctx, key).Result()
		if err != nil {
			t.Fatalf("read Redis type for %q: %v", key, err)
		}
		var depth int64
		switch typeName {
		case "none":
			continue
		case "zset":
			depth, err = rdb.ZCard(ctx, key).Result()
		case "hash":
			depth, err = rdb.HLen(ctx, key).Result()
		default:
			t.Fatalf("unexpected Redis type %q for recovery key %q", typeName, key)
		}
		if err != nil {
			t.Fatalf("read recovery key %q: %v", key, err)
		}
		if depth != 0 {
			t.Fatalf("recovery key %q is not clean before test: depth=%d", key, depth)
		}
	}
}

func assertRecoveryResultQueuesClean(t *testing.T, rdb *redis.Client) {
	t.Helper()
	depths, err := recoveryResultQueueDepths(context.Background(), rdb)
	if err != nil {
		t.Fatalf("read recovery result queues: %v", err)
	}
	for queue, depth := range depths {
		if depth != 0 {
			t.Fatalf("recovery result queue %q is not clean before test: depth=%d", queue, depth)
		}
	}
}

func waitForRecoveryStateDrainedWithError(rdb *redis.Client, timeout time.Duration) error {
	ctx := context.Background()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		pending, err := rdb.ZCard(ctx, gateReqQueue).Result()
		if err != nil {
			return fmt.Errorf("read pending recovery requests: %w", err)
		}
		claimed, err := rdb.HLen(ctx, gateReqQueue+":claimed").Result()
		if err != nil {
			return fmt.Errorf("read claimed recovery requests: %w", err)
		}
		owners, err := rdb.HLen(ctx, gateReqQueue+":claim-owners").Result()
		if err != nil {
			return fmt.Errorf("read recovery claim owners: %w", err)
		}
		indexDepth, err := rdb.ZCard(ctx, gateReqQueue+":claims-idx").Result()
		if err != nil {
			return fmt.Errorf("read recovery claim index: %w", err)
		}
		resultDepths, err := recoveryResultQueueDepths(ctx, rdb)
		if err != nil {
			return err
		}
		resultDepth := int64(0)
		for _, depth := range resultDepths {
			resultDepth += depth
		}
		if pending == 0 && claimed == 0 && owners == 0 && indexDepth == 0 && resultDepth == 0 {
			return nil
		}
		last = fmt.Sprintf("pending=%d claimed=%d owners=%d index=%d results=%v",
			pending, claimed, owners, indexDepth, resultDepths)
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("Async request/claim/result state did not fully drain after %v (%s)", timeout, last)
}

func recoveryResultQueueDepths(ctx context.Context, rdb *redis.Client) (map[string]int64, error) {
	depths := make(map[string]int64)
	iter := rdb.Scan(ctx, 0, gateResultQueue+"*", 0).Iterator()
	for iter.Next(ctx) {
		queue := iter.Val()
		depth, err := rdb.LLen(ctx, queue).Result()
		if err != nil {
			return nil, fmt.Errorf("read recovery result queue %q: %w", queue, err)
		}
		depths[queue] = depth
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("scan recovery result queues: %w", err)
	}
	return depths, nil
}

func waitForRecoveryBatchTerminal(batchID string, timeout time.Duration) error {
	client := newClient()
	deadline := time.Now().Add(timeout)
	var lastStatus openai.BatchStatus
	for time.Now().Before(deadline) {
		batch, err := client.Batches.Get(context.Background(), batchID)
		if err != nil {
			return fmt.Errorf("retrieve recovery batch %s: %w", batchID, err)
		}
		lastStatus = batch.Status
		if terminalBatchStatuses[batch.Status] {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("recovery batch %s did not terminate within %v (last status: %q)", batchID, timeout, lastStatus)
}

func waitForRedisCount(t *testing.T, timeout time.Duration, read func() (int64, error), want int64, description string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last int64
	for time.Now().Before(deadline) {
		got, err := read()
		if err != nil {
			t.Fatalf("read %s: %v", description, err)
		}
		last = got
		if got == want {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s = %d, want %d within %v", description, last, want, timeout)
}

func sameStringMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for key, value := range a {
		if b[key] != value {
			return false
		}
	}
	return true
}
