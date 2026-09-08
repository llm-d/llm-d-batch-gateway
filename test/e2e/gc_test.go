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
	"errors"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
)

func testGarbageCollection(t *testing.T) {
	t.Run("CollectsExpiredFile", doTestGCCollectsExpiredFile)
	t.Run("CollectsExpiredBatch", doTestGCCollectsExpiredBatch)
}

// expireInDB sets the expiry of the given item to 1 (epoch second 1 = long past).
// It dispatches to PostgreSQL or Redis depending on the configured DB client type.
func expireInDB(t *testing.T, table, id string) {
	t.Helper()

	if testDBClientType == "redis" || testDBClientType == "valkey" {
		expireInRedis(t, table, id)
	} else {
		expireInPostgresql(t, table, id)
	}
}

func expireInPostgresql(t *testing.T, table, id string) {
	t.Helper()

	sql := fmt.Sprintf("UPDATE %s SET expiry = 1 WHERE id = '%s'", table, id)
	out := psqlExec(t, testDBName, sql)
	t.Logf("expired %s/%s in PostgreSQL: %s", table, id, out)
}

// psqlExec runs sql against database db through psql in the PostgreSQL pod
// and returns the trimmed output.
func psqlExec(t *testing.T, db, sql string) string {
	t.Helper()

	podName := testDBPod
	if podName == "" {
		podName = fmt.Sprintf("%s-0", testPostgresqlRelease)
	}

	ns := testDBNamespace
	if ns == "" {
		ns = testNamespace
	}

	cmd := fmt.Sprintf(
		`PGPASSWORD="${POSTGRESQL_PASSWORD:-$(cat "$POSTGRES_PASSWORD_FILE" 2>/dev/null)}" psql -U '%s' -d '%s' -tA -c %q`,
		testDBUser, db, sql,
	)
	out, err := exec.Command("kubectl", "exec",
		podName,
		"-n", ns,
		"--", "bash", "-c", cmd,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl exec psql failed: %v\n%s", err, out)
	}
	return strings.TrimSpace(string(out))
}

func expireInRedis(t *testing.T, table, id string) {
	t.Helper()

	// Redis key format: llmd_batch:store:<type>:<id>
	// table is "batch_items" or "file_items"; map to the Redis type prefix.
	itemType := strings.TrimSuffix(table, "_items")
	key := fmt.Sprintf("llmd_batch:store:%s:%s", itemType, id)

	// Valkey containers have valkey-cli instead of redis-cli.
	cliTool := "redis-cli"
	podSuffix := "master-0"
	if testExchangeClientType == "valkey" {
		cliTool = "valkey-cli"
		podSuffix = "valkey-primary-0"
	}

	cmd := fmt.Sprintf("%s HSET %s expiry 1", cliTool, key)
	out, err := exec.Command("kubectl", "exec",
		fmt.Sprintf("%s-%s", testRedisRelease, podSuffix),
		"-n", testNamespace,
		"--", "bash", "-c", cmd,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl exec %s failed: %v\n%s", cliTool, err, out)
	}
	t.Logf("expired %s/%s via %s: %s", table, id, cliTool, strings.TrimSpace(string(out)))
}

// doTestGCCollectsExpiredFile creates a file, force-expires it in the DB,
// then waits for the GC to delete it.
func doTestGCCollectsExpiredFile(t *testing.T) {
	t.Helper()

	if !testKubectlAvailable {
		t.Skip("kubectl not available, skipping GC test")
	}

	client := newClient()
	ctx := context.Background()

	// Create a file
	filename := fmt.Sprintf("test-gc-file-%s.jsonl", testRunID)
	fileID := mustCreateFile(t, filename, testJSONL)

	// Verify it exists
	_, err := client.Files.Get(ctx, fileID)
	if err != nil {
		t.Fatalf("expected file to exist before GC: %v", err)
	}

	// Force-expire it in the database
	expireInDB(t, "file_items", fileID)

	// Wait for the GC to collect it (GC interval is 5s in dev-deploy)
	const (
		pollInterval = 2 * time.Second
		maxWait      = 1 * time.Minute
	)
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		_, err := client.Files.Get(ctx, fileID)
		if err != nil {
			var apiErr *openai.Error
			if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
				t.Logf("file %s was collected by GC", fileID)
				return
			}
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("file %s was not collected by GC within %v", fileID, maxWait)
}

// doTestGCCollectsExpiredBatch creates a batch, waits for completion,
// force-expires it in the DB, then waits for the GC to delete it.
func doTestGCCollectsExpiredBatch(t *testing.T) {
	t.Helper()

	if !testKubectlAvailable {
		t.Skip("kubectl not available, skipping GC test")
	}

	client := newClient()
	ctx := context.Background()

	// Create a file and batch, wait for completion
	fileID := mustCreateFile(t, fmt.Sprintf("test-gc-batch-%s.jsonl", testRunID), testJSONL)
	batchID := mustCreateBatch(t, fileID)
	_, _ = waitForBatchStatus(t, batchID, 5*time.Minute, openai.BatchStatusCompleted)

	// Verify it exists
	_, err := client.Batches.Get(ctx, batchID)
	if err != nil {
		t.Fatalf("expected batch to exist before GC: %v", err)
	}

	// Force-expire it in the database
	expireInDB(t, "batch_items", batchID)

	// Wait for the GC to collect it (GC interval is 5s in dev-deploy)
	const (
		pollInterval = 2 * time.Second
		maxWait      = 1 * time.Minute
	)
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		_, err := client.Batches.Get(ctx, batchID)
		if err != nil {
			var apiErr *openai.Error
			if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
				t.Logf("batch %s was collected by GC", batchID)
				return
			}
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("batch %s was not collected by GC within %v", batchID, maxWait)
}
