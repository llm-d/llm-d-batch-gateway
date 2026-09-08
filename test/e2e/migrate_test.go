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
	"encoding/base64"
	"fmt"
	"net/url"
	"os/exec"
	"strings"
	"testing"
)

const (
	migrateJobParallelism = 5
	migrateJobTimeout     = "3m"
)

func testSchemaMigrations(t *testing.T) {
	if testDBClientType != "postgresql" {
		t.Skipf("db client type is %s, schema migrations only apply to postgresql", testDBClientType)
	}
	if !testKubectlAvailable {
		t.Skip("kubectl not available")
	}

	t.Run("DeploymentsRunMigrateInitContainer", doTestMigrateInitContainers)
	t.Run("DeployedDatabaseIsMigrated", doTestDeployedDatabaseMigrated)
	t.Run("ConcurrentMigrateOnFreshDatabase", doTestConcurrentMigrate)
}

func doTestMigrateInitContainers(t *testing.T) {
	for _, component := range []string{"apiserver", "processor", "gc"} {
		deploy := fmt.Sprintf("%s-%s", testHelmRelease, component)
		out := kubectlGet(t, "deployment", deploy, "{.spec.template.spec.initContainers[*].name}")
		if out != "migrate" {
			t.Errorf("deployment %s initContainers = %q, want %q", deploy, out, "migrate")
		}
	}
}

func doTestDeployedDatabaseMigrated(t *testing.T) {
	out := psqlExec(t, testDBName, "SELECT count(*) || ',' || max(version) FROM schema_migrations")
	if out != "1,1" {
		t.Fatalf("schema_migrations count,max = %q, want %q", out, "1,1")
	}
	for _, table := range []string{"batch_items", "file_items"} {
		if got := psqlExec(t, testDBName, fmt.Sprintf("SELECT to_regclass('%s') IS NOT NULL", table)); got != "t" {
			t.Errorf("table %s exists = %q, want t", table, got)
		}
	}
}

// doTestConcurrentMigrate creates an empty database and runs the migrate
// subcommand from the deployed apiserver image against it from several pods
// at once, then checks the schema was created exactly once.
func doTestConcurrentMigrate(t *testing.T) {
	dbName := "migrate_e2e_" + testRunID
	psqlExec(t, testDBName, "CREATE DATABASE "+dbName)
	t.Cleanup(func() {
		psqlExec(t, testDBName, "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
	})

	dbURL := postgresqlURLForDatabase(t, dbName)
	image := kubectlGet(t, "deployment", testHelmRelease+"-apiserver", "{.spec.template.spec.containers[0].image}")
	pullPolicy := kubectlGet(t, "deployment", testHelmRelease+"-apiserver", "{.spec.template.spec.containers[0].imagePullPolicy}")

	jobName := "migrate-e2e-" + testRunID
	manifest := fmt.Sprintf(`apiVersion: batch/v1
kind: Job
metadata:
  name: %s
  namespace: %s
spec:
  parallelism: %d
  completions: %d
  backoffLimit: 0
  template:
    spec:
      restartPolicy: Never
      securityContext:
        runAsNonRoot: true
        seccompProfile:
          type: RuntimeDefault
      containers:
      - name: migrate
        image: %s
        imagePullPolicy: %s
        args: ["migrate", "--postgresql-url=%s"]
        securityContext:
          allowPrivilegeEscalation: false
          capabilities:
            drop: ["ALL"]
`, jobName, testNamespace, migrateJobParallelism, migrateJobParallelism, image, pullPolicy, dbURL)

	apply := exec.Command("kubectl", "apply", "-f", "-")
	apply.Stdin = strings.NewReader(manifest)
	if out, err := apply.CombinedOutput(); err != nil {
		t.Fatalf("kubectl apply job: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		_ = exec.Command("kubectl", "delete", "job", jobName, "-n", testNamespace, "--ignore-not-found", "--wait=false").Run()
	})

	if out, err := exec.Command("kubectl", "wait", "--for=condition=complete", "job/"+jobName,
		"-n", testNamespace, "--timeout="+migrateJobTimeout).CombinedOutput(); err != nil {
		logs, _ := exec.Command("kubectl", "logs", "-n", testNamespace, "-l", "job-name="+jobName, "--tail=50").CombinedOutput()
		t.Fatalf("migrate job did not complete: %v\n%s\npod logs:\n%s", err, out, logs)
	}

	succeeded := kubectlGet(t, "job", jobName, "{.status.succeeded}")
	if succeeded != fmt.Sprint(migrateJobParallelism) {
		t.Fatalf("job succeeded = %q, want %d", succeeded, migrateJobParallelism)
	}

	versions := psqlExec(t, dbName, "SELECT string_agg(version::text, ',' ORDER BY version) FROM schema_migrations")
	if versions != "1" {
		t.Fatalf("schema_migrations versions after %d concurrent runs = %q, want %q", migrateJobParallelism, versions, "1")
	}
	for _, table := range []string{"batch_items", "file_items"} {
		if got := psqlExec(t, dbName, fmt.Sprintf("SELECT to_regclass('%s') IS NOT NULL", table)); got != "t" {
			t.Errorf("table %s exists = %q, want t", table, got)
		}
	}
}

// postgresqlURLForDatabase reads the postgresql-url key from the app secret
// mounted by the apiserver and swaps the database name.
func postgresqlURLForDatabase(t *testing.T, dbName string) string {
	t.Helper()
	secretName := kubectlGet(t, "deployment", testHelmRelease+"-apiserver",
		`{.spec.template.spec.volumes[?(@.name=="secrets")].projected.sources[0].secret.name}`)
	encoded := kubectlGet(t, "secret", secretName, "{.data.postgresql-url}")
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("decode postgresql-url: %v", err)
	}
	u, err := url.Parse(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatalf("parse postgresql-url: %v", err)
	}
	u.Path = "/" + dbName
	return u.String()
}

func kubectlGet(t *testing.T, kind, name, jsonpath string) string {
	t.Helper()
	out, err := exec.Command("kubectl", "get", kind, name, "-n", testNamespace, "-o", "jsonpath="+jsonpath).CombinedOutput()
	if err != nil {
		t.Fatalf("kubectl get %s %s: %v\n%s", kind, name, err, out)
	}
	return strings.TrimSpace(string(out))
}
