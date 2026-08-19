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
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// kindBackend runs scenarios against a dev-deploy'd Kind cluster
// (`make sim-kind-deploy`, optionally with ENABLE_GIE=true for the EPP on
// the inference path). The cluster persists across scenarios; each scenario
// gets fresh data by flushing Redis, truncating Postgres, and clearing the
// bucket. Pod deletion is a real replacement with a fresh emptyDir, and the
// processor can be scaled to multiple replicas for async scenarios.
type kindBackend struct {
	t       *testing.T
	ns      string
	release string
	// desired failpoint env per component, from the scenario's abstract keys.
	env map[string]string
	// staleHeartbeat mirrors the compose PROCESSOR_CONFIG knob via a helm
	// value on the processor.
	staleHeartbeat bool
}

var _ stackBackend = (*kindBackend)(nil)

const (
	kindAPIURL    = "https://127.0.0.1:8000"
	jaegerBaseURL = "http://127.0.0.1:16686"
	kindContext   = "kind-batch-gateway-dev"
)

var kindDeployments = map[string]string{
	"apiserver": "batch-gateway-apiserver",
	"processor": "batch-gateway-processor",
	"gc":        "batch-gateway-gc",
}

func newKindBackend(t *testing.T) *kindBackend {
	t.Helper()
	for _, tool := range []string{"kubectl", "helm"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found; kind backend requires it", tool)
		}
	}
	b := &kindBackend{
		t:       t,
		ns:      envOr("SIM_KIND_NAMESPACE", "default"),
		release: envOr("SIM_KIND_RELEASE", "batch-gateway"),
		env:     map[string]string{},
	}
	if _, err := b.kubectl("get", "deployment", b.release+"-apiserver"); err != nil {
		t.Skipf("kind backend: deployment %s-apiserver not found in context %s; run `make sim-kind-deploy` first", b.release, kindContext)
	}
	t.Cleanup(func() { b.disarmAll() })
	return b
}

func (b *kindBackend) apiURL() string { return kindAPIURL }

func (b *kindBackend) ensureUp(env map[string]string) {
	b.t.Helper()
	for k, v := range env {
		b.applyEnv(k, v)
	}
	b.applyProcessorConfig()
	for svc := range kindDeployments {
		b.syncEnv(svc)
	}
	for _, dep := range kindDeployments {
		b.rolloutWait(dep)
	}
	b.resetState()
}

func (b *kindBackend) applyEnv(key, value string) {
	switch key {
	case "PROCESSOR_CONFIG":
		b.staleHeartbeat = value != "" && value != "processor.yaml"
	default:
		b.env[key] = value
	}
}

// syncEnv reconciles one deployment's FAILPOINTS env with the desired state.
// kubectl set env triggers a rollout when the value changes.
func (b *kindBackend) syncEnv(service string) {
	b.t.Helper()
	dep := kindDeployments[service]
	val := b.env[strings.ToUpper(service)+"_FAILPOINTS"]
	if val == "" {
		if _, err := b.kubectl("set", "env", "deployment/"+dep, "FAILPOINTS-"); err != nil {
			b.t.Fatalf("clear FAILPOINTS on %s: %v", dep, err)
		}
		return
	}
	if _, err := b.kubectl("set", "env", "deployment/"+dep, "FAILPOINTS="+val); err != nil {
		b.t.Fatalf("set FAILPOINTS on %s: %v", dep, err)
	}
}

// applyProcessorConfig maps the stale-heartbeat knob to a helm value.
// dev-deploy sets heartbeatInterval=10s; the stale variant pushes it past
// the reconciler's staleness threshold.
func (b *kindBackend) applyProcessorConfig() {
	b.t.Helper()
	interval := "10s"
	if b.staleHeartbeat {
		interval = "10m"
	}
	_, thisFile, _, _ := runtime.Caller(0)
	chart := filepath.Join(filepath.Dir(thisFile), "..", "..", "charts", "batch-gateway")
	out, err := exec.Command("helm", "upgrade", b.release, chart,
		"--kube-context", kindContext, "-n", b.ns,
		"--reuse-values",
		"--set", "processor.config.heartbeatInterval="+interval,
	).CombinedOutput()
	if err != nil {
		b.t.Fatalf("helm upgrade heartbeatInterval=%s: %v\n%s", interval, err, out)
	}
	if _, err := b.kubectl("rollout", "restart", "deployment/"+b.release+"-processor"); err != nil {
		b.t.Fatalf("rollout restart processor: %v", err)
	}
}

func (b *kindBackend) restart(service string) {
	b.t.Helper()
	dep := kindDeployments[service]
	if _, err := b.kubectl("scale", "deployment/"+dep, "--replicas=1"); err != nil {
		b.t.Fatalf("scale up %s: %v", dep, err)
	}
	b.syncEnv(service)
	b.rolloutWait(dep)
}

func (b *kindBackend) stop(service string) {
	b.t.Helper()
	dep := kindDeployments[service]
	if _, err := b.kubectl("scale", "deployment/"+dep, "--replicas=0"); err != nil {
		b.t.Fatalf("scale down %s: %v", dep, err)
	}
	if _, err := b.kubectl("wait", "--for=delete", "pod", "-l", "app.kubernetes.io/name="+dep, "--timeout=60s"); err != nil {
		b.t.Logf("wait for %s pods to terminate: %v", dep, err)
	}
}

// kill force-deletes the service's pods. The deployment immediately creates
// replacements with fresh emptyDirs, matching a node-eviction pod
// replacement.
func (b *kindBackend) kill(service string) {
	b.t.Helper()
	dep := kindDeployments[service]
	if _, err := b.kubectl("delete", "pod", "-l", "app.kubernetes.io/name="+dep,
		"--force", "--grace-period=0"); err != nil {
		b.t.Fatalf("force delete %s pods: %v", dep, err)
	}
}

func (b *kindBackend) rolloutWait(deployment string) {
	b.t.Helper()
	if _, err := b.kubectl("rollout", "status", "deployment/"+deployment, "--timeout=180s"); err != nil {
		b.t.Fatalf("rollout of %s: %v", deployment, err)
	}
	b.settle(deployment, 1)
}

// settle waits until the deployment has exactly `want` pods and none are
// terminating. rollout status returns once new pods are ready, but the old
// pod may still hold the service endpoints and reset connections as it dies.
func (b *kindBackend) settle(deployment string, want int) {
	b.t.Helper()
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		out, err := b.kubectl("get", "pods", "-l", "app.kubernetes.io/name="+deployment,
			"--no-headers")
		if err == nil {
			lines := strings.Split(strings.TrimSpace(out), "\n")
			if strings.TrimSpace(out) == "" {
				lines = nil
			}
			ready := 0
			terminating := false
			for _, l := range lines {
				if strings.Contains(l, "Terminating") {
					terminating = true
				}
				if strings.Contains(l, "Running") && strings.Contains(l, "1/1") {
					ready++
				}
			}
			if len(lines) == want && ready == want && !terminating {
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	b.t.Fatalf("pods of %s did not settle to %d ready", deployment, want)
}

func (b *kindBackend) disarmAll() {
	for svc := range kindDeployments {
		b.env[strings.ToUpper(svc)+"_FAILPOINTS"] = ""
		b.syncEnv(svc)
	}
}

// resetState wipes batch state between scenarios: Redis (queue, in-flight,
// events, progress), Postgres records, and the object bucket.
func (b *kindBackend) resetState() {
	b.t.Helper()
	if out, err := b.kubectl("exec", "statefulset/redis-master", "--", "redis-cli", "flushall"); err != nil {
		b.t.Fatalf("flush redis: %v\n%s", err, out)
	}
	// The tables may not exist before the first component boot; tolerate that.
	if out, err := b.kubectl("exec", "statefulset/postgresql", "--", "env", "PGPASSWORD=postgres",
		"psql", "-U", "postgres", "-c", "TRUNCATE TABLE batch_items, file_items"); err != nil {
		b.t.Logf("truncate postgres (tolerated on first run): %v\n%s", err, out)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := clearBucket(ctx); err != nil {
		b.t.Fatalf("clear bucket: %v", err)
	}
}

func (b *kindBackend) kubectl(args ...string) (string, error) {
	full := append([]string{"--context", kindContext, "-n", b.ns}, args...)
	out, err := exec.Command("kubectl", full...).CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, out)
	}
	return string(out), nil
}

// The kind backend has no toxiproxy on the store paths and its inference
// layer (vllm-sim) keeps no request log; scenarios needing either skip.
func (b *kindBackend) toxiproxyAddr() (string, bool)  { return "", false }
func (b *kindBackend) inferenceRequests() (int, bool) { return 0, false }

func (b *kindBackend) dumpLogs() string {
	var sb strings.Builder
	for _, dep := range kindDeployments {
		out, _ := b.kubectl("logs", "deployment/"+dep, "--tail", "200")
		fmt.Fprintf(&sb, "--- %s ---\n%s\n", dep, out)
	}
	return sb.String()
}

// harvest saves the scenario window's traces from Jaeger (dev-deploy ships
// jaeger all-in-one on NodePort 30086), one JSON file per service.
func (b *kindBackend) harvest(dir string, start time.Time) {
	time.Sleep(8 * time.Second)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.t.Logf("trace harvest: create dir: %v", err)
		return
	}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, svc := range traceServices {
		q := url.Values{
			"service": {svc},
			"start":   {fmt.Sprint(start.UnixMicro())},
			"end":     {fmt.Sprint(time.Now().Add(time.Minute).UnixMicro())},
			"limit":   {"500"},
		}
		resp, err := client.Get(jaegerBaseURL + "/api/traces?" + q.Encode())
		if err != nil {
			b.t.Logf("trace harvest: query jaeger for %s: %v", svc, err)
			continue
		}
		data, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != http.StatusOK {
			b.t.Logf("trace harvest: read jaeger response for %s: status %d err %v", svc, resp.StatusCode, err)
			continue
		}
		if err := os.WriteFile(filepath.Join(dir, "jaeger-"+svc+".json"), data, 0o644); err != nil {
			b.t.Logf("trace harvest: write %s: %v", svc, err)
		}
	}
	b.t.Logf("harvested jaeger traces into %s", dir)
}
