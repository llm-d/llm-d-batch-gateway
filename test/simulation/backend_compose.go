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
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const composeProject = "bgw-sim"

// composeBackend runs the topology with docker compose, falling back to
// host-run vllm-rs + vllm-vcr processes when the vcr container image is not
// available locally.
type composeBackend struct {
	t          *testing.T
	dir        string
	composeCmd []string
	files      []string
	env        map[string]string
	hostVCR    []*exec.Cmd
}

var _ stackBackend = (*composeBackend)(nil)

func newComposeBackend(t *testing.T) *composeBackend {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	b := &composeBackend{t: t, dir: filepath.Dir(thisFile), env: map[string]string{}}
	b.composeCmd = detectCompose(t)
	simCreds = generateCredentials(t)
	simCreds.writeSecretFiles(t, filepath.Join(b.dir, "secrets"))
	b.env["SIM_PG_PASSWORD"] = simCreds.pgPassword
	b.env["SIM_S3_SECRET_KEY"] = simCreds.s3SecretKey
	b.files = []string{filepath.Join(b.dir, "compose.yaml")}
	if _, err := exec.Command("docker", "image", "inspect", b.vcrImage()).Output(); err != nil {
		b.startHostVCR()
		b.files = append(b.files, filepath.Join(b.dir, "compose.hostvcr.yaml"))
	}
	b.compose("down", "-v", "--remove-orphans")
	t.Cleanup(func() {
		b.compose("down", "-v", "--remove-orphans")
		b.stopHostVCR()
	})
	return b
}

func (b *composeBackend) apiURL() string { return "http://127.0.0.1:18080" }

func (b *composeBackend) ensureUp(env map[string]string) {
	b.t.Helper()
	for k, v := range env {
		b.env[k] = v
	}
	b.compose("up", "-d", "--build", "--wait")
}

func (b *composeBackend) applyEnv(key, value string) { b.env[key] = value }

func (b *composeBackend) restart(service string) {
	b.t.Helper()
	b.compose("up", "-d", "--wait", service)
}

func (b *composeBackend) stop(service string) {
	b.t.Helper()
	b.compose("stop", service)
}

func (b *composeBackend) kill(service string) {
	b.t.Helper()
	b.compose("kill", "-s", "SIGKILL", service)
}

func (b *composeBackend) dumpLogs() string {
	cmd := b.composeArgs("logs", "--no-color", "--tail", "200", "apiserver", "processor", "gc")
	out, _ := cmd.CombinedOutput()
	return string(out)
}

// detectCompose returns the compose invocation: the docker compose plugin
// when present, otherwise the standalone docker-compose v2 binary.
func detectCompose(t *testing.T) []string {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not found; simulation harness requires a container runtime")
	}
	if err := exec.Command("docker", "info").Run(); err != nil {
		t.Skip("docker daemon unavailable; skipping simulation harness")
	}
	if err := exec.Command("docker", "compose", "version").Run(); err == nil {
		return []string{"docker", "compose"}
	}
	if path, err := exec.LookPath("docker-compose"); err == nil {
		return []string{path}
	}
	t.Skip("no compose implementation found (docker compose plugin or docker-compose)")
	return nil
}

func (b *composeBackend) vcrImage() string {
	if img := b.env["VCR_IMAGE"]; img != "" {
		return img
	}
	if img := os.Getenv("VCR_IMAGE"); img != "" {
		return img
	}
	return "ghcr.io/neuralmagic/vllm-vcr:dev"
}

func (b *composeBackend) composeArgs(args ...string) *exec.Cmd {
	full := b.composeCmd[1:]
	for _, f := range b.files {
		full = append(full, "-f", f)
	}
	full = append(full, "-p", composeProject)
	full = append(full, args...)
	cmd := exec.Command(b.composeCmd[0], full...)
	cmd.Env = os.Environ()
	for k, v := range b.env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	return cmd
}

func (b *composeBackend) compose(args ...string) {
	b.t.Helper()
	cmd := b.composeArgs(args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		b.t.Fatalf("%s failed: %v\n%s", strings.Join(cmd.Args, " "), err, out)
	}
}

const (
	hostVCRPort      = 18000
	hostHandshake    = "tcp://127.0.0.1:29551"
	hostVCRWaitLimit = 5 * time.Minute // first run downloads the tokenizer from HF
)

// startHostVCR launches the vLLM Rust frontend and the vllm-vcr mock engine
// on the host; the compose.hostvcr.yaml override forwards the vcr service to
// them. Skips the scenario when the binaries are not built.
func (b *composeBackend) startHostVCR() {
	b.t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		b.t.Fatalf("resolve home dir: %v", err)
	}
	frontendBin := envOr("VLLM_RS_BIN", filepath.Join(home, "git/vllm-main/rust/target/debug/vllm-rs"))
	engineBin := envOr("VLLM_VCR_BIN", filepath.Join(home, "git/vllm-vcr/target/debug/vllm-vcr"))
	for _, bin := range []string{frontendBin, engineBin} {
		if _, err := os.Stat(bin); err != nil {
			b.t.Skipf("host-vcr binary %s not found (and no %s container image); build it or set VCR_IMAGE", bin, b.vcrImage())
		}
	}

	logDir := b.t.TempDir()
	frontend := exec.Command(frontendBin, "serve", simModel(),
		"--data-parallel-size", "1",
		"--data-parallel-size-local", "0",
		"--handshake-port", "29551",
		"--host", "127.0.0.1",
		"--port", fmt.Sprint(hostVCRPort),
		"--reasoning-parser", "none",
		"--tool-call-parser", "none",
	)
	engine := exec.Command(engineBin, "play",
		"--handshake-address", hostHandshake,
		"--log-requests",
		"--time-to-first-token", envOr("SIM_TTFT_MS", "200"),
		"--inter-token-latency", envOr("SIM_ITL_MS", "30"),
	)
	for name, cmd := range map[string]*exec.Cmd{"frontend": frontend, "engine": engine} {
		logFile, err := os.Create(filepath.Join(logDir, name+".log"))
		if err != nil {
			b.t.Fatalf("create %s log: %v", name, err)
		}
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			b.stopHostVCR()
			b.t.Fatalf("start host %s: %v", name, err)
		}
		b.hostVCR = append(b.hostVCR, cmd)
	}
	b.t.Logf("host-vcr started (frontend :%d, logs in %s)", hostVCRPort, logDir)

	deadline := time.Now().Add(hostVCRWaitLimit)
	healthURL := fmt.Sprintf("http://127.0.0.1:%d/health", hostVCRPort)
	for time.Now().Before(deadline) {
		resp, err := http.Get(healthURL)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(1 * time.Second)
	}
	b.stopHostVCR()
	b.t.Fatalf("host-vcr frontend did not become healthy within %s (logs in %s)", hostVCRWaitLimit, logDir)
}

func (b *composeBackend) stopHostVCR() {
	for _, cmd := range b.hostVCR {
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}
	b.hostVCR = nil
}
