//go:build integration

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

// Integration tests for the GCS client using a real GCS or a GCS-compatible
// emulator. Authentication uses Application Default Credentials; point the
// cloud.google.com/go/storage SDK at an emulator by setting STORAGE_EMULATOR_HOST.
//
// Option 1 (recommended): make test-integration-gcs
//
//   Auto-provisions a fake-gcs-server container (fsouza/fake-gcs-server) in the
//   test process, points the SDK at it via STORAGE_EMULATOR_HOST, and runs every
//   TestGCS* test. Requires Docker or Podman; tests skip gracefully if neither is
//   available, so `make test-integration` stays green on machines without a
//   container runtime. The image is pinned by digest for reproducibility.
//
// Option 2: stand up fake-gcs-server manually
//
//   docker run -d --name fake-gcs -p 4443:4443 \
//     fsouza/fake-gcs-server -scheme http -port 4443 -public-host localhost:4443
//
//   STORAGE_EMULATOR_HOST=http://localhost:4443 \
//   GCS_TEST_BUCKET=integration-test \
//   go test -v -tags=integration -run TestGCS ./test/integration/...
//
// Option 3: real GCS via Workload Identity / gcloud application-default login
//
//   GCS_TEST_PROJECT=my-project GCS_TEST_BUCKET=my-bucket \
//   go test -v -tags=integration -run TestGCS ./test/integration/...
//
// Skip gating: tests skip when neither GCS_TEST_EMULATOR=1 nor a real
// GCS_TEST_BUCKET is configured, preserving the "graceful skip when
// unavailable" contract for machines without Docker/GCP access.

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	gcsclient "github.com/llm-d/llm-d-batch-gateway/internal/files_store/gcs"
)

const (
	gcsIntegrationFolderName = "gcs-test-tenant-folder"

	// fakeGCSImage is pinned by digest so test runs (local + CI) use the same
	// multi-arch index regardless of upstream tag movement. The digest resolves
	// both linux/amd64 (CI) and linux/arm64 (Apple silicon), so the same pin
	// works everywhere. Bump the tag and re-resolve the digest on upgrade.
	// Resolved via: docker buildx imagetools inspect fsouza/fake-gcs-server:1.55.1
	fakeGCSImage = "fsouza/fake-gcs-server:1.55.1@sha256:91afded49de804aa61b5f3eb6c7cd65205acf9e5c5e047cf0ba7d9507af806c8"
)

// gcsConfig builds a Client config for the active test run. When pointing at the
// emulator (GCS_TEST_EMULATOR=1 or STORAGE_EMULATOR_HOST set) AutoCreateBucket is
// left false so the bucket must be pre-created by gcsTestEnv; against real GCS,
// ProjectID drives auto-create since the bucket is shared and pre-existing.
func gcsConfig() gcsclient.Config {
	autoCreate := os.Getenv("GCS_TEST_PROJECT") != "" && !gcsEmulatorActive()
	return gcsclient.Config{
		Bucket:              os.Getenv("GCS_TEST_BUCKET"),
		Prefix:              "",
		ProjectID:           os.Getenv("GCS_TEST_PROJECT"),
		AutoCreateBucket:    autoCreate, // real GCS can create; emulator needs the bucket pre-created
		VerifyBucketOnStart: true,       // matches the chart default; integration tests run the verify-on path
	}
}

func newGCSIntegrationClient(t *testing.T, cfg gcsclient.Config) *gcsclient.Client {
	t.Helper()

	if cfg.Bucket == "" {
		t.Skip("GCS_TEST_BUCKET not set, skipping GCS integration test")
	}

	client, err := gcsclient.New(context.Background(), cfg)
	if err != nil {
		// Emulator-not-running and no-ADC both surface here; treat as skip so the
		// integration suite is green on machines without GCP credentials.
		if os.Getenv("GCS_TEST_BUCKET") == "" {
			t.Skipf("GCS client creation failed and GCS_TEST_BUCKET unset: %v", err)
		}
		t.Fatalf("failed to create GCS client: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })
	return client
}

// gcsEmulatorActive reports whether the SDK should be pointed at a GCS
// emulator instead of real GCS. STORAGE_EMULATOR_HOST is the env var the
// cloud.google.com/go/storage SDK consumes directly, so honor it when set;
// GCS_TEST_EMULATOR=1 is the in-test provisioning signal used by
// `make test-integration-gcs`.
func gcsEmulatorActive() bool {
	return os.Getenv("STORAGE_EMULATOR_HOST") != "" || os.Getenv("GCS_TEST_EMULATOR") == "1"
}

// gcsRequiresSetup reports whether GCS integration tests should run. They
// require a backend: either the in-test emulator (GCS_TEST_EMULATOR=1), a
// manually-configured emulator (STORAGE_EMULATOR_HOST), or real GCS
// (GCS_TEST_BUCKET). When none are configured the tests skip gracefully.
func gcsRequiresSetup() bool { return gcsEmulatorActive() || os.Getenv("GCS_TEST_BUCKET") != "" }

// gcsTestEnv provisions (or reuses) the backend the rest of the suite needs and
// arranges cleanup. It is safe to call from every TestGCS* test:
//
//   - When GCS_TEST_EMULATOR=1 (the `make test-integration-gcs` path) and no
//     emulator is already running, it starts a per-test fake-gcs-server
//     container, exports STORAGE_EMULATOR_HOST + a unique GCS_TEST_BUCKET into
//     the process environment, pre-creates the bucket, and registers cleanup.
//   - When STORAGE_EMULATOR_HOST or GCS_TEST_BUCKET is already set (manual
//     run), it assumes the caller owns the backend and just returns.
//
// gcsConfig() reads the env vars this function sets, so call gcsTestEnv before
// constructing a config. Returns a context.Background-derived ctx for callers
// that don't need special cancellation.
func gcsTestEnv(t *testing.T) context.Context {
	t.Helper()

	// Manual run (real GCS or hand-started emulator): caller owns lifecycle.
	if os.Getenv("GCS_TEST_BUCKET") != "" || (os.Getenv("STORAGE_EMULATOR_HOST") != "" && os.Getenv("GCS_TEST_EMULATOR") != "1") {
		return context.Background()
	}

	// Opt-in provisioning only when explicitly requested.
	if os.Getenv("GCS_TEST_EMULATOR") != "1" {
		t.Skip("GCS_TEST_EMULATOR=1 or GCS_TEST_BUCKET not set, skipping GCS integration test")
	}

	tool := findContainerTool()
	if tool == "" {
		t.Skip("GCS_TEST_EMULATOR=1 but no container runtime (docker/podman) available, skipping")
	}

	host, port, cleanup, err := startFakeGCSServer(tool)
	if err != nil {
		t.Skipf("failed to start fake-gcs-server, skipping: %v", err)
	}
	t.Cleanup(cleanup)

	bucket := "gcs-integration-" + uuid.NewString()[:8]
	os.Setenv("STORAGE_EMULATOR_HOST", fmt.Sprintf("http://%s:%d", host, port))
	os.Setenv("GCS_TEST_BUCKET", bucket)
	t.Cleanup(func() {
		os.Unsetenv("STORAGE_EMULATOR_HOST")
		os.Unsetenv("GCS_TEST_BUCKET")
	})

	// fake-gcs-server lazily creates a bucket on the first object write, but the
	// verify-on-startup path (VerifyBucketOnStart=true below) issues a HeadBucket
	// before any object op, so the bucket must exist up front. Create it via the
	// REST endpoint so the test exercises the same creation surface GCS exposes.
	if err := createFakeGCSBucket(host, port, bucket); err != nil {
		t.Fatalf("failed to pre-create bucket %q on fake-gcs-server: %v", bucket, err)
	}

	return context.Background()
}

// startFakeGCSServer runs fsouza/fake-gcs-server in a detached container and polls
// the bucket-list endpoint until it is ready. It returns the host:port the SDK
// should talk to and a cleanup closure that removes the container. The image is
// digest-pinned (see fakeGCSImage).
//
// The host port is allocated up front (via net.Listen on :0) and bound
// explicitly with -p <port>:4443, so the SDK's STORAGE_EMULATOR_HOST and the
// container's -public-host agree. fake-gcs-server rejects path-style object GETs
// (GET /<bucket>/<object>, which the cloud.google.com/go/storage SDK uses for
// NewReader) when the request Host doesn't match -public-host — binding a random
// port with -p 0:4443 and hardcoding -public-host localhost:4443 made every such
// GET return 404. Allocating the port first lets us set -public-host to the exact
// localhost:<port> the SDK will use, and avoids parsing `docker port` output.
func startFakeGCSServer(tool string) (host string, port int, cleanup func(), err error) {
	// Grab a free host port without keeping the listener (close immediately; the
	// listen+close race with docker binding is negligible for tests and matches
	// the standard pattern for pre-allocating ports).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", 0, nil, fmt.Errorf("allocate port: %w", err)
	}
	host = "localhost"
	port = ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	containerName := "fake-gcs-test-" + uuid.NewString()[:8]
	publicHost := fmt.Sprintf("%s:%d", host, port)
	cmd := exec.Command(tool, "run", "-d", "--rm",
		"--name", containerName,
		"-p", fmt.Sprintf("%d:4443", port),
		fakeGCSImage,
		"-scheme", "http",
		"-port", "4443",
		"-public-host", publicHost, // must match the host:port the SDK hits, or path-style GETs 404
	)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err = cmd.Run(); err != nil {
		return "", 0, nil, fmt.Errorf("docker run: %w (%s)", err, stderr.String())
	}

	endpoint := fmt.Sprintf("http://%s", publicHost)
	for i := 0; i < 30; i++ {
		time.Sleep(200 * time.Millisecond)
		resp, herr := http.Get(endpoint + "/storage/v1/b")
		if herr == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				cleanup = func() { _ = exec.Command(tool, "rm", "-f", containerName).Run() }
				return host, port, cleanup, nil
			}
		}
	}
	_ = exec.Command(tool, "rm", "-f", containerName).Run()
	return "", 0, nil, fmt.Errorf("fake-gcs-server not ready at %s after 6s", endpoint)
}

// createFakeGCSBucket creates a bucket on fake-gcs-server via the JSON REST API.
// This mirrors the surface GCS exposes (POST /storage/v1/b?project=…) without
// pulling the SDK into a setup-side path.
func createFakeGCSBucket(host string, port int, bucket string) error {
	endpoint := fmt.Sprintf("http://%s:%d", host, port)
	body := []byte(fmt.Sprintf(`{"name":%q}`, bucket))
	url := endpoint + "/storage/v1/b?project=fake-gcs-test-project"
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i < 30; i++ {
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("bucket %q not created at %s after 6s", bucket, url)
}

// waitForObjectReadable retrieves an object, retrying briefly on os.ErrNotExist.
// It only retries when running against the emulator (gcsEmulatorActive): fake-gcs-server
// returns 200 from a multipart upload before the object is fully committed to its
// in-memory backend, so an immediate GET can transiently 404; the object appears a
// few milliseconds later (a re-Store then gets 412, proving it committed). Real GCS
// is strongly consistent, so against it the retry loop is a no-op (the first call
// either succeeds or returns a genuine ErrNotExist that the caller should see).
func waitForObjectReadable(t *testing.T, client *gcsclient.Client, ctx context.Context, fileName, folderName string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	t.Helper()
	attempts := 1
	if gcsEmulatorActive() {
		attempts = 30
	}
	var (
		r   io.ReadCloser
		md  *api.BatchFileMetadata
		err error
	)
	for i := 0; i < attempts; i++ {
		r, md, err = client.Retrieve(ctx, fileName, folderName)
		if err == nil || !errors.Is(err, os.ErrNotExist) {
			return r, md, err
		}
		time.Sleep(100 * time.Millisecond)
	}
	return r, md, err
}

func TestGCSStoreAndRetrieve(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	content := []byte("hello gcs integration test\nline2\nline3\n")
	fileName := fmt.Sprintf("test-store-retrieve-%s-%s", t.Name(), uuid.NewString()[:8])

	md, err := client.Store(ctx, fileName, gcsIntegrationFolderName, 1024, 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, fileName, gcsIntegrationFolderName) })

	if md.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), md.Size)
	}
	if md.LinesNumber != 3 {
		t.Errorf("expected 3 lines, got %d", md.LinesNumber)
	}

	reader, md2, err := waitForObjectReadable(t, client, ctx, fileName, gcsIntegrationFolderName)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Errorf("content mismatch: got %q, want %q", data, content)
	}
	if md2.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), md2.Size)
	}
}

func TestGCSStoreExistingFile(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-existing-%s-%s", t.Name(), uuid.NewString()[:8])

	_, err := client.Store(ctx, fileName, gcsIntegrationFolderName, 1024, 0, bytes.NewReader([]byte("first")))
	if err != nil {
		t.Fatalf("first Store failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, fileName, gcsIntegrationFolderName) })

	_, err = client.Store(ctx, fileName, gcsIntegrationFolderName, 1024, 0, bytes.NewReader([]byte("second")))
	if !errors.Is(err, api.ErrFileExists) {
		t.Errorf("expected ErrFileExists, got %v", err)
	}
}

func TestGCSStoreFileTooLarge(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-toolarge-%s-%s", t.Name(), uuid.NewString()[:8])

	_, err := client.Store(ctx, fileName, gcsIntegrationFolderName, 5, 0, bytes.NewReader([]byte("this is too large")))
	if !errors.Is(err, api.ErrFileTooLarge) {
		t.Errorf("expected ErrFileTooLarge, got %v", err)
	}

	// The upload must be aborted, not committed: when the limit trips mid-stream
	// the writer's context is canceled (and Close never runs), so the resumable
	// session is discarded and no partial object exists under the key. Probe with
	// Retrieve rather than Delete so a regression (partial object committed)
	// surfaces as a hard error instead of covering it up.
	if _, _, err := client.Retrieve(ctx, fileName, gcsIntegrationFolderName); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("no partial object may remain after a size-limit rejection; Retrieve err = %v", err)
	}
}

func TestGCSStoreTooManyLines(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-toomanylines-%s-%s", t.Name(), uuid.NewString()[:8])

	_, err := client.Store(ctx, fileName, gcsIntegrationFolderName, 1024, 2, bytes.NewReader([]byte("l1\nl2\nl3\n")))
	if !errors.Is(err, api.ErrTooManyLines) {
		t.Errorf("expected ErrTooManyLines, got %v", err)
	}

	// Aborted upload: no partial object may remain under the key.
	if _, _, err := client.Retrieve(ctx, fileName, gcsIntegrationFolderName); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("no partial object may remain after a line-limit rejection; Retrieve err = %v", err)
	}
}

func TestGCSDelete(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-delete-%s-%s", t.Name(), uuid.NewString()[:8])

	_, err := client.Store(ctx, fileName, gcsIntegrationFolderName, 1024, 0, bytes.NewReader([]byte("delete me")))
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	if err := client.Delete(ctx, fileName, gcsIntegrationFolderName); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, _, err = client.Retrieve(ctx, fileName, gcsIntegrationFolderName)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist after delete, got %v", err)
	}
}

func TestGCSOpenStream(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-stream-%s-%s", t.Name(), uuid.NewString()[:8])

	w, err := client.OpenStream(ctx, fileName, gcsIntegrationFolderName)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	for _, line := range []string{"line1\n", "line2\n"} {
		if _, err := w.Write([]byte(line)); err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}
	if _, _, err := client.Retrieve(ctx, fileName, gcsIntegrationFolderName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("object must not be readable before Commit; Retrieve err = %v", err)
	}

	md, err := w.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, fileName, gcsIntegrationFolderName) })
	if md.Size != int64(len("line1\nline2\n")) {
		t.Errorf("expected size %d, got %d", len("line1\nline2\n"), md.Size)
	}

	reader, _, err := waitForObjectReadable(t, client, ctx, fileName, gcsIntegrationFolderName)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(data) != "line1\nline2\n" {
		t.Errorf("content mismatch: got %q", data)
	}

	if _, err := client.OpenStream(ctx, fileName, gcsIntegrationFolderName); !errors.Is(err, api.ErrFileExists) {
		t.Errorf("expected ErrFileExists when reopening a committed file, got %v", err)
	}
}

func TestGCSOpenStreamAbort(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-stream-abort-%s-%s", t.Name(), uuid.NewString()[:8])

	w, err := client.OpenStream(ctx, fileName, gcsIntegrationFolderName)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	if _, err := w.Write([]byte("partial\n")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if err := w.Abort(ctx); err != nil {
		t.Fatalf("Abort failed: %v", err)
	}

	if _, _, err := client.Retrieve(ctx, fileName, gcsIntegrationFolderName); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("no object may remain after Abort; Retrieve err = %v", err)
	}
}

func TestGCSOpenStreamCommitLosesRace(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-stream-race-%s-%s", t.Name(), uuid.NewString()[:8])

	w, err := client.OpenStream(ctx, fileName, gcsIntegrationFolderName)
	if err != nil {
		t.Fatalf("OpenStream failed: %v", err)
	}
	if _, err := w.Write([]byte("loser\n")); err != nil {
		t.Fatalf("Write failed: %v", err)
	}
	if _, err := client.Store(ctx, fileName, gcsIntegrationFolderName, 1024, 0, bytes.NewReader([]byte("winner\n"))); err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, fileName, gcsIntegrationFolderName) })

	if _, err := w.Commit(ctx); !errors.Is(err, api.ErrFileExists) {
		t.Errorf("expected ErrFileExists from a commit that lost the race, got %v", err)
	}

	reader, _, err := waitForObjectReadable(t, client, ctx, fileName, gcsIntegrationFolderName)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(data) != "winner\n" {
		t.Errorf("the winner's object must stand, got %q", data)
	}
}

func TestGCSDeletePrefix(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()
	jobID := "job-" + uuid.NewString()[:8]

	for _, name := range []string{"a", "b", "sub/c"} {
		if _, err := client.Store(ctx, "results/"+jobID+"/"+name, gcsIntegrationFolderName, 1024, 0, bytes.NewReader([]byte(name))); err != nil {
			t.Fatalf("Store %s failed: %v", name, err)
		}
	}
	// A sibling directory sharing the prefix string must survive.
	sibling := "results/" + jobID + "x/keep"
	if _, err := client.Store(ctx, sibling, gcsIntegrationFolderName, 1024, 0, bytes.NewReader([]byte("keep"))); err != nil {
		t.Fatalf("Store sibling failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, sibling, gcsIntegrationFolderName) })

	n, err := client.DeletePrefix(ctx, gcsIntegrationFolderName, "results/"+jobID)
	if err != nil {
		t.Fatalf("DeletePrefix failed: %v", err)
	}
	if n != 3 {
		t.Errorf("expected 3 deleted objects, got %d", n)
	}
	for _, name := range []string{"a", "b", "sub/c"} {
		if _, _, err := client.Retrieve(ctx, "results/"+jobID+"/"+name, gcsIntegrationFolderName); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected %s to be deleted; Retrieve err = %v", name, err)
		}
	}
	if _, _, err := client.Retrieve(ctx, sibling, gcsIntegrationFolderName); err != nil {
		t.Errorf("sibling %s must survive; Retrieve err = %v", sibling, err)
	}

	n, err = client.DeletePrefix(ctx, gcsIntegrationFolderName, "results/"+jobID+"/")
	if err != nil || n != 0 {
		t.Errorf("second DeletePrefix must be a no-op, got (%d, %v)", n, err)
	}
}

func TestGCSRetryDeleteNonExistent(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()

	// Deleting a file that was never created returns os.ErrNotExist (GCS
	// surfaces storage.ErrObjectNotExist, which the client maps); the apiserver
	// cleanup path and the GC reconciler tolerate this. We assert the contract:
	// it is NOT a generic error — it is exactly ErrNotExist.
	fileName := fmt.Sprintf("test-delete-nonexistent-%s-%s", t.Name(), uuid.NewString()[:8])
	if err := client.Delete(ctx, fileName, gcsIntegrationFolderName); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Delete of non-existent file should return os.ErrNotExist, got: %v", err)
	}
}

func TestGCSRetrieveNonExistent(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())
	ctx := context.Background()

	_, _, err := client.Retrieve(ctx, "nonexistent-gcs-file-xyz", gcsIntegrationFolderName)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist, got %v", err)
	}
}

func TestGCSPrefix(t *testing.T) {
	gcsTestEnv(t)
	cfg := gcsConfig()
	cfg.Prefix = "gcstestprefix"
	client := newGCSIntegrationClient(t, cfg)
	ctx := context.Background()
	fileName := fmt.Sprintf("test-prefix-%s-%s", t.Name(), uuid.NewString()[:8])

	md, err := client.Store(ctx, fileName, gcsIntegrationFolderName, 1024, 0, bytes.NewReader([]byte("prefixed")))
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, fileName, gcsIntegrationFolderName) })

	expectedLocation := "gcstestprefix/" + gcsIntegrationFolderName + "/" + fileName
	if md.Location != expectedLocation {
		t.Errorf("expected location %s, got %s", expectedLocation, md.Location)
	}

	reader, _, err := waitForObjectReadable(t, client, ctx, fileName, gcsIntegrationFolderName)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if string(data) != "prefixed" {
		t.Errorf("expected 'prefixed', got %q", data)
	}
}

func TestGCSGetContext(t *testing.T) {
	gcsTestEnv(t)
	client := newGCSIntegrationClient(t, gcsConfig())

	ctx, cancel := client.GetContext(context.Background(), 5*time.Second)
	defer cancel()

	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected context to have deadline")
	}

	remaining := time.Until(deadline)
	if remaining < 4*time.Second || remaining > 6*time.Second {
		t.Errorf("expected ~5s remaining, got %v", remaining)
	}
}

// TestGCSNewVerifyBucketOnStart covers the chart-default VerifyBucketOnStart=true
// path: construction must fail fast when the configured bucket is missing, rather
// than deferring the failure to the first object operation. It exercises the
// ensureBucket HeadBucket + ErrBucketNotExist branch (and, against an emulator
// whose 404 may not map to ErrBucketNotExist, the generic HeadBucket-error
// branch) via the real/emulator API. Gated behind GCS_TEST_EMULATOR=1 or
// GCS_TEST_BUCKET (see gcsRequiresSetup); autoCreate is false so a missing
// bucket stays missing.
//
// The assertion is intentionally relaxed to the "ensure bucket" wrapper that
// newFromClient adds around every ensureBucket error path, so the test is
// emulator-agnostic: it proves construction failed fast regardless of whether
// the underlying error surfaced as ErrBucketNotExist or a generic 404.
func TestGCSNewVerifyBucketOnStart(t *testing.T) {
	gcsTestEnv(t)

	cfg := gcsclient.Config{
		Bucket:              "definitely-nonexistent-batch-gateway-" + uuid.NewString(),
		VerifyBucketOnStart: true,
		AutoCreateBucket:    false,
	}

	client, err := gcsclient.New(context.Background(), cfg)
	if err == nil {
		_ = client.Close()
		t.Fatalf("expected construction to fail fast for a missing bucket with VerifyBucketOnStart=true")
	}
	if !strings.Contains(err.Error(), "ensure bucket") {
		t.Errorf("expected error to mention 'ensure bucket' (the newFromClient wrapper around every ensureBucket failure), got: %v", err)
	}
}
