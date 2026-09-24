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

// Integration tests for the S3 client using a real S3-compatible service (SeaweedFS).
// These tests are skipped when S3_TEST_ENDPOINT is not set.
//
// Option 1: Standalone SeaweedFS via Docker
//
//   docker run -d --name seaweedfs -p 8333:8333 \
//     -e AWS_ACCESS_KEY_ID=s3admin \
//     -e AWS_SECRET_ACCESS_KEY=s3secret \
//     ghcr.io/chrislusf/seaweedfs:4.47 mini -dir=/data \
//       -webdav=false -admin.ui=false -admin.port=12646 \
//       -s3.port.iceberg=0 -s3.port.lance=0
//
//   S3_TEST_ENDPOINT=http://localhost:8333 go test -v -tags=integration -run TestS3 ./test/integration/...
//
// Option 2: After "make dev-deploy" (SeaweedFS is exposed on localhost:9002)
//
//   S3_TEST_ENDPOINT=http://localhost:9002 go test -v -tags=integration -run TestS3 ./test/integration/...

package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	s3client "github.com/llm-d/llm-d-batch-gateway/internal/files_store/s3"
)

const (
	integrationBucket     = "integration-test"
	integrationFolderName = "test-tenant-folder"
)

func s3Config() s3client.Config {
	accessKey := os.Getenv("S3_TEST_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "s3admin"
	}
	secretKey := os.Getenv("S3_TEST_SECRET_KEY")
	if secretKey == "" {
		secretKey = "s3secret"
	}

	return s3client.Config{
		Region:           "us-east-1",
		Bucket:           integrationBucket,
		Endpoint:         os.Getenv("S3_TEST_ENDPOINT"),
		AccessKeyID:      accessKey,
		SecretAccessKey:  secretKey,
		UsePathStyle:     true,
		AutoCreateBucket: true,
	}
}

func newS3IntegrationClient(t *testing.T, cfg s3client.Config) *s3client.Client {
	t.Helper()

	if cfg.Endpoint == "" {
		t.Skip("S3_TEST_ENDPOINT not set, skipping S3 integration test")
	}

	client, err := s3client.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("failed to create S3 client: %v", err)
	}

	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestS3StoreAndRetrieve(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())
	ctx := context.Background()
	content := []byte("hello integration test\nline2\nline3\n")
	fileName := fmt.Sprintf("test-store-retrieve-%s-%s", t.Name(), uuid.NewString()[:8])

	md, err := client.Store(ctx, fileName, integrationFolderName, 1024, 0, bytes.NewReader(content))
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, fileName, integrationFolderName) })

	if md.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), md.Size)
	}
	if md.LinesNumber != 3 {
		t.Errorf("expected 3 lines, got %d", md.LinesNumber)
	}

	reader, md2, err := client.Retrieve(ctx, fileName, integrationFolderName)
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

func TestS3StoreExistingFile(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-existing-%s-%s", t.Name(), uuid.NewString()[:8])

	_, err := client.Store(ctx, fileName, integrationFolderName, 1024, 0, bytes.NewReader([]byte("first")))
	if err != nil {
		t.Fatalf("first Store failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, fileName, integrationFolderName) })

	_, err = client.Store(ctx, fileName, integrationFolderName, 1024, 0, bytes.NewReader([]byte("second")))
	if !errors.Is(err, api.ErrFileExists) {
		t.Errorf("expected ErrFileExists, got %v", err)
	}
}

func TestS3StoreFileTooLarge(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-toolarge-%s-%s", t.Name(), uuid.NewString()[:8])

	_, err := client.Store(ctx, fileName, integrationFolderName, 5, 0, bytes.NewReader([]byte("this is too large")))
	if !errors.Is(err, api.ErrFileTooLarge) {
		t.Errorf("expected ErrFileTooLarge, got %v", err)
	}

	_, _, err = client.Retrieve(ctx, fileName, integrationFolderName)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected rejected file to not exist in storage, got: %v", err)
	}
}

func TestS3StoreTooManyLines(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-toomanylines-%s-%s", t.Name(), uuid.NewString()[:8])

	_, err := client.Store(ctx, fileName, integrationFolderName, 1024, 2, bytes.NewReader([]byte("l1\nl2\nl3\n")))
	if !errors.Is(err, api.ErrTooManyLines) {
		t.Errorf("expected ErrTooManyLines, got %v", err)
	}

	_, _, err = client.Retrieve(ctx, fileName, integrationFolderName)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected rejected file to not exist in storage, got: %v", err)
	}
}

func TestS3Delete(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())
	ctx := context.Background()
	fileName := fmt.Sprintf("test-delete-%s-%s", t.Name(), uuid.NewString()[:8])

	_, err := client.Store(ctx, fileName, integrationFolderName, 1024, 0, bytes.NewReader([]byte("delete me")))
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	err = client.Delete(ctx, fileName, integrationFolderName)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	_, _, err = client.Retrieve(ctx, fileName, integrationFolderName)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist after delete, got %v", err)
	}
}

func TestS3DeleteNonExistent(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())
	ctx := context.Background()

	// Deleting a file that was never created should succeed (S3 DeleteObject is idempotent).
	fileName := fmt.Sprintf("test-delete-nonexistent-%s-%s", t.Name(), uuid.NewString()[:8])
	err := client.Delete(ctx, fileName, integrationFolderName)
	if err != nil {
		t.Fatalf("Delete of non-existent file should succeed (S3 idempotent delete), got: %v", err)
	}
}

func TestS3RetrieveNonExistent(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())
	ctx := context.Background()

	_, _, err := client.Retrieve(ctx, "nonexistent-file-xyz", integrationFolderName)
	if !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected os.ErrNotExist, got %v", err)
	}
}

func TestS3Prefix(t *testing.T) {
	cfg := s3Config()
	cfg.Prefix = "testprefix"
	client := newS3IntegrationClient(t, cfg)
	ctx := context.Background()
	fileName := fmt.Sprintf("test-prefix-%s-%s", t.Name(), uuid.NewString()[:8])

	md, err := client.Store(ctx, fileName, integrationFolderName, 1024, 0, bytes.NewReader([]byte("prefixed")))
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	t.Cleanup(func() { _ = client.Delete(ctx, fileName, integrationFolderName) })

	expectedLocation := "testprefix/" + integrationFolderName + "/" + fileName
	if md.Location != expectedLocation {
		t.Errorf("expected location %s, got %s", expectedLocation, md.Location)
	}

	reader, _, err := client.Retrieve(ctx, fileName, integrationFolderName)
	if err != nil {
		t.Fatalf("Retrieve failed: %v", err)
	}
	defer reader.Close()

	data, _ := io.ReadAll(reader)
	if string(data) != "prefixed" {
		t.Errorf("expected 'prefixed', got %q", data)
	}
}

func TestS3GetContext(t *testing.T) {
	client := newS3IntegrationClient(t, s3Config())

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
