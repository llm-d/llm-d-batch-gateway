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

package s3

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
)

const (
	testBucketName = "test-bucket"
	testFolderName = "test-folder"
)

type mockS3Client struct {
	objects map[string]mockObject
	buckets map[string]bool
	getErr  error
	headErr error
	delErr  error
	listErr error
	// lastPutBodySeekable records whether the last PutObject body
	// implemented io.ReadSeeker — the SDK's S3 payload-hash middleware
	// reads and rewinds the body, so a streaming body breaks plain-HTTP
	// S3-compatible endpoints.
	lastPutBodySeekable bool
}

type mockObject struct {
	data        []byte
	lastModTime time.Time
}

type mockUploader struct {
	s3Client  *mockS3Client
	uploadErr error
}

func newMockS3Client() *mockS3Client {
	return &mockS3Client{
		objects: make(map[string]mockObject),
		buckets: map[string]bool{testBucketName: true},
	}
}

func (m *mockS3Client) HeadBucket(_ context.Context, params *s3.HeadBucketInput, _ ...func(*s3.Options)) (*s3.HeadBucketOutput, error) {
	if m.buckets[*params.Bucket] {
		return &s3.HeadBucketOutput{}, nil
	}
	return nil, &types.NotFound{}
}

func (m *mockS3Client) CreateBucket(_ context.Context, params *s3.CreateBucketInput, _ ...func(*s3.Options)) (*s3.CreateBucketOutput, error) {
	m.buckets[*params.Bucket] = true
	return &s3.CreateBucketOutput{}, nil
}

func (m *mockUploader) Upload(ctx context.Context, params *s3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) { //nolint:staticcheck // TODO: migrate to feature/s3/transfermanager
	if m.uploadErr != nil {
		return nil, m.uploadErr
	}
	// Read the body concurrently so a cancelled context interrupts the
	// upload, matching the real uploader's behavior.
	type readResult struct {
		data []byte
		err  error
	}
	read := make(chan readResult, 1)
	go func() {
		data, err := io.ReadAll(params.Body)
		read <- readResult{data: data, err: err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-read:
		if r.err != nil {
			return nil, r.err
		}
		m.s3Client.objects[*params.Key] = mockObject{
			data:        r.data,
			lastModTime: time.Now(),
		}
		return &manager.UploadOutput{}, nil
	}
}

func (m *mockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	obj, ok := m.objects[*params.Key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(obj.data)),
		ContentLength: aws.Int64(int64(len(obj.data))),
		LastModified:  aws.Time(obj.lastModTime),
	}, nil
}

func (m *mockS3Client) PutObject(_ context.Context, params *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	_, m.lastPutBodySeekable = params.Body.(io.ReadSeeker)
	if params.IfNoneMatch != nil && *params.IfNoneMatch == "*" {
		if _, exists := m.objects[*params.Key]; exists {
			return nil, &smithy.GenericAPIError{Code: "PreconditionFailed", Message: "object already exists"}
		}
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.objects[*params.Key] = mockObject{data: data, lastModTime: time.Now()}
	return &s3.PutObjectOutput{}, nil
}

func (m *mockS3Client) HeadObject(_ context.Context, params *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	if m.headErr != nil {
		return nil, m.headErr
	}
	obj, ok := m.objects[*params.Key]
	if !ok {
		return nil, &types.NotFound{}
	}
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(int64(len(obj.data))),
		LastModified:  aws.Time(obj.lastModTime),
	}, nil
}

func (m *mockS3Client) DeleteObject(_ context.Context, params *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	if m.delErr != nil {
		return nil, m.delErr
	}
	delete(m.objects, *params.Key)
	return &s3.DeleteObjectOutput{}, nil
}

func (m *mockS3Client) DeleteObjects(_ context.Context, params *s3.DeleteObjectsInput, _ ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	if m.delErr != nil {
		return nil, m.delErr
	}
	out := &s3.DeleteObjectsOutput{}
	for _, id := range params.Delete.Objects {
		delete(m.objects, *id.Key)
	}
	return out, nil
}

func (m *mockS3Client) ListObjectsV2(_ context.Context, params *s3.ListObjectsV2Input, _ ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	prefix := aws.ToString(params.Prefix)
	var contents []types.Object
	for key, obj := range m.objects {
		if len(prefix) == 0 || (len(key) >= len(prefix) && key[:len(prefix)] == prefix) {
			contents = append(contents, types.Object{
				Key:          aws.String(key),
				Size:         aws.Int64(int64(len(obj.data))),
				LastModified: aws.Time(obj.lastModTime),
			})
		}
	}
	return &s3.ListObjectsV2Output{
		Contents:    contents,
		IsTruncated: aws.Bool(false),
	}, nil
}

func newTestClient(mock *mockS3Client) *Client {
	return &Client{
		s3Client: mock,
		uploader: &mockUploader{s3Client: mock},
		prefix:   "",
		bucket:   testBucketName,
	}
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	t.Run("stores file successfully", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		content := []byte("hello world")

		md, err := client.Store(ctx, "test.txt", testFolderName, 1024, 0, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if md.Size != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), md.Size)
		}
		expectedKey := testFolderName + "/test.txt"
		if md.Location != expectedKey {
			t.Errorf("expected location %s, got %s", expectedKey, md.Location)
		}

		if _, ok := mock.objects[expectedKey]; !ok {
			t.Fatal("expected object to be stored")
		}
	})

	t.Run("returns error for file too large", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		content := []byte("this content is too large")

		_, err := client.Store(ctx, "large.txt", testFolderName, 5, 0, bytes.NewReader(content))
		if !errors.Is(err, api.ErrFileTooLarge) {
			t.Errorf("expected ErrFileTooLarge, got %v", err)
		}

		if _, ok := mock.objects[testFolderName+"/large.txt"]; ok {
			t.Error("expected object not to be stored")
		}
	})

	t.Run("returns error for too many lines", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		content := []byte("line1\nline2\nline3\n")

		_, err := client.Store(ctx, "toomany.txt", testFolderName, 1024, 2, bytes.NewReader(content))
		if !errors.Is(err, api.ErrTooManyLines) {
			t.Errorf("expected ErrTooManyLines, got %v", err)
		}
	})

	t.Run("stores file at exact line limit", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		content := []byte("line1\nline2\n")

		md, err := client.Store(ctx, "exactlines.txt", testFolderName, 1024, 2, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if md.LinesNumber != 2 {
			t.Errorf("expected 2 lines, got %d", md.LinesNumber)
		}
	})

	t.Run("stores file at exact size limit", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		content := []byte("12345")

		md, err := client.Store(ctx, "exact.txt", testFolderName, 5, 0, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if md.Size != 5 {
			t.Errorf("expected size 5, got %d", md.Size)
		}
	})

	t.Run("returns error for existing file", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)

		_, err := client.Store(ctx, "existing.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("original")))
		if err != nil {
			t.Fatalf("expected no error on first store, got %v", err)
		}

		_, err = client.Store(ctx, "existing.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("new content")))
		if !errors.Is(err, api.ErrFileExists) {
			t.Errorf("expected ErrFileExists, got %v", err)
		}
	})

	t.Run("uses prefix", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		client.prefix = "myprefix"

		md, err := client.Store(ctx, "test.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("content")))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expectedKey := "myprefix/" + testFolderName + "/test.txt"
		if md.Location != expectedKey {
			t.Errorf("expected location %s, got %s", expectedKey, md.Location)
		}

		if _, ok := mock.objects[expectedKey]; !ok {
			t.Error("expected object to be stored with prefix")
		}
	})

	t.Run("isolates files by folder name", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)

		_, err := client.Store(ctx, "file.txt", "tenant-a", 1024, 0, bytes.NewReader([]byte("a")))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		_, err = client.Store(ctx, "file.txt", "tenant-b", 1024, 0, bytes.NewReader([]byte("b")))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if _, ok := mock.objects["tenant-a/file.txt"]; !ok {
			t.Error("expected tenant-a file to be stored")
		}
		if _, ok := mock.objects["tenant-b/file.txt"]; !ok {
			t.Error("expected tenant-b file to be stored")
		}
	})
}

func TestRetrieve(t *testing.T) {
	ctx := context.Background()

	t.Run("retrieves existing file", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		content := []byte("retrieve me")

		_, err := client.Store(ctx, "retrieve.txt", testFolderName, 1024, 0, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("failed to store: %v", err)
		}

		reader, md, err := client.Retrieve(ctx, "retrieve.txt", testFolderName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer func() {
			if closer, ok := reader.(io.Closer); ok {
				_ = closer.Close()
			}
		}()

		if md.Size != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), md.Size)
		}

		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, content) {
			t.Errorf("expected content %q, got %q", content, data)
		}
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)

		_, _, err := client.Retrieve(ctx, "nonexistent.txt", testFolderName)
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected os.ErrNotExist, got %v", err)
		}
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes existing file", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		_, _ = client.Store(ctx, "delete.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("delete me")))

		err := client.Delete(ctx, "delete.txt", testFolderName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		_, _, err = client.Retrieve(ctx, "delete.txt", testFolderName)
		if !errors.Is(err, os.ErrNotExist) {
			t.Error("expected file to be deleted")
		}
	})

	t.Run("succeeds for non-existent file", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)

		err := client.Delete(ctx, "nonexistent.txt", testFolderName)
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})
}

func TestDeletePrefix(t *testing.T) {
	ctx := context.Background()
	prefix := "results/job-1/"

	seed := func(t *testing.T) *mockS3Client {
		t.Helper()
		mock := newMockS3Client()
		client := newTestClient(mock)
		for _, name := range []string{
			"results/job-1/req-1.jsonl",
			"results/job-1/req-2.jsonl",
			"results/job-1/req-2.fallback.jsonl",
			"results/job-10/req-1.jsonl",
			"other.txt",
		} {
			_, _ = client.Store(ctx, name, testFolderName, 1024, 0, bytes.NewReader([]byte("data")))
		}
		return mock
	}

	t.Run("deletes every object under the prefix", func(t *testing.T) {
		mock := seed(t)
		client := newTestClient(mock)

		deleted, err := client.DeletePrefix(ctx, testFolderName, prefix)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if deleted != 3 {
			t.Errorf("deleted = %d, want 3", deleted)
		}
		for _, key := range []string{
			testFolderName + "/results/job-1/req-1.jsonl",
			testFolderName + "/results/job-1/req-2.jsonl",
			testFolderName + "/results/job-1/req-2.fallback.jsonl",
		} {
			if _, ok := mock.objects[key]; ok {
				t.Errorf("expected object %s to be deleted", key)
			}
		}
	})

	t.Run("keeps sibling prefixes sharing the prefix string", func(t *testing.T) {
		mock := seed(t)
		client := newTestClient(mock)

		// The trailing slash is missing, but the prefix must still not match
		// results/job-10/.
		if _, err := client.DeletePrefix(ctx, testFolderName, "results/job-1"); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if _, ok := mock.objects[testFolderName+"/results/job-10/req-1.jsonl"]; !ok {
			t.Error("expected sibling object to survive")
		}
	})

	t.Run("keeps objects outside the prefix", func(t *testing.T) {
		mock := seed(t)
		client := newTestClient(mock)

		if _, err := client.DeletePrefix(ctx, testFolderName, prefix); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		for _, key := range []string{testFolderName + "/results/job-10/req-1.jsonl", testFolderName + "/other.txt"} {
			if _, ok := mock.objects[key]; !ok {
				t.Errorf("expected object %s to survive", key)
			}
		}
	})

	t.Run("nonexistent prefix is a no-op", func(t *testing.T) {
		mock := seed(t)
		client := newTestClient(mock)

		deleted, err := client.DeletePrefix(ctx, testFolderName, "results/never-seen/")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if deleted != 0 {
			t.Errorf("deleted = %d, want 0", deleted)
		}
	})

	t.Run("returns the list error", func(t *testing.T) {
		mock := seed(t)
		mock.listErr = errors.New("list failed")
		client := newTestClient(mock)

		if _, err := client.DeletePrefix(ctx, testFolderName, prefix); err == nil {
			t.Fatal("expected error when listing fails")
		}
	})
}

func TestEnsureBucket(t *testing.T) {
	ctx := context.Background()

	t.Run("auto-creates bucket when enabled", func(t *testing.T) {
		mock := newMockS3Client()
		delete(mock.buckets, testBucketName)

		c := &Client{
			s3Client: mock,
			uploader: &mockUploader{s3Client: mock},
			bucket:   testBucketName,
		}

		if err := c.ensureBucket(ctx, true); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !mock.buckets[testBucketName] {
			t.Error("expected bucket to be created")
		}
	})

	t.Run("returns error when bucket missing and auto-create disabled", func(t *testing.T) {
		mock := newMockS3Client()
		delete(mock.buckets, testBucketName)

		c := &Client{
			s3Client: mock,
			uploader: &mockUploader{s3Client: mock},
			bucket:   testBucketName,
		}

		err := c.ensureBucket(ctx, false)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
	})

	t.Run("succeeds when bucket already exists", func(t *testing.T) {
		mock := newMockS3Client()

		c := &Client{
			s3Client: mock,
			uploader: &mockUploader{s3Client: mock},
			bucket:   testBucketName,
		}

		if err := c.ensureBucket(ctx, false); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
	})
}

func TestGetContext(t *testing.T) {
	t.Run("uses default timeout when zero", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)

		ctx, cancel := client.GetContext(context.Background(), 0)
		defer cancel()

		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected context to have deadline")
		}

		expectedDeadline := time.Now().Add(defaultTimeout)
		if deadline.Before(expectedDeadline.Add(-time.Second)) || deadline.After(expectedDeadline.Add(time.Second)) {
			t.Errorf("deadline %v not within expected range around %v", deadline, expectedDeadline)
		}
	})

	t.Run("uses provided timeout", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		customTimeout := 5 * time.Second

		ctx, cancel := client.GetContext(context.Background(), customTimeout)
		defer cancel()

		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected context to have deadline")
		}

		expectedDeadline := time.Now().Add(customTimeout)
		if deadline.Before(expectedDeadline.Add(-time.Second)) || deadline.After(expectedDeadline.Add(time.Second)) {
			t.Errorf("deadline %v not within expected range around %v", deadline, expectedDeadline)
		}
	})
}

func TestClose(t *testing.T) {
	mock := newMockS3Client()
	client := newTestClient(mock)

	err := client.Close()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}

func TestBuildKey(t *testing.T) {
	tests := []struct {
		name       string
		prefix     string
		folderName string
		fileName   string
		expected   string
	}{
		{"no prefix", "", "folder", "file.txt", "folder/file.txt"},
		{"with prefix", "myprefix", "folder", "file.txt", "myprefix/folder/file.txt"},
		{"nested fileName", "prefix", "folder", "a/b/file.txt", "prefix/folder/a/b/file.txt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{prefix: tt.prefix}
			result := client.buildKey(tt.folderName, tt.fileName)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	t.Run("valid config", func(t *testing.T) {
		cfg := &Config{Region: "us-east-1", Bucket: "my-bucket"}
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("missing region", func(t *testing.T) {
		cfg := &Config{Bucket: "my-bucket"}
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for missing region")
		}
	})

	t.Run("missing bucket", func(t *testing.T) {
		cfg := &Config{Region: "us-east-1"}
		if err := cfg.Validate(); err == nil {
			t.Error("expected error for missing bucket")
		}
	})
}

func TestOpenStream(t *testing.T) {
	t.Run("commit uploads the streamed data", func(t *testing.T) {
		ctx := context.Background()
		mock := newMockS3Client()
		client := newTestClient(mock)
		content := []byte("line1\nline2\n")
		key := testFolderName + "/out.jsonl"

		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if _, err := w.Write(content); err != nil {
			t.Fatalf("write failed: %v", err)
		}

		md, err := w.Commit(ctx)
		if err != nil {
			t.Fatalf("commit failed: %v", err)
		}
		if md.Size != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), md.Size)
		}
		if md.Location != key {
			t.Errorf("expected location %s, got %s", key, md.Location)
		}

		obj, ok := mock.objects[key]
		if !ok {
			t.Fatal("expected object to be uploaded")
		}
		if !bytes.Equal(obj.data, content) {
			t.Errorf("expected content %q, got %q", content, obj.data)
		}
	})

	t.Run("abort leaves no object behind", func(t *testing.T) {
		ctx := context.Background()
		mock := newMockS3Client()
		client := newTestClient(mock)

		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if _, err := w.Write([]byte("discarded")); err != nil {
			t.Fatalf("write failed: %v", err)
		}
		if err := w.Abort(ctx); err != nil {
			t.Fatalf("abort failed: %v", err)
		}

		if _, _, err := client.Retrieve(ctx, "out.jsonl", testFolderName); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected no object after abort, got %v", err)
		}
	})

	t.Run("upload failure surfaces from commit", func(t *testing.T) {
		ctx := context.Background()
		mock := newMockS3Client()
		client := &Client{
			s3Client: mock,
			uploader: &mockUploader{s3Client: mock, uploadErr: errors.New("upload failed")},
			bucket:   testBucketName,
		}

		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		// The upload may already be dead, in which case the write fails
		// with the upload error; either way Commit must surface it.
		_, _ = w.Write([]byte("data"))

		if _, err := w.Commit(ctx); err == nil {
			t.Fatal("expected commit to fail")
		}
	})

	t.Run("context cancellation fails writes", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)

		ctx, cancel := context.WithCancel(context.Background())
		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		cancel()

		// The upload goroutine observes the cancellation and closes the
		// pipe, so a subsequent write fails instead of blocking forever.
		deadline := time.Now().Add(5 * time.Second)
		for {
			_, err = w.Write([]byte("data"))
			if err != nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("write did not fail after context cancellation")
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err := w.Abort(context.Background()); err != nil {
			t.Fatalf("abort failed: %v", err)
		}
	})

	t.Run("commit is create-only: a stale writer cannot replace the winner's object", func(t *testing.T) {
		ctx := context.Background()
		mock := newMockS3Client()
		client := newTestClient(mock)
		key := testFolderName + "/out.jsonl"

		// Both writers pass the open-time existence check — the takeover
		// shape: a stale owner that opened its stream before the current
		// owner published. Each streams into its own staging object.
		winner, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("open winner: %v", err)
		}
		stale, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("open stale: %v", err)
		}
		if _, err := winner.Write([]byte("winner")); err != nil {
			t.Fatalf("write winner: %v", err)
		}
		if _, err := stale.Write([]byte("stale")); err != nil {
			t.Fatalf("write stale: %v", err)
		}

		if _, err := winner.Commit(ctx); err != nil {
			t.Fatalf("winner commit: %v", err)
		}
		if _, err := stale.Commit(ctx); !errors.Is(err, api.ErrFileExists) {
			t.Fatalf("stale commit: expected ErrFileExists, got %v", err)
		}

		// The winner's publication stands, byte for byte.
		obj, ok := mock.objects[key]
		if !ok {
			t.Fatal("expected the destination object to be published")
		}
		if string(obj.data) != "winner" {
			t.Errorf("published content = %q, want the winner's %q", obj.data, "winner")
		}

		// Both staging objects were cleaned up: only the publication
		// remains.
		if len(mock.objects) != 1 {
			t.Errorf("expected only the destination object, got %d objects: %v", len(mock.objects), mock.objects)
		}
	})

	t.Run("commit publishes a seekable body", func(t *testing.T) {
		// Regression guard for the SeaweedFS e2e failure: the SDK's S3
		// payload-hash middleware reads the PUT body to compute
		// x-amz-content-sha256 and rewinds it. A streaming (non-seekable)
		// body makes every publish fail against plain-HTTP S3-compatible
		// endpoints, parking batches in finalizing forever.
		ctx := context.Background()
		mock := newMockS3Client()
		client := newTestClient(mock)

		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("open: %v", err)
		}
		if _, err := w.Write([]byte("line1\n")); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := w.Commit(ctx); err != nil {
			t.Fatalf("commit: %v", err)
		}
		if !mock.lastPutBodySeekable {
			t.Fatal("publish PUT body was not seekable: the SDK must be able to hash and rewind it")
		}
	})

	t.Run("returns ErrFileExists for stored object", func(t *testing.T) {
		ctx := context.Background()
		mock := newMockS3Client()
		client := newTestClient(mock)

		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if _, err := w.Commit(ctx); err != nil {
			t.Fatalf("commit failed: %v", err)
		}

		if _, err := client.OpenStream(ctx, "out.jsonl", testFolderName); !errors.Is(err, api.ErrFileExists) {
			t.Errorf("expected ErrFileExists, got %v", err)
		}
	})
}

func TestOpenStream_AbortAfterFailedCommitDoesNotHang(t *testing.T) {
	ctx := context.Background()
	mock := newMockS3Client()
	// Force the upload to fail so Commit consumes the error result.
	client := &Client{
		s3Client: mock,
		uploader: &mockUploader{s3Client: mock, uploadErr: errors.New("injected upload failure")},
		prefix:   "",
		bucket:   testBucketName,
	}

	w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// The failed upload breaks the pipe, so the write error is expected.
	_, _ = w.Write([]byte("data"))
	if _, err := w.Commit(ctx); err == nil {
		t.Fatal("expected commit to fail")
	}

	// The caller's cleanup Abort must not block on the already-consumed
	// result channel (the upload goroutine sends exactly one value).
	done := make(chan error, 1)
	go func() { done <- w.Abort(ctx) }()
	// The only property that matters here: Abort returns instead of
	// blocking on the already-consumed result channel. Its error value is
	// irrelevant — the stream already failed.
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Abort blocked after Commit consumed the result channel")
	}
}
