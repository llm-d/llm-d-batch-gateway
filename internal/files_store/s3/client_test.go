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
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
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

func (m *mockUploader) Upload(_ context.Context, params *s3.PutObjectInput, _ ...func(*manager.Uploader)) (*manager.UploadOutput, error) { //nolint:staticcheck // TODO: migrate to feature/s3/transfermanager
	if m.uploadErr != nil {
		return nil, m.uploadErr
	}
	data, err := io.ReadAll(params.Body)
	if err != nil {
		return nil, err
	}
	m.s3Client.objects[*params.Key] = mockObject{
		data:        data,
		lastModTime: time.Now(),
	}
	return &manager.UploadOutput{}, nil
}

func (m *mockS3Client) GetObject(_ context.Context, params *s3.GetObjectInput, _ ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	obj, ok := m.objects[*params.Key]
	if !ok {
		return nil, &types.NoSuchKey{}
	}
	data := obj.data
	if params.Range != nil {
		// Parse "bytes=start-end" range header.
		var start, end int64
		if _, err := fmt.Sscanf(*params.Range, "bytes=%d-%d", &start, &end); err == nil {
			if end >= int64(len(data)) {
				end = int64(len(data)) - 1
			}
			data = data[start : end+1]
		}
	}
	return &s3.GetObjectOutput{
		Body:          io.NopCloser(bytes.NewReader(data)),
		ContentLength: aws.Int64(int64(len(data))),
		LastModified:  aws.Time(obj.lastModTime),
	}, nil
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

func TestRetrieveRange(t *testing.T) {
	ctx := context.Background()

	t.Run("retrieves byte range", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)
		content := []byte("hello world range test")

		if _, err := client.Store(ctx, "range.txt", testBucketName, 1024, 0, bytes.NewReader(content)); err != nil {
			t.Fatalf("failed to store: %v", err)
		}

		rc, err := client.RetrieveRange(ctx, "range.txt", testBucketName, 6, 5)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer rc.Close()

		data, _ := io.ReadAll(rc)
		if string(data) != "world" {
			t.Errorf("expected %q, got %q", "world", string(data))
		}
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		mock := newMockS3Client()
		client := newTestClient(mock)

		_, err := client.RetrieveRange(ctx, "nonexistent.txt", testBucketName, 0, 10)
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
