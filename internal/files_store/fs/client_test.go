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

package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
)

const testFolder = "tenant1"

func newTestClient(t *testing.T) *Client {
	t.Helper()
	client, err := New(t.TempDir())
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}
	return client
}

func TestNew(t *testing.T) {
	t.Run("creates client with valid path", func(t *testing.T) {
		tmpDir := t.TempDir()
		basePath := filepath.Join(tmpDir, "storage")

		client, err := New(basePath)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if client == nil {
			t.Fatal("expected client to be non-nil")
		}

		// Verify directory was created.
		if _, err := os.Stat(basePath); os.IsNotExist(err) {
			t.Error("expected base directory to be created")
		}
	})
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	t.Run("stores file successfully", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("hello world")

		md, err := client.Store(ctx, "test.txt", testFolder, 1024, 0, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if md.Size != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), md.Size)
		}

		// Verify file content.
		data, err := os.ReadFile(filepath.Join(client.root.Name(), md.Location))
		if err != nil {
			t.Fatalf("failed to read file: %v", err)
		}
		if !bytes.Equal(data, content) {
			t.Errorf("expected content %q, got %q", content, data)
		}
	})

	t.Run("creates nested directories", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("nested content")

		md, err := client.Store(ctx, "file.txt", "tenant1/a/b/c", 1024, 0, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if !strings.Contains(md.Location, filepath.Join("tenant1", "a", "b", "c")) {
			t.Errorf("expected nested path, got %s", md.Location)
		}
	})

	t.Run("returns error for file too large", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("this content is too large")

		_, err := client.Store(ctx, "large.txt", testFolder, 5, 0, bytes.NewReader(content))
		if !errors.Is(err, api.ErrFileTooLarge) {
			t.Errorf("expected ErrFileTooLarge, got %v", err)
		}

		// Verify file was not created.
		if _, err := client.root.Stat(filepath.Join(testFolder, "large.txt")); !os.IsNotExist(err) {
			t.Error("expected file to not exist after size limit exceeded")
		}
	})

	t.Run("returns error for too many lines", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("line1\nline2\nline3\n")

		_, err := client.Store(ctx, "toomany.txt", testFolder, 1024, 2, bytes.NewReader(content))
		if !errors.Is(err, api.ErrTooManyLines) {
			t.Errorf("expected ErrTooManyLines, got %v", err)
		}

		// Verify file was not created.
		if _, err := client.root.Stat(filepath.Join(testFolder, "toomany.txt")); !os.IsNotExist(err) {
			t.Error("expected file to not exist after line limit exceeded")
		}
	})

	t.Run("stores file at exact line limit", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("line1\nline2\n")

		md, err := client.Store(ctx, "exactlines.txt", testFolder, 1024, 2, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if md.LinesNumber != 2 {
			t.Errorf("expected 2 lines, got %d", md.LinesNumber)
		}
	})

	t.Run("stores file at exact size limit", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("12345")

		md, err := client.Store(ctx, "exact.txt", testFolder, 5, 0, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		if md.Size != 5 {
			t.Errorf("expected size 5, got %d", md.Size)
		}
	})

	t.Run("returns error for existing file", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("original content")

		// Store first file.
		_, err := client.Store(ctx, "existing.txt", testFolder, 1024, 0, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("expected no error on first store, got %v", err)
		}

		// Attempt to store again with same location.
		_, err = client.Store(ctx, "existing.txt", testFolder, 1024, 0, bytes.NewReader([]byte("new content")))
		if !errors.Is(err, api.ErrFileExists) {
			t.Errorf("expected ErrFileExists, got %v", err)
		}

		// Verify original content is unchanged.
		reader, _, _ := client.Retrieve(ctx, "existing.txt", testFolder)
		defer func() {
			if closer, ok := reader.(io.Closer); ok {
				_ = closer.Close()
			}
		}()
		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, content) {
			t.Errorf("expected original content to be unchanged")
		}
	})
}

func TestStore_ConcurrentSameDir(t *testing.T) {
	ctx := context.Background()
	client := newTestClient(t)

	const n = 10
	errs := make([]error, n)
	mds := make([]*api.BatchFileMetadata, n)

	var wg sync.WaitGroup
	wg.Add(n)
	for i := range n {
		go func() {
			defer wg.Done()
			content := []byte(fmt.Sprintf("content-%d", i))
			mds[i], errs[i] = client.Store(ctx, fmt.Sprintf("file-%d.txt", i), testFolder, 1024, 0, bytes.NewReader(content))
		}()
	}
	wg.Wait()

	for i := range n {
		if errs[i] != nil {
			t.Fatalf("Store goroutine %d failed: %v", i, errs[i])
		}
	}

	for i := range n {
		reader, _, err := client.Retrieve(ctx, fmt.Sprintf("file-%d.txt", i), testFolder)
		if err != nil {
			t.Fatalf("failed to retrieve file-%d.txt: %v", i, err)
		}
		data, _ := io.ReadAll(reader)
		if closer, ok := reader.(io.Closer); ok {
			_ = closer.Close()
		}
		expected := fmt.Sprintf("content-%d", i)
		if string(data) != expected {
			t.Errorf("file-%d.txt: expected %q, got %q (data corruption from temp file collision)", i, expected, string(data))
		}
		if mds[i].Size != int64(len(expected)) {
			t.Errorf("file-%d.txt: expected size %d, got %d", i, len(expected), mds[i].Size)
		}
	}
}

func TestRetrieve(t *testing.T) {
	ctx := context.Background()

	t.Run("retrieves existing file", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("retrieve me")

		// Store first.
		_, err := client.Store(ctx, "retrieve.txt", testFolder, 1024, 0, bytes.NewReader(content))
		if err != nil {
			t.Fatalf("failed to store: %v", err)
		}

		// Retrieve.
		reader, md, err := client.Retrieve(ctx, "retrieve.txt", testFolder)
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
		client := newTestClient(t)

		_, _, err := client.Retrieve(ctx, "nonexistent.txt", testFolder)
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected os.ErrNotExist, got %v", err)
		}
	})

}

func TestRetrieveRange(t *testing.T) {
	ctx := context.Background()

	t.Run("retrieves byte range", func(t *testing.T) {
		client := newTestClient(t)
		content := []byte("hello world range test")

		if _, err := client.Store(ctx, "range.txt", testFolder, 1024, 0, bytes.NewReader(content)); err != nil {
			t.Fatalf("failed to store: %v", err)
		}

		rc, err := client.RetrieveRange(ctx, "range.txt", testFolder, 6, 5)
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
		client := newTestClient(t)

		_, err := client.RetrieveRange(ctx, "nonexistent.txt", testFolder, 0, 10)
		if err != nil {
			// Expect an error (file not found).
			return
		}
		t.Error("expected error for non-existent file")
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes existing file", func(t *testing.T) {
		client := newTestClient(t)
		_, err := client.Store(ctx, "delete.txt", testFolder, 1024, 0, bytes.NewReader([]byte("delete me")))
		if err != nil {
			t.Fatalf("failed to store: %v", err)
		}

		err = client.Delete(ctx, "delete.txt", testFolder)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Verify file is gone.
		_, _, err = client.Retrieve(ctx, "delete.txt", testFolder)
		if !errors.Is(err, os.ErrNotExist) {
			t.Error("expected file to be deleted")
		}
	})

	t.Run("rejects empty fileName and folderName", func(t *testing.T) {
		client := newTestClient(t)

		err := client.Delete(ctx, "", "")
		if err == nil {
			t.Fatal("expected error when both fileName and folderName are empty")
		}
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		client := newTestClient(t)

		err := client.Delete(ctx, "nonexistent.txt", testFolder)
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected os.ErrNotExist, got %v", err)
		}
	})

	t.Run("removes empty parent directory after last file deleted", func(t *testing.T) {
		client := newTestClient(t)
		folder := "gc-tenant"

		_, err := client.Store(ctx, "only-file.txt", folder, 1024, 0, bytes.NewReader([]byte("data")))
		if err != nil {
			t.Fatalf("failed to store: %v", err)
		}

		dirPath := filepath.Join(client.root.Name(), folder)
		if _, err := os.Stat(dirPath); err != nil {
			t.Fatalf("expected directory to exist before delete: %v", err)
		}

		if err := client.Delete(ctx, "only-file.txt", folder); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if _, err := os.Stat(dirPath); !os.IsNotExist(err) {
			t.Error("expected empty directory to be removed after last file deleted")
		}
	})

	t.Run("rejects empty folderName", func(t *testing.T) {
		client := newTestClient(t)

		err := client.Delete(ctx, "some-file.txt", "")
		if err == nil {
			t.Fatal("expected error when folderName is empty")
		}
	})

	t.Run("keeps parent directory when other files remain", func(t *testing.T) {
		client := newTestClient(t)
		folder := "gc-tenant-multi"

		_, err := client.Store(ctx, "file1.txt", folder, 1024, 0, bytes.NewReader([]byte("data1")))
		if err != nil {
			t.Fatalf("failed to store file1: %v", err)
		}
		_, err = client.Store(ctx, "file2.txt", folder, 1024, 0, bytes.NewReader([]byte("data2")))
		if err != nil {
			t.Fatalf("failed to store file2: %v", err)
		}

		if err := client.Delete(ctx, "file1.txt", folder); err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		dirPath := filepath.Join(client.root.Name(), folder)
		if _, err := os.Stat(dirPath); os.IsNotExist(err) {
			t.Error("expected directory to still exist when other files remain")
		}
	})

}

func TestGetContext(t *testing.T) {
	t.Run("uses default timeout when zero", func(t *testing.T) {
		client := newTestClient(t)

		ctx, cancel := client.GetContext(context.Background(), 0)
		defer cancel()

		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected context to have deadline")
		}

		// Deadline should be approximately defaultTimeout from now.
		expectedDeadline := time.Now().Add(defaultTimeout)
		if deadline.Before(expectedDeadline.Add(-time.Second)) || deadline.After(expectedDeadline.Add(time.Second)) {
			t.Errorf("deadline %v not within expected range around %v", deadline, expectedDeadline)
		}
	})

	t.Run("uses provided timeout", func(t *testing.T) {
		client := newTestClient(t)
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
	client := newTestClient(t)

	err := client.Close()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
}
