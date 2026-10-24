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

package gcs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"google.golang.org/api/googleapi"
)

const (
	testBucketName = "test-bucket"
	testFolderName = "test-folder"
)

// mockObject is the in-memory representation of a stored GCS object.
type mockObject struct {
	data    []byte
	modTime time.Time
}

// mockBucket is an in-memory GCS store implementing the bucket bits the client needs.
type mockBucket struct {
	mu      sync.Mutex
	objects map[string]mockObject
	exists  bool
	// attrsErr, when non-nil, is returned by Attrs instead of the exists /
	// ErrBucketNotExist path. Used to simulate a HeadBucket permission error
	// (e.g. googleapi 403) for the verify-bucket-on-start opt-out tests.
	attrsErr error
	// createErr, when non-nil, is returned by Create instead of the
	// exists-flip path. Used to simulate a 409 Conflict (bucket already owned)
	// for the ensureBucket already-owned tolerance test.
	createErr error
}

func newMockBucket() *mockBucket {
	return &mockBucket{objects: make(map[string]mockObject), exists: true}
}

func (m *mockBucket) Attrs(_ context.Context) (*storage.BucketAttrs, error) {
	if m.attrsErr != nil {
		return nil, m.attrsErr
	}
	if !m.exists {
		return nil, storage.ErrBucketNotExist
	}
	return &storage.BucketAttrs{Name: testBucketName}, nil
}

func (m *mockBucket) Create(_ context.Context, _ string, _ *storage.BucketAttrs) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.createErr != nil {
		return m.createErr
	}
	if m.exists {
		return nil // benign: concurrent create, like BucketAlreadyOwnedByYou
	}
	m.exists = true
	return nil
}

// mockWriter captures bytes written by the client into the mock store on Close.
// It mirrors the real *storage.Writer closely enough to exercise the abort path:
// it honors its context — once canceled, Write fails and the buffered bytes are
// discarded, so a canceled upload can never commit, even if Close is called.
type mockWriter struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	key    string
	bucket *mockBucket
	closed bool
	ctx    context.Context
}

func (w *mockWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, errors.New("write after close")
	}
	if err := w.ctx.Err(); err != nil {
		return 0, err
	}
	return w.buf.Write(p)
}

func (w *mockWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	w.closed = true
	// Mirror the SDK's monitorCancel: once the writer's context is canceled the
	// upload is aborted — Close reports the context error and the buffered bytes
	// are never committed, regardless of who calls Close.
	if err := w.ctx.Err(); err != nil {
		return err
	}
	w.bucket.mu.Lock()
	// Mirror the IfDoesNotExist conditional write: if the object already exists,
	// Close returns a 412 Precondition Failed (a *googleapi.Error) rather than
	// overwriting it. The client maps this back to api.ErrFileExists.
	if _, exists := w.bucket.objects[w.key]; exists {
		w.bucket.mu.Unlock()
		return &googleapi.Error{Code: http.StatusPreconditionFailed, Message: " precondition=IfNonExistent"}
	}
	w.bucket.objects[w.key] = mockObject{data: append([]byte(nil), w.buf.Bytes()...), modTime: time.Now()}
	w.bucket.mu.Unlock()
	return nil
}

// errAfterReader serves its first failAfter bytes, then fails with the given
// error (io.ErrUnexpectedEOF when nil) instead of returning EOF.
type errAfterReader struct {
	data      []byte
	failAfter int
	failed    bool
}

func (r *errAfterReader) Read(p []byte) (int, error) {
	if r.failed {
		return 0, io.ErrUnexpectedEOF
	}
	if len(r.data) <= r.failAfter {
		n := copy(p, r.data)
		r.data = r.data[n:]
		return n, nil
	}
	n := copy(p, r.data[:r.failAfter])
	r.data = nil
	r.failed = true
	return n, io.ErrUnexpectedEOF
}

// mockReader wraps stored bytes as an io.ReadCloser and reports size/mtime.
type mockReader struct {
	*bytes.Reader
	size    int64
	modTime time.Time
}

func (r *mockReader) Size() int64        { return r.size }
func (r *mockReader) ModTime() time.Time { return r.modTime }
func (r *mockReader) Close() error       { return nil }

// newTestClient builds a Client backed by an in-memory mockBucket, exposing the
// bucket so tests can inspect state and inject errors via the returned closures.
func newTestClient(bucket *mockBucket, prefix string) (*Client, *mockBucket) {
	c := &Client{
		bucket:     bucket,
		prefix:     prefix,
		bucketName: testBucketName,
		newWriter: func(wctx context.Context, key string, _ int64) writer {
			return &mockWriter{bucket: bucket, key: key, ctx: wctx}
		},
		newReader: func(_ context.Context, key string) (reader, error) {
			bucket.mu.Lock()
			defer bucket.mu.Unlock()
			obj, ok := bucket.objects[key]
			if !ok {
				return nil, storage.ErrObjectNotExist
			}
			return &mockReader{Reader: bytes.NewReader(obj.data), size: int64(len(obj.data)), modTime: obj.modTime}, nil
		},
		deleteObject: func(_ context.Context, key string) error {
			bucket.mu.Lock()
			defer bucket.mu.Unlock()
			if _, ok := bucket.objects[key]; !ok {
				return storage.ErrObjectNotExist
			}
			delete(bucket.objects, key)
			return nil
		},
		statObject: func(_ context.Context, key string) error {
			bucket.mu.Lock()
			defer bucket.mu.Unlock()
			if _, ok := bucket.objects[key]; !ok {
				return storage.ErrObjectNotExist
			}
			return nil
		},
		listObjects: func(_ context.Context, prefix string, fn func(key string) error) error {
			bucket.mu.Lock()
			var keys []string
			for k := range bucket.objects {
				if strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
				}
			}
			bucket.mu.Unlock()
			sort.Strings(keys)
			for _, k := range keys {
				if err := fn(k); err != nil {
					return err
				}
			}
			return nil
		},
	}
	return c, bucket
}

// putObject seeds the mock bucket directly, bypassing the client.
func (m *mockBucket) putObject(key, data string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = mockObject{data: []byte(data), modTime: time.Now()}
}

// hasObject reports whether key is stored in the mock bucket.
func (m *mockBucket) hasObject(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.objects[key]
	return ok
}

func TestStore(t *testing.T) {
	ctx := context.Background()

	t.Run("stores file successfully", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
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

		bucket.mu.Lock()
		obj, ok := bucket.objects[expectedKey]
		bucket.mu.Unlock()
		if !ok {
			t.Fatal("expected object to be stored")
		}
		if string(obj.data) != string(content) {
			t.Errorf("expected stored content %q, got %q", content, obj.data)
		}
	})

	t.Run("returns error for file too large", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		content := []byte("this content is too large")

		_, err := client.Store(ctx, "large.txt", testFolderName, 5, 0, bytes.NewReader(content))
		if !errors.Is(err, api.ErrFileTooLarge) {
			t.Errorf("expected ErrFileTooLarge, got %v", err)
		}

		// The upload must be aborted, not committed: the writer's context is
		// canceled when the limit trips and Close is never called, so no partial
		// object may exist under the key.
		key := testFolderName + "/large.txt"
		bucket.mu.Lock()
		_, committed := bucket.objects[key]
		bucket.mu.Unlock()
		if committed {
			t.Errorf("partial object must not be committed when the size limit trips; found %d bytes under %s", len(content), key)
		}
	})

	t.Run("returns error for too many lines", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		content := []byte("line1\nline2\nline3\n")

		_, err := client.Store(ctx, "toomany.txt", testFolderName, 1024, 2, bytes.NewReader(content))
		if !errors.Is(err, api.ErrTooManyLines) {
			t.Errorf("expected ErrTooManyLines, got %v", err)
		}

		// Aborted upload: no partial object may exist under the key.
		key := testFolderName + "/toomany.txt"
		bucket.mu.Lock()
		_, committed := bucket.objects[key]
		bucket.mu.Unlock()
		if committed {
			t.Errorf("partial object must not be committed when the line limit trips; found bytes under %s", key)
		}
	})

	t.Run("stores file at exact line limit", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
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
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
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
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")

		if _, err := client.Store(ctx, "existing.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("original"))); err != nil {
			t.Fatalf("expected no error on first store, got %v", err)
		}

		_, err := client.Store(ctx, "existing.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("new content")))
		if !errors.Is(err, api.ErrFileExists) {
			t.Errorf("expected ErrFileExists, got %v", err)
		}
	})

	t.Run("uses prefix", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "myprefix")

		md, err := client.Store(ctx, "test.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("content")))
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		expectedKey := "myprefix/" + testFolderName + "/test.txt"
		if md.Location != expectedKey {
			t.Errorf("expected location %s, got %s", expectedKey, md.Location)
		}

		bucket.mu.Lock()
		if _, ok := bucket.objects[expectedKey]; !ok {
			t.Errorf("expected object at prefixed key %s", expectedKey)
		}
		bucket.mu.Unlock()
	})

	t.Run("existing file is not overwritten (conditional write 412)", func(t *testing.T) {
		// Store uses an IfDoesNotExist conditional write. When the object already
		// exists the mockWriter.Close returns a 412 *googleapi.Error, which the
		// client maps to api.ErrFileExists — never overwriting the stored bytes.
		// This is the unit-level assertion of the atomicity that the real GCS
		// API enforces server-side; concurrent-writer determinism is left to the
		// integration suite (TestGCSStoreExistingFile against the real/emulator API).
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		key := testFolderName + "/keep.txt"

		bucket.objects[key] = mockObject{data: []byte("original"), modTime: time.Now()}

		_, err := client.Store(ctx, "keep.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("new")))
		if !errors.Is(err, api.ErrFileExists) {
			t.Fatalf("expected ErrFileExists from conditional write, got %v", err)
		}

		bucket.mu.Lock()
		obj := bucket.objects[key]
		bucket.mu.Unlock()
		if string(obj.data) != "original" {
			t.Errorf("existing object must not be overwritten; want %q, got %q", "original", obj.data)
		}
	})

	t.Run("aborted upload leaves no partial object and retry stores cleanly", func(t *testing.T) {
		// Regression guard for a review finding: a source-read error that is
		// neither limit sentinel must abort the upload, never committing the
		// bytes read so far. Previously such an error fell through to Close,
		// which finalized the resumable session and published a truncated
		// object; the retry then hit ErrFileExists on the partial object.
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		key := testFolderName + "/partial.txt"

		errReader := &errAfterReader{data: []byte("first chunk of data that will fail mid-read"), failAfter: 10}
		_, err := client.Store(ctx, "partial.txt", testFolderName, 1024, 0, errReader)
		if err == nil || errors.Is(err, api.ErrFileExists) {
			t.Fatalf("expected the read error to surface, got %v", err)
		}

		if bucket.hasObject(key) {
			t.Fatalf("failed Store must not leave a partial object under %s", key)
		}

		// The abort leaves a clean not-exists: the same key must now Store
		// without tripping the IfDoesNotExist precondition on a ghost object.
		md, err := client.Store(ctx, "partial.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("complete content")))
		if err != nil {
			t.Fatalf("retry after aborted upload should succeed, got %v", err)
		}
		if md.Size != int64(len("complete content")) {
			t.Errorf("expected size %d, got %d", len("complete content"), md.Size)
		}
	})
}

func TestRetrieve(t *testing.T) {
	ctx := context.Background()

	t.Run("retrieves file successfully", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		content := []byte("retrieve me")
		if _, err := client.Store(ctx, "get.txt", testFolderName, 1024, 0, bytes.NewReader(content)); err != nil {
			t.Fatalf("store: %v", err)
		}

		r, md, err := client.Retrieve(ctx, "get.txt", testFolderName)
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}
		defer r.Close()

		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, content) {
			t.Errorf("expected %q, got %q", content, got)
		}
		expectedKey := testFolderName + "/get.txt"
		if md.Location != expectedKey {
			t.Errorf("expected location %s, got %s", expectedKey, md.Location)
		}
		if md.Size != int64(len(content)) {
			t.Errorf("expected size %d, got %d", len(content), md.Size)
		}
	})

	t.Run("returns os.ErrNotExist for missing file", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")

		_, _, err := client.Retrieve(ctx, "missing.txt", testFolderName)
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected os.ErrNotExist, got %v", err)
		}
	})
}

func TestDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes existing file", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		if _, err := client.Store(ctx, "delete.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("store: %v", err)
		}

		if err := client.Delete(ctx, "delete.txt", testFolderName); err != nil {
			t.Fatalf("delete: %v", err)
		}

		bucket.mu.Lock()
		_, ok := bucket.objects[testFolderName+"/delete.txt"]
		bucket.mu.Unlock()
		if ok {
			t.Error("expected object to be deleted")
		}
	})

	t.Run("returns os.ErrNotExist for missing file", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")

		err := client.Delete(ctx, "nonexistent.txt", testFolderName)
		if !errors.Is(err, os.ErrNotExist) {
			t.Errorf("expected os.ErrNotExist, got %v", err)
		}
	})
}

func TestChunkSizeFor(t *testing.T) {
	const def = googleapi.DefaultUploadChunkSize
	for _, tc := range []struct {
		hint int64
		want int
	}{
		{hint: -1, want: def},
		{hint: 0, want: 1},
		{hint: 4 << 10, want: 4<<10 + 1},
		{hint: def - 1, want: def},
		{hint: def, want: def},
		{hint: 1 << 30, want: def},
	} {
		if got := chunkSizeFor(tc.hint); got != tc.want {
			t.Errorf("chunkSizeFor(%d) = %d, want %d", tc.hint, got, tc.want)
		}
	}
}

func TestSizeHint(t *testing.T) {
	ctx := context.Background()
	bucket := newMockBucket()
	client, _ := newTestClient(bucket, "")
	var hints []int64
	client.newWriter = func(wctx context.Context, key string, sizeHint int64) writer {
		hints = append(hints, sizeHint)
		return &mockWriter{bucket: bucket, key: key, ctx: wctx}
	}

	if _, err := client.Store(ctx, "known.txt", testFolderName, 0, 0, bytes.NewReader([]byte("checkpoint"))); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if _, err := client.Store(ctx, "unknown.txt", testFolderName, 0, 0, io.MultiReader(strings.NewReader("x"))); err != nil {
		t.Fatalf("Store: %v", err)
	}
	w, err := client.OpenStream(ctx, "stream.txt", testFolderName)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	if err := w.Abort(ctx); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	want := []int64{int64(len("checkpoint")), -1, -1}
	if fmt.Sprint(hints) != fmt.Sprint(want) {
		t.Errorf("size hints = %v, want %v", hints, want)
	}
}

func TestDeletePrefix(t *testing.T) {
	ctx := context.Background()

	t.Run("deletes only objects under the directory prefix", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "pre")
		bucket.putObject("pre/"+testFolderName+"/results/job1/a", "a")
		bucket.putObject("pre/"+testFolderName+"/results/job1/sub/b", "b")
		bucket.putObject("pre/"+testFolderName+"/results/job10/c", "c")
		bucket.putObject("pre/other-folder/results/job1/d", "d")

		n, err := client.DeletePrefix(ctx, testFolderName, "results/job1")
		if err != nil {
			t.Fatalf("DeletePrefix: %v", err)
		}
		if n != 2 {
			t.Errorf("expected 2 deleted objects, got %d", n)
		}
		for _, key := range []string{"pre/" + testFolderName + "/results/job1/a", "pre/" + testFolderName + "/results/job1/sub/b"} {
			if bucket.hasObject(key) {
				t.Errorf("expected %s to be deleted", key)
			}
		}
		// A sibling sharing the prefix string and another tenant's folder survive.
		for _, key := range []string{"pre/" + testFolderName + "/results/job10/c", "pre/other-folder/results/job1/d"} {
			if !bucket.hasObject(key) {
				t.Errorf("expected %s to survive", key)
			}
		}
	})

	t.Run("empty prefix is a no-op", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")

		n, err := client.DeletePrefix(ctx, testFolderName, "results/none/")
		if err != nil || n != 0 {
			t.Errorf("expected (0, nil), got (%d, %v)", n, err)
		}
	})

	t.Run("rejects empty folder or prefix", func(t *testing.T) {
		client, _ := newTestClient(newMockBucket(), "")
		if _, err := client.DeletePrefix(ctx, "", "results/"); err == nil {
			t.Error("expected error for empty folderName")
		}
		if _, err := client.DeletePrefix(ctx, testFolderName, ""); err == nil {
			t.Error("expected error for empty prefix")
		}
	})

	t.Run("object deleted concurrently is not an error", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		bucket.putObject(testFolderName+"/results/job1/a", "a")
		bucket.putObject(testFolderName+"/results/job1/b", "b")
		client.deleteObject = func(_ context.Context, key string) error {
			bucket.mu.Lock()
			defer bucket.mu.Unlock()
			if strings.HasSuffix(key, "/a") {
				delete(bucket.objects, key)
				return storage.ErrObjectNotExist
			}
			delete(bucket.objects, key)
			return nil
		}

		n, err := client.DeletePrefix(ctx, testFolderName, "results/job1/")
		if err != nil {
			t.Fatalf("DeletePrefix: %v", err)
		}
		if n != 1 {
			t.Errorf("expected 1 deleted object, got %d", n)
		}
	})

	t.Run("delete error is returned", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		bucket.putObject(testFolderName+"/results/job1/a", "a")
		boom := errors.New("boom")
		client.deleteObject = func(context.Context, string) error { return boom }

		if _, err := client.DeletePrefix(ctx, testFolderName, "results/job1/"); !errors.Is(err, boom) {
			t.Errorf("expected delete error, got %v", err)
		}
	})

	t.Run("list error is returned", func(t *testing.T) {
		client, _ := newTestClient(newMockBucket(), "")
		boom := errors.New("boom")
		client.listObjects = func(context.Context, string, func(string) error) error { return boom }

		if _, err := client.DeletePrefix(ctx, testFolderName, "results/job1/"); !errors.Is(err, boom) {
			t.Errorf("expected list error, got %v", err)
		}
	})
}

func TestOpenStream(t *testing.T) {
	ctx := context.Background()

	t.Run("object is visible only after commit", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "pre")
		key := "pre/" + testFolderName + "/out.jsonl"

		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		for _, line := range []string{"one\n", "two\n"} {
			if _, err := w.Write([]byte(line)); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
		if bucket.hasObject(key) {
			t.Fatal("object must not be visible before commit")
		}

		md, err := w.Commit(ctx)
		if err != nil {
			t.Fatalf("Commit: %v", err)
		}
		if md.Location != key || md.Size != int64(len("one\ntwo\n")) {
			t.Errorf("unexpected metadata: %+v", md)
		}
		bucket.mu.Lock()
		got := string(bucket.objects[key].data)
		bucket.mu.Unlock()
		if got != "one\ntwo\n" {
			t.Errorf("expected streamed content, got %q", got)
		}
		// The finalizer's cleanup aborts after a successful commit.
		if err := w.Abort(ctx); err != nil {
			t.Errorf("Abort after Commit must be a no-op, got %v", err)
		}
		if !bucket.hasObject(key) {
			t.Error("Abort after Commit must not remove the object")
		}
	})

	t.Run("existing object returns ErrFileExists at open", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		bucket.putObject(testFolderName+"/out.jsonl", "winner")

		if _, err := client.OpenStream(ctx, "out.jsonl", testFolderName); !errors.Is(err, api.ErrFileExists) {
			t.Errorf("expected ErrFileExists, got %v", err)
		}
	})

	t.Run("stat error other than not-exist is returned", func(t *testing.T) {
		client, _ := newTestClient(newMockBucket(), "")
		boom := errors.New("boom")
		client.statObject = func(context.Context, string) error { return boom }

		if _, err := client.OpenStream(ctx, "out.jsonl", testFolderName); !errors.Is(err, boom) {
			t.Errorf("expected stat error, got %v", err)
		}
	})

	t.Run("commit loses to a writer that published after open", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		key := testFolderName + "/out.jsonl"

		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		if _, err := w.Write([]byte("loser")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		bucket.putObject(key, "winner")

		if _, err := w.Commit(ctx); !errors.Is(err, api.ErrFileExists) {
			t.Errorf("expected ErrFileExists, got %v", err)
		}
		bucket.mu.Lock()
		got := string(bucket.objects[key].data)
		bucket.mu.Unlock()
		if got != "winner" {
			t.Errorf("the winner's object must stand, got %q", got)
		}
	})

	t.Run("abort leaves no object behind", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")

		w, err := client.OpenStream(ctx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		if _, err := w.Write([]byte("partial")); err != nil {
			t.Fatalf("Write: %v", err)
		}
		if err := w.Abort(ctx); err != nil {
			t.Fatalf("Abort: %v", err)
		}
		if bucket.hasObject(testFolderName + "/out.jsonl") {
			t.Error("aborted stream must not leave an object")
		}
		if _, err := w.Commit(ctx); err == nil {
			t.Error("Commit after Abort must fail")
		}
		if _, err := w.Write([]byte("more")); err == nil {
			t.Error("Write after Abort must fail")
		}
	})

	t.Run("cancelling the open context aborts the upload", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		openCtx, cancel := context.WithCancel(ctx)

		w, err := client.OpenStream(openCtx, "out.jsonl", testFolderName)
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		cancel()
		if _, err := w.Write([]byte("late")); err == nil {
			t.Error("Write after the open context is cancelled must fail")
		}
		if _, err := w.Commit(ctx); err == nil {
			t.Error("Commit after the open context is cancelled must fail")
		}
		if bucket.hasObject(testFolderName + "/out.jsonl") {
			t.Error("cancelled stream must not leave an object")
		}
	})
}

func TestPrefix(t *testing.T) {
	ctx := context.Background()

	t.Run("prefix is included in retrieve key", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "pre")
		if _, err := client.Store(ctx, "p.txt", testFolderName, 1024, 0, bytes.NewReader([]byte("y"))); err != nil {
			t.Fatalf("store: %v", err)
		}

		// Object must be addressable via the prefixed key, not the bare key.
		if _, _, err := client.Retrieve(ctx, "p.txt", testFolderName); err != nil {
			t.Errorf("retrieve via prefixed key failed: %v", err)
		}
		bucket.mu.Lock()
		if _, ok := bucket.objects[testFolderName+"/p.txt"]; ok {
			t.Error("a non-prefixed key should not exist")
		}
		bucket.mu.Unlock()
	})
}

func TestGetContext(t *testing.T) {
	bucket := newMockBucket()
	client, _ := newTestClient(bucket, "")

	t.Run("defaults to 30s when zero", func(t *testing.T) {
		ctx, cancel := client.GetContext(context.Background(), 0)
		defer cancel()
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected a deadline on the returned context")
		}
		remaining := time.Until(deadline)
		if remaining > defaultTimeout || remaining < defaultTimeout-2*time.Second {
			t.Errorf("expected deadline ~%s away, got %s", defaultTimeout, remaining)
		}
	})

	t.Run("honors explicit duration", func(t *testing.T) {
		ctx, cancel := client.GetContext(context.Background(), 5*time.Second)
		defer cancel()
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected a deadline on the returned context")
		}
		remaining := time.Until(deadline)
		if remaining > 5*time.Second || remaining < 3*time.Second {
			t.Errorf("expected deadline ~5s away, got %s", remaining)
		}
	})
}

func TestValidate(t *testing.T) {
	t.Run("empty bucket is invalid", func(t *testing.T) {
		cfg := Config{ProjectID: "p", AutoCreateBucket: true}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "bucket") {
			t.Errorf("expected bucket error, got %v", err)
		}
	})

	t.Run("auto_create without project_id is invalid", func(t *testing.T) {
		cfg := Config{Bucket: "b", AutoCreateBucket: true}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "project_id") {
			t.Errorf("expected project_id error, got %v", err)
		}
	})

	t.Run("bucket without auto_create is valid without project_id", func(t *testing.T) {
		cfg := Config{Bucket: "b"}
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected valid, got %v", err)
		}
	})

	t.Run("auto_create with verify_off is invalid (auto-create only runs on the startup HeadBucket path)", func(t *testing.T) {
		cfg := Config{Bucket: "b", ProjectID: "p", AutoCreateBucket: true, VerifyBucketOnStart: false}
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "verify_bucket_on_start") {
			t.Errorf("expected verify_bucket_on_start error, got %v", err)
		}
	})

	t.Run("auto_create with verify_on is valid", func(t *testing.T) {
		cfg := Config{Bucket: "b", ProjectID: "p", AutoCreateBucket: true, VerifyBucketOnStart: true}
		if err := cfg.Validate(); err != nil {
			t.Errorf("expected valid, got %v", err)
		}
	})
}

func TestEnsureBucket(t *testing.T) {
	ctx := context.Background()

	t.Run("succeeds when bucket exists", func(t *testing.T) {
		bucket := newMockBucket()
		client, _ := newTestClient(bucket, "")
		if err := client.ensureBucket(ctx, false, ""); err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})

	t.Run("fails when bucket missing and autoCreate false", func(t *testing.T) {
		bucket := newMockBucket()
		bucket.exists = false
		client, _ := newTestClient(bucket, "")
		err := client.ensureBucket(ctx, false, "")
		if err == nil || !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("expected 'does not exist' error, got %v", err)
		}
	})

	t.Run("creates bucket when missing and autoCreate true", func(t *testing.T) {
		bucket := newMockBucket()
		bucket.exists = false
		client, _ := newTestClient(bucket, "")
		if err := client.ensureBucket(ctx, true, "test-project"); err != nil {
			t.Fatalf("expected nil, got %v", err)
		}
		if !bucket.exists {
			t.Error("expected bucket to be marked as created")
		}
	})

	t.Run("create returns 409 already-owned succeeds", func(t *testing.T) {
		// Two apiserver replicas starting against an auto-create bucket can race:
		// both see the bucket as missing on HeadBucket, then both issue Create.
		// GCS surfaces the loser as a 409 Conflict (already owned). The create is
		// benign — the desired end state (bucket exists) is reached — so
		// ensureBucket must tolerate the 409 and return nil, matching the S3
		// backend's BucketAlreadyOwnedByYou handling.
		bucket := newMockBucket()
		bucket.exists = false // forces the HeadBucket-missing path so Create is attempted
		bucket.createErr = &googleapi.Error{Code: http.StatusConflict, Message: "You already own this bucket."}
		client, _ := newTestClient(bucket, "")
		if err := client.ensureBucket(ctx, true, "test-project"); err != nil {
			t.Fatalf("expected 409 already-owned to be tolerated (nil), got %v", err)
		}
	})
}

// TestNew_VerifyBucketOnStart covers the opt-in gate around the startup HeadBucket
// (ensureBucket) call in newFromClient. The gate's behaviour is exercised against
// the real ensureBucket via a Client built with newTestClient, mirroring the
// newFromClient conditional: when VerifyBucketOnStart is true the HeadBucket runs
// (and its error fails construction); when false construction succeeds and the
// underlying bucket error is deferred to the first object operation.
func TestNew_VerifyBucketOnStart(t *testing.T) {
	ctx := context.Background()

	// permDeniedErr simulates the engops production failure: a workload service
	// account with only objectUser gets 403 on HeadBucket (storage.buckets.get).
	permDeniedErr := errors.New("googleapi: Error 403: tfm-gke-workload@… does not have storage.buckets.get access")

	// runGate reproduces the newFromClient verify gate against a Client built via
	// newTestClient (which, unlike newFromClient, accepts a mock bucket). This is
	// the minimal way to assert the gate's conditional without standing up a real
	// *storage.Client (which would require a httptest-backed GCS server).
	runGate := func(cfg Config, bucket *mockBucket) error {
		client, _ := newTestClient(bucket, cfg.Prefix)
		if cfg.VerifyBucketOnStart {
			if err := client.ensureBucket(ctx, cfg.AutoCreateBucket, cfg.ProjectID); err != nil {
				return fmt.Errorf("ensure bucket %s: %w", cfg.Bucket, err)
			}
		}
		_ = client // gate skipped -> construction succeeds
		return nil
	}

	t.Run("verify_on + bucket missing + autoCreate false fails at startup", func(t *testing.T) {
		bucket := newMockBucket()
		bucket.exists = false
		cfg := Config{
			Bucket:              testBucketName,
			VerifyBucketOnStart: true,
			AutoCreateBucket:    false,
		}
		err := runGate(cfg, bucket)
		if err == nil || !strings.Contains(err.Error(), "does not exist") {
			t.Errorf("expected 'does not exist' error, got %v", err)
		}
	})

	t.Run("verify_on + HeadBucket 403 fails at startup (engops scenario)", func(t *testing.T) {
		bucket := newMockBucket()
		bucket.attrsErr = permDeniedErr
		cfg := Config{
			Bucket:              testBucketName,
			VerifyBucketOnStart: true,
			AutoCreateBucket:    false,
		}
		err := runGate(cfg, bucket)
		if err == nil || !strings.Contains(err.Error(), "head bucket") {
			t.Errorf("expected 'head bucket' error wrapping the 403, got %v", err)
		}
		if !errors.Is(err, permDeniedErr) {
			t.Errorf("expected the 403 to be wrapped (errors.Is), got %v", err)
		}
	})

	t.Run("verify_off + HeadBucket 403 succeeds at startup", func(t *testing.T) {
		// VerifyBucketOnStart false: the gate is skipped, so the 403 HeadBucket is
		// never issued and construction succeeds. The broken bucket only surfaces
		// on the first object operation (fail-loud shifts from startup to request).
		bucket := newMockBucket()
		bucket.attrsErr = permDeniedErr
		cfg := Config{
			Bucket:              testBucketName,
			VerifyBucketOnStart: false,
			AutoCreateBucket:    false,
		}
		if err := runGate(cfg, bucket); err != nil {
			t.Fatalf("expected nil construction error when verify is off, got %v", err)
		}
		// Sanity: Attrs was not invoked during construction (attrsErr would have
		// surfaced otherwise via runGate's ensureBucket). A first Store would now
		// hit objectAttrs, which is the request-time failure path the chart relies
		// on — out of scope to assert here, but documented by the skip.
	})

	t.Run("verify_off + bucket missing succeeds at startup (fail-loud at request)", func(t *testing.T) {
		bucket := newMockBucket()
		bucket.exists = false
		cfg := Config{
			Bucket:              testBucketName,
			VerifyBucketOnStart: false,
			AutoCreateBucket:    false,
		}
		if err := runGate(cfg, bucket); err != nil {
			t.Fatalf("expected nil construction error when verify is off, got %v", err)
		}
	})
}
