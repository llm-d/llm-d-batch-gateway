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

package retryclient

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/retry"
)

type mockFilesClient struct {
	storeCalls    int
	retrieveCalls int
	deleteCalls   int
	failUntil     int
}

func (m *mockFilesClient) Store(_ context.Context, _, _ string, _, _ int64, _ io.Reader) (*api.BatchFileMetadata, error) {
	m.storeCalls++
	if m.storeCalls <= m.failUntil {
		return nil, errors.New("transient store error")
	}
	return &api.BatchFileMetadata{Size: 100}, nil
}

func (m *mockFilesClient) Retrieve(_ context.Context, _, _ string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	m.retrieveCalls++
	if m.retrieveCalls <= m.failUntil {
		return nil, nil, errors.New("transient retrieve error")
	}
	return io.NopCloser(bytes.NewReader([]byte("data"))), &api.BatchFileMetadata{Size: 4}, nil
}

func (m *mockFilesClient) Delete(_ context.Context, _, _ string) error {
	m.deleteCalls++
	if m.deleteCalls <= m.failUntil {
		return errors.New("transient delete error")
	}
	return nil
}

func (m *mockFilesClient) Close() error { return nil }

func retryCfg() retry.Config {
	return retry.Config{MaxRetries: 3, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}
}

type nonSeekableReader struct {
	data []byte
	pos  int
}

func (r *nonSeekableReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

type failingSeekerReader struct {
	data      []byte
	pos       int
	seekCalls int
}

func (r *failingSeekerReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *failingSeekerReader) Seek(offset int64, whence int) (int64, error) {
	r.seekCalls++
	return 0, errors.New("seek failed")
}

func TestStore_SucceedsAfterRetry(t *testing.T) {
	mock := &mockFilesClient{failUntil: 2}
	c := New(mock, retryCfg(), "test")

	meta, err := c.Store(context.Background(), "f.txt", "folder", 0, 0, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if meta.Size != 100 {
		t.Fatalf("expected size 100, got %d", meta.Size)
	}
	if mock.storeCalls != 3 {
		t.Fatalf("expected 3 store calls, got %d", mock.storeCalls)
	}
}

func TestStore_ExhaustsRetries(t *testing.T) {
	mock := &mockFilesClient{failUntil: 10}
	c := New(mock, retryCfg(), "test")

	_, err := c.Store(context.Background(), "f.txt", "folder", 0, 0, bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if mock.storeCalls != 4 { // 1 initial + 3 retries
		t.Fatalf("expected 4 store calls, got %d", mock.storeCalls)
	}
}

func TestStore_NoRetryOnZeroConfig(t *testing.T) {
	mock := &mockFilesClient{failUntil: 1}
	c := New(mock, retry.Config{MaxRetries: 0}, "test")

	_, err := c.Store(context.Background(), "f.txt", "folder", 0, 0, bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error")
	}
	if mock.storeCalls != 1 {
		t.Fatalf("expected 1 store call, got %d", mock.storeCalls)
	}
}

func TestStore_NonSeekableReaderDoesNotRetry(t *testing.T) {
	mock := &mockFilesClient{failUntil: 10}
	c := New(mock, retryCfg(), "test")

	reader := &nonSeekableReader{data: []byte("data")}
	_, err := c.Store(context.Background(), "f.txt", "folder", 0, 0, reader)
	if err == nil {
		t.Fatal("expected error")
	}
	if mock.storeCalls != 1 {
		t.Fatalf("expected 1 store call for non-seekable reader, got %d", mock.storeCalls)
	}
}

func TestStore_SeekFailureAbortsImmediately(t *testing.T) {
	mock := &mockFilesClient{failUntil: 1}
	c := New(mock, retryCfg(), "test")

	reader := &failingSeekerReader{data: []byte("data")}
	_, err := c.Store(context.Background(), "f.txt", "folder", 0, 0, reader)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "seek failed") {
		t.Fatalf("expected seek failure error, got %v", err)
	}
	if mock.storeCalls != 1 {
		t.Fatalf("expected abort after first failed store, got %d calls", mock.storeCalls)
	}
	if reader.seekCalls != 1 {
		t.Fatalf("expected 1 seek attempt, got %d", reader.seekCalls)
	}
}

// permanentErrClient is a mock that returns a fixed error on every call, used to
// verify that permanent sentinels are not retried (call count stays 1) while the
// returned error still unwraps to the bare sentinel so callers' errors.Is match.
type permanentErrClient struct {
	storeErr    error
	retrieveErr error
	deleteErr   error
	storeCalls  int
	retrCalls   int
	delCalls    int
}

func (m *permanentErrClient) Store(_ context.Context, _, _ string, _, _ int64, _ io.Reader) (*api.BatchFileMetadata, error) {
	m.storeCalls++
	return nil, m.storeErr
}

func (m *permanentErrClient) Retrieve(_ context.Context, _, _ string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	m.retrCalls++
	return nil, nil, m.retrieveErr
}

func (m *permanentErrClient) Delete(_ context.Context, _, _ string) error {
	m.delCalls++
	return m.deleteErr
}

func (m *permanentErrClient) Close() error { return nil }

// TestStore_PermanentErrorsAreNotRetried asserts the four permanent storage
// sentinels short-circuit retryclient.Store after exactly one attempt, and that
// the returned error still satisfies errors.Is against the bare sentinel
// (retry.Permanent unwraps, so callers' type checks are preserved). ErrFileTooLarge
// and ErrTooManyLines are produced by the inner client's own limit enforcement;
// ErrFileExists by the conditional-write path; os.ErrNotExist is included for
// symmetry with Retrieve/Delete even though Store doesn't normally return it.
func TestStore_PermanentErrorsAreNotRetried(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"ErrFileExists", api.ErrFileExists},
		{"ErrFileTooLarge", api.ErrFileTooLarge},
		{"ErrTooManyLines", api.ErrTooManyLines},
		{"ErrNotExist", os.ErrNotExist},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := &permanentErrClient{storeErr: tc.err}
			c := New(mock, retryCfg(), "test")

			_, err := c.Store(context.Background(), "f.txt", "folder", 0, 0, bytes.NewReader([]byte("data")))
			if !errors.Is(err, tc.err) {
				t.Fatalf("expected errors.Is(%v) to match, got %v", tc.err, err)
			}
			if mock.storeCalls != 1 {
				t.Fatalf("permanent %v must not retry: expected 1 store call, got %d", tc.err, mock.storeCalls)
			}
		})
	}
}

// TestRetrieve_PermanentErrorsAreNotRetried asserts a missing object (the only
// permanent Retrieve sentinel across all backends: fs/S3/GCS return os.ErrNotExist
// for a missing object/bucket) short-circuits retryclient.Retrieve after one attempt.
func TestRetrieve_PermanentErrorsAreNotRetried(t *testing.T) {
	mock := &permanentErrClient{retrieveErr: os.ErrNotExist}
	c := New(mock, retryCfg(), "test")

	_, _, err := c.Retrieve(context.Background(), "f.txt", "folder")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected errors.Is(os.ErrNotExist) to match, got %v", err)
	}
	if mock.retrCalls != 1 {
		t.Fatalf("permanent os.ErrNotExist must not retry retrieve: expected 1 call, got %d", mock.retrCalls)
	}
}

// TestDelete_PermanentErrorsAreNotRetried asserts a missing object/bucket
// (os.ErrNotExist) short-circuits retryclient.Delete after one attempt. S3 returns
// os.ErrNotExist only for a missing bucket (object DELETE is idempotent → nil),
// but the sentinel is the same and the classification is identical.
func TestDelete_PermanentErrorsAreNotRetried(t *testing.T) {
	mock := &permanentErrClient{deleteErr: os.ErrNotExist}
	c := New(mock, retryCfg(), "test")

	err := c.Delete(context.Background(), "f.txt", "folder")
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected errors.Is(os.ErrNotExist) to match, got %v", err)
	}
	if mock.delCalls != 1 {
		t.Fatalf("permanent os.ErrNotExist must not retry delete: expected 1 call, got %d", mock.delCalls)
	}
}

func TestRetrieve_SucceedsAfterRetry(t *testing.T) {
	mock := &mockFilesClient{failUntil: 1}
	c := New(mock, retryCfg(), "test")

	rc, meta, err := c.Retrieve(context.Background(), "f.txt", "folder")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()
	if meta.Size != 4 {
		t.Fatalf("expected size 4, got %d", meta.Size)
	}
	if mock.retrieveCalls != 2 {
		t.Fatalf("expected 2 retrieve calls, got %d", mock.retrieveCalls)
	}
}

func TestDelete_SucceedsAfterRetry(t *testing.T) {
	mock := &mockFilesClient{failUntil: 2}
	c := New(mock, retryCfg(), "test")

	err := c.Delete(context.Background(), "f.txt", "folder")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if mock.deleteCalls != 3 {
		t.Fatalf("expected 3 delete calls, got %d", mock.deleteCalls)
	}
}

func metricsCtx() context.Context {
	return context.Background()
}

func counterValue(t *testing.T, name string, wantLabels map[string]string) float64 {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if hasLabels(m, wantLabels) {
				return m.GetCounter().GetValue()
			}
		}
	}

	t.Fatalf("metric %q with labels %v not found", name, wantLabels)
	return 0
}

func counterValueOrZero(t *testing.T, name string, wantLabels map[string]string) float64 {
	t.Helper()

	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("Gather() error: %v", err)
	}

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if hasLabels(m, wantLabels) {
				return m.GetCounter().GetValue()
			}
		}
	}
	return 0
}

func hasLabels(m *dto.Metric, wantLabels map[string]string) bool {
	if len(m.GetLabel()) != len(wantLabels) {
		return false
	}
	for _, lp := range m.GetLabel() {
		wantValue, ok := wantLabels[lp.GetName()]
		if !ok || lp.GetValue() != wantValue {
			return false
		}
	}
	return true
}

func TestMetrics_RetriesAndSuccessRecorded(t *testing.T) {
	operationsTotal.Reset()

	mock := &mockFilesClient{failUntil: 2}
	c := New(mock, retryCfg(), "test")

	_, err := c.Store(metricsCtx(), "f.txt", "folder", 0, 0, bytes.NewReader([]byte("data")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retries := counterValue(t, "file_storage_operations_total", map[string]string{
		"operation": "store",
		"component": "test",
		"status":    "retry",
	})
	if retries != 2 {
		t.Fatalf("file_storage_operations_total{store,retry} = %v, want 2", retries)
	}

	success := counterValue(t, "file_storage_operations_total", map[string]string{
		"operation": "store",
		"component": "test",
		"status":    "success",
	})
	if success != 1 {
		t.Fatalf("file_storage_operations_total{store,success} = %v, want 1", success)
	}
}

func TestMetrics_ExhaustedRecorded(t *testing.T) {
	operationsTotal.Reset()

	mock := &mockFilesClient{failUntil: 10}
	c := New(mock, retryCfg(), "test")

	_, err := c.Store(metricsCtx(), "f.txt", "folder", 0, 0, bytes.NewReader([]byte("data")))
	if err == nil {
		t.Fatal("expected error")
	}

	retries := counterValue(t, "file_storage_operations_total", map[string]string{
		"operation": "store",
		"component": "test",
		"status":    "retry",
	})
	if retries != 3 {
		t.Fatalf("file_storage_operations_total{store,retry} = %v, want 3", retries)
	}

	exhausted := counterValue(t, "file_storage_operations_total", map[string]string{
		"operation": "store",
		"component": "test",
		"status":    "exhausted",
	})
	if exhausted != 1 {
		t.Fatalf("file_storage_operations_total{store,exhausted} = %v, want 1", exhausted)
	}
}

func TestMetrics_SeekFailureDoesNotRecordExhausted(t *testing.T) {
	operationsTotal.Reset()

	mock := &mockFilesClient{failUntil: 1}
	c := New(mock, retryCfg(), "test")

	reader := &failingSeekerReader{data: []byte("data")}
	_, err := c.Store(metricsCtx(), "f.txt", "folder", 0, 0, reader)
	if err == nil {
		t.Fatal("expected error")
	}

	exhausted := counterValueOrZero(t, "file_storage_operations_total", map[string]string{
		"operation": "store",
		"component": "test",
		"status":    "exhausted",
	})
	if exhausted != 0 {
		t.Fatalf("file_storage_operations_total{store,exhausted} = %v, want 0 (permanent error should not count as exhausted)", exhausted)
	}
}

func TestMetrics_DeleteRetries(t *testing.T) {
	operationsTotal.Reset()

	mock := &mockFilesClient{failUntil: 1}
	c := New(mock, retryCfg(), "test")

	err := c.Delete(metricsCtx(), "f.txt", "folder")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	retries := counterValue(t, "file_storage_operations_total", map[string]string{
		"operation": "delete",
		"component": "test",
		"status":    "retry",
	})
	if retries != 1 {
		t.Fatalf("file_storage_operations_total{delete,retry} = %v, want 1", retries)
	}

	success := counterValue(t, "file_storage_operations_total", map[string]string{
		"operation": "delete",
		"component": "test",
		"status":    "success",
	})
	if success != 1 {
		t.Fatalf("file_storage_operations_total{delete,success} = %v, want 1", success)
	}
}
