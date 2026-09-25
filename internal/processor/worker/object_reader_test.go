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

package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
)

// rangeRecord is one RetrieveRange call seen by the fake storage.
type rangeRecord struct {
	offset int64
	length int64
}

// fakeRangeStorage serves ranges from an in-memory object and records every
// call, so tests can assert on the number and shape of the reads issued.
type fakeRangeStorage struct {
	filesapi.BatchFilesClient

	data []byte

	mu    sync.Mutex
	calls []rangeRecord

	// inFlight/maxInFlight track prefetch concurrency.
	inFlight    atomic.Int64
	maxInFlight atomic.Int64

	// delay is applied to every range read.
	delay time.Duration
	// gate, when non-nil, blocks the read at the given offset until closed.
	gateOffset int64
	gate       chan struct{}
	// failAtOffset makes the read at that offset fail.
	failAtOffset int64
	failErr      error
	// shortAtOffset makes the read at that offset return fewer bytes.
	shortAtOffset int64
}

func newFakeRangeStorage(content string) *fakeRangeStorage {
	return &fakeRangeStorage{
		data:          []byte(content),
		gateOffset:    -1,
		failAtOffset:  -1,
		shortAtOffset: -1,
	}
}

func (f *fakeRangeStorage) RetrieveRange(ctx context.Context, _, _ string, offset, length int64) (io.ReadCloser, error) {
	cur := f.inFlight.Add(1)
	for {
		maxSeen := f.maxInFlight.Load()
		if cur <= maxSeen || f.maxInFlight.CompareAndSwap(maxSeen, cur) {
			break
		}
	}
	defer f.inFlight.Add(-1)

	f.mu.Lock()
	f.calls = append(f.calls, rangeRecord{offset: offset, length: length})
	f.mu.Unlock()

	if f.gate != nil && offset == f.gateOffset {
		select {
		case <-f.gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset == f.failAtOffset {
		return nil, f.failErr
	}

	end := offset + length
	if end > int64(len(f.data)) {
		end = int64(len(f.data))
	}
	chunk := f.data[offset:end]
	if offset == f.shortAtOffset && len(chunk) > 1 {
		chunk = chunk[:len(chunk)-1]
	}
	return io.NopCloser(bytes.NewReader(chunk)), nil
}

func (f *fakeRangeStorage) recorded() []rangeRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]rangeRecord, len(f.calls))
	copy(out, f.calls)
	return out
}

func TestObjectReader(t *testing.T) {
	t.Run("reassembles the object in order", func(t *testing.T) {
		content := strings.Repeat("abcdefghij", 500) // 5000 bytes
		storage := newFakeRangeStorage(content)

		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: int64(len(content)),
			ChunkSize: 512, PrefetchBytes: 2048,
		})
		defer func() { _ = r.Close() }()

		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if string(got) != content {
			t.Fatal("reassembled object differs from the stored object")
		}
	})

	t.Run("costs one read per chunk instead of one per line", func(t *testing.T) {
		const size = 5000
		content := strings.Repeat("x", size)
		storage := newFakeRangeStorage(content)

		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: size, ChunkSize: 1000, PrefetchBytes: 2000,
		})
		defer func() { _ = r.Close() }()
		if _, err := io.ReadAll(r); err != nil {
			t.Fatalf("ReadAll: %v", err)
		}

		calls := storage.recorded()
		if len(calls) != 5 {
			t.Fatalf("issued %d ranged reads, want 5", len(calls))
		}
		if r.Chunks() != 5 {
			t.Fatalf("Chunks() = %d, want 5", r.Chunks())
		}

		// Ranges must tile the object exactly once, with no gap or overlap.
		byOffset := map[int64]int64{}
		for _, c := range calls {
			if _, dup := byOffset[c.offset]; dup {
				t.Fatalf("offset %d read twice", c.offset)
			}
			byOffset[c.offset] = c.length
		}
		var total int64
		for off := int64(0); off < size; off += 1000 {
			length, ok := byOffset[off]
			if !ok {
				t.Fatalf("no read covering offset %d", off)
			}
			total += length
		}
		if total != size {
			t.Fatalf("ranges cover %d bytes, want %d", total, size)
		}
	})

	t.Run("a trailing partial chunk is sized exactly", func(t *testing.T) {
		content := strings.Repeat("y", 2500)
		storage := newFakeRangeStorage(content)

		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: 2500, ChunkSize: 1000, PrefetchBytes: 1000,
		})
		defer func() { _ = r.Close() }()
		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(got) != 2500 {
			t.Fatalf("read %d bytes, want 2500", len(got))
		}
		calls := storage.recorded()
		if len(calls) != 3 {
			t.Fatalf("issued %d reads, want 3", len(calls))
		}
		for _, c := range calls {
			if c.offset+c.length > 2500 {
				t.Fatalf("range [%d,%d) reads past the object end", c.offset, c.offset+c.length)
			}
		}
	})

	t.Run("prefetches ahead but stays within the budget", func(t *testing.T) {
		content := strings.Repeat("z", 10000)
		storage := newFakeRangeStorage(content)
		storage.delay = 20 * time.Millisecond

		// Budget of 3 chunks: the chunk being consumed plus 3 in flight.
		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: 10000, ChunkSize: 1000, PrefetchBytes: 3000,
		})
		defer func() { _ = r.Close() }()
		if _, err := io.ReadAll(r); err != nil {
			t.Fatalf("ReadAll: %v", err)
		}

		if got := storage.maxInFlight.Load(); got < 2 {
			t.Fatalf("max concurrent reads = %d, expected prefetch to overlap storage latency", got)
		}
		if got := storage.maxInFlight.Load(); got > 4 {
			t.Fatalf("max concurrent reads = %d, exceeds the prefetch budget", got)
		}
	})

	t.Run("a slow consumer stops the prefetching", func(t *testing.T) {
		content := strings.Repeat("w", 10000)
		storage := newFakeRangeStorage(content)

		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: 10000, ChunkSize: 1000, PrefetchBytes: 2000,
		})
		defer func() { _ = r.Close() }()

		// Consume a single chunk, then stall.
		buf := make([]byte, 1000)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read first chunk: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		// Without backpressure the producer would have fetched all ten chunks.
		if got := len(storage.recorded()); got > 5 {
			t.Fatalf("%d reads issued while the consumer was stalled; prefetch is unbounded", got)
		}
	})

	t.Run("surfaces a failed range read", func(t *testing.T) {
		content := strings.Repeat("q", 3000)
		storage := newFakeRangeStorage(content)
		storage.failAtOffset = 1000
		storage.failErr = errors.New("storage unavailable")

		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: 3000, ChunkSize: 1000, PrefetchBytes: 1000,
		})
		defer func() { _ = r.Close() }()

		_, err := io.ReadAll(r)
		if err == nil {
			t.Fatal("expected the failed range read to surface")
		}
		if !strings.Contains(err.Error(), "storage unavailable") {
			t.Fatalf("unexpected error %v", err)
		}
	})

	t.Run("rejects a short range read", func(t *testing.T) {
		content := strings.Repeat("s", 3000)
		storage := newFakeRangeStorage(content)
		storage.shortAtOffset = 1000

		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: 3000, ChunkSize: 1000, PrefetchBytes: 1000,
		})
		defer func() { _ = r.Close() }()

		if _, err := io.ReadAll(r); err == nil {
			t.Fatal("a short range read must not pass as valid content")
		}
	})

	t.Run("a cancelled read is an error, not a clean end of input", func(t *testing.T) {
		content := strings.Repeat("c", 10000)
		storage := newFakeRangeStorage(content)
		storage.gateOffset = 2000
		storage.gate = make(chan struct{})

		ctx, cancel := context.WithCancel(context.Background())
		r := newObjectReader(ctx, objectReaderConfig{
			Storage: storage, Size: 10000, ChunkSize: 1000, PrefetchBytes: 1000,
		})
		defer func() { _ = r.Close() }()

		buf := make([]byte, 1000)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read first chunk: %v", err)
		}

		cancel()
		close(storage.gate)

		_, err := io.ReadAll(r)
		if err == nil {
			t.Fatal("truncating the input must not look like EOF")
		}
	})

	t.Run("an empty object reads as empty", func(t *testing.T) {
		storage := newFakeRangeStorage("")
		r := newObjectReader(context.Background(), objectReaderConfig{Storage: storage, Size: 0})
		defer func() { _ = r.Close() }()

		got, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("read %d bytes from an empty object", len(got))
		}
		if len(storage.recorded()) != 0 {
			t.Fatal("an empty object must not be read at all")
		}
	})

	t.Run("Close releases prefetch goroutines", func(t *testing.T) {
		content := strings.Repeat("g", 100000)
		storage := newFakeRangeStorage(content)
		storage.delay = 10 * time.Millisecond

		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: 100000, ChunkSize: 1000, PrefetchBytes: 5000,
		})
		buf := make([]byte, 100)
		if _, err := io.ReadFull(r, buf); err != nil {
			t.Fatalf("read: %v", err)
		}

		done := make(chan struct{})
		go func() { _ = r.Close(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Close did not return; prefetch goroutines are leaking")
		}
		// Close must be safe to call again.
		_ = r.Close()
	})

	t.Run("survives tiny consumer buffers", func(t *testing.T) {
		content := strings.Repeat("abcde", 400)
		storage := newFakeRangeStorage(content)

		r := newObjectReader(context.Background(), objectReaderConfig{
			Storage: storage, Size: int64(len(content)), ChunkSize: 128, PrefetchBytes: 512,
		})
		defer func() { _ = r.Close() }()

		var out bytes.Buffer
		one := make([]byte, 1)
		for {
			n, err := r.Read(one)
			out.Write(one[:n])
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
		}
		if out.String() != content {
			t.Fatal("byte-at-a-time read differs from the stored object")
		}
	})
}

func TestObjectReaderConfigDefaults(t *testing.T) {
	cfg := objectReaderConfig{}
	cfg.applyDefaults()

	if cfg.ChunkSize != defaultObjectChunkSize || cfg.PrefetchBytes != defaultObjectPrefetchBytes {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.readAhead() != int(defaultObjectPrefetchBytes/defaultObjectChunkSize) {
		t.Fatalf("readAhead = %d", cfg.readAhead())
	}

	// A budget smaller than one chunk must still allow progress.
	tight := objectReaderConfig{ChunkSize: 1000, PrefetchBytes: 10}
	tight.applyDefaults()
	if tight.readAhead() != 1 {
		t.Fatalf("readAhead = %d, want at least 1", tight.readAhead())
	}
}

// TestObjectReaderChunkCountAtScale documents the headline win: a full-size
// input costs tens of range reads rather than one per request.
func TestObjectReaderChunkCountAtScale(t *testing.T) {
	const (
		lines      = 50000
		lineSize   = 400
		objectSize = int64(lines * lineSize)
	)
	cfg := objectReaderConfig{Size: objectSize}
	cfg.applyDefaults()

	chunks := (objectSize + cfg.ChunkSize - 1) / cfg.ChunkSize
	if chunks > 10 {
		t.Fatalf("a %d-line input would need %d ranged reads", lines, chunks)
	}
	t.Logf("%d lines (%d bytes) => %d ranged reads instead of %d",
		lines, objectSize, chunks, lines)
}
