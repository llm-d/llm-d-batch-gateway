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
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
)

const (
	// defaultObjectChunkSize is the span of a single ranged read. Large enough
	// that a full-size input costs tens of requests rather than tens of
	// thousands, small enough that the prefetch budget below still holds
	// several chunks in flight.
	defaultObjectChunkSize int64 = 8 << 20

	// defaultObjectPrefetchBytes caps how much fetched-but-unconsumed data one
	// job may hold. Together with the chunk size it fixes the read-ahead
	// depth, and therefore the per-job memory cost of reading the input.
	defaultObjectPrefetchBytes int64 = 32 << 20

	// defaultObjectReadTimeout bounds a single ranged read.
	defaultObjectReadTimeout = 30 * time.Second
)

// objectReaderConfig describes one sequential pass over a stored object.
type objectReaderConfig struct {
	Storage     filesapi.BatchFilesClient
	StorageName string
	FolderName  string

	// Size is the exact object size. Reads stop there, so a truncated or
	// grown object surfaces as an error instead of silently changing the
	// request set.
	Size int64

	ChunkSize     int64
	PrefetchBytes int64
	ReadTimeout   time.Duration
}

func (c *objectReaderConfig) applyDefaults() {
	if c.ChunkSize <= 0 {
		c.ChunkSize = defaultObjectChunkSize
	}
	if c.PrefetchBytes <= 0 {
		c.PrefetchBytes = defaultObjectPrefetchBytes
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = defaultObjectReadTimeout
	}
}

// readAhead is how many chunks may be in flight beyond the one being consumed.
func (c *objectReaderConfig) readAhead() int {
	n := int(c.PrefetchBytes / c.ChunkSize)
	if n < 1 {
		return 1
	}
	return n
}

// objectReader streams a stored object as a bounded sequence of ranged reads.
//
// A batch dispatches as fast as the inference backends drain it, which for a
// large job is hours. Holding one response body open for that long is not
// viable, so the object is pulled in fixed spans instead: each read is short
// lived and independently retryable by the storage client.
//
// Chunks are fetched concurrently to hide storage latency but are handed to
// the caller strictly in offset order, so the byte stream — and therefore the
// dispatch order encoded in it — is exactly the stored order. Read-ahead is
// bounded by the prefetch budget, so a consumer that stalls stops the fetching
// rather than accumulating the whole object in memory.
//
// objectReader is not safe for concurrent use; it is driven by a single
// producer goroutine.
type objectReader struct {
	cfg objectReaderConfig

	cancel  context.CancelFunc
	futures <-chan chan chunkResult

	// current holds the chunk being drained.
	current []byte
	// chunks counts the ranged reads consumed so far.
	chunks int
	// expected is how many chunks the object is made of. Comparing it against
	// chunks is what separates a complete read from one cut short by
	// cancellation, which must not look like a clean end of input.
	expected int
	err      error
	done     bool

	closeOnce sync.Once
	wg        sync.WaitGroup
}

type chunkResult struct {
	data []byte
	err  error
}

var _ io.ReadCloser = (*objectReader)(nil)

// newObjectReader starts prefetching immediately. The caller must Close the
// reader to release the fetch goroutines.
//
// ctx governs the reads themselves and is deliberately the caller's choice:
// input enumeration has to continue past a dispatch abort so that every
// custom_id can still be accounted for, so callers pass a context that is not
// cancelled by the batch aborting.
func newObjectReader(ctx context.Context, cfg objectReaderConfig) *objectReader {
	cfg.applyDefaults()

	ctx, cancel := context.WithCancel(ctx)
	futures := make(chan chan chunkResult, cfg.readAhead())

	expected := int((cfg.Size + cfg.ChunkSize - 1) / cfg.ChunkSize)
	r := &objectReader{cfg: cfg, cancel: cancel, futures: futures, expected: expected}

	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(futures)
		for offset := int64(0); offset < cfg.Size; offset += cfg.ChunkSize {
			length := cfg.ChunkSize
			if remaining := cfg.Size - offset; length > remaining {
				length = remaining
			}

			future := make(chan chunkResult, 1)
			select {
			case futures <- future:
			case <-ctx.Done():
				return
			}

			r.wg.Add(1)
			go func(offset, length int64) {
				defer r.wg.Done()
				data, err := r.fetch(ctx, offset, length)
				future <- chunkResult{data: data, err: err}
			}(offset, length)
		}
	}()

	return r
}

// fetch performs one ranged read and insists on the exact span requested. A
// short read means the object is not what the recorded metadata described, so
// it is an error rather than a silently truncated request set.
func (r *objectReader) fetch(ctx context.Context, offset, length int64) ([]byte, error) {
	readCtx, cancel := context.WithTimeout(ctx, r.cfg.ReadTimeout)
	defer cancel()

	rc, err := r.cfg.Storage.RetrieveRange(readCtx, r.cfg.StorageName, r.cfg.FolderName, offset, length)
	if err != nil {
		return nil, fmt.Errorf("read input range at offset %d length %d: %w", offset, length, err)
	}
	defer rc.Close()

	buf := make([]byte, length)
	if _, err := io.ReadFull(rc, buf); err != nil {
		return nil, fmt.Errorf("read input range at offset %d length %d: %w", offset, length, err)
	}
	return buf, nil
}

func (r *objectReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}

	for len(r.current) == 0 {
		if r.done {
			return 0, io.EOF
		}
		future, ok := <-r.futures
		if !ok {
			r.done = true
			// A closed queue before every chunk arrived means the read was
			// abandoned, not finished. Reporting EOF here would hand the
			// caller a silently truncated input.
			if r.chunks < r.expected {
				r.err = fmt.Errorf("input read stopped after %d of %d ranges: %w",
					r.chunks, r.expected, io.ErrUnexpectedEOF)
				return 0, r.err
			}
			return 0, io.EOF
		}
		result := <-future
		if result.err != nil {
			r.err = result.err
			return 0, r.err
		}
		r.current = result.data
		r.chunks++
	}

	n := copy(p, r.current)
	r.current = r.current[n:]
	return n, nil
}

// Chunks reports how many ranged reads have been consumed so far.
func (r *objectReader) Chunks() int { return r.chunks }

// Close cancels outstanding prefetches and waits for them, so that a finished
// or abandoned job leaves no fetch goroutines behind. Every future is buffered
// and the producer selects on the context, so nothing can be left blocked on a
// handoff once the context is cancelled.
func (r *objectReader) Close() error {
	r.closeOnce.Do(func() {
		r.cancel()
		r.wg.Wait()
	})
	return nil
}
