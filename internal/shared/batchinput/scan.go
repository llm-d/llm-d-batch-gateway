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

package batchinput

import (
	"bufio"
	"errors"
	"fmt"
	"io"
)

// ErrTooManyLines is returned by Scan when the input exceeds the configured
// line budget. Scanning stops at that point so that a pathological upload
// cannot force the scanner to materialise an unbounded entry table.
var ErrTooManyLines = errors.New("input exceeds line limit")

const (
	// scanBufferSize is the read buffer used to walk the input. bufio grows
	// past it for a long line, so it is a throughput knob, not a limit.
	scanBufferSize = 1 << 20

	// MaxLineBytes caps a single request line. Without it a pathological
	// upload could be one enormous line, and Entry.Length could not represent
	// it. Comfortably above any real batch request.
	MaxLineBytes = 64 << 20
)

// ErrLineTooLong is returned when a single request line exceeds MaxLineBytes.
var ErrLineTooLong = errors.New("input line exceeds the maximum length")

// ValidationError describes why an input file is not a usable batch input.
// It is recorded with the stored object so that the batch that later
// references the file can fail during its validating phase without the
// processor having to re-download and re-parse the content.
type ValidationError struct {
	// Line is the 1-based line number the failure was found on, or 0 when the
	// failure is not attributable to a specific line.
	Line int64 `json:"line,omitempty"`
	// Message is the human-readable reason.
	Message string `json:"message"`
}

func (e *ValidationError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("line %d: %s", e.Line, e.Message)
	}
	return e.Message
}

// ScanResult is the outcome of a single pass over an input file.
type ScanResult struct {
	// Entries holds one entry per request line, in the order they appeared in
	// the source. Only meaningful when Invalid is nil.
	Entries []Entry

	// LineCount is the number of request lines found.
	LineCount int64

	// SourceSize is the number of bytes consumed from the source.
	SourceSize int64

	// FinalLineIndex is the zero-based index of the last line in the source
	// when that line has no trailing newline, or -1 when every line is
	// newline-terminated. Reordering can move that line away from the end, at
	// which point the writer must terminate it explicitly.
	FinalLineIndex int32

	// Invalid is non-nil when the input is not a valid batch input. Entries is
	// then incomplete and must not be ordered or stored as an executable
	// object.
	Invalid *ValidationError
}

// Valid reports whether the scanned input can be executed as a batch.
func (r *ScanResult) Valid() bool { return r.Invalid == nil }

// ScanOptions bounds a scan.
type ScanOptions struct {
	// MaxLines caps how many request lines are accepted. Zero means unlimited,
	// which callers should only use for trusted input.
	MaxLines int64
}

// Scan reads the whole input once and derives the ordering metadata for every
// request line. It returns an error only for I/O failures and for breaching
// MaxLines; malformed content is reported through ScanResult.Invalid so the
// caller can still store the file and fail the batch later.
func Scan(ra io.ReaderAt, size int64, opts ScanOptions) (*ScanResult, error) {
	res := &ScanResult{FinalLineIndex: -1}
	if size <= 0 {
		return res, nil
	}

	reader := bufio.NewReaderSize(io.NewSectionReader(ra, 0, size), scanBufferSize)
	seenCustomIDs := make(map[string]struct{})

	var offset int64
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("read line %d: %w", res.LineCount+1, err)
		}
		if len(line) == 0 && err == io.EOF {
			break
		}

		if len(line) > MaxLineBytes {
			return nil, fmt.Errorf("%w at line %d: %d bytes", ErrLineTooLong, res.LineCount+1, len(line))
		}
		streamBytes := int32(len(line))
		terminated := line[len(line)-1] == '\n'

		index := int32(res.LineCount)
		res.LineCount++
		if opts.MaxLines > 0 && res.LineCount > opts.MaxLines {
			return nil, ErrTooManyLines
		}
		if !terminated {
			res.FinalLineIndex = index
		}

		meta, parseErr := ParseLine(line)
		if parseErr != nil {
			res.Invalid = &ValidationError{
				Line:    res.LineCount,
				Message: fmt.Sprintf("validate request: %v", parseErr),
			}
			return res, nil
		}
		if _, exists := seenCustomIDs[meta.CustomID]; exists {
			res.Invalid = &ValidationError{
				Line:    res.LineCount,
				Message: fmt.Sprintf("duplicate custom_id %q", meta.CustomID),
			}
			return res, nil
		}
		seenCustomIDs[meta.CustomID] = struct{}{}

		res.Entries = append(res.Entries, Entry{
			Offset:     offset,
			Length:     streamBytes,
			Index:      index,
			ModelID:    meta.ModelID,
			PrefixHash: meta.PrefixHash,
		})

		offset += int64(streamBytes)
		if err == io.EOF {
			break
		}
	}

	res.SourceSize = offset
	return res, nil
}
