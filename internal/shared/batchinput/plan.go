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
	"fmt"
	"io"
	"sort"
)

// Plan is the result of applying an ordering policy to a scanned input. It
// describes the object that will be written to storage without holding any
// request bodies in memory: each entry still points back into the source.
type Plan struct {
	// Policy is the identifier recorded with the stored object.
	Policy PolicyID

	// Entries are the source lines in dispatch order.
	Entries []Entry

	// LineCount is the number of request lines in the object.
	LineCount int64

	// Size is the exact byte size of the object Reader produces. It differs
	// from the source size only when an unterminated final line had to be
	// newline-terminated.
	Size int64

	// terminate marks the single entry, if any, whose source bytes lack a
	// trailing newline. Identified by source offset because ordering moves
	// entries around.
	terminate    int64
	hasTerminate bool
}

// NewPlan applies policy to a successful scan. Scans that failed validation
// have no usable entry table and are rejected: store such files verbatim and
// surface the recorded validation error instead.
func NewPlan(res *ScanResult, policy OrderPolicy) (*Plan, error) {
	if res == nil {
		return nil, fmt.Errorf("scan result is nil")
	}
	if !res.Valid() {
		return nil, fmt.Errorf("cannot order an invalid input: %w", res.Invalid)
	}
	if policy == nil {
		return nil, fmt.Errorf("ordering policy is nil")
	}

	// Copy so that ordering never mutates the caller's scan result.
	entries := make([]Entry, len(res.Entries))
	copy(entries, res.Entries)
	policy.Order(entries)

	plan := &Plan{
		Policy:    policy.ID(),
		Entries:   entries,
		LineCount: res.LineCount,
	}

	if res.FinalLineIndex >= 0 {
		for i := range res.Entries {
			if res.Entries[i].Index == res.FinalLineIndex {
				plan.terminate = res.Entries[i].Offset
				plan.hasTerminate = true
				break
			}
		}
	}

	for i := range entries {
		plan.Size += int64(entries[i].Length)
	}
	if plan.needsTerminator() {
		plan.Size++
	}

	return plan, nil
}

// needsTerminator reports whether the unterminated source line ended up
// somewhere other than last, where a missing newline would run two requests
// together. A policy that leaves it last — including any ordering that does
// not move it, such as original-v1 — keeps the object byte-identical to the
// upload.
func (p *Plan) needsTerminator() bool {
	if !p.hasTerminate || len(p.Entries) == 0 {
		return false
	}
	return p.Entries[len(p.Entries)-1].Offset != p.terminate
}

// Models returns the sorted, de-duplicated model IDs referenced by the plan.
func (p *Plan) Models() []string {
	seen := make(map[string]struct{}, 8)
	models := make([]string, 0, 8)
	for i := range p.Entries {
		if _, ok := seen[p.Entries[i].ModelID]; ok {
			continue
		}
		seen[p.Entries[i].ModelID] = struct{}{}
		models = append(models, p.Entries[i].ModelID)
	}
	sort.Strings(models)
	return models
}

// Reader returns a reader over the ordered object. src must be the same source
// the plan was scanned from. The returned reader is rewindable so that an
// upload can be retried.
func (p *Plan) Reader(src io.ReaderAt) *OrderedReader {
	terminate := p.needsTerminator()
	segments := make([]segment, len(p.Entries))
	offsets := make([]int64, len(p.Entries)+1)
	var out int64
	for i := range p.Entries {
		e := p.Entries[i]
		addNewline := terminate && e.Offset == p.terminate
		segments[i] = segment{srcOffset: e.Offset, length: e.Length, addNewline: addNewline}
		offsets[i] = out
		out += int64(e.Length)
		if addNewline {
			out++
		}
	}
	offsets[len(p.Entries)] = out

	return &OrderedReader{src: src, segments: segments, offsets: offsets, total: out}
}

type segment struct {
	srcOffset  int64
	length     int32
	addNewline bool
}

// OrderedReader streams the source lines in plan order. It implements
// io.ReadSeeker so that the retrying storage client can rewind and repeat a
// failed upload; no data is buffered beyond the caller's own slice.
type OrderedReader struct {
	src      io.ReaderAt
	segments []segment
	offsets  []int64
	total    int64
	pos      int64
}

var (
	_ io.Reader = (*OrderedReader)(nil)
	_ io.Seeker = (*OrderedReader)(nil)
)

// Size returns the total number of bytes the reader will produce.
func (r *OrderedReader) Size() int64 { return r.total }

func (r *OrderedReader) Read(p []byte) (int, error) {
	if r.pos >= r.total {
		return 0, io.EOF
	}

	n := 0
	for n < len(p) && r.pos < r.total {
		i := r.segmentIndex(r.pos)
		seg := r.segments[i]
		within := r.pos - r.offsets[i]

		bodyLen := int64(seg.length)
		if within >= bodyLen {
			// Only the synthesised newline is left in this segment.
			p[n] = '\n'
			n++
			r.pos++
			continue
		}

		want := int64(len(p) - n)
		if remaining := bodyLen - within; want > remaining {
			want = remaining
		}
		buf := p[n : n+int(want)]
		m, err := r.src.ReadAt(buf, seg.srcOffset+within)
		n += m
		r.pos += int64(m)
		if m < len(buf) {
			if err == nil || err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			return n, err
		}
	}

	return n, nil
}

func (r *OrderedReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = r.pos + offset
	case io.SeekEnd:
		abs = r.total + offset
	default:
		return 0, fmt.Errorf("batchinput: invalid whence %d", whence)
	}
	if abs < 0 || abs > r.total {
		return 0, fmt.Errorf("batchinput: position %d is outside the object (%d bytes)", abs, r.total)
	}
	r.pos = abs
	return abs, nil
}

// segmentIndex returns the index of the segment covering output position pos.
// pos must be < total.
func (r *OrderedReader) segmentIndex(pos int64) int {
	return sort.Search(len(r.segments), func(i int) bool {
		return r.offsets[i+1] > pos
	})
}
