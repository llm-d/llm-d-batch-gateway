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

package file

import (
	"fmt"
	"io"
	"mime/multipart"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batchinput"
)

// preparedBatchInput is what CreateFile should actually store for a
// purpose=batch upload, plus the metadata that has to travel with it.
type preparedBatchInput struct {
	// reader produces the bytes to store. For a valid input this is the
	// reordered object; for an invalid one it is the upload verbatim.
	reader io.Reader

	// sizeLimit is the byte budget to hand to the storage client. Reordering
	// can add a single newline, so it may exceed the configured maximum by
	// that one byte; the client's own upload was already checked against the
	// real limit.
	sizeLimit int64

	// meta records the ordering policy and validation outcome for the stored
	// object.
	meta *batchinput.Metadata
}

// prepareBatchInput scans a batch upload once, applies the configured ordering
// policy, and returns a reader over the reordered object.
//
// Ordering happens here, while the upload is still a seekable local file, so
// that the processor can later stream the object in a handful of contiguous
// range reads instead of one read per request.
//
// A file that fails validation is stored exactly as uploaded and the failure
// is recorded in the metadata. Uploads are not the place to report a bad batch
// body: a batch referencing this file still starts in `validating` and fails
// there, which is the behaviour the OpenAI Batch API describes.
func prepareBatchInput(
	src multipart.File,
	size int64,
	maxLines int64,
	maxBytes int64,
	policy batchinput.OrderPolicy,
) (*preparedBatchInput, error) {
	if policy == nil {
		return nil, fmt.Errorf("no batch input ordering policy configured")
	}

	res, err := batchinput.Scan(src, size, batchinput.ScanOptions{MaxLines: maxLines})
	if err != nil {
		return nil, err
	}

	meta := &batchinput.Metadata{Version: batchinput.MetadataVersion}

	if !res.Valid() {
		// The scan stopped at the offending line, so its line count describes
		// only the prefix that was read and is deliberately left unset.
		// Rewind: the scan consumed the reader, and storing needs it whole.
		if _, err := src.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("rewind upload after failed validation: %w", err)
		}
		meta.Policy = batchinput.PolicyOriginalV1 // stored verbatim
		meta.Invalid = res.Invalid
		return &preparedBatchInput{reader: src, sizeLimit: maxBytes, meta: meta}, nil
	}

	plan, err := batchinput.NewPlan(res, policy)
	if err != nil {
		return nil, err
	}

	meta.Policy = plan.Policy
	meta.LineCount = plan.LineCount
	meta.Models = plan.Models()

	// Terminating a reordered final line can push the object one byte past the
	// configured maximum even though the client stayed within it.
	sizeLimit := maxBytes
	if plan.Size > sizeLimit {
		sizeLimit = plan.Size
	}

	return &preparedBatchInput{reader: plan.Reader(src), sizeLimit: sizeLimit, meta: meta}, nil
}
