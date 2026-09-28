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
	"encoding/json"
	"fmt"
)

// TagKey is the database tag under which input metadata travels with a stored
// file record. It is internal bookkeeping and is never surfaced through the
// public file API.
const TagKey = "batch_input"

// MetadataVersion versions the encoding below, independently of the ordering
// policy. A consumer that does not recognise the version must not interpret
// any other field and has to fall back to reading the object itself.
const MetadataVersion = 1

// Metadata records what the API server did to an input file at upload time so
// that the processor can consume the object without re-deriving it.
type Metadata struct {
	// Version is the encoding version; see MetadataVersion.
	Version int `json:"v"`

	// Policy is the ordering policy the object was written under. It is the
	// authority on the object's layout: never substitute the consumer's own
	// configured policy for it.
	Policy PolicyID `json:"policy"`

	// LineCount is the number of request lines in the object.
	LineCount int64 `json:"lines"`

	// Bytes is the stored object size.
	Bytes int64 `json:"bytes"`

	// Models lists the distinct models referenced by the object.
	Models []string `json:"models,omitempty"`

	// Invalid is set when the upload failed batch validation. The object was
	// stored verbatim and unordered; any batch referencing it must fail during
	// its validating phase with this error.
	Invalid *ValidationError `json:"invalid,omitempty"`
}

// Encode serialises the metadata for storage in a tag value.
func (m *Metadata) Encode() (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("encode batch input metadata: %w", err)
	}
	return string(data), nil
}

// DecodeMetadata extracts input metadata from a file record's tags. It returns
// (nil, nil) when the file predates this metadata, which callers must treat as
// "layout unknown" rather than as an error.
func DecodeMetadata(tags map[string]string) (*Metadata, error) {
	raw, ok := tags[TagKey]
	if !ok || raw == "" {
		return nil, nil
	}
	var m Metadata
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, fmt.Errorf("decode batch input metadata: %w", err)
	}
	return &m, nil
}

// Readable reports whether this consumer can rely on the recorded layout.
// It is false when the metadata is absent, encoded by a newer version, or
// written by a policy this binary does not know — every one of which means the
// consumer must fall back to reading the stored object to learn its layout.
func (m *Metadata) Readable() bool {
	if m == nil || m.Version != MetadataVersion {
		return false
	}
	policy, ok := Lookup(m.Policy)
	if !ok {
		return false
	}
	return policy.DispatchOrder() == DispatchSequential
}
