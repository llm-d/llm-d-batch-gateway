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

package common

import "testing"

func TestParseAfter(t *testing.T) {
	tests := []struct {
		name       string
		after      string
		wantOffset int
		wantID     string
		wantErr    bool
	}{
		{name: "empty", after: ""},
		{name: "zero offset", after: "0"},
		{name: "integer offset", after: "42", wantOffset: 42},
		{name: "object id", after: "file_0a6abd39-1111-2222-3333-444455556666", wantID: "file_0a6abd39-1111-2222-3333-444455556666"},
		{name: "negative offset", after: "-1", wantErr: true},
		{name: "min int64", after: "-9223372036854775808", wantErr: true},
		{name: "offset overflow", after: "99999999999999999999", wantErr: true},
		{name: "not an id", after: "invalid", wantErr: true},
		{name: "wrong prefix", after: "batch_123", wantErr: true},
		{name: "prefix only", after: "file_", wantErr: true},
		{name: "mixed digits", after: "12abc", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			offset, id, err := ParseAfter(tt.after, "file_")
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseAfter(%q) error = %v, wantErr %v", tt.after, err, tt.wantErr)
			}
			if offset != tt.wantOffset || id != tt.wantID {
				t.Errorf("ParseAfter(%q) = (%d, %q), want (%d, %q)", tt.after, offset, id, tt.wantOffset, tt.wantID)
			}
		})
	}
}
