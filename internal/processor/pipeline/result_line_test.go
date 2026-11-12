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

package pipeline

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

// TestResultItemSucceeded verifies the success classification of the output
// line: the full 2xx range is success — a 201 or 204 response must not be
// checkpointed with failed accounting — while anything outside 2xx, an error
// object, or a missing response is a failure.
func TestResultItemSucceeded(t *testing.T) {
	tests := []struct {
		name string
		item ResultItem
		want bool
	}{
		{name: "200 is success", item: responseItem(http.StatusOK), want: true},
		{name: "201 is success", item: responseItem(http.StatusCreated), want: true},
		{name: "202 is success", item: responseItem(http.StatusAccepted), want: true},
		{name: "204 is success", item: responseItem(http.StatusNoContent), want: true},
		{name: "299 is success", item: responseItem(299), want: true},
		{name: "199 is not success", item: responseItem(199), want: false},
		{name: "300 is not success", item: responseItem(http.StatusMultipleChoices), want: false},
		{name: "404 is not success", item: responseItem(http.StatusNotFound), want: false},
		{name: "500 is not success", item: responseItem(http.StatusInternalServerError), want: false},
		{
			name: "error object overrides a 2xx response",
			item: func() ResultItem {
				item := responseItem(http.StatusOK)
				item.Error = &OutputError{Code: "server_error", Message: "boom"}
				return item
			}(),
			want: false,
		},
		{
			name: "missing response is not success",
			item: ResultItem{RequestID: "req-1", CustomID: "c-1"},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.item.Succeeded(); got != tt.want {
				t.Fatalf("Succeeded() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestResultItemErrorCode verifies the error classification of the output
// line: the output error's code when present, otherwise the HTTP status,
// otherwise unknown.
func TestResultItemErrorCode(t *testing.T) {
	tests := []struct {
		name string
		item ResultItem
		want string
	}{
		{name: "error object code", item: func() ResultItem {
			item := responseItem(http.StatusOK)
			item.Error = &OutputError{Code: "server_error", Message: "boom"}
			return item
		}(), want: "server_error"},
		{name: "http status of a non-2xx response", item: responseItem(http.StatusForbidden), want: "http_403"},
		{name: "missing response", item: ResultItem{RequestID: "req-1", CustomID: "c-1"}, want: "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.item.ErrorCode(); got != tt.want {
				t.Fatalf("ErrorCode() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestMarshalResultLine verifies the JSONL rendering of one result and the
// checkpoint metadata (SHA-256 digest, size) derived from it.
func TestMarshalResultLine(t *testing.T) {
	msg := responseItem(http.StatusCreated)
	msg.Response.Body = map[string]any{"choices": []any{map[string]any{"text": "created"}}}

	line, digest, size, err := MarshalResultLine(&msg)
	if err != nil {
		t.Fatalf("MarshalResultLine: %v", err)
	}

	if !bytes.HasSuffix(line, []byte("\n")) {
		t.Fatalf("line %q has no trailing newline", line)
	}
	if size != int64(len(line)) {
		t.Fatalf("size = %d, want %d", size, len(line))
	}
	sum := sha256.Sum256(line)
	if digest != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest = %s, want the SHA-256 of the line", digest)
	}

	var decoded outputLine
	if err := json.Unmarshal(bytes.TrimSuffix(line, []byte("\n")), &decoded); err != nil {
		t.Fatalf("unmarshal line: %v", err)
	}
	if decoded.ID != msg.RequestID || decoded.CustomID != msg.CustomID {
		t.Fatalf("line identity = (%s, %s), want (%s, %s)", decoded.ID, decoded.CustomID, msg.RequestID, msg.CustomID)
	}
	if decoded.Error != nil || decoded.Response == nil || decoded.Response.StatusCode != http.StatusCreated {
		t.Fatalf("line = %+v, want the 201 response line", decoded)
	}
}

// responseItem builds a result carrying only an HTTP response.
func responseItem(statusCode int) ResultItem {
	return ResultItem{
		RequestID: "req-1",
		CustomID:  "c-1",
		Response:  &batch_types.ResponseData{StatusCode: statusCode, RequestID: "req-1"},
	}
}
