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

// Package batchinput owns the batch input JSONL contract shared by the API
// server (which orders lines before storing them) and the processor (which
// consumes the stored object at dispatch time). Both sides must derive the
// same metadata from a request line, so the parsing, validation and
// prefix-hash logic lives here exactly once.
package batchinput

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"strings"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// NoPrefixHash is used when a request has no system prompt.
// Set to MaxUint32 so that no-prompt requests sort last, allowing
// requests with actual system prompts to be dispatched first.
const NoPrefixHash uint32 = math.MaxUint32

// requestLine is the subset of an input JSONL line needed to validate it and
// derive its ordering keys. The full line is always forwarded verbatim to the
// inference backend, so fields not listed here stay untouched.
type requestLine struct {
	CustomID string `json:"custom_id"`
	Method   string `json:"method"`
	URL      string `json:"url"`
	Body     struct {
		Model    string `json:"model"`
		Stream   *bool  `json:"stream,omitempty"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	} `json:"body"`
}

// LineMeta is the per-request metadata derived from one input JSONL line.
type LineMeta struct {
	CustomID   string
	ModelID    string
	PrefixHash uint32
}

// ParseLine validates a single request line and returns its ordering metadata.
// The trailing newline is optional. Errors describe why the line is not a
// valid batch request; callers map them to a batch validation failure.
func ParseLine(line []byte) (LineMeta, error) {
	var req requestLine
	trimmed := bytes.TrimSuffix(line, []byte{'\n'})
	if err := json.Unmarshal(trimmed, &req); err != nil {
		return LineMeta{}, err
	}
	if req.CustomID == "" {
		return LineMeta{}, fmt.Errorf("custom_id is required")
	}
	if req.Method == "" {
		return LineMeta{}, fmt.Errorf("method is required")
	}
	if req.Method != "POST" {
		return LineMeta{}, fmt.Errorf("invalid method: %s", req.Method)
	}
	if req.URL == "" {
		return LineMeta{}, fmt.Errorf("url is required")
	}
	if !strings.HasPrefix(req.URL, "/") || strings.HasPrefix(req.URL, "//") || strings.Contains(req.URL, "://") {
		return LineMeta{}, fmt.Errorf("url must be a relative path: %s", req.URL)
	}
	if !openai.IsValidEndpoint(req.URL) {
		return LineMeta{}, fmt.Errorf("invalid endpoint: %s", req.URL)
	}
	if req.Body.Model == "" {
		return LineMeta{}, fmt.Errorf("model id is empty")
	}
	if req.Body.Stream != nil && *req.Body.Stream {
		return LineMeta{}, fmt.Errorf("streaming is not supported in batch requests (model: %s)", req.Body.Model)
	}

	prefixHash := NoPrefixHash
	for _, msg := range req.Body.Messages {
		if msg.Role != "system" {
			continue
		}
		text, err := messageText(msg.Content)
		if err != nil {
			return LineMeta{}, fmt.Errorf("system message content: %w", err)
		}
		if text != "" {
			h := fnv.New32a()
			h.Write([]byte(text))
			prefixHash = h.Sum32()
			break
		}
	}

	return LineMeta{
		CustomID:   req.CustomID,
		ModelID:    req.Body.Model,
		PrefixHash: prefixHash,
	}, nil
}

// messageText extracts only the system text needed for prefix grouping.
// Other content stays encoded and the original input is used for forwarding.
func messageText(content json.RawMessage) (string, error) {
	content = bytes.TrimSpace(content)
	if len(content) == 0 || bytes.Equal(content, []byte("null")) {
		return "", nil
	}
	switch content[0] {
	case '"':
		var text string
		err := json.Unmarshal(content, &text)
		return text, err
	case '[':
		var parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if err := json.Unmarshal(content, &parts); err != nil {
			return "", err
		}
		var builder strings.Builder
		for _, part := range parts {
			if part.Type == "text" {
				builder.WriteString(part.Text)
			}
		}
		return builder.String(), nil
	default:
		return "", fmt.Errorf("must be a string or an array of content parts")
	}
}
