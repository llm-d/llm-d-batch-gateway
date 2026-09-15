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

// The file defines the Batch API data structures matching the OpenAI specification.
package openai

import (
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"
)

// https://platform.openai.com/docs/api-reference/batch

// Endpoint represents a batch API endpoint
type Endpoint string

// Supported batch API endpoints
const (
	EndpointResponses       Endpoint = "/v1/responses"
	EndpointChatCompletions Endpoint = "/v1/chat/completions"
	EndpointEmbeddings      Endpoint = "/v1/embeddings"
	EndpointCompletions     Endpoint = "/v1/completions"
	EndpointModerations     Endpoint = "/v1/moderations"
)

// EndpointAllowlist reports which batch endpoints are accepted. The zero value accepts
// exactly the OpenAI Batch API endpoints.
type EndpointAllowlist struct {
	extra map[Endpoint]struct{}
}

// NewEndpointAllowlist returns an allowlist covering the OpenAI endpoints plus extra.
// Entries duplicating a default are ignored. Malformed entries are rejected so a
// bad deployment config fails at startup, not per request.
func NewEndpointAllowlist(extra []string) (EndpointAllowlist, error) {
	var a EndpointAllowlist
	for _, e := range extra {
		if err := validateEndpointPath(e); err != nil {
			return EndpointAllowlist{}, fmt.Errorf("endpoint %q: %w", e, err)
		}
		ep := Endpoint(e)
		if isDefaultEndpoint(ep) {
			continue
		}
		if a.extra == nil {
			a.extra = make(map[Endpoint]struct{}, len(extra))
		}
		a.extra[ep] = struct{}{}
	}
	return a, nil
}

// IsValid reports whether endpoint is accepted by this allowlist
func (a EndpointAllowlist) IsValid(endpoint string) bool {
	ep := Endpoint(endpoint)
	if isDefaultEndpoint(ep) {
		return true
	}
	_, ok := a.extra[ep]
	return ok
}

func isDefaultEndpoint(endpoint Endpoint) bool {
	switch endpoint {
	case EndpointResponses,
		EndpointChatCompletions,
		EndpointEmbeddings,
		EndpointCompletions,
		EndpointModerations:
		return true
	default:
		return false
	}
}

func validateEndpointPath(endpoint string) error {
	if endpoint == "" {
		return errors.New("path cannot be empty")
	}
	if endpoint[0] != '/' || len(endpoint) > 1 && endpoint[1] == '/' {
		return errors.New("must be an absolute path beginning with exactly one slash")
	}
	if strings.ContainsAny(endpoint, "?#") {
		return errors.New("must not contain a query string or fragment")
	}
	if path.Clean(endpoint) != endpoint {
		return errors.New("must be a canonical path without dot segments or repeated slashes")
	}
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil {
		return fmt.Errorf("must be a valid URL path: %w", err)
	}
	if parsed.Path != endpoint || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("must not contain a query string, fragment, or escaped path")
	}
	return nil
}

func (e Endpoint) String() string {
	return string(e)
}

// IsValidEndpoint reports whether the given endpoint is one of the supported
// Batch API endpoints.
func IsValidEndpoint(endpoint string) bool {
	return (EndpointAllowlist{}).IsValid(endpoint)
}

type BatchStatus string

const (
	BatchStatusValidating BatchStatus = "validating"
	BatchStatusFailed     BatchStatus = "failed"
	BatchStatusInProgress BatchStatus = "in_progress"
	BatchStatusFinalizing BatchStatus = "finalizing"
	BatchStatusCompleted  BatchStatus = "completed"
	BatchStatusExpired    BatchStatus = "expired"
	BatchStatusCancelling BatchStatus = "cancelling"
	BatchStatusCancelled  BatchStatus = "cancelled"
)

func (s BatchStatus) String() string {
	return string(s)
}

// terminalStatuses is the single source of truth for terminal batch statuses.
// Declared as an array (not slice) so it cannot be appended to or resliced.
var terminalStatuses = [4]BatchStatus{
	BatchStatusCompleted,
	BatchStatusFailed,
	BatchStatusExpired,
	BatchStatusCancelled,
}

func (s BatchStatus) IsTerminal() bool {
	for _, ts := range terminalStatuses {
		if s == ts {
			return true
		}
	}
	return false
}

// TerminalStatuses returns a copy so callers cannot mutate the canonical list.
func TerminalStatuses() []BatchStatus {
	result := terminalStatuses
	return result[:]
}

func (s BatchStatus) IsCancellable() bool {
	return s == BatchStatusValidating || s == BatchStatusInProgress || s == BatchStatusCancelling
}

type BatchSpec struct {
	// required. The object type, which is always `batch`.
	Object string `json:"object"`

	// required. The OpenAI API endpoint used by the batch.
	Endpoint Endpoint `json:"endpoint"`

	// required. The ID of the input file for the batch.
	InputFileID string `json:"input_file_id"`

	// required. The time frame within which the batch should be processed.
	CompletionWindow string `json:"completion_window"`

	// optional. Set of 16 key-value pairs that can be attached to an object. This can be useful for storing additional information about the object in a structured format, and querying for objects via API or the dashboard.   Keys are strings with a maximum length of 64 characters. Values are strings with a maximum length of 512 characters.
	Metadata map[string]string `json:"metadata"`

	// required. The Unix timestamp (in seconds) for when the batch was created.
	CreatedAt int64 `json:"created_at"`
}

type BatchStatusInfo struct {
	// required. The current status of the batch.
	Status BatchStatus `json:"status"`

	// optional, nullable. The ID of the file containing the outputs of successfully executed requests.
	OutputFileID *string `json:"output_file_id"`

	// optional, nullable. The ID of the file containing the outputs of requests with errors.
	ErrorFileID *string `json:"error_file_id"`

	// optional. The Unix timestamp (in seconds) for when the batch was cancelled.
	CancelledAt *int64 `json:"cancelled_at"`

	// optional. The Unix timestamp (in seconds) for when the batch started cancelling.
	CancellingAt *int64 `json:"cancelling_at"`

	// optional. The Unix timestamp (in seconds) for when the batch was completed.
	CompletedAt *int64 `json:"completed_at"`

	// optional. The Unix timestamp (in seconds) for when the batch expired.
	ExpiredAt *int64 `json:"expired_at"`

	// optional. The Unix timestamp (in seconds) for when the batch will expire.
	ExpiresAt *int64 `json:"expires_at"`

	// optional. The Unix timestamp (in seconds) for when the batch failed.
	FailedAt *int64 `json:"failed_at"`

	// optional. The Unix timestamp (in seconds) for when the batch started finalizing.
	FinalizingAt *int64 `json:"finalizing_at"`

	// optional. The Unix timestamp (in seconds) for when the batch started processing.
	InProgressAt *int64 `json:"in_progress_at"`

	// optional. The Model ID used to process the batch
	Model string `json:"model,omitempty"`

	// optional. The request counts for different statuses within the batch.
	RequestCounts BatchRequestCounts `json:"request_counts"`

	// optional, nullable.
	Errors *BatchErrors `json:"errors"`

	// optional, nullable. Represents token usage details including input tokens, output tokens, a
	// breakdown of output tokens, and the total tokens used.
	Usage *BatchUsage `json:"usage"`
}

type Batch struct {
	// required.
	ID string `json:"id"`
	BatchSpec
	BatchStatusInfo
}

type ListBatchResponse struct {
	// required. The type of object returned, must be `list`.
	Object string `json:"object"`

	// required. A list of items used to generate this response.
	Data []Batch `json:"data"`

	// optional. The ID of the first item in the list.
	FirstID *string `json:"first_id"`

	// optional. The ID of the last item in the list.
	LastID *string `json:"last_id"`

	// required. Whether there are more items available.
	HasMore bool `json:"has_more"`
}

type BatchError struct {

	// optional. An error code identifying the error type.
	Code string `json:"code"`

	// optional. A human-readable message providing more details about the error.
	Message string `json:"message"`

	// optional, nullable. The name of the parameter that caused the error, if applicable.
	Param *string `json:"param"`

	// optional, nullable. The line number of the input file where the error occurred, if applicable.
	Line *int64 `json:"line"`
}

type BatchErrors struct {

	// optional. The object type, which is always `list`.
	Object string `json:"object"`

	// optional.
	Data []BatchError `json:"data"`
}

// BatchRequestCounts - The request counts for different statuses within the batch.
type BatchRequestCounts struct {

	// required. Total number of requests in the batch.
	Total int64 `json:"total"`

	// required. Number of requests that have been completed successfully.
	Completed int64 `json:"completed"`

	// required. Number of requests that have failed.
	Failed int64 `json:"failed"`
}

func (c *BatchRequestCounts) AllSucceeded() bool {
	return c.Failed == 0 && c.Completed == c.Total
}

type BatchUsage struct {
	// required. The number of input tokens.
	InputTokens int64 `json:"input_tokens"`

	// required. A detailed breakdown of the input tokens.
	InputTokensDetails BatchUsageInputTokensDetails `json:"input_tokens_details"`

	// required. The number of output tokens.
	OutputTokens int64 `json:"output_tokens"`

	// required. A detailed breakdown of the output tokens.
	OutputTokensDetails BatchUsageOutputTokensDetails `json:"output_tokens_details"`

	// required. The total number of tokens used.
	TotalTokens int64 `json:"total_tokens"`
}

type BatchUsageInputTokensDetails struct {
	// required. The number of tokens that were retrieved from the cache.
	// [More on prompt caching](https://platform.openai.com/docs/guides/prompt-caching).
	CachedTokens int64 `json:"cached_tokens"`
}

type BatchUsageOutputTokensDetails struct {
	// required. The number of reasoning tokens.
	ReasoningTokens int64 `json:"reasoning_tokens"`
}

type CreateBatchRequest struct {

	// required. The ID of an uploaded file that contains requests for the new batch.  See [upload file](/docs/api-reference/files/create) for how to upload a file.  Your input file must be formatted as a [JSONL file](/docs/api-reference/batch/request-input), and must be uploaded with the purpose `batch`. The file can contain up to 50,000 requests, and can be up to 200 MB in size.
	InputFileID string `json:"input_file_id"`

	// required. The endpoint to be used for all requests in the batch. Currently `/v1/responses`, `/v1/chat/completions`, `/v1/embeddings`, and `/v1/completions` are supported. Note that `/v1/embeddings` batches are also restricted to a maximum of 50,000 embedding inputs across all requests in the batch.
	Endpoint Endpoint `json:"endpoint"`

	// required. The time frame within which the batch should be processed. Currently only `24h` is supported.
	CompletionWindow string `json:"completion_window"`

	// optional. Set of 16 key-value pairs that can be attached to an object. This can be useful for storing additional information about the object in a structured format, and querying for objects via API or the dashboard.   Keys are strings with a maximum length of 64 characters. Values are strings with a maximum length of 512 characters.
	Metadata map[string]string `json:"metadata,omitempty"`

	// optional. The expiration policy for the output and/or error file that are generated for a batch.
	OutputExpiresAfter *OutputExpiresAfter `json:"output_expires_after"`
}

type OutputExpiresAfter struct {
	// required. The number of seconds after the anchor time that the file will expire. Must be
	// between 3600 (1 hour) and 2592000 (30 days).
	Seconds int64 `json:"seconds"`
	// required. Anchor timestamp after which the expiration policy applies. Supported anchors:
	// `created_at`. Note that the anchor is the file creation time, not the time the
	// batch is created.
	//
	// This field can be elided, and will marshal its zero value as "created_at".
	Anchor string `json:"anchor"`
}

func (r *CreateBatchRequest) Validate() error {
	return r.ValidateWithEndpointAllowlist(EndpointAllowlist{})
}

// ValidateWithEndpointAllowlist validates the request using the supplied endpoint allowlist.
func (r *CreateBatchRequest) ValidateWithEndpointAllowlist(allowlist EndpointAllowlist) error {
	if r.CompletionWindow == "" {
		return errors.New("completion_window is required")
	}

	if _, err := time.ParseDuration(r.CompletionWindow); err != nil {
		return errors.New("completion_window must be a valid duration (e.g., 24h)")
	}

	if r.Endpoint == "" {
		return errors.New("endpoint is required")
	}

	if !allowlist.IsValid(r.Endpoint.String()) {
		return errors.New("invalid endpoint: " + string(r.Endpoint))
	}

	if r.InputFileID == "" {
		return errors.New("input_file_id is required")
	}

	if len(r.Metadata) > 16 {
		return fmt.Errorf("metadata: too many keys (max 16, got %d)", len(r.Metadata))
	}
	for k, v := range r.Metadata {
		if len(k) > 64 {
			return fmt.Errorf("metadata: key exceeds 64 characters")
		}
		if len(v) > 512 {
			return fmt.Errorf("metadata: value for key %q exceeds 512 characters", k)
		}
	}

	if r.OutputExpiresAfter != nil {
		if r.OutputExpiresAfter.Anchor != "created_at" {
			return errors.New("output_expires_after.anchor must be 'created_at'")
		}

		if r.OutputExpiresAfter.Seconds < MinExpirationSeconds || r.OutputExpiresAfter.Seconds > MaxExpirationSeconds {
			return fmt.Errorf("output_expires_after.seconds must be between %d (1 hour) and %d (30 days)", MinExpirationSeconds, MaxExpirationSeconds)
		}
	}

	return nil
}
