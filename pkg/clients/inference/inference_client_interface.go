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

package inference

import (
	"context"

	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
)

// InferenceClientI defines the interface for making inference requests
type InferenceClient interface {
	Generate(ctx context.Context, req *GenerateRequest) (*GenerateResponse, *ClientError)
}

// GenerateRequest represents an inference generation request
type GenerateRequest struct {
	RequestID string                 // unique request id set by user
	Endpoint  string                 // API endpoint (e.g., "/v1/chat/completions")
	Params    map[string]interface{} // parameters (must include "model")
	Headers   map[string]string      // extra headers to forward to the endpoint
	Metadata  map[string]string      // async request metadata for dispatch gates; not mutated
}

// GenerateResponse represents an inference generation response.
//
// For async results (from llm-d-async ResultMessage):
//   - StatusCode > 0: an HTTP response was received; Response holds the body.
//   - StatusCode == 0 with ErrorCode/ErrorMessage: no HTTP response (deadline,
//     cancel, gate drop, etc.).
//   - StatusCode == 0 with empty ErrorCode: legacy success payload (treat as 200).
type GenerateResponse struct {
	RequestID        string
	Response         []byte
	StatusCode       int    // HTTP status from async ResultMessage; 0 = unset/non-HTTP
	ErrorCode        string // non-HTTP failure code from async ResultMessage
	ErrorMessage     string // non-HTTP failure message from async ResultMessage
	RawData          interface{}
	HadCapacityRetry bool // true if any retry was caused by 429/5xx (not network error)
}

// IsNonHTTPFailure reports whether the response represents a failure that did
// not produce an HTTP status (e.g. deadline exceeded, cancel, gate drop).
func (r *GenerateResponse) IsNonHTTPFailure() bool {
	return r.StatusCode == 0 && (r.ErrorCode != "" || r.ErrorMessage != "")
}

// ClientError represents an inference client error
type ClientError = httpclient.ClientError

// HTTPClientConfig is an alias for httpclient.Config
type HTTPClientConfig = httpclient.Config
