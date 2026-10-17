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
	"fmt"

	"github.com/google/uuid"

	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

// outputLine represents a single line in the output JSONL file following the OpenAI batch output format.
type outputLine struct {
	ID       string                    `json:"id"`
	CustomID string                    `json:"custom_id"`
	Response *batch_types.ResponseData `json:"response"`
	Error    *outputError              `json:"error"`
}

type outputError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

const (
	sloTTFTMSHeader          = "x-slo-ttft-ms"
	inferenceObjectiveHeader = "x-gateway-inference-objective"
	fairnessIDHeader         = "x-gateway-inference-fairness-id"
)

// newBatchRequestID formats requestID into the "batch_req_<uuid>" form required by the
// OpenAI Batch API for output/error line IDs.
func newBatchRequestID(requestID string) string {
	return fmt.Sprintf("batch_req_%s", requestID)
}

// stableBatchRequestID derives a deterministic request ID from the batch and
// custom IDs, so re-preprocessing the same input yields the same request IDs.
func stableBatchRequestID(batchID, customID string) string {
	u := uuid.NewSHA1(uuid.NameSpaceOID, []byte(batchID+"\x00"+customID))
	return newBatchRequestID(u.String())
}
