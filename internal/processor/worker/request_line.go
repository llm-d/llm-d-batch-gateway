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
	"bytes"
	"encoding/json"
	"strconv"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

// decodeRequestLine parses one input JSONL line into the request the
// dispatcher forwards. The trailing newline is optional.
func decodeRequestLine(line []byte) (*batch_types.Request, error) {
	var req batch_types.Request
	if err := json.Unmarshal(bytes.TrimSuffix(line, []byte{'\n'}), &req); err != nil {
		return nil, err
	}
	return &req, nil
}

// mergeDispatchHeaders adds the gateway routing headers to the job's
// pass-through headers. Shared by every request source so that a batch is
// routed identically no matter how its input was read.
//
// headers may be nil and is mutated in place when it is not; callers that need
// to keep their own copy should clone first.
func mergeDispatchHeaders(
	headers map[string]string,
	cfg *config.ProcessorConfig,
	modelID string,
	tenantID string,
	sloDeadline time.Time,
) map[string]string {
	if headers == nil {
		headers = make(map[string]string)
	}

	if !sloDeadline.IsZero() {
		if ms := time.Until(sloDeadline).Milliseconds(); ms >= 0 {
			headers[sloTTFTMSHeader] = strconv.FormatInt(ms, 10)
		}
	}

	if obj := cfg.InferenceObjectiveFor(modelID); obj != "" {
		headers[inferenceObjectiveHeader] = obj
	}

	if cfg.SendFairnessHeader && tenantID != "" {
		if _, exists := headers[fairnessIDHeader]; !exists {
			headers[fairnessIDHeader] = tenantID
		}
	}

	return headers
}
