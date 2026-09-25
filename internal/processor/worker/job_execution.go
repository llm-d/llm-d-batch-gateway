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
	"context"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

// jobExecutionParams holds the job-scoped state shared across processing stages.
// Contexts are NOT stored here — they are passed explicitly per Go convention.
// cancelUser is an exception: it is a no-arg closure that trips the abort
// context's cause func with batchctx.ErrCancelled, stored here so the watchCancel
// goroutine can call it when a user cancel event arrives. The recorded cause is
// later read via batchctx.Cause to classify the terminal state.
type jobExecutionParams struct {
	updater *StatusUpdater
	jobItem *db.BatchItem
	jobInfo *batch_types.JobInfo
	task    *db.BatchJobPriority

	eventWatcher *db.BatchEventsChan
	cancelUser   context.CancelFunc
	// onOwnershipLost trips the abort context with the neutral cause when a
	// fenced progress write reveals this processor lost ownership of the job.
	// Set by runJob, invoked by the per-job progress updater.
	onOwnershipLost func()

	requestCounts *openai.BatchRequestCounts
}
