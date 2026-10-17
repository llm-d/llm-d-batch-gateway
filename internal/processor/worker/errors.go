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

import "errors"

// Sentinel errors used for routing within the processor worker.
// The cancellation-cause sentinels (cancelled / expired / shutdown) live in the
// batchctx package, which owns the abort context and its cause mapping.

var (
	// errRequestInputRead signals a fatal failure reading a request entry from the
	// plan input file. Unlike per-request errors (which are embedded in output),
	// this prevents the entire model from processing further.
	errRequestInputRead = errors.New("failed to read request from input file")

	// errFinalizeFailedOver signals that a terminal status transition (completed,
	// cancelled) or an upload of the legacy ingestion-phase cancel path failed,
	// but the fallback to failed status with surviving file IDs succeeded. The
	// caller must NOT call handleFailed again, as that would overwrite file IDs
	// with empty strings and orphan already-uploaded files. The resumable
	// finalizers never fall back: they return their errors and leave the row
	// non-terminal for the reconciler requeue.
	errFinalizeFailedOver = errors.New("finalization fell back to failed")
)
