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

// this file contains the status updater logic for the processor
package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batch_utils"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/failpoint"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

type StatusUpdater struct {
	db             db.BatchDBClient
	status         db.BatchStatusClient
	progressTTLSec int
}

func NewStatusUpdater(db db.BatchDBClient, status db.BatchStatusClient, progressTTLSec int) *StatusUpdater {
	return &StatusUpdater{
		db:             db,
		status:         status,
		progressTTLSec: progressTTLSec,
	}
}

func (s *StatusUpdater) validate() error {
	if s.db == nil {
		return fmt.Errorf("database client is missing")
	}
	if s.status == nil {
		return fmt.Errorf("status client is missing")
	}
	return nil
}

// UpdateProgressCounts pushes request counts to the volatile status store (e.g. Redis).
// This is NOT a persistent DB update — it is a lightweight, frequent update used to power
// real-time progress polling. The data expires after progressTTLSec.
func (s *StatusUpdater) UpdateProgressCounts(
	ctx context.Context,
	jobID string,
	requestCounts *openai.BatchRequestCounts,
) error {
	if requestCounts == nil {
		return fmt.Errorf("requestCounts is nil")
	}

	// light payload for frequent updates
	payload := []byte(fmt.Sprintf(`{"total": %d, "completed": %d, "failed": %d}`, requestCounts.Total, requestCounts.Completed, requestCounts.Failed))

	if err := s.status.StatusSet(ctx, jobID, s.progressTTLSec, payload); err != nil {
		return err
	}
	return nil
}

// UpdatePersistentStatus writes the job status to the persistent database (e.g. PostgreSQL).
// Unlike UpdateProgressCounts, this is the authoritative, durable record of job state.
// Optional modifiers are applied to the status info before marshaling.
func (s *StatusUpdater) UpdatePersistentStatus(
	ctx context.Context,
	dbJob *db.BatchItem,
	newStatus openai.BatchStatus,
	counts *openai.BatchRequestCounts,
	slo *time.Time,
	modifiers ...func(*openai.BatchStatusInfo),
) error {
	if dbJob == nil {
		return fmt.Errorf("dbJob is nil")
	}
	if len(dbJob.Status) == 0 {
		return fmt.Errorf("dbJob.Status is empty")
	}

	var original openai.BatchStatusInfo
	if err := json.Unmarshal(dbJob.Status, &original); err != nil {
		return err
	}

	// Build base status first (status, timestamps, counts), then apply modifiers
	// to set additional fields (e.g. file IDs) that aren't part of the standard transition.
	updated, err := batch_utils.BuildUpdatedStatusInfo(&original, newStatus, counts, slo)
	if err != nil {
		return err
	}

	for _, fn := range modifiers {
		fn(updated)
	}

	statusBytes, err := json.Marshal(updated)
	if err != nil {
		return err
	}

	if newStatus.IsTerminal() {
		failpoint.Inject("processor/before-terminal-write")
	}

	if err := s.db.DBUpdate(ctx, &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID: dbJob.ID,
		},
		BaseContents: db.BaseContents{
			Status: statusBytes,
		},
		Epoch: dbJob.Epoch,
	}, nil); err != nil {
		return err
	}

	dbJob.Status = statusBytes

	logger := logr.FromContextOrDiscard(ctx)
	logger.V(logging.INFO).Info("Batch status updated successfully", "newStatus", newStatus)
	return nil
}

// withFileIDs returns a modifier that sets output and error file IDs on the status info.
// Empty IDs are skipped (per the OpenAI batch spec, both are optional).
func withFileIDs(outputFileID, errorFileID string) func(*openai.BatchStatusInfo) {
	return func(info *openai.BatchStatusInfo) {
		if outputFileID != "" {
			s := outputFileID
			info.OutputFileID = &s
		}
		if errorFileID != "" {
			s := errorFileID
			info.ErrorFileID = &s
		}
	}
}

// UpdateCompletedStatus transitions the job to completed status and records file IDs.
// Per the OpenAI batch spec, both IDs are optional: outputFileID is empty when all requests
// failed, and errorFileID is empty when no requests failed.
func (s *StatusUpdater) UpdateCompletedStatus(
	ctx context.Context,
	dbJob *db.BatchItem,
	counts *openai.BatchRequestCounts,
	outputFileID string,
	errorFileID string,
) error {
	return s.UpdatePersistentStatus(ctx, dbJob, openai.BatchStatusCompleted, counts, nil,
		withFileIDs(outputFileID, errorFileID),
	)
}

// UpdateFailedStatus transitions the job to failed status and records partial file IDs when available.
// counts may be nil when the job failed before execution (ingestion).
func (s *StatusUpdater) UpdateFailedStatus(
	ctx context.Context,
	dbJob *db.BatchItem,
	counts *openai.BatchRequestCounts,
	outputFileID string,
	errorFileID string,
) error {
	return s.UpdatePersistentStatus(ctx, dbJob, openai.BatchStatusFailed, counts, nil,
		withFileIDs(outputFileID, errorFileID),
	)
}

// UpdateCancelledStatus transitions the job to cancelled status and records partial file IDs.
// counts may be nil when the job was cancelled before execution (ingestion).
func (s *StatusUpdater) UpdateCancelledStatus(
	ctx context.Context,
	dbJob *db.BatchItem,
	counts *openai.BatchRequestCounts,
	outputFileID string,
	errorFileID string,
) error {
	return s.UpdatePersistentStatus(ctx, dbJob, openai.BatchStatusCancelled, counts, nil,
		withFileIDs(outputFileID, errorFileID),
	)
}

// UpdateExpiredStatus transitions the job to expired status and records partial file IDs.
// Per the OpenAI batch spec, completed requests are preserved in the output file and unexecuted
// requests are recorded in the error file with code "batch_expired". Both file IDs are optional.
func (s *StatusUpdater) UpdateExpiredStatus(
	ctx context.Context,
	dbJob *db.BatchItem,
	counts *openai.BatchRequestCounts,
	outputFileID string,
	errorFileID string,
) error {
	return s.UpdatePersistentStatus(ctx, dbJob, openai.BatchStatusExpired, counts, nil,
		withFileIDs(outputFileID, errorFileID),
	)
}
