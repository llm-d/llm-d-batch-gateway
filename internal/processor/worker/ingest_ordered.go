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
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batchinput"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

// ingestOrderedInput is ingestion for an input whose stored layout this build
// understands.
//
// The API server already validated the content and ordered it for dispatch, so
// there is nothing left to derive: no download, no parse, no plan files. All
// that remains is to record the handoff manifest execution reads and to reset
// the error file, which execution opens in append mode.
func (p *Processor) ingestOrderedInput(
	ctx context.Context,
	jobInfo *batch_types.JobInfo,
	jobRootDir string,
	meta *batchinput.Metadata,
	planBuildStart time.Time,
) error {
	logger := logr.FromContextOrDiscard(ctx)

	if s := batchctx.Cause(ctx); s != nil {
		return s
	}

	// A file the API server could not validate is stored verbatim with the
	// reason attached. Surfacing it here is what turns the batch's validating
	// phase into a failure, which is where a malformed input is supposed to be
	// reported.
	if meta.Invalid != nil {
		metrics.RecordInputIngest(metrics.IngestModeRejected, string(meta.Policy))
		return fmt.Errorf("input file failed validation: %w", meta.Invalid)
	}

	// Re-enqueued jobs must not inherit error lines from a previous attempt.
	if err := p.truncateErrorFile(jobInfo.JobID, jobInfo.TenantID); err != nil {
		return err
	}

	manifest := modelMapFile{
		ModelToSafe: map[string]string{},
		SafeToModel: map[string]string{},
		LineCount:   meta.LineCount,
		InputMode:   inputModeSequential,
		InputPolicy: meta.Policy,
		InputBytes:  meta.Bytes,
		InputModels: meta.Models,
	}
	if err := writeModelMapFile(jobRootDir, manifest); err != nil {
		return fmt.Errorf("write model map: %w", err)
	}

	sizeBucket := metrics.GetSizeBucket(int(meta.LineCount))
	metrics.RecordPlanBuildDuration(time.Since(planBuildStart), sizeBucket)
	metrics.RecordInputIngest(metrics.IngestModeSequential, string(meta.Policy))

	uotel.SetAttr(ctx,
		attribute.Int64(uotel.AttrInputLineCount, meta.LineCount),
		attribute.Int(uotel.AttrModelCount, len(meta.Models)),
		attribute.Int64(uotel.AttrRejectedCount, 0),
		attribute.String(uotel.AttrSizeBucket, sizeBucket),
	)

	logger.V(logging.INFO).Info("Pre-processing skipped: input is already in dispatch order",
		"policy", meta.Policy, "lineCount", meta.LineCount,
		"bytes", meta.Bytes, "models", meta.Models)
	return nil
}

// truncateErrorFile resets error.jsonl so a retried job starts clean.
// Execution opens the same path in append mode.
func (p *Processor) truncateErrorFile(jobID, tenantID string) error {
	path, err := p.jobErrorFilePath(jobID, tenantID)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("failed to create error file: %w", err)
	}
	return f.Close()
}
