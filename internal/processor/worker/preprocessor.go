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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batchinput"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// preProcessJob performs the pre-processing steps for the job.
// There are two ways in: when the stored object records an ordering policy
// this build understands, ingestion is metadata-only and the input is never
// downloaded — execution streams the object instead. Otherwise it falls back
// to downloading the input, validating it, and building per-model plan files,
// writing error entries for requests targeting unregistered models.
// The rejected count is persisted in model_map.json so executeJob can
// seed BatchRequestCounts.Failed without an extra parameter.
func (p *Processor) preProcessJob(ctx context.Context, jobInfo *batch_types.JobInfo) (err error) {
	logger := logr.FromContextOrDiscard(ctx)
	logger.V(logging.INFO).Info("Pre-processing job") // job id is in the logger already

	// The single ctx now also cancels the input-file download, so a cancellation
	// can surface as an I/O error. Reclassify only errors that actually stem from
	// ctx cancellation (they wrap ctx.Err()); a genuine preprocessing failure that
	// merely races a cancel must still surface as-is with its diagnostics.
	defer func() {
		if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			if s := batchctx.Cause(ctx); s != nil {
				err = s
			}
		}
	}()
	planBuildStart := time.Now()
	jobID := jobInfo.JobID
	inputFileID := jobInfo.BatchJob.InputFileID
	if inputFileID == "" {
		return fmt.Errorf("input file ID is empty")
	}

	jobRootDir, err := p.jobRootDir(jobID, jobInfo.TenantID)
	if err != nil {
		return fmt.Errorf("resolve job root directory: %w", err)
	}

	// job directory creation
	if err := os.MkdirAll(jobRootDir, 0o700); err != nil {
		return fmt.Errorf("create job root directory %q: %w", jobRootDir, err)
	}

	inputRef, err := p.resolveInputFileCoords(ctx, inputFileID)
	if err != nil {
		return fmt.Errorf("resolve input file %q: %w", inputFileID, err)
	}

	// The layout recorded with the object is the authority here, never this
	// processor's own configuration: an object written by some other policy
	// must not be read as though it were written by ours.
	//
	// Streaming the stored object is wired for async dispatch only. Sync
	// dispatch still derives its per-endpoint concurrency state from the model
	// map that plan-file ingestion produces, and it is on its way out, so it
	// keeps the scanning path rather than growing a second implementation.
	if p.asyncInference != nil && inputRef.meta.Readable() {
		return p.ingestOrderedInput(ctx, jobInfo, jobRootDir, inputRef.meta, planBuildStart)
	}
	switch {
	case inputRef.meta == nil:
		metrics.RecordInputIngest(metrics.IngestModeScan, metrics.InputPolicyNone)
	case !inputRef.meta.Readable():
		logger.V(logging.INFO).Info(
			"Stored input layout is not readable by this build, scanning the input instead",
			"policy", inputRef.meta.Policy, "metadataVersion", inputRef.meta.Version)
		metrics.RecordInputIngest(metrics.IngestModeScan, string(inputRef.meta.Policy))
	default:
		metrics.RecordInputIngest(metrics.IngestModeScan, string(inputRef.meta.Policy))
	}

	// input file stream open
	reader, metadata, err := p.openInputFileStream(ctx, inputFileID)
	if err != nil {
		return fmt.Errorf("open input file stream %q: %w", inputFileID, err)
	}
	defer reader.Close()

	if metadata != nil {
		logger.V(logging.INFO).Info("Input file metadata", "metadata", metadata)
	}

	acc := newPlanAccumulator(jobRootDir)

	// model intern tables
	used := make(map[string]int)           // to prevent duplicate model IDs
	modelToSafe := make(map[string]string) // to map the model ID to a safe file name

	seenCustomIDs := make(map[string]struct{})

	// streaming loop
	// In per-model mode, check each model against the resolver and reject
	// unregistered models early. In global mode, all models are routed to
	// the same endpoint so no check is needed.
	// p.inference != nil: NewProcessor does not validate clients; unit tests often
	// call preProcessJob without Run() and omit Inference (treat as non-per-model).
	// Production paths hit Processor.validate() in prepare() before work runs.
	// The guard also avoids panicking if a future caller wires a nil resolver.
	isPerModelGateway := p.inference != nil && !p.inference.IsGlobal()
	registeredModels := make(map[string]bool) // route key -> registered (per-model only)

	// Always truncate error.jsonl at the start of ingestion so that re-enqueued
	// jobs don't carry stale error entries from a previous attempt.
	// Execution opens the same file in append mode.
	// In global mode this creates an empty file that finalization omits (size 0).
	errorFilePath, err := p.jobErrorFilePath(jobID, jobInfo.TenantID)
	if err != nil {
		return err
	}
	errorFile, err := os.OpenFile(errorFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("failed to create error file: %w", err)
	}
	errorWriter := bufio.NewWriter(errorFile)
	defer func() {
		_ = errorWriter.Flush()
		errorFile.Close()
	}()

	var offset int64
	var lineCount int64 // to count the number of lines in the input file for logging
	var rejectedCount int64
	inputFileReader := bufio.NewReaderSize(reader, 1024*1024)

	for {
		// The abort context records why it stopped (SLO / user cancel / SIGTERM);
		// batchctx.Cause maps that to the terminal routing sentinel. First-cause
		// wins, so a user cancel racing SIGTERM is honoured by whichever fired first.
		if s := batchctx.Cause(ctx); s != nil {
			return s
		}

		// read a line from the input file
		line, streamBytes, done, err := readNormalizedLine(inputFileReader)
		if err != nil {
			return fmt.Errorf("read line %d from input file: %w", lineCount+1, err)
		}
		if done {
			break
		}

		lineCount++

		requestMeta, err := extractAndValidateLine(line)
		if err != nil {
			return fmt.Errorf("validate request at line %d: %w", lineCount, err)
		}

		if _, exists := seenCustomIDs[requestMeta.CustomID]; exists {
			return fmt.Errorf("line %d: duplicate custom_id %q", lineCount, requestMeta.CustomID)
		}
		seenCustomIDs[requestMeta.CustomID] = struct{}{}

		if isPerModelGateway {
			// Look up the gateway by the route key (tenant-scoped when
			// route_key_method is "tenant"). The raw model ID stays in the
			// error message and plan grouping below.
			lookupID := routeKey(p.cfg.RouteKeyMethod, jobInfo.TenantID, requestMeta.ModelID)
			registered, checked := registeredModels[lookupID]
			if !checked {
				registered = p.inference.ClientFor(lookupID) != nil
				registeredModels[lookupID] = registered
			}
			if !registered {
				// No plan entry exists yet, so generate a UUID for the batch request ID.
				// newBatchRequestID adds the "batch_req_" prefix for format consistency.
				errLine := &outputLine{
					ID:       newBatchRequestID(uuid.NewString()),
					CustomID: requestMeta.CustomID,
					Error: &outputError{
						Code:    inference.ErrCodeModelNotFound,
						Message: fmt.Sprintf("model %q is not configured in any gateway", requestMeta.ModelID),
					},
				}
				lineBytes, marshalErr := json.Marshal(errLine)
				if marshalErr != nil {
					return fmt.Errorf("failed to marshal model_not_found error: %w", marshalErr)
				}
				lineBytes = append(lineBytes, '\n')
				if _, writeErr := errorWriter.Write(lineBytes); writeErr != nil {
					return fmt.Errorf("failed to write model_not_found error: %w", writeErr)
				}
				rejectedCount++
				// Error metric rides the route key so labels stay consistent
				// with the dispatch path, whose items carry the scoped ID.
				metrics.RecordRequestError(lookupID)
				logger.V(logging.DEBUG).Info("Rejected request for unregistered model",
					"customId", requestMeta.CustomID, "model", requestMeta.ModelID)
				offset += int64(streamBytes)
				continue
			}
		}

		nextOffset := accumulatePlanEntry(
			acc, requestMeta.ModelID, modelToSafe, used, offset, uint32(streamBytes), requestMeta.PrefixHash,
		)
		offset = nextOffset
	}

	if err := finalizePlanFiles(acc, modelToSafe); err != nil {
		return fmt.Errorf("finalize plan files: %w", err)
	}

	// model map file writing
	if err := writeModelMappings(jobRootDir, modelToSafe, lineCount, rejectedCount); err != nil {
		return fmt.Errorf("write model map: %w", err)
	}

	sizeBucket := metrics.GetSizeBucket(int(lineCount))
	metrics.RecordPlanBuildDuration(time.Since(planBuildStart), sizeBucket)

	uotel.SetAttr(ctx,
		attribute.Int64(uotel.AttrInputLineCount, lineCount),
		attribute.Int(uotel.AttrModelCount, len(modelToSafe)),
		attribute.Int64(uotel.AttrRejectedCount, rejectedCount),
		attribute.String(uotel.AttrSizeBucket, sizeBucket),
	)

	modelCounts := make(map[string]int, len(modelToSafe))
	for model, safe := range modelToSafe {
		modelCounts[model] = len(acc.entries[safe])
	}
	logger.V(logging.INFO).Info("Processor Pre-processing job completed",
		"planFilePath", acc.plansDir(),
		"lineCount", lineCount, "rejected", rejectedCount, "models", modelCounts)

	return nil
}

// readNormalizedLine reads the next line from the reader, ensuring it ends with '\n'.
// streamBytes is the number of bytes consumed from the underlying stream (may differ from
// len(line) for the last line in a file that has no trailing newline).
func readNormalizedLine(r *bufio.Reader) (line []byte, streamBytes int, done bool, err error) {
	line, err = r.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, 0, false, err
	}
	if len(line) == 0 && err == io.EOF {
		return nil, 0, true, nil
	}
	streamBytes = len(line)
	if line[len(line)-1] != '\n' {
		line = append(line, '\n')
	}
	return line, streamBytes, false, nil
}

// requestMeta is the ingestion-time view of a request line.
type requestMeta = batchinput.LineMeta

// extractAndValidateLine parses and validates a request line and returns the
// metadata needed during ingestion. The implementation is shared with the API
// server so that a line ordered at upload time and the same line validated
// here can never disagree on its model or prefix hash.
func extractAndValidateLine(line []byte) (requestMeta, error) {
	return batchinput.ParseLine(line)
}

func writeModelMappings(jobRootDir string, modelToSafe map[string]string, lineCount, rejectedCount int64) error {
	safeToModel := make(map[string]string, len(modelToSafe))
	for modelID, safeID := range modelToSafe {
		safeToModel[safeID] = modelID
	}

	modelMap := modelMapFile{
		ModelToSafe:   modelToSafe,
		SafeToModel:   safeToModel,
		LineCount:     lineCount,
		RejectedCount: rejectedCount,
		// Stated rather than left implicit, even though it is the zero value:
		// this path is the one that produced the plan files, so it should say
		// so.
		InputMode: inputModePlanFiles,
	}
	return writeModelMapFile(jobRootDir, modelMap)
}
