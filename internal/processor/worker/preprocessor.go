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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"

	"go.opentelemetry.io/otel/attribute"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// preProcessJob performs the pre-processing steps for the job.
// It downloads the input file, creates per-model plan files, and writes
// error entries for requests targeting unregistered models.
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

	// input file stream open
	reader, metadata, err := p.openInputFileStream(ctx, inputFileID)
	if err != nil {
		return fmt.Errorf("open input file stream %q: %w", inputFileID, err)
	}
	defer reader.Close()

	if metadata != nil {
		logger.V(logging.INFO).Info("Input file metadata", "metadata", metadata)
	}

	// create local input file
	localInputFile, localInputFilePath, err := p.createLocalInputFile(jobID, jobInfo.TenantID)
	if err != nil {
		return fmt.Errorf("create local input file: %w", err)
	}
	defer localInputFile.Close()

	writer := bufio.NewWriterSize(localInputFile, 1024*1024)

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
		line, done, err := readNormalizedLine(inputFileReader)
		if err != nil {
			return fmt.Errorf("read line %d from input file: %w", lineCount+1, err)
		}
		if done {
			break
		}

		lineCount++

		// write the line to the input file.
		if _, err := writer.Write(line); err != nil {
			return fmt.Errorf("write line %d to input file %q: %w", lineCount, localInputFilePath, err)
		}

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
				offset += int64(len(line))
				continue
			}
		}

		nextOffset := accumulatePlanEntry(
			acc, requestMeta.ModelID, modelToSafe, used, offset, uint32(len(line)), requestMeta.PrefixHash,
		)
		offset = nextOffset
	}

	// flush input.jsonl file
	if err := writer.Flush(); err != nil {
		return fmt.Errorf("flush input file %q: %w", localInputFilePath, err)
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
		"inputFilePath", localInputFilePath, "planFilePath", acc.plansDir(),
		"lineCount", lineCount, "rejected", rejectedCount, "models", modelCounts)

	return nil
}

// readNormalizedLine reads the next line from the reader, ensuring it ends with '\n'.
// Returns (line, eof, err): line is the normalized bytes, eof is true when input is exhausted.
func readNormalizedLine(r *bufio.Reader) ([]byte, bool, error) {
	line, err := r.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, false, err
	}
	if len(line) == 0 && err == io.EOF {
		return nil, true, nil
	}
	// if last line is not terminated with '\n', append '\n' to the line
	if line[len(line)-1] != '\n' {
		line = append(line, '\n')
	}
	return line, false, nil
}

type requestMeta struct {
	CustomID   string
	ModelID    string
	PrefixHash uint32
}

// extractAndValidateLine parses and validates a request line and returns the
// metadata needed during ingestion.
func extractAndValidateLine(line []byte) (requestMeta, error) {
	var req planRequestLine
	trimmedLine := bytes.TrimSuffix(line, []byte{'\n'})
	if err := json.Unmarshal(trimmedLine, &req); err != nil {
		return requestMeta{}, err
	}
	if req.CustomID == "" {
		return requestMeta{}, fmt.Errorf("custom_id is required")
	}
	if req.Method == "" {
		return requestMeta{}, fmt.Errorf("method is required")
	}
	if req.Method != "POST" {
		return requestMeta{}, fmt.Errorf("invalid method: %s", req.Method)
	}
	if req.URL == "" {
		return requestMeta{}, fmt.Errorf("url is required")
	}
	if !strings.HasPrefix(req.URL, "/") || strings.HasPrefix(req.URL, "//") || strings.Contains(req.URL, "://") {
		return requestMeta{}, fmt.Errorf("url must be a relative path: %s", req.URL)
	}
	if !openai.IsValidEndpoint(req.URL) {
		return requestMeta{}, fmt.Errorf("invalid endpoint: %s", req.URL)
	}
	if req.Body.Model == "" {
		return requestMeta{}, fmt.Errorf("model id is empty")
	}
	if req.Body.Stream != nil && *req.Body.Stream {
		return requestMeta{}, fmt.Errorf("streaming is not supported in batch requests (model: %s)", req.Body.Model)
	}

	prefixHash := NoPrefixHash
	for _, msg := range req.Body.Messages {
		if msg.Role != "system" {
			continue
		}
		text, err := messageText(msg.Content)
		if err != nil {
			return requestMeta{}, fmt.Errorf("system message content: %w", err)
		}
		if text != "" {
			h := fnv.New32a()
			h.Write([]byte(text))
			prefixHash = h.Sum32()
			break
		}
	}

	return requestMeta{
		CustomID:   req.CustomID,
		ModelID:    req.Body.Model,
		PrefixHash: prefixHash,
	}, nil
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
	}
	return writeModelMapFile(jobRootDir, modelMap)
}
