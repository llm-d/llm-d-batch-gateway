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
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/attribute"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
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
// It also accumulates a requestManifest: the per-line metadata (stable
// request IDs, rejected-line error objects) that resumable activation
// persists as batch entries.
func (p *Processor) preProcessJob(ctx context.Context, jobInfo *batch_types.JobInfo) (manifest *requestManifest, err error) {
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
		return nil, fmt.Errorf("input file ID is empty")
	}

	jobRootDir, err := p.jobRootDir(jobID, jobInfo.TenantID)
	if err != nil {
		return nil, fmt.Errorf("resolve job root directory: %w", err)
	}

	// job directory creation
	if err := os.MkdirAll(jobRootDir, 0o700); err != nil {
		return nil, fmt.Errorf("create job root directory %q: %w", jobRootDir, err)
	}

	// input file stream open
	reader, metadata, err := p.openInputFileStream(ctx, inputFileID)
	if err != nil {
		return nil, fmt.Errorf("open input file stream %q: %w", inputFileID, err)
	}
	defer reader.Close()

	if metadata != nil {
		logger.V(logging.INFO).Info("Input file metadata", "metadata", metadata)
	}

	// create local input file
	localInputFile, localInputFilePath, err := p.createLocalInputFile(jobID, jobInfo.TenantID)
	if err != nil {
		return nil, fmt.Errorf("create local input file: %w", err)
	}
	defer localInputFile.Close()

	writer := bufio.NewWriterSize(localInputFile, 1024*1024)

	acc := newPlanAccumulator(jobRootDir)

	manifest = &requestManifest{}

	// model intern tables
	used := make(map[string]int)           // to prevent duplicate model IDs
	modelToSafe := make(map[string]string) // to map the model ID to a safe file name

	seenCustomIDs := make(map[string]struct{})
	dispatchDeadline := requestDeadline(ctx, jobInfo)

	// streaming loop
	// Reject unregistered models at ingestion. Per-model sync gateways resolve
	// by route key; async pools resolve by the bare model ID — the same key
	// their dispatch paths use, so the error-metric label stays consistent.
	// In global mode, all models are routed to the same endpoint so no check
	// is needed. The nil-resolver guards keep unit-test constructions that
	// omit clients on the no-check path; production paths hit
	// Processor.validate() in prepare() before work runs.
	var lookupModel func(modelID string) (key string, registered bool)
	registeredModels := make(map[string]bool) // route key -> registered (per-model sync only)
	switch {
	case p.inference != nil && !p.inference.IsGlobal():
		lookupModel = func(modelID string) (string, bool) {
			// Look up the gateway by the route key (tenant-scoped when
			// route_key_method is "tenant"). The raw model ID stays in the
			// error message and plan grouping below.
			key := routeKey(p.cfg.RouteKeyMethod, jobInfo.TenantID, modelID)
			if registered, checked := registeredModels[key]; checked {
				return key, registered
			}
			registered := p.inference.ClientFor(key) != nil
			registeredModels[key] = registered
			return key, registered
		}
	case p.asyncInference != nil:
		lookupModel = func(modelID string) (string, bool) {
			return modelID, p.asyncInference.SharedClientFor(modelID) != nil
		}
	}

	var offset int64
	var lineCount int64 // to count the number of lines in the input file for logging
	var rejectedCount int64
	inputFileReader := bufio.NewReaderSize(reader, 1024*1024)

	for {
		// The abort context records why it stopped (SLO / user cancel / SIGTERM);
		// batchctx.Cause maps that to the terminal routing sentinel. First-cause
		// wins, so a user cancel racing SIGTERM is honoured by whichever fired first.
		if s := batchctx.Cause(ctx); s != nil {
			return nil, s
		}

		// read a line from the input file
		line, done, err := readNormalizedLine(inputFileReader)
		if err != nil {
			return nil, fmt.Errorf("read line %d from input file: %w", lineCount+1, err)
		}
		if done {
			break
		}

		lineCount++

		// write the line to the input file.
		if _, err := writer.Write(line); err != nil {
			return nil, fmt.Errorf("write line %d to input file %q: %w", lineCount, localInputFilePath, err)
		}

		requestMeta, err := p.validateLineForDispatch(line, jobInfo.BatchJob.Endpoint.String(), dispatchDeadline)
		if err != nil {
			return nil, fmt.Errorf("validate request at line %d: %w", lineCount, err)
		}

		if _, exists := seenCustomIDs[requestMeta.CustomID]; exists {
			return nil, fmt.Errorf("line %d: duplicate custom_id %q", lineCount, requestMeta.CustomID)
		}
		seenCustomIDs[requestMeta.CustomID] = struct{}{}

		if lookupModel != nil {
			lookupID, registered := lookupModel(requestMeta.ModelID)
			if !registered {
				// No plan entry exists yet, so derive the request ID from the
				// batch and custom IDs. The ID is stable, so re-preprocessing
				// the same input yields the same error object.
				requestID := stableBatchRequestID(jobID, requestMeta.CustomID)
				errLine := &outputLine{
					ID:       requestID,
					CustomID: requestMeta.CustomID,
					Error: &outputError{
						Code:    inference.ErrCodeModelNotFound,
						Message: fmt.Sprintf("model %q is not configured in any gateway", requestMeta.ModelID),
					},
				}
				lineBytes, marshalErr := json.Marshal(errLine)
				if marshalErr != nil {
					return nil, fmt.Errorf("failed to marshal model_not_found error: %w", marshalErr)
				}
				lineBytes = append(lineBytes, '\n')
				rejectedCount++
				manifest.add(rejectedEntry(jobID, lineCount-1, requestMeta.CustomID, requestID,
					inference.ErrCodeModelNotFound, lineBytes))
				// Error metric rides the lookup key so labels stay consistent
				// with the dispatch path.
				metrics.RecordRequestError(lookupID)
				logger.V(logging.DEBUG).Info("Rejected request for unregistered model",
					"customId", requestMeta.CustomID, "model", requestMeta.ModelID)
				offset += int64(len(line))
				continue
			}
		}

		// This runs after the registration check on purpose. An unknown model
		// has no provider, and must get its own model_not_found result instead
		// of failing the whole batch.
		if requestMeta.usesGCSURI && p.cfg.AsyncProviderFor(requestMeta.ModelID) == "" {
			return nil, fmt.Errorf("validate request at line %d: model %q has no async provider metadata", lineCount, requestMeta.ModelID)
		}

		manifest.add(manifestEntry{
			Ordinal:   lineCount - 1,
			CustomID:  requestMeta.CustomID,
			RequestID: stableBatchRequestID(jobID, requestMeta.CustomID),
		})

		nextOffset := accumulatePlanEntry(
			acc, requestMeta.ModelID, modelToSafe, used, offset, uint32(len(line)), requestMeta.PrefixHash,
		)
		offset = nextOffset
	}

	// flush input.jsonl file
	if err := writer.Flush(); err != nil {
		return nil, fmt.Errorf("flush input file %q: %w", localInputFilePath, err)
	}

	if err := finalizePlanFiles(acc, modelToSafe); err != nil {
		return nil, fmt.Errorf("finalize plan files: %w", err)
	}

	// model map file writing
	if err := writeModelMappings(jobRootDir, modelToSafe, lineCount, rejectedCount); err != nil {
		return nil, fmt.Errorf("write model map: %w", err)
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

	return manifest, nil
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
	CustomID     string
	ModelID      string
	PrefixHash   uint32
	mayHaveMedia bool
	usesGCSURI   bool
}

// extractAndValidateLine parses and validates a request line and returns the
// metadata needed during ingestion.
func extractAndValidateLine(line []byte, endpointAllowlist openai.EndpointAllowlist, batchEndpoint string) (requestMeta, error) {
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
	if !endpointAllowlist.IsValid(req.URL) {
		return requestMeta{}, fmt.Errorf("invalid endpoint: %s", req.URL)
	}
	if req.URL != batchEndpoint {
		return requestMeta{}, fmt.Errorf("request endpoint %q does not match batch endpoint %q", req.URL, batchEndpoint)
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
		CustomID:     req.CustomID,
		ModelID:      req.Body.Model,
		PrefixHash:   prefixHash,
		mayHaveMedia: len(req.Body.GCSURI) > 0 || len(req.Body.File) > 0,
	}, nil
}

func requestDeadline(ctx context.Context, jobInfo *batch_types.JobInfo) time.Time {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline
	}
	if jobInfo != nil && jobInfo.BatchJob != nil && jobInfo.BatchJob.ExpiresAt != nil {
		return time.Unix(*jobInfo.BatchJob.ExpiresAt, 0)
	}
	return time.Now().Add(inference.DefaultAsyncRequestDeadline)
}

func (p *Processor) validateLineForDispatch(line []byte, batchEndpoint string, deadline time.Time) (requestMeta, error) {
	meta, err := extractAndValidateLine(line, p.endpointAllowlist, batchEndpoint)
	if err != nil {
		return requestMeta{}, err
	}

	// Check the entries that will be forwarded, not the ones the generic parse
	// matched. A nil map is fine: every lookup below then finds nothing.
	var body map[string]any
	if meta.mayHaveMedia {
		body, err = forwardedBody(line)
		if err != nil {
			return requestMeta{}, err
		}
	}
	gcsURI, hasGCSURI := body["gcs_uri"]
	_, hasFileKey := body["file"]
	fileBase64, _ := body["file_base64"].(string)

	if isAudioEndpoint(batchEndpoint) {
		if format, ok := body["response_format"].(string); ok && isPlainTextAudioFormat(format) {
			return requestMeta{}, fmt.Errorf("response_format %q is not supported: batch results must be JSON, use json or verbose_json", format)
		}
	}

	if hasGCSURI {
		if hasFileKey || fileBase64 != "" {
			return requestMeta{}, fmt.Errorf("body.gcs_uri is mutually exclusive with body.file and body.file_base64")
		}
		if !isAudioEndpoint(batchEndpoint) {
			return requestMeta{}, fmt.Errorf("gcs_uri is supported only on audio endpoints")
		}
		if p.cfg == nil || !p.cfg.IsAsync() {
			return requestMeta{}, fmt.Errorf("gcs_uri requires dispatch_mode async")
		}
		signedURL, ok := gcsURI.(string)
		if !ok {
			return requestMeta{}, fmt.Errorf("gcs_uri must be a string")
		}
		if err := validateGCSURI(signedURL, p.cfg.AsyncDispatchConfig.GCSSignedURLHosts, deadline); err != nil {
			return requestMeta{}, err
		}
		meta.usesGCSURI = true
		return meta, nil
	}
	if isAudioEndpoint(batchEndpoint) {
		return requestMeta{}, fmt.Errorf("audio requests require body.gcs_uri; inline body.file media is not supported")
	}
	return meta, nil
}

// forwardedBody decodes the request body the way PlanFileSource.readEntry does.
// Keys are kept exactly as written, so "gcs_uri" and "GCS_URI" are two entries.
// The struct parse in extractAndValidateLine cannot be used here: it matches
// keys in any letter case and would validate a different entry than the one sent.
func forwardedBody(line []byte) (map[string]any, error) {
	var req batch_types.Request
	if err := json.Unmarshal(bytes.TrimSuffix(line, []byte{'\n'}), &req); err != nil {
		return nil, err
	}
	return req.Body, nil
}

func isAudioEndpoint(endpoint string) bool {
	return endpoint == openai.EndpointAudioTranscriptions.String() ||
		endpoint == openai.EndpointAudioTranslations.String()
}

// isPlainTextAudioFormat reports whether an audio response_format returns plain
// text instead of JSON. The result pipeline only stores JSON bodies, so these
// responses would be lost.
func isPlainTextAudioFormat(format string) bool {
	switch format {
	case "text", "srt", "vtt":
		return true
	}
	return false
}

func validateGCSURI(signedURL string, hosts []string, deadline time.Time) error {
	parsed, err := url.Parse(signedURL)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil ||
		parsed.Path == "" || parsed.Path == "/" || parsed.RawQuery == "" || parsed.Fragment != "" {
		return fmt.Errorf("gcs_uri must be an HTTPS URL for a GCS object")
	}
	if !hasGCSBucketAndObject(parsed.Host, parsed.Path) {
		return fmt.Errorf("gcs_uri must identify a GCS bucket and object")
	}
	if !allowedGCSHost(parsed.Host, hosts) {
		return fmt.Errorf("gcs_uri host is not allowed")
	}

	// Only V4 signed URLs are accepted. The legacy "Signature" form is rejected.
	query := parsed.Query()
	signature := query.Get("X-Goog-Signature")
	if signature == "" {
		return fmt.Errorf("gcs_uri is missing a signature")
	}
	if query.Get("X-Goog-Date") == "" || query.Get("X-Goog-Expires") == "" {
		return fmt.Errorf("gcs_uri is missing required V4 expiry fields")
	}
	switch query.Get("X-Goog-Algorithm") {
	case "GOOG4-RSA-SHA256":
		if !isHexSignature(signature) || len(signature)%2 != 0 {
			return fmt.Errorf("gcs_uri has an invalid signature")
		}
	case "GOOG4-HMAC-SHA256":
		// An HMAC-SHA256 signature is 32 bytes, hex-encoded.
		if !isHexSignature(signature) || len(signature) != 64 {
			return fmt.Errorf("gcs_uri has an invalid signature")
		}
	default:
		return fmt.Errorf("gcs_uri has an invalid signature")
	}
	if query.Get("X-Goog-Credential") == "" || query.Get("X-Goog-SignedHeaders") == "" {
		return fmt.Errorf("gcs_uri has an invalid signature")
	}

	expiry, err := signedGCSURLExpiry(query)
	if err != nil {
		return fmt.Errorf("gcs_uri has an invalid expiry")
	}
	if !expiry.After(deadline) {
		return fmt.Errorf("gcs_uri expires at or before the request deadline")
	}
	return nil
}

// hasGCSBucketAndObject accepts path-style and virtual-hosted GCS object URLs.
// Path-style URLs split only once after the bucket, because an object name can
// start with "/".
func hasGCSBucketAndObject(host, path string) bool {
	objectPath := strings.TrimPrefix(path, "/")
	if strings.HasSuffix(strings.ToLower(host), ".storage.googleapis.com") {
		return objectPath != ""
	}
	bucket, object, found := strings.Cut(objectPath, "/")
	return found && bucket != "" && object != ""
}

func allowedGCSHost(host string, hosts []string) bool {
	if len(hosts) == 0 {
		hosts = []string{config.DefaultGCSSignedURLHost}
	}
	for _, allowed := range hosts {
		if strings.EqualFold(host, allowed) {
			return true
		}
	}
	return false
}

func isHexSignature(signature string) bool {
	if signature == "" {
		return false
	}
	for _, c := range signature {
		isHex := '0' <= c && c <= '9' ||
			'a' <= c && c <= 'f' ||
			'A' <= c && c <= 'F'
		if !isHex {
			return false
		}
	}
	return true
}

// maxGCSV4ExpiresSeconds is the longest X-Goog-Expires value GCS accepts for a
// V4 signed URL (seven days).
const maxGCSV4ExpiresSeconds = 604800

// signedGCSURLExpiry returns the expiry time of a V4 signed URL: X-Goog-Date
// plus X-Goog-Expires seconds. The caller checks both fields are present.
func signedGCSURLExpiry(query url.Values) (time.Time, error) {
	start, err := time.Parse("20060102T150405Z", query.Get("X-Goog-Date"))
	if err != nil {
		return time.Time{}, err
	}
	// The upper bound is checked before the multiplication below, so a huge
	// value cannot overflow time.Duration.
	seconds, err := strconv.Atoi(query.Get("X-Goog-Expires"))
	if err != nil || seconds <= 0 || seconds > maxGCSV4ExpiresSeconds {
		return time.Time{}, fmt.Errorf("invalid expiry duration")
	}
	return start.Add(time.Duration(seconds) * time.Second), nil
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
