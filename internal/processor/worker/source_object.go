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
	"fmt"
	"maps"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"

	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batchinput"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

// objectSourceReadBuffer is the read buffer bufio keeps over the chunked
// reader, matching the one ingestion uses. bufio grows past it for a longer
// line; the hard per-line limit is enforced when the input is scanned.
const objectSourceReadBuffer = 1 << 20

// ObjectSource produces RequestItems by streaming a stored input object that
// is already in dispatch order.
//
// It exists because the API server orders batch inputs before storing them: if
// the bytes are already in the order requests should be submitted, reading the
// object front to back is all the planning that is needed. No plan files, no
// model map, and no per-request range read.
type ObjectSource struct {
	storage            filesapi.BatchFilesClient
	ref                *inputFileRef
	size               int64
	lineCount          int64
	policy             batchinput.PolicyID
	cfg                *config.ProcessorConfig
	passThroughHeaders map[string]string
	sloDeadline        time.Time
	tenantID           string
	logger             logr.Logger

	// readTuning is exposed for tests; zero values take the package defaults.
	chunkSize     int64
	prefetchBytes int64
}

var _ pipeline.RequestSource = (*ObjectSource)(nil)

// ObjectSourceConfig configures an ObjectSource.
type ObjectSourceConfig struct {
	Storage            filesapi.BatchFilesClient
	InputRef           *inputFileRef
	Size               int64
	LineCount          int64
	Policy             batchinput.PolicyID
	Cfg                *config.ProcessorConfig
	PassThroughHeaders map[string]string
	SLODeadline        time.Time
	TenantID           string
	Logger             logr.Logger

	ChunkSize     int64
	PrefetchBytes int64
}

func NewObjectSource(cfg ObjectSourceConfig) *ObjectSource {
	return &ObjectSource{
		storage:            cfg.Storage,
		ref:                cfg.InputRef,
		size:               cfg.Size,
		lineCount:          cfg.LineCount,
		policy:             cfg.Policy,
		cfg:                cfg.Cfg,
		passThroughHeaders: cfg.PassThroughHeaders,
		sloDeadline:        cfg.SLODeadline,
		tenantID:           cfg.TenantID,
		logger:             cfg.Logger,
		chunkSize:          cfg.ChunkSize,
		prefetchBytes:      cfg.PrefetchBytes,
	}
}

// Produce streams the stored object and emits one item per request line, in
// stored order.
//
// The read deliberately does not use the dispatch context. Every request has
// to reach the dispatcher even after the batch is cancelled or expires,
// because the drain path needs each original custom_id to write an accurate
// error line; dropping lines here would leave output_lines + error_lines short
// of total_requests. Cancellation is instead handled downstream, where the
// dispatcher turns the remaining items into batch_cancelled/batch_expired
// results.
func (s *ObjectSource) Produce(_ context.Context, outgoingRequestCh chan<- pipeline.RequestItem) error {
	defer close(outgoingRequestCh)

	if s.storage == nil || s.ref == nil {
		return fmt.Errorf("%w: no storage or input reference provided", errRequestInputRead)
	}

	// Detached from the dispatch context so that enumeration survives a cancel
	// or expiry, and deliberately given no overall deadline.
	//
	// Prefetch is bounded, so ranged reads are issued as the dispatcher
	// consumes them: an overall deadline here would really be a deadline on
	// submitting the whole batch, and a slow inference queue would trip it and
	// truncate the request set — the exact accounting failure this source is
	// written to avoid. A wedged storage backend is caught by the per-read
	// timeout inside the reader instead, and an aborted batch drains quickly
	// because the dispatcher keeps consuming to turn the rest into error
	// results.
	readCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	reader := newObjectReader(readCtx, objectReaderConfig{
		Storage:       s.storage,
		StorageName:   s.ref.storageName,
		FolderName:    s.ref.folderName,
		Size:          s.size,
		ChunkSize:     s.chunkSize,
		PrefetchBytes: s.prefetchBytes,
	})
	defer func() { _ = reader.Close() }()

	// bufio owns line splitting, so a request that straddles a chunk boundary
	// needs no handling here.
	buffered := bufio.NewReaderSize(reader, objectSourceReadBuffer)

	var emitted int64
	for {
		line, _, done, err := readNormalizedLine(buffered)
		if err != nil {
			return fmt.Errorf("%w after %d requests: %w", errRequestInputRead, emitted, err)
		}
		if done {
			break
		}

		outgoingRequestCh <- *s.itemFor(line)
		emitted++
	}

	if s.lineCount > 0 && emitted != s.lineCount {
		return fmt.Errorf("%w: stored object yielded %d requests, expected %d",
			errRequestInputRead, emitted, s.lineCount)
	}

	s.logger.V(logging.INFO).Info("Streamed input object",
		"policy", s.policy, "requests", emitted, "rangeReads", reader.Chunks())
	return nil
}

// itemFor turns one stored line into a dispatchable request. A line that fails
// to parse becomes a parse-error item rather than failing the job: the object
// was validated at upload time, so reaching this branch means it was corrupted
// afterwards and the remaining requests should still run.
//
// Unlike the plan-file source, the model comes from the line itself rather
// than from a per-model plan, because a sequential object carries no grouping
// metadata alongside it. A line with no usable model resolves to no client and
// is reported as model_not_found by the dispatcher, exactly as an unregistered
// model is.
func (s *ObjectSource) itemFor(line []byte) *pipeline.RequestItem {
	req, err := decodeRequestLine(line)
	if err != nil {
		s.logger.Error(err, "Failed to parse request line, recording as error")
		reqID := newBatchRequestID(uuid.NewString())
		return &pipeline.RequestItem{
			RequestID: reqID,
			CustomID:  reqID,
			ParseError: &pipeline.OutputError{
				Code:    "parse_error",
				Message: fmt.Sprintf("failed to parse request line: %v", err),
			},
		}
	}

	modelID, _ := req.Body["model"].(string)

	// When route_key_method is "tenant", scope the gateway lookup key by
	// tenant so identically-named models of different tenants route to their
	// own backends. The request body is forwarded verbatim.
	lookupID := routeKey(s.cfg.RouteKeyMethod, s.tenantID, modelID)

	return &pipeline.RequestItem{
		RequestID: newBatchRequestID(uuid.NewString()),
		CustomID:  req.CustomID,
		ModelID:   lookupID,
		ModelName: modelID,
		Endpoint:  req.URL,
		Body:      req.Body,
		Headers: mergeDispatchHeaders(
			maps.Clone(s.passThroughHeaders), s.cfg, lookupID, s.tenantID, s.sloDeadline),
	}
}
