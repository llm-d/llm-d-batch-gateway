package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"

	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

func (p *Processor) executeJobAsync(ctx context.Context, params *jobExecutionParams) (*openai.BatchRequestCounts, error) {
	logger := logr.FromContextOrDiscard(ctx)
	logger.V(logging.INFO).Info("Starting execution (v2 pipeline)")

	jobRootDir, err := p.jobRootDir(params.jobInfo.JobID, params.jobInfo.TenantID)
	if err != nil {
		return nil, fmt.Errorf("resolve job root directory: %w", err)
	}

	modelMap, err := readModelMap(jobRootDir)
	if err != nil {
		return nil, fmt.Errorf("read model map: %w", err)
	}

	files, err := p.openDataFiles(params)
	if err != nil {
		return nil, err
	}
	defer files.close()

	plansDir, err := p.jobPlansDir(params.jobInfo.JobID, params.jobInfo.TenantID)
	if err != nil {
		return nil, err
	}

	// A zero SLODeadline means "no SLO"; the source treats it accordingly.
	var sloDeadline time.Time
	if params.task != nil {
		sloDeadline = params.task.SLO
	}

	var (
		inputRef  *inputFileRef
		inputFile *os.File
	)
	if params.jobInfo != nil && params.jobInfo.BatchJob != nil && params.jobInfo.BatchJob.InputFileID != "" {
		ref, err := p.resolveInputFileCoords(ctx, params.jobInfo.BatchJob.InputFileID)
		if err != nil {
			return nil, fmt.Errorf("resolve input file coordinates: %w", err)
		}
		inputRef = ref
	} else {
		localInputPath := filepath.Join(jobRootDir, "input.jsonl")
		if f, err := os.Open(localInputPath); err == nil {
			inputFile = f
			defer inputFile.Close()
		}
	}

	var storage filesapi.BatchFilesClient
	if p.files != nil {
		storage = p.files.storage
	}

	// Setup pipeline.

	tracker := pipeline.NewProgressTracker(
		modelMap.LineCount,
		params.updater,
		params.jobInfo.JobID,
		time.Second,
		logger,
	)
	tracker.AddFailed(modelMap.RejectedCount)

	source := NewPlanFileSource(PlanFileSourceConfig{
		Storage:            storage,
		InputRef:           inputRef,
		InputFile:          inputFile,
		PlansDir:           plansDir,
		ModelMap:           modelMap,
		Resolver:           p.inference,
		Cfg:                p.cfg,
		PassThroughHeaders: params.jobInfo.PassThroughHeaders,
		SLODeadline:        sloDeadline,
		TenantID:           params.jobInfo.TenantID,
		Logger:             logger,
	})

	// The dispatcher forwards requests for processing.
	pending := pipeline.NewPendingRequests(modelMap.LineCount)
	dispatcher, err := p.buildRequestDispatcher(modelMap, pending, params.jobInfo.TenantID, logger)
	if err != nil {
		return nil, fmt.Errorf("build dispatcher: %w", err)
	}

	// Collects the result and logs them.
	resultCollector := pipeline.NewResultCollector(
		files.output,
		files.error,
		pending,
		tracker,
		logger,
	)

	// Orchestrates Job execution.
	executor := pipeline.NewJobExecutor(pipeline.JobExecutorConfig{
		Source:     source,
		Dispatcher: dispatcher,
		Collector:  resultCollector,
		Tracker:    tracker,
		Logger:     logger,
	})

	// Finally, start and wait for completion.
	counts, execErr := executor.Execute(ctx)

	return counts, classifyOutcome(batchctx.Cause(ctx), counts, execErr)
}

// classifyOutcome maps the abort cause, request counts, and executor error to the
// job's terminal error (nil = complete). Expiry and user cancel are terminal
// regardless of progress; a shutdown that lands after every request already
// succeeded is ignored so the job still finalizes. Any other stop surfaces the
// executor's own error (nil on the happy path).
func classifyOutcome(cause error, counts *openai.BatchRequestCounts, execErr error) error {
	switch {
	case errors.Is(cause, batchctx.ErrExpired), errors.Is(cause, batchctx.ErrCancelled):
		return cause
	case errors.Is(cause, batchctx.ErrShutdown):
		if counts.AllSucceeded() {
			return nil // finished before shutdown landed — let the job finalize
		}
		return cause
	}
	// No terminal cause: surface the executor's error, if any (e.g. a persistence
	// failure that arrived after every request was recorded as completed).
	return execErr
}

func (p *Processor) buildRequestDispatcher(modelMap *modelMapFile, pending *pipeline.PendingRequests, tenantID string, logger logr.Logger) (pipeline.RequestDispatcher, error) {
	switch {
	case p.asyncInference != nil:
		broadcasters := p.broadcasters.forModels(modelMap)
		async := pipeline.NewAsyncDispatcher(p.asyncInference, broadcasters, pending, logger)
		return pipeline.NewPreDispatcher(async), nil
	case p.cfg.Concurrency.AIMD.Enabled:
		models := buildAIMDModels(modelMap, p.inference, p.endpointLimits, p.cfg.RouteKeyMethod, tenantID)
		direct := pipeline.NewDirectDispatcher(p.inference, logger)
		aimd, err := pipeline.NewAIMDDispatcher(direct, models, p.cfg.Concurrency.Global, logger)
		if err != nil {
			return nil, err
		}
		return pipeline.NewPreDispatcher(aimd), nil
	default:
		// AIMD is used even when adaptive limits are disabled: with AIMD.Enabled=false,
		// EndpointAIMD.AIMD is nil so recordAIMDSignal is a no-op, but the semaphores
		// still enforce fixed concurrency limits (global + per-endpoint). Without this,
		// DirectDispatcher would dispatch all requests as unbounded goroutines.
		models := buildAIMDModels(modelMap, p.inference, p.endpointLimits, p.cfg.RouteKeyMethod, tenantID)
		direct := pipeline.NewDirectDispatcher(p.inference, logger)
		aimd, err := pipeline.NewAIMDDispatcher(direct, models, p.cfg.Concurrency.Global, logger)
		if err != nil {
			return nil, err
		}
		return pipeline.NewPreDispatcher(aimd), nil
	}
}

type dataFiles struct {
	output, error *os.File
}

func (f *dataFiles) close() {
	for _, file := range []*os.File{f.output, f.error} {
		if file != nil {
			_ = file.Close()
		}
	}
}

func (p *Processor) openDataFiles(params *jobExecutionParams) (*dataFiles, error) {
	jobID := params.jobInfo.JobID
	tenantID := params.jobInfo.TenantID

	outputPath, err := p.jobOutputFilePath(jobID, tenantID)
	if err != nil {
		return nil, err
	}
	outputFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create output file: %w", err)
	}

	errorPath, err := p.jobErrorFilePath(jobID, tenantID)
	if err != nil {
		outputFile.Close()
		return nil, err
	}
	errorFile, err := os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		outputFile.Close()
		return nil, fmt.Errorf("create error file: %w", err)
	}

	return &dataFiles{output: outputFile, error: errorFile}, nil
}

// buildAIMDModels keys per-endpoint concurrency state by route key: dispatch
// items carry the route key as their ModelID, so the lookup key, the resolver
// key and the map key must all be derived the same way. Keying by the bare
// model ID would leave the map empty under tenant-scoped routing and silently
// disable per-endpoint limiting and AIMD backoff.
func buildAIMDModels(modelMap *modelMapFile, resolver *inference.GatewayResolver, endpointLimits map[inference.InferenceClient]*endpointLimit, method config.RouteKeyMethod, tenantID string) map[string]*pipeline.EndpointAIMD {
	models := make(map[string]*pipeline.EndpointAIMD)
	for _, modelID := range modelMap.SafeToModel {
		key := routeKey(method, tenantID, modelID)
		client := resolver.ClientFor(key)
		if client == nil {
			continue
		}
		ep := endpointLimits[client]
		if ep == nil {
			continue
		}
		models[key] = &pipeline.EndpointAIMD{
			Sem:   ep.sem,
			AIMD:  ep.aimd,
			Label: ep.label,
		}
	}
	return models
}
