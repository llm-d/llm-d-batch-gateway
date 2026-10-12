package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

// epochProgressUpdater fences progress writes by an explicit epoch.
// *StatusUpdater satisfies it.
type epochProgressUpdater interface {
	UpdateProgressCounts(ctx context.Context, jobID string, epoch int64, counts *openai.BatchRequestCounts) error
}

// jobProgressUpdater scopes a shared updater to one job's epoch so every
// progress write for that job is fenced by its ownership epoch.
type jobProgressUpdater struct {
	inner epochProgressUpdater
	jobID string
	epoch int64
	// onFencedOut, when non-nil, is invoked when a progress write is fenced out
	// by an epoch bump (this processor lost ownership). runJob uses it to abort
	// without a terminal write.
	onFencedOut func()
}

func (u jobProgressUpdater) UpdateProgressCounts(ctx context.Context, jobID string, counts *openai.BatchRequestCounts) error {
	err := u.inner.UpdateProgressCounts(ctx, u.jobID, u.epoch, counts)
	if err == nil {
		return nil
	}
	if errors.Is(err, db.ErrConflict) {
		// The write was fenced out by an epoch bump: a reclaimer took ownership.
		// Signal runJob to abort (neutral cause) without a terminal write. Return
		// nil so the progress tracker does not log a spurious update failure.
		if u.onFencedOut != nil {
			u.onFencedOut()
		}
		return nil
	}
	return err
}

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

	// Setup pipeline.
	progressInterval := p.cfg.ProgressUpdateInterval
	if progressInterval <= 0 {
		progressInterval = 15 * time.Second
	}

	// jobItem is always set on the production path (the polling loop skips
	// jobs with no DB item); the guard only covers test constructions.
	jobEpoch := int64(0)
	if params.jobItem != nil {
		jobEpoch = params.jobItem.Epoch
	}

	tracker := pipeline.NewProgressTracker(
		modelMap.LineCount,
		jobProgressUpdater{inner: params.updater, jobID: params.jobInfo.JobID, epoch: jobEpoch, onFencedOut: params.onOwnershipLost},
		params.jobInfo.JobID,
		progressInterval,
		logger,
	)
	tracker.AddFailed(modelMap.RejectedCount)

	source := NewPlanFileSource(PlanFileSourceConfig{
		InputFile:          files.input,
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
	input, output, error *os.File
}

func (f *dataFiles) close() {
	for _, file := range []*os.File{f.input, f.output, f.error} {
		if file != nil {
			_ = file.Close()
		}
	}
}

func (p *Processor) openDataFiles(params *jobExecutionParams) (*dataFiles, error) {
	jobID := params.jobInfo.JobID
	tenantID := params.jobInfo.TenantID

	inputPath, err := p.jobInputFilePath(jobID, tenantID)
	if err != nil {
		return nil, err
	}
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("open input file: %w", err)
	}

	outputPath, err := p.jobOutputFilePath(jobID, tenantID)
	if err != nil {
		inputFile.Close()
		return nil, err
	}
	outputFile, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		inputFile.Close()
		return nil, fmt.Errorf("create output file: %w", err)
	}

	errorPath, err := p.jobErrorFilePath(jobID, tenantID)
	if err != nil {
		inputFile.Close()
		outputFile.Close()
		return nil, err
	}
	errorFile, err := os.OpenFile(errorPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		inputFile.Close()
		outputFile.Close()
		return nil, fmt.Errorf("create error file: %w", err)
	}

	return &dataFiles{input: inputFile, output: outputFile, error: errorFile}, nil
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
