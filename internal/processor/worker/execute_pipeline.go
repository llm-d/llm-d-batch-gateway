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

// executeJob runs the unified dispatch pipeline: it streams the plan-file
// source through the dispatcher (sync or async, chosen by buildRequestDispatcher)
// and checkpoints every result through the sink.
func (p *Processor) executeJob(ctx context.Context, params *jobExecutionParams) (*openai.BatchRequestCounts, error) {
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

	inputFile, err := p.openInputFile(params)
	if err != nil {
		return nil, err
	}
	defer inputFile.Close()

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
	if params.resumeSeed != nil {
		// A resumed batch starts from its checkpointed counts. Terminal
		// entries include the activation-rejected ones, which were seeded as
		// failed at activation — RejectedCount must not be added again.
		tracker.AddCompleted(params.resumeSeed.Completed)
		tracker.AddFailed(params.resumeSeed.Failed)
	} else {
		tracker.AddFailed(modelMap.RejectedCount)
	}

	sourceCfg := PlanFileSourceConfig{
		InputFile:          inputFile,
		PlansDir:           plansDir,
		ModelMap:           modelMap,
		Resolver:           p.inference,
		Cfg:                p.cfg,
		PassThroughHeaders: params.jobInfo.PassThroughHeaders,
		SLODeadline:        sloDeadline,
		TenantID:           params.jobInfo.TenantID,
		Logger:             logger,
		// Stable request IDs so dispatched IDs match the activation entry
		// set across retries.
		BatchID: params.jobInfo.JobID,
	}
	if params.resumeSeed != nil {
		// Skip entries whose results a previous incarnation already
		// checkpointed; only unfinished work is re-dispatched.
		terminal := make(map[string]struct{}, len(params.resumeSeed.TerminalRequestIDs))
		for _, id := range params.resumeSeed.TerminalRequestIDs {
			terminal[id] = struct{}{}
		}
		sourceCfg.Skip = func(requestID string) bool {
			_, ok := terminal[requestID]
			return ok
		}
	}
	// The dispatcher forwards requests for processing. The pending set exists
	// before the source: recovering entries are pre-stored into it so their
	// stranded deliveries route to this job from the start.
	pending := pipeline.NewPendingRequests(modelMap.LineCount)

	// In-flight entries a previous owner submitted: pre-store them in the
	// pending set and defer their re-submission for the recovery window, so
	// their stranded delivery is recovered through the submitter's result
	// route instead of re-running the inference.
	recovering, submitters := recoveryPlan(params.resumeSeed, p.processorID, p.asyncInference != nil)
	if len(recovering) > 0 {
		sourceCfg.Recover = recovering
		sourceCfg.RecoveryWindow = p.cfg.Resumable.RecoveryWindow
		sourceCfg.PreStore = pending.Store
		sourceCfg.StillPending = pending.Has
	}
	source := NewPlanFileSource(sourceCfg)

	// Pre-store the recovering entries before the recovery broadcasters
	// start claiming the previous submitter's route: a delivery claimed
	// before its entry is in the pending set routes to no owner and bounces
	// on its claim lease instead of being checkpointed.
	if err := source.PreStoreRecovering(); err != nil {
		return nil, fmt.Errorf("pre-store recovering entries: %w", err)
	}

	// The dispatcher forwards requests for processing; the release function
	// drops the job's recovery-broadcaster references after execution.
	dispatcher, releaseBroadcasters, err := p.buildRequestDispatcher(ctx, modelMap, pending, params, logger, submitters)
	if err != nil {
		return nil, fmt.Errorf("build dispatcher: %w", err)
	}
	defer releaseBroadcasters()

	// Every result is checkpointed through the sink; finalization streams
	// the artifacts from the entry set.
	executorCfg := pipeline.JobExecutorConfig{
		Source:     source,
		Dispatcher: dispatcher,
		Tracker:    tracker,
		Collector:  pipeline.NewResultCheckpointer(params.resultSink, pending, tracker, logger),
		Logger:     logger,
	}

	// Orchestrates Job execution.
	executor := pipeline.NewJobExecutor(executorCfg)

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

func (p *Processor) buildRequestDispatcher(ctx context.Context, modelMap *modelMapFile, pending *pipeline.PendingRequests, params *jobExecutionParams, logger logr.Logger, submitters []string) (pipeline.RequestDispatcher, func(), error) {
	// Fence each request into in_flight before dispatch so a zombie
	// incarnation stops at its next submit instead of streaming the
	// remaining requests into inference.
	fence := p.markEntryInFlightHook(ctx, params)

	if p.asyncInference != nil {
		// Besides this processor's own per-model routes, the group consumes
		// each previous submitter's route so in-flight entries' stranded
		// deliveries are recovered (refcounted; released after execution).
		broadcasters, release := p.broadcasters.forModels(modelMap, submitters)
		async := pipeline.NewAsyncDispatcher(p.asyncInference, broadcasters, pending, logger)
		return pipeline.NewPreDispatcher(async).WithBeforeSubmit(fence), release, nil
	}

	// AIMD is used even when adaptive limits are disabled: with AIMD.Enabled=false,
	// EndpointAIMD.AIMD is nil so recordAIMDSignal is a no-op, but the semaphores
	// still enforce fixed concurrency limits (global + per-endpoint). Without this,
	// DirectDispatcher would dispatch all requests as unbounded goroutines.
	models := buildAIMDModels(modelMap, p.inference, p.endpointLimits, p.cfg.RouteKeyMethod, params.jobInfo.TenantID)
	direct := pipeline.NewDirectDispatcher(p.inference, logger)
	aimd, err := pipeline.NewAIMDDispatcher(direct, models, p.cfg.Concurrency.Global, logger)
	if err != nil {
		return nil, nil, err
	}
	return pipeline.NewPreDispatcher(aimd).WithBeforeSubmit(fence), func() {}, nil
}

// recoveryPlan splits a resume seed's in-flight entries into the recovering
// set — request IDs whose re-submission is deferred while their stranded
// delivery is recovered from the submitter's route — and the distinct
// previous submitters whose routes the job must consume. Async dispatch
// only: sync results never travel per-processor routes. Entries submitted
// by this processor (same-identity restart) defer too, but need no extra
// broadcaster: their deliveries arrive on the processor's own route.
// Entries with an unknown submitter (written before submitter tracking)
// re-submit directly: no route can hold their delivery, so deferral would
// only add latency. The seed builder enforces the same contract by never
// mapping an empty submitter; skipping here is defense in depth.
func recoveryPlan(seed *db.ResumableResumeState, self string, async bool) (map[string]struct{}, []string) {
	if seed == nil || !async || len(seed.InFlightSubmittedBy) == 0 {
		return nil, nil
	}
	recovering := make(map[string]struct{}, len(seed.InFlightSubmittedBy))
	seen := make(map[string]struct{})
	var submitters []string
	for requestID, submitter := range seed.InFlightSubmittedBy {
		if submitter == "" {
			continue // written before submitter tracking: re-submit directly
		}
		recovering[requestID] = struct{}{}
		if submitter == self {
			continue
		}
		if _, dup := seen[submitter]; dup {
			continue
		}
		seen[submitter] = struct{}{}
		submitters = append(submitters, submitter)
	}
	return recovering, submitters
}

// openInputFile opens the preprocessed input file. Results are checkpointed
// to the result store and finalization streams the artifacts from there —
// no local output files are kept.
func (p *Processor) openInputFile(params *jobExecutionParams) (*os.File, error) {
	inputPath, err := p.jobInputFilePath(params.jobInfo.JobID, params.jobInfo.TenantID)
	if err != nil {
		return nil, err
	}
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("open input file: %w", err)
	}
	return inputFile, nil
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
