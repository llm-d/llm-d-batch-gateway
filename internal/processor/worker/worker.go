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

// this file contains the worker logic for processing batch requests.
package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-logr/logr"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/batch_utils"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/semaphore"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// endpointLimit pairs an adaptive semaphore with its AIMD controller for a
// single inference endpoint. Models sharing the same endpoint share one
// endpointLimit, so a 429 from endpoint A only reduces concurrency for
// models routed to A.
type endpointLimit struct {
	sem   *semaphore.AdaptiveSemaphore
	aimd  *semaphore.AIMDController
	label string // human-readable endpoint identifier for metrics/logs
}

type Processor struct {
	cfg         *config.ProcessorConfig
	processorID string
	tokens      semaphore.Semaphore
	wg          sync.WaitGroup

	// globalSem limits total in-flight inference requests across all workers.
	// Fixed capacity — serves as a ceiling only.
	globalSem semaphore.Semaphore

	// endpointLimits maps each unique InferenceClient to its per-endpoint
	// adaptive semaphore + AIMD controller. Created in Run() from the
	// resolver's client set.
	//
	// IMPORTANT: map keys rely on concrete client identity (pointer-equal
	// InferenceClient instances). This is safe because GatewayResolver deduplicates
	// and reuses concrete clients for identical endpoint configs.
	endpointLimits map[inference.InferenceClient]*endpointLimit

	poller  *Poller
	updater *StatusUpdater

	batchDB        db.BatchDBClient                // job status lookups
	event          db.BatchEventChannelClient      // cancel-event subscription
	inference      *inference.GatewayResolver      // model → gateway routing (sync)
	asyncInference *inference.AsyncGatewayResolver // model → async client routing
	broadcasters   *broadcasterRegistry            // per-model result broadcasters (async only)
	files          *fileManager
}

func NewProcessor(
	cfg *config.ProcessorConfig,
	clients *clientset.Clientset,
	processorID string,
	logger logr.Logger,
) (*Processor, error) {
	if cfg.NumWorkers <= 0 {
		return nil, fmt.Errorf("worker semaphore (NumWorkers=%d): %w", cfg.NumWorkers, semaphore.ErrCap)
	}
	if cfg.Concurrency.Global <= 0 {
		return nil, fmt.Errorf("global semaphore (concurrency.global=%d): %w", cfg.Concurrency.Global, semaphore.ErrCap)
	}
	poller := NewPoller(clients.Queue, clients.BatchDB)
	updater := NewStatusUpdater(clients.BatchDB)
	return &Processor{
		cfg:            cfg,
		processorID:    processorID,
		poller:         poller,
		updater:        updater,
		batchDB:        clients.BatchDB,
		event:          clients.Event,
		inference:      clients.Inference,
		asyncInference: clients.AsyncInference,
		files:          newFileManager(clients.File, clients.FileDB),
	}, nil
}

// Run starts processor orchestration and enters the polling loop.
// If onReady is provided, it is called after pre-flight checks succeed and
// stale job recovery completes, right before the polling loop begins accepting work.
func (p *Processor) Run(ctx context.Context, onReady func()) error {
	if err := p.prepare(ctx); err != nil {
		return err
	}

	p.recoverOwnedJobs(ctx)

	if onReady != nil {
		onReady()
	}

	// Two context branches:
	//   pollingCtx — controls the polling loop; cancelled by semaphore guard or SIGTERM.
	//   ctx (unchanged) — parent for job contexts; only cancelled by SIGTERM.
	// This separation ensures stopAccepting() halts new-job intake without
	// cancelling in-flight jobs.
	pollingCtx, stopAccepting := context.WithCancel(ctx)
	defer stopAccepting()

	logger := logr.FromContextOrDiscard(ctx)

	if err := p.initConcurrencyControls(logger, stopAccepting); err != nil {
		return err
	}

	// If async inference is used (llm-d-async), set up a registry of ResultBroadcasters.
	// These will propagate results from the async queues to the JobExecutor's individual result collectors.
	if p.asyncInference != nil {
		bs := newBroadcasterRegistry(p.asyncInference, logger)
		bs.Run(ctx)
		defer bs.Wait()
		p.broadcasters = bs
	}

	return p.runPollingLoop(pollingCtx, ctx)
}

// initConcurrencyControls creates semaphores and per-endpoint AIMD controllers.
// In async mode (p.inference == nil), only the job-level worker semaphore is
// created — inference concurrency is controlled by the llm-d-async dispatcher.
func (p *Processor) initConcurrencyControls(logger logr.Logger, stopAccepting context.CancelFunc) error {
	// Create semaphores here (not in NewProcessor) so the double-release guard
	// callback can capture stopAccepting. This keeps semaphores immutable after
	// construction — no mutex, no OnDoubleRelease method.
	makeGuard := func(name string) func() {
		return func() {
			logger.Error(fmt.Errorf("semaphore double-release"), "Initiating graceful shutdown", "semaphore", name)
			stopAccepting()
		}
	}
	var err error
	p.tokens, err = semaphore.New(p.cfg.NumWorkers, makeGuard("num-workers"))
	if err != nil {
		return fmt.Errorf("worker semaphore (NumWorkers=%d): %w", p.cfg.NumWorkers, err)
	}

	if p.asyncInference != nil {
		logger.V(logging.INFO).Info(
			"Processor run started (async dispatch)",
			"loopInterval", p.cfg.PollInterval,
			"maxWorkers", p.cfg.NumWorkers,
		)
		return nil
	}

	cc := &p.cfg.Concurrency
	p.globalSem, err = semaphore.New(cc.Global, makeGuard("global-concurrency"))
	if err != nil {
		return fmt.Errorf("global semaphore (concurrency.global=%d): %w", cc.Global, err)
	}

	clients := p.inference.Clients()
	p.endpointLimits = make(map[inference.InferenceClient]*endpointLimit, len(clients))
	for _, client := range clients {
		epLabel := p.inference.ClientLabel(client)
		epSem, epErr := semaphore.NewAdaptive(cc.PerEndpoint, makeGuard("endpoint-concurrency"))
		if epErr != nil {
			return fmt.Errorf("endpoint semaphore (concurrency.per_endpoint=%d): %w", cc.PerEndpoint, epErr)
		}
		var epAIMD *semaphore.AIMDController
		if cc.AIMD.Enabled {
			epAIMD = semaphore.NewAIMDController(
				semaphore.AIMDConfig{
					MinLimit:         cc.AIMD.Min,
					MaxLimit:         cc.PerEndpoint,
					BackoffFactor:    cc.AIMD.BackoffFactor,
					AdditiveIncrease: cc.AIMD.AdditiveIncrease,
				},
				cc.PerEndpoint,
				func(limit int) { epSem.SetLimit(limit) },
				logger.WithValues("endpoint", epLabel),
			)
			metrics.SetAIMDConcurrencyLimit(epLabel, float64(cc.PerEndpoint))
		}
		p.endpointLimits[client] = &endpointLimit{sem: epSem, aimd: epAIMD, label: epLabel}
	}
	const highCardinalityThreshold = 50
	if cc.AIMD.Enabled && len(clients) > highCardinalityThreshold {
		logger.Info("AIMD metrics may cause high cardinality: "+
			"each endpoint creates up to 5 time series (1 gauge + 1 increase counter + 3 decrease counters by signal); "+
			"verify your TSDB can handle the load or reduce the number of distinct gateway endpoints in config",
			"num_endpoints", len(clients),
			"estimated_series", len(clients)*5,
		)
	}
	logger.V(logging.INFO).Info(
		"Processor run started",
		"loopInterval", p.cfg.PollInterval,
		"maxWorkers", p.cfg.NumWorkers,
		"concurrency.global", cc.Global,
		"concurrency.per_endpoint", cc.PerEndpoint,
		"concurrency.aimd.enabled", cc.AIMD.Enabled,
		"num_endpoints", len(clients),
	)
	return nil
}

// Stop gracefully stops the processor, waiting for all workers to finish.
func (p *Processor) Stop(ctx context.Context) {
	logger := logr.FromContextOrDiscard(ctx)
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done(): // context cancelled
		logger.V(logging.INFO).Info("Processor stopped due to context cancellation")

	case <-done: // all workers have finished
		logger.V(logging.INFO).Info("All workers have finished")
	}
}

// runPollingLoop runs the job polling loop and dispatches jobs to workers.
//
// pollingCtx controls the loop: cancelled by semaphore guard or SIGTERM.
// jobBaseCtx is the parent for per-job contexts: only cancelled by SIGTERM.
// This ensures stopAccepting() halts new-job intake without killing in-flight jobs.
func (p *Processor) runPollingLoop(pollingCtx, jobBaseCtx context.Context) error {
	logger := logr.FromContextOrDiscard(pollingCtx)
	logger.V(logging.INFO).Info("Polling loop started")
	// worker driven non-busy wait
	for {
		if !p.acquire(pollingCtx) {
			return nil
		}

		// check queue for available tasks
		logger.V(logging.DEBUG).Info("Checking queue for available tasks")
		task, err := p.poller.dequeueOne(pollingCtx)

		// when there's no waiting tasks in the queue or poller returned an error
		if task == nil || err != nil {
			// wait for poll interval to protect db from frequent queueing
			if !p.releaseAndWaitPollInterval(pollingCtx) {
				return nil
			}
			continue
		}

		// Pre-launch: use pollingCtx so guard cancel / SIGTERM interrupts
		// DB fetch and validation promptly. jobBaseCtx is only used once
		// we commit to launching the job goroutine.
		pollLogger := logr.FromContextOrDiscard(pollingCtx).WithValues("jobId", task.ID)
		pollCtx := logr.NewContext(pollingCtx, pollLogger)

		// get job item from db
		jobItem, err := p.poller.fetchJobItemByID(pollCtx, task.ID)
		if err != nil {
			pollLogger.Error(err, "Failed to fetch job item from DB")
			p.releaseForNextPoll()
			pollLogger.V(logging.DEBUG).Info("Re-enqueue the job to the queue")
			// Use a detached context because pollCtx may already be cancelled
			// (e.g. guard fired or SIGTERM arrived during the DB call).
			bgCtx, bgSpan := uotel.DetachedContext(pollCtx, "re-enqueue-fetch-failure")
			if reEnqueueErr := p.poller.enqueueOne(bgCtx, task); reEnqueueErr != nil {
				pollLogger.Error(reEnqueueErr, "Failed to re-enqueue the job to the queue")
				metrics.RecordJobProcessed(metrics.ResultFailed, metrics.ReasonSystemError)
			} else {
				metrics.RecordJobProcessed(metrics.ResultReEnqueued, metrics.ReasonDBTransient)
				pollLogger.V(logging.INFO).Info("Re-enqueued the job to the queue")
			}
			bgSpan.End()
			continue
		}

		// job item is not found in the db.
		if jobItem == nil {
			pollLogger.Error(fmt.Errorf("job item is not found in the DB"), "Ignoring job (data inconsistency)")
			p.releaseForNextPoll()
			metrics.RecordJobProcessed(metrics.ResultSkipped, metrics.ReasonDBInconsistency)
			continue
		}

		pollLogger.V(logging.TRACE).Info("Job item found in the DB")

		// db job item to job info object conversion
		jobInfo, err := batch_utils.FromDBItemToJobInfoObject(jobItem)
		if err != nil {
			pollLogger.Error(err, "Failed to convert job object in DB to job info object")
			p.releaseForNextPoll()
			if failErr := p.handleFailed(pollCtx, p.updater, jobItem, nil, nil); failErr != nil {
				pollLogger.Error(failErr, "Failed to mark malformed job as failed")
			}
			continue
		}

		pollLogger = pollLogger.WithValues("tenantId", jobInfo.TenantID)
		pollCtx = logr.NewContext(pollCtx, pollLogger)

		if jobInfo.BatchJob.CreatedAt > 0 {
			queueWait := time.Since(time.Unix(jobInfo.BatchJob.CreatedAt, 0))
			metrics.RecordQueueWaitDuration(queueWait)
			pollLogger.V(logging.TRACE).Info("Queue wait duration recorded", "duration", queueWait)
		}

		pollLogger.V(logging.TRACE).Info("Job info object converted")

		if batch_utils.IsJobExpired(task) {
			pollLogger.V(logging.INFO).Info("Job is expired.")

			if err := p.updater.UpdatePersistentStatus(pollCtx, jobItem, openai.BatchStatusExpired, nil, nil); err != nil {
				pollLogger.Error(err, "Failed to update job status in DB", "newStatus", openai.BatchStatusExpired, "slo", task.SLO)
				recordE2ELatency(jobInfo, metrics.E2EStatusFailed)
				p.releaseForNextPoll()
				metrics.RecordJobProcessed(metrics.ResultFailed, metrics.ReasonSystemError)
				continue
			}

			p.releaseForNextPoll()
			recordE2ELatency(jobInfo, metrics.E2EStatusExpired)
			metrics.RecordJobProcessed(metrics.ResultExpired, metrics.ReasonExpiredDequeue)
			continue
		}

		// job is not in runnable state.
		if !batch_utils.IsJobRunnable(jobInfo.BatchJob) {
			if jobInfo.BatchJob.Status == openai.BatchStatusCancelling {
				pollLogger.V(logging.INFO).Info("Job is in cancelling state after dequeue, transitioning to cancelled")
				if err := p.updater.UpdateCancelledStatus(pollCtx, jobItem, nil, "", ""); err != nil {
					pollLogger.Error(err, "Failed to update job status to cancelled")
					recordE2ELatency(jobInfo, metrics.E2EStatusFailed)
					p.releaseForNextPoll()
					metrics.RecordJobProcessed(metrics.ResultFailed, metrics.ReasonSystemError)
					continue
				}
				p.releaseForNextPoll()
				recordE2ELatency(jobInfo, metrics.E2EStatusCancelled)
				metrics.RecordCancellation(metrics.CancelPhaseQueued)
				metrics.RecordJobProcessed(metrics.ResultSuccess, metrics.ReasonNone)
				continue
			}

			pollLogger.V(logging.INFO).Info("job is not in processible state. skipping this job.", "status", jobInfo.BatchJob.Status)

			p.releaseForNextPoll()
			metrics.RecordJobProcessed(metrics.ResultSkipped, metrics.ReasonNotRunnableState)
			continue
		}

		// Guard: if pollingCtx was cancelled between dequeue and here
		// (e.g. semaphore double-release or SIGTERM), re-enqueue instead of launching.
		// Use a detached context because both pollingCtx and jobBaseCtx may already
		// be cancelled (SIGTERM cancels the parent of both).
		if pollingCtx.Err() != nil {
			pollLogger.V(logging.INFO).Info("Polling context cancelled before job launch, re-enqueueing")
			p.releaseForNextPoll()
			bgCtx, bgSpan := uotel.DetachedContext(pollCtx, "re-enqueue-guard")
			defer bgSpan.End()
			if reEnqueueErr := p.poller.enqueueOne(bgCtx, task); reEnqueueErr != nil {
				pollLogger.Error(reEnqueueErr, "Failed to re-enqueue job during graceful shutdown, marking as failed")
				if failErr := p.handleFailed(bgCtx, p.updater, jobItem, nil, jobInfo); failErr != nil {
					pollLogger.Error(failErr, "Failed to mark dequeued job as failed after re-enqueue failure")
				}
				metrics.RecordJobProcessed(metrics.ResultFailed, metrics.ReasonGuardShutdown)
			} else {
				metrics.RecordJobProcessed(metrics.ResultReEnqueued, metrics.ReasonGuardShutdown)
				pollLogger.V(logging.INFO).Info("Re-enqueued the job to the queue during graceful shutdown")
			}
			return nil
		}

		// Commit to launching: create job context from jobBaseCtx so in-flight
		// jobs survive pollingCtx cancellation (guard shutdown).
		jobLogger := logr.FromContextOrDiscard(jobBaseCtx).WithValues("jobId", task.ID, "tenantId", jobInfo.TenantID)
		jobCtx := logr.NewContext(jobBaseCtx, jobLogger)

		p.wg.Add(1)
		go p.runJob(jobCtx, &jobExecutionParams{
			updater: p.updater,
			jobItem: jobItem,
			jobInfo: jobInfo,
			task:    task,
		})
	}
}

func (p *Processor) acquire(ctx context.Context) bool {
	if err := p.tokens.Acquire(ctx); err != nil {
		return false
	}
	return true
}

func (p *Processor) release() {
	p.tokens.Release()
}

func (p *Processor) releaseAndWaitPollInterval(ctx context.Context) bool {
	p.release()
	select {
	case <-ctx.Done():
		return false
	case <-time.After(p.cfg.PollInterval):
		return true
	}
}

func (p *Processor) releaseForNextPoll() {
	p.release()
}

// pre-flight check
func (p *Processor) prepare(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)

	if err := p.validate(); err != nil {
		return fmt.Errorf("critical clients are missing in processor: %w", err)
	}

	logger.V(logging.DEBUG).Info("Processor pre-flight check done", "max_workers", p.cfg.NumWorkers)
	return nil
}

func (p *Processor) validate() error {
	if p.poller == nil {
		return fmt.Errorf("poller is missing")
	}
	if err := p.poller.validate(); err != nil {
		return err
	}
	if p.updater == nil {
		return fmt.Errorf("status updater is missing")
	}
	if err := p.updater.validate(); err != nil {
		return err
	}
	if p.batchDB == nil {
		return fmt.Errorf("batch DB client is missing")
	}
	if p.event == nil {
		return fmt.Errorf("event channel client is missing")
	}
	if p.inference == nil && p.asyncInference == nil {
		return fmt.Errorf("inference client is missing")
	}
	if p.inference != nil && p.asyncInference != nil {
		return fmt.Errorf("sync and async inference clients are mutually exclusive")
	}
	if p.files == nil {
		return fmt.Errorf("file manager is missing")
	}
	return p.files.validate()
}
