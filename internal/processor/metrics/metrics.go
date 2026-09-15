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

package metrics

import (
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/prometheus/client_golang/prometheus"
)

// labels definition
const (
	// -- Result --
	// ResultSuccess: Job reached a terminal state treated as success by policy (completed; cancelled is treated as success because it is user-initiated).
	// ResultFailed: Job is failed and updated to failed status in the db
	// ResultSkipped: Job was not processed by this worker (e.g. already terminal, not runnable, data inconsistency)
	// ResultReEnqueued: Job was re-enqueued for retry due to transient backend/system issues
	// ResultExpired: Job exceeded SLO deadline (either at dequeue time or mid-execution)

	// Result labels
	ResultSuccess       = "success"
	ResultFailed        = "failed"
	ResultSkipped       = "skipped"
	ResultReEnqueued    = "re_enqueued"
	ResultExpired       = "expired"        // job exceeded SLO deadline (dequeue-time or mid-execution)
	ResultLostOwnership = "lost_ownership" // job aborted mid-execution after losing ownership (epoch fenced out)

	// -- Reason --
	// - If expired at dequeue time, use ReasonExpiredDequeue
	// - If expired mid-execution, use ReasonExpiredExecution
	// - If data inconsistency, use ReasonDBInconsistency
	// - If retryable backend error, use ReasonDBTransient
	// - If not runnable, use ReasonNotRunnableState
	// - If semaphore guard triggered graceful shutdown, use ReasonGuardShutdown
	// - Otherwise, fall back to ReasonSystemError

	// Reason labels
	ReasonSystemError      = "system_error"       // unexpected internal errors (panic, serialization failure, invariant violation)
	ReasonGuardShutdown    = "guard_shutdown"     // semaphore double-release guard triggered graceful shutdown; job re-enqueued
	ReasonDBTransient      = "db_transient"       // temporary backend/storage error; safe to retry
	ReasonDBInconsistency  = "db_inconsistency"   // PQ item exists but DB item missing or corrupted
	ReasonNotRunnableState = "not_runnable_state" // job status is not runnable by processor policy
	ReasonExpiredDequeue   = "expired_dequeue"    // SLO already exceeded before execution started; skipped at dequeue
	ReasonExpiredExecution = "expired_execution"  // SLO deadline fired during execution; partial results preserved
	ReasonNone             = "none"               // no additional reason beyond the result (e.g. success, cancelled)

	// size bucket labels
	Bucket100   = "100"   // less than 100 lines
	Bucket1000  = "1000"  // less than 1000 lines
	Bucket10000 = "10000" // less than 10000 lines
	Bucket30000 = "30000" // less than 30000 lines
	BucketLarge = "large" // more than 30000 lines
)

func GetSizeBucket(totalLines int) string {
	switch {
	case totalLines < 100:
		return Bucket100
	case totalLines < 1000:
		return Bucket1000
	case totalLines < 10000:
		return Bucket10000
	case totalLines < 30000:
		return Bucket30000
	default:
		return BucketLarge
	}
}

var (
	jobsProcessed                 *prometheus.CounterVec
	jobProcessingDuration         *prometheus.HistogramVec
	jobQueueWaitDuration          *prometheus.HistogramVec
	totalWorkers                  prometheus.Gauge
	activeWorkers                 prometheus.Gauge
	requestErrorsModelTotal       *prometheus.CounterVec
	processorInflightRequests     prometheus.Gauge
	processorMaxInflightConc      prometheus.Gauge
	planBuildDuration             *prometheus.HistogramVec
	modelInflightRequests         *prometheus.GaugeVec
	modelRequestExecutionDuration *prometheus.HistogramVec
	startupRecoveryTotal          *prometheus.CounterVec
	requestPromptTokensTotal      *prometheus.CounterVec
	requestGenerationTokensTotal  *prometheus.CounterVec
	jobE2ELatency                 *prometheus.HistogramVec
	cancellationTotal             *prometheus.CounterVec
	aimdConcurrencyLimit          *prometheus.GaugeVec
	aimdDecreasesTotal            *prometheus.CounterVec
	aimdIncreasesTotal            *prometheus.CounterVec
)

// FileType labels for file upload metrics.
type FileType string

const (
	FileTypeOutput FileType = "output"
	FileTypeError  FileType = "error"
)

func InitMetrics(cfg config.ProcessorConfig) error {
	// number of jobs processed
	jobsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "jobs_processed_total",
			Help: "Total number of jobs processed",
		}, []string{"result", "reason"},
	)

	// total number of workers for utilization %
	// this is set once on initialization
	totalWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "total_workers",
			Help: "Total number of configured workers",
		},
	)
	totalWorkers.Set(float64(cfg.NumWorkers))

	// current number of active workers
	activeWorkers = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_workers",
			Help: "Current number of active workers processing jobs",
		},
	)

	// errors by model
	requestErrorsModelTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "request_errors_by_model_total",
			Help: "Total number of request errors by model",
		},
		[]string{"model"},
	)

	// global in-flight request count during execution
	processorInflightRequests = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "processor_inflight_requests",
			Help: "Current number of in-flight inference requests for the processor",
		},
	)

	// configured GlobalConcurrency value for utilization calculation
	processorMaxInflightConc = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "processor_max_inflight_concurrency",
			Help: "Configured maximum number of concurrent in-flight inference requests (GlobalConcurrency)",
		},
	)
	processorMaxInflightConc.Set(float64(cfg.Concurrency.Global))

	// ingestion plan build duration
	planBuildDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "plan_build_duration_seconds",
			Help: "Duration of ingestion and plan build in seconds",
			Buckets: prometheus.ExponentialBuckets(
				cfg.ProcessTimeBucket.BucketStart,
				cfg.ProcessTimeBucket.BucketFactor,
				cfg.ProcessTimeBucket.BucketCount,
			),
		}, []string{"size_bucket"},
	)

	// per-model in-flight requests during execution
	modelInflightRequests = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "model_inflight_requests",
			Help: "Current number of in-flight inference requests per model",
		},
		[]string{"model"},
	)

	// per-request execution duration by model
	modelRequestExecutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "model_request_execution_duration_seconds",
			Help: "Per-request execution duration in seconds by model",
			Buckets: prometheus.ExponentialBuckets(
				cfg.ProcessTimeBucket.BucketStart,
				cfg.ProcessTimeBucket.BucketFactor,
				cfg.ProcessTimeBucket.BucketCount,
			),
		}, []string{"model"},
	)

	// job processing duration
	jobProcessingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "job_processing_duration_seconds",
			Help: "Duration of job processing in seconds",
			Buckets: prometheus.ExponentialBuckets(
				cfg.ProcessTimeBucket.BucketStart,
				cfg.ProcessTimeBucket.BucketFactor,
				cfg.ProcessTimeBucket.BucketCount,
			),
		}, []string{"size_bucket"},
	)

	// duration of queue wait time
	jobQueueWaitDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "job_queue_wait_duration_seconds",
			Help: "Time spent in the priority queue before being picked up",
			Buckets: prometheus.ExponentialBuckets(
				cfg.QueueTimeBucket.BucketStart,
				cfg.QueueTimeBucket.BucketFactor,
				cfg.QueueTimeBucket.BucketCount,
			),
		}, nil,
	)

	// per-request prompt token count by model.
	// Only counted when the inference response includes usage data.
	requestPromptTokensTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "batch_request_prompt_tokens_total",
			Help: "Total prompt tokens consumed by batch inference requests. Only counted when the inference response includes usage data.",
		},
		[]string{"model"},
	)

	// per-request generation (completion) token count by model.
	// Only counted when the inference response includes usage data.
	requestGenerationTokensTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "batch_request_generation_tokens_total",
			Help: "Total generation tokens produced by batch inference requests. Only counted when the inference response includes usage data.",
		},
		[]string{"model"},
	)

	jobE2ELatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "batch_job_e2e_latency_seconds",
			Help: "End-to-end job latency from submission to terminal state (completed, cancelled, expired, failed)",
			Buckets: prometheus.ExponentialBuckets(
				cfg.E2ELatencyBucket.BucketStart,
				cfg.E2ELatencyBucket.BucketFactor,
				cfg.E2ELatencyBucket.BucketCount,
			),
		}, []string{"status"},
	)

	cancellationTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "batch_cancellation_total",
			Help: "Total number of batch job cancellations",
		},
		[]string{"phase"},
	)

	// Startup recovery: counts jobs discovered in workdir after a container restart.
	// Non-zero values indicate container-level crashes (OOM, panic) occurred.
	//
	// Operational signals:
	//   - status="in_progress" is high → frequent crashes during inference.
	//   - status="finalizing" is high → frequent crashes during upload.
	//   - action="re_enqueued" is high → most crashes happen early (low wasted inference cost).
	//   - action="failed" is high → significant inference results lost on crash.
	//   - action="finalized" → jobs successfully completed despite crash.
	//   - Sustained failed{status="in_progress"} suggests checkpoint/resume investment.
	startupRecoveryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "batch_startup_recovery_total",
			Help: "Jobs recovered during processor startup after container restart",
		},
		[]string{"status", "action"},
	)

	aimdConcurrencyLimit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "batch_processor_aimd_concurrency_limit",
			Help: "Current effective AIMD concurrency limit per inference endpoint",
		},
		[]string{"endpoint"},
	)

	aimdDecreasesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "batch_processor_aimd_decreases_total",
			Help: "Backpressure signals that trigger multiplicative decrease (429, 5xx, capacity_retry). " +
				"Increments on every signal, even when the limit is already at the AIMD floor.",
		},
		[]string{"endpoint", "signal"},
	)

	aimdIncreasesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "batch_processor_aimd_increases_total",
			Help: "Additive increases after sustained success windows",
		},
		[]string{"endpoint"},
	)

	// metrics to register
	metricsToRegister := []prometheus.Collector{
		jobProcessingDuration,
		jobQueueWaitDuration,
		totalWorkers,
		activeWorkers,
		jobsProcessed,
		requestErrorsModelTotal,
		processorInflightRequests,
		processorMaxInflightConc,
		planBuildDuration,
		modelInflightRequests,
		modelRequestExecutionDuration,
		startupRecoveryTotal,
		requestPromptTokensTotal,
		requestGenerationTokensTotal,
		jobE2ELatency,
		cancellationTotal,
		aimdConcurrencyLimit,
		aimdDecreasesTotal,
		aimdIncreasesTotal,
	}

	for _, metric := range metricsToRegister {
		if err := prometheus.Register(metric); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
				continue
			}
			return err
		}
	}

	return nil
}

// Recorder funcs

// RecordQueueWait observes the queue time
func RecordQueueWaitDuration(duration time.Duration) {
	jobQueueWaitDuration.WithLabelValues().Observe(duration.Seconds())
}

// RecordJobProcessed increments the total processed jobs count.
func RecordJobProcessed(result string, reason string) {
	jobsProcessed.WithLabelValues(result, reason).Inc()
}

// RecordJobProcessingDuration observes the time taken to process a job.
func RecordJobProcessingDuration(duration time.Duration, sizeBucket string) {
	jobProcessingDuration.WithLabelValues(sizeBucket).Observe(duration.Seconds())
}

// IncActiveWorkers increments the gauge for active workers.
func IncActiveWorkers() {
	activeWorkers.Inc()
}

// DecActiveWorkers decrements the gauge for active workers.
func DecActiveWorkers() {
	activeWorkers.Dec()
}

// RecordRequestError increments the error count for a specific model.
func RecordRequestError(model string) {
	requestErrorsModelTotal.WithLabelValues(model).Inc()
}

// IncProcessorInflightRequests increments the processor global in-flight request gauge.
func IncProcessorInflightRequests() {
	processorInflightRequests.Inc()
}

// DecProcessorInflightRequests decrements the processor global in-flight request gauge.
func DecProcessorInflightRequests() {
	processorInflightRequests.Dec()
}

// RecordPlanBuildDuration observes ingestion plan build duration.
func RecordPlanBuildDuration(duration time.Duration, sizeBucket string) {
	planBuildDuration.WithLabelValues(sizeBucket).Observe(duration.Seconds())
}

// IncModelInflightRequests increments the in-flight request gauge for a model.
func IncModelInflightRequests(model string) {
	modelInflightRequests.WithLabelValues(model).Inc()
}

// DecModelInflightRequests decrements the in-flight request gauge for a model.
func DecModelInflightRequests(model string) {
	modelInflightRequests.WithLabelValues(model).Dec()
}

// RecordModelRequestExecutionDuration observes per-request execution duration by model.
func RecordModelRequestExecutionDuration(duration time.Duration, model string) {
	modelRequestExecutionDuration.WithLabelValues(model).Observe(duration.Seconds())
}

// RecordStartupRecovery increments the startup recovery counter.
func RecordStartupRecovery(status, action string) {
	startupRecoveryTotal.WithLabelValues(status, action).Inc()
}

// RecordTokenUsage adds prompt and generation token counts for a model.
// Only called when the inference response includes a usage object with valid numeric fields.
func RecordTokenUsage(promptTokens, generationTokens float64, model string) {
	requestPromptTokensTotal.WithLabelValues(model).Add(promptTokens)
	requestGenerationTokensTotal.WithLabelValues(model).Add(generationTokens)
}

// RecordJobE2ELatency observes the full lifecycle duration of a batch job.
// In the execution path (runJob), the status label reflects the intended terminal state
// even if the DB write fails. In the polling loop and startup recovery, DB write failures
// are recorded as E2EStatusFailed to avoid misrepresenting the actual outcome.
func RecordJobE2ELatency(duration time.Duration, status string) {
	jobE2ELatency.WithLabelValues(status).Observe(duration.Seconds())
}

// RecordCancellation increments the cancellation counter for a given phase.
func RecordCancellation(phase string) {
	cancellationTotal.WithLabelValues(phase).Inc()
}

// Cancellation phase labels.
const (
	CancelPhaseQueued     = "queued"
	CancelPhaseInProgress = "in_progress"
	CancelPhaseFinalizing = "finalizing"
)

// SetAIMDConcurrencyLimit sets the current effective AIMD concurrency limit for an endpoint.
func SetAIMDConcurrencyLimit(endpoint string, limit float64) {
	aimdConcurrencyLimit.WithLabelValues(endpoint).Set(limit)
}

// RecordAIMDDecrease increments the AIMD decrease counter for an endpoint.
func RecordAIMDDecrease(endpoint, signal string) {
	aimdDecreasesTotal.WithLabelValues(endpoint, signal).Inc()
}

// RecordAIMDIncrease increments the AIMD increase counter for an endpoint.
func RecordAIMDIncrease(endpoint string) {
	aimdIncreasesTotal.WithLabelValues(endpoint).Inc()
}

// AIMD signal labels for decrease counter.
const (
	AIMDSignal429           = "429"
	AIMDSignal5xx           = "5xx"
	AIMDSignalCapacityRetry = "capacity_retry"
)

// E2E latency status labels. Must match the terminal states used in
// job_runner.go, worker.go, and recovery.go.
const (
	E2EStatusCompleted = "completed"
	E2EStatusCancelled = "cancelled"
	E2EStatusExpired   = "expired"
	E2EStatusFailed    = "failed"
)
