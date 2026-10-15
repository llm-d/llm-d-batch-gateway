# Metrics

**Revision:** 1.4
**Last Modified:** 2026-05-29

Metric names below match `internal/*/metrics/metrics.go` (and related packages). Deployments may add a namespace/subsystem prefix when registering; check `/metrics` on the running binary for the exact series name.

## API Server

The API server exposes the following Prometheus metrics (`internal/apiserver/metrics/metrics.go`):

**Request Metrics:**

- `http_requests_total{method,path,status}` (Counter) - Total HTTP requests by method, path, and status code.
- `http_request_duration_seconds{method,path,status}` (Histogram) - HTTP request latency histogram.
- `http_requests_in_flight{method,path}` (Gauge) - Current number of HTTP requests being processed by the api server.

## Processor

The processor exposes the following Prometheus metrics (`internal/processor/metrics/metrics.go`):

Processor metrics intentionally omit unbounded identifiers such as tenant IDs. Per-tenant breakdown belongs in logs or traces, not Prometheus labels, to avoid cardinality growth.

**Job-Level Metrics:**

- `jobs_processed_total{result,reason}` (Counter) - Total jobs processed by result and reason.

  **result** values: `success`, `failed`, `skipped`, `re_enqueued`, `expired`.

  **reason** values (see `internal/processor/metrics/metrics.go`): `system_error`, `guard_shutdown`, `db_transient`, `db_inconsistency`, `not_runnable_state`, `expired_dequeue`, `expired_execution`, `none`.

- `job_processing_duration_seconds{size_bucket}` (Histogram) - End-to-end job processing duration. `size_bucket` is derived from input line count (`100`, `1000`, `10000`, `30000`, `large`).

- `job_queue_wait_duration_seconds` (Histogram) - Time spent in the priority queue before being picked up.

- `plan_build_duration_seconds{size_bucket}` (Histogram) - Duration of ingestion and plan build in seconds.

**Worker Metrics:**

- `total_workers` (Gauge) - Configured worker pool size (`NumWorkers`).

- `active_workers` (Gauge) - Currently active workers.

- `processor_inflight_requests` (Gauge) - Global in-flight inference requests during execution.

- `processor_max_inflight_concurrency` (Gauge) - Configured `GlobalConcurrency` ceiling.

**Model Metrics:**

- `model_inflight_requests{model}` (Gauge) - Per-model in-flight requests.

- `model_request_execution_duration_seconds{model}` (Histogram) - Per-request execution duration by model.

**Error Metrics:**

- `request_errors_by_model_total{model}` (Counter) - Total number of request errors by model.

**Token Metrics:**

- `batch_request_prompt_tokens_total{model}` (Counter) - Total prompt tokens consumed by batch inference requests. Only counted when the inference response includes usage data (non-streaming responses from OpenAI-compatible backends typically include this).

- `batch_request_generation_tokens_total{model}` (Counter) - Total generation (completion) tokens produced by batch inference requests. Same availability caveat as prompt tokens.

**Job Lifecycle Metrics:**

- `batch_job_e2e_latency_seconds{status}` (Histogram) - End-to-end job latency from submission (`created_at`) to terminal state. `status` values: `completed`, `cancelled`, `expired`, `failed`. In the execution path (`runJob`), the status label reflects the intended terminal state even if the DB write fails (matching the `jobs_processed_total` convention). In the polling loop and startup recovery, DB write failures are recorded as `failed` to avoid misrepresenting the actual outcome.

**Cancellation Metrics:**

- `batch_cancellation_total{phase}` (Counter) - Total batch job cancellations. `phase` values: `queued` (cancelled before execution started), `in_progress` (cancelled during execution), `finalizing` (cancelled during file upload/finalization).

**Startup Recovery:**

- `batch_startup_recovery_total{status,action}` (Counter) - Jobs recovered during processor startup after a container restart. `status` is the recovered job status (common values: `in_progress`, `finalizing`, `cancelling`, `validating`, `unknown`). `action` is the recovery action taken (common values: `re_enqueued`, `failed`, `finalized`, `cancelled`, `expired`, `cleaned_up`, `error`). Non-zero values indicate prior container-level crashes (OOM, panic) or stale on-disk artifacts from a prior processor instance.

## Garbage Collector — Reconciler

The GC reconciler exposes the following Prometheus metrics (`internal/gc/metrics/metrics.go`) on port 9091 (configurable via `metrics_addr`):

**Orphan Recovery:**

- `batch_reconciler_orphans_recovered_total{action}` (Counter) - Orphans recovered by action type. `action` values: `cancelled`, `expired`, `re_enqueued`, `failed`. Not incremented when `dry_run: true` (no actual state change occurs).

**Cycle Metrics:**

- `batch_reconciler_cycle_duration_seconds` (Histogram) - Wall-clock time per reconciliation cycle. Uses exponential buckets from 100ms to ~30min (`ExponentialBuckets(0.1, 3, 10)`). Recorded for all cycles including failed ones (early-return error paths).

**Conflict & Error Metrics:**

- `batch_reconciler_cas_conflicts_total` (Counter) - CAS conflicts where another actor won the race during orphan transition.

- `batch_reconciler_stale_cleanup_total` (Counter) - Stale in-flight entries cleaned up (entries for jobs no longer in a non-terminal state). Not incremented when `dry_run: true`.

- `batch_reconciler_errors_total` (Counter) - Errors encountered during a reconciliation cycle (DB fetch failures, transition errors, etc.).

**Alerting (`BatchReconcilerErrors`):**

The Helm chart ships a PrometheusRule (`prometheusRule.rules.reconcilerErrors`) that fires when:

```promql
increase(batch_reconciler_errors_total{namespace="<release namespace>"}[<window>]) > 0
```

**Why `increase`, not `rate`:** the reconciler runs on a long interval (default 60 minutes). A cycle-level failure such as a DB fetch error increments the counter once per cycle, which is too sparse for a meaningful `rate()` threshold.

**Tuning `window`:** set it to at least one full reconciler scan interval plus a small buffer for Prometheus scrape/evaluation delay. The default `65m` matches the default reconciler interval of 60 minutes. If you change `reconciler.interval` in the GC config, increase `window` to match (for example, interval `90m` → window `95m`). A window shorter than the interval can miss once-per-cycle failures; a much longer window keeps the alert firing longer after an error ages out of the window.

**Tuning `for`:** default `5m` avoids paging on a single brief evaluation blip. It does not need to track the reconciler interval.

## Garbage Collector — Event Purge

The GC process periodically purges expired rows from the `batch_events` table (default every 30s, configurable via `event_purge.interval` in the GC config). The purger shares the metrics endpoint above and uses the `batch_gc_` prefix — the going-forward prefix for batch-gc process metrics (the legacy `batch_reconciler_` family predates the collector and purger living in the same binary; it is not renamed for dashboard compatibility).

**Purge Metrics:**

- `batch_gc_events_purged_total` (Counter) - Expired events removed from the batch events table. A persistently zero value alongside a non-empty events table indicates the purge loop is not running (e.g. `dry_run: true`, under which the purger is not started).

- `batch_gc_event_purge_failures_total` (Counter) - Failed event purge attempts. Outcomes that race context cancellation during shutdown are neither logged nor counted, so every increment represents a real purge failure (typically DB connectivity loss).

Note: unlike `batch_reconciler_errors_total`, there is currently no shipped alert rule for `batch_gc_event_purge_failures_total`; watch it on dashboards.

## Shared (file storage retry client)

Used by components that wrap file storage with retries (`internal/files_store/retryclient/metrics.go`):

- `file_storage_operations_total{operation,component,status}` (Counter) - File storage operations by outcome. `operation` is `store` / `retrieve` / `delete`; `component` is `processor` / `apiserver` / `garbage-collector`; `status` is `success`, `retry`, or `exhausted`.

## Dashboard PromQL: aggregated quantile caveat

The default Grafana dashboards (`charts/batch-gateway/dashboards/`) compute histogram quantiles by summing buckets across all pods first, then applying `histogram_quantile`:

```promql
histogram_quantile(0.95, sum(rate(..._bucket{namespace="$namespace"}[5m])) by (le))
```

This yields a **fleet-wide approximate percentile**, not a mathematically exact one. The approximation is acceptable for this system because each job is processed by exactly one pod, and the priority queue distributes work roughly evenly. However, if pod-level distributions diverge significantly (e.g., one pod consistently receives larger jobs), the aggregated quantile can mask per-pod outliers.

For per-pod breakdown, add `by (le, pod)` inside `histogram_quantile` and use a Grafana variable to select individual pods. The default dashboards intentionally omit this to keep the fleet-level SLO view simple.
