# Batch Dispatcher Queue Design

-   **Revision**: 3
-   **Last Updated**: 2026-07-29
-   **Related Jira**: [INFERENG-5607](https://redhat.atlassian.net/browse/INFERENG-5607)

Related:
- [Batch Dispatcher](batch-dispatcher.md)
- [Dispatch Budget](https://github.com/llm-d/llm-d-async/blob/main/docs/dispatch-budget.md) (llm-d-async)
- [Batch Processor Design](batch_processor_architecture.md)
- [Batch Inference Architecture](batch_inference_architecture.md)

---

## Summary

This document describes the design of the request and result queues that connect the **batch-processor** to the **batch dispatcher** ([llm-d-async](https://github.com/llm-d/llm-d-async)). At this time we always assume that each target inference pool corresponds to a single connector. In other words, we assume that there will never be 2 queues targeting the same inference pool at once; we reserve this for future extensions, if needed.

The batch-processor supports two mutually exclusive dispatch modes, selected via `dispatch_mode`:

- **`sync`** (default): The executor dispatches inference requests directly to the inference gateway via HTTP, using the existing AIMD + semaphore flow control.
- **`async`** (opt-in): The executor enqueues individual requests into **the dispatcher's request queue**; **the dispatcher pulls and forwards** them to the inference gateway based on the [dispatch budget](https://github.com/llm-d/llm-d-async/blob/main/docs/dispatch-budget.md). A **result consumer** in the batch-processor reads completed responses from **the dispatcher's result queue** and routes them back to the appropriate job's output writer.

This document describes the **async** dispatch mode and its queue design.

This document uses **producer** and **consumer** from the batch-processor's perspective, consistent with the batch-gateway codebase (cf. `ECProducerSendEvents`, `ECConsumerGetChannel`). The batch-processor is the **producer** of the request queue and the **consumer** of the result queue.

---

## Problem Statement

The batch-processor currently sends requests directly to the inference gateway with no awareness of inference pool saturation beyond HTTP 429 backpressure and AIMD-based concurrency control. This reactive approach has limitations:

- **Late feedback**: The processor only learns about overload after sending a request and getting a 429, wasting a round-trip and consuming gateway resources.
- **No coordination with online traffic**: The processor cannot preemptively yield capacity to interactive requests — it reacts only after the gateway is already saturated.
- **No cross-processor coordination**: Multiple batch-processor replicas independently manage their own concurrency limits without a shared view of system load.

The dispatcher solves these by acting as a system-load-aware gatekeeper that meters batch requests into the gateway based on real-time Prometheus metrics (EPP fullness, vLLM saturation).

---

## Architecture

<img src="diagrams/dispatcher-queues.png" width="50%" alt="dispatch queue diagram" />

Inside the batch-processor, async dispatch is not a separate execution path: it is the **`AsyncDispatcher` leaf** of the same channel-based execution pipeline used for sync dispatch (see [Batch Processor Design — Execution](batch_processor_architecture.md#execution)). In this mode the pipeline runs *without* the `AIMDDispatcher` stage, so the batch-processor holds no per-request semaphores — flow control is delegated entirely to the dispatcher's dispatch budget. (In sync mode, semaphore-based concurrency control lives solely in the `AIMDDispatcher` stage.)

---

## Queue Naming Convention

Queue names follow a fixed convention keyed by the inference pool name:

| Queue | Redis Type | Name Pattern | Example |
|-------|-----------|--------------|---------|
| Request queue | Sorted Set | `llm-d-async:requests:{pool_name}` | `llm-d-async:requests:optimized-baseline` |
| Result queue | List | `llm-d-async:results:{pool_name}:{processor_id}` | `llm-d-async:results:optimized-baseline:batch-gateway-processor-7d9f6c8b5f-k2m4n` |

The `pool_name` corresponds to the target [InferencePool](https://gateway-api-inference-extension.sigs.k8s.io/api-types/inferencepool/). The request queue and result queue base name are derived from the pool name. The batch-processor's async resolver appends the Processor hostname to the result queue base name, so replicas share requests but cannot consume one another's results. The resolved names are `asyncQueuePrefix + "requests:" + poolName` and `asyncQueuePrefix + "results:" + poolName + ":" + processorID`.

The prefix `llm-d-async` (`asyncQueuePrefix`) is currently hardcoded but can be made configurable, so that multiple installations can share the same Redis instance without key collisions (e.g., `staging`, `prod`, or an application-specific identifier).

The llm-d-async producer treats `ResultQueueName` as the full Redis list key and carries it on each request. The Async Processor must preserve that per-request destination. When using a `queuesConfig`, leave its per-queue `result_queue_name` unset; llm-d Async versions through v0.9.0 give that static value precedence over the request-provided result queue. A top-level default result queue may still be configured as a fallback.

For an existing deployment, first remove the per-queue `result_queue_name` override and wait for the Async Processor rollout to complete. Only then upgrade the Batch Processors. Upgrading a Batch Processor first strands its results even with a single replica: the Processor listens on its replica-specific queue while the old Async configuration continues writing to the static queue.

Replica-specific queues prevent healthy Processors from stealing results from one another. They do not recover results after the owning Processor is lost; pod-loss recovery and cleanup of abandoned result queues are tracked in [#645](https://github.com/llm-d/llm-d-batch-gateway/issues/645).

When the dispatcher is used, the inference gateway endpoint configuration lives entirely on the dispatcher side: the batch-processor does not need to know about gateway URLs, TLS settings, or routing modes. The batch-processor only needs the pool name, the connector type, and the connector endpoint.

### Batch-Processor Configuration

The batch-processor selects the dispatch backend via `dispatch_mode: sync | async`. In the default `sync` mode, it dispatches directly via HTTP using the existing AIMD + semaphore flow. In opt-in `async` mode, the executor enqueues to the dispatcher's request queue and collects results from the result queue.

Each model resolves to an `inference_pool_name` that derives the queue pair. The config uses `dispatch_mode` on `ProcessorConfig` and `inference_pool_name` on each `ModelGatewayConfig` entry (see [#430](https://github.com/llm-d/llm-d-batch-gateway/pull/430)):

```yaml
dispatch_mode: "async"
async_dispatch:
  result_poll_timeout: "5s"
model_gateways:
  "llama-3":
    url: "http://gateway-a:8000"              # used in sync mode
    inference_pool_name: "pool-a"             # used in async mode → llm-d-async:requests:pool-a
  "mistral":
    url: "http://gateway-b:8000"
    inference_pool_name: "pool-b"
```

The Redis URL is read from a mounted secret at runtime (not stored in the config file). Queue names are derived from `inference_pool_name` by the async resolver — they are not configured directly.

### Dispatcher Configuration

The dispatcher (llm-d-async) already supports the Redis sorted-set flow with dispatch budget gating. It is configured with a single [transport configuration document](https://github.com/llm-d/llm-d-async/blob/main/README.md#transport-configuration), passed via `--transport redis-sortedset` and `--transport-config` (or `--transport-config-file`). The request queue is defined by the `queues` array; the result queue is set via the top-level `result_queue_name`:

```json
{
  "result_queue_name": "llm-d-async:results:optimized-baseline",
  "queues": [
    {
      "queue_name": "llm-d-async:requests:optimized-baseline",
      "igw_base_url": "http://llm-d-inference-gateway-istio:80",
      "request_path_url": "/v1/completions",
      "gate_type": "prometheus-budget",
      "gate_params": {
        "pool": "optimized-baseline",
        "max_concurrency": "100",
        "baseline": "0.05"
      }
    }
  ]
}
```

The request queue must match the name derived by the batch-processor's async resolver. The result queue shown here is a fallback; each Batch Processor request carries its replica-specific result destination.

The dispatcher pulls up to `max_SYS × budget` requests per poll cycle and forwards them to the inference gateway. See the [llm-d-async README](https://github.com/llm-d/llm-d-async/blob/main/README.md) and [Helm chart values](https://github.com/llm-d/llm-d-async/tree/main/charts/async-processor) for the full configuration.

### Future Extension: Queue Registry

For deployments where queue names need to be decoupled from pool names (e.g., migrations, multi-tenant namespacing), a registry-based approach could be introduced:

- A shared ConfigMap, Redis hash, or CRD maps `pool_name → {request_queue, result_queue}`.
- Both the batch-processor and dispatcher resolve queue names dynamically from the registry, allowing queue mappings to change at runtime without restarting either side.

This is not needed for the 1:1 topology and is deferred to a future iteration.

---

## Request Queue

For a given inference pool, the batch-processor **produces** requests into the dispatcher's **request queue** for that pool; the dispatcher reads from it, gated by the dispatch budget.

Note: The request queue is currently implemented by a **Redis SortedSet** that holds individual inference requests awaiting dispatch.

### Why the Producer Can Enqueue Liberally

Unlike direct dispatch to the inference gateway — where the EPP's flow control limits how many requests can be in-flight and excess requests are rejected with HTTP 429 — **the request queue is a passive buffer with no backpressure on writes**. The producer can enqueue requests as fast as it can read plan entries, without throttling or semaphore gating. This is safe because:

1. **Flow control is deferred to the dispatcher.** The [dispatch budget](https://github.com/llm-d/llm-d-async/blob/main/docs/dispatch-budget.md) gates how many requests leave the queue per poll cycle. The gate returns a `budget` value in [0, 1] representing remaining system capacity (generally `budget = D − B`, where `D` is the dispatch budget and `B` is the reserved baseline). The dispatcher pops up to `max_SYS × budget` requests per cycle, where `max_SYS` is a configurable measure of total system capacity. Enqueuing more requests than the dispatcher can immediately process simply means they wait in the queue until capacity opens up — they do not reach the inference gateway or compete with online traffic.

2. **The queue is cheap storage.** Redis sorted sets are memory-efficient for this workload. Each request message is a few KB; even a full 50,000-request batch job at ~2 KB per message is ~100 MB — well within Redis capacity and far cheaper than holding in-flight HTTP connections.

3. **Deadline ordering is automatic.** Because the sorted-set score is the SLO deadline, enqueuing all requests upfront means the dispatcher always picks the most urgent request across all active jobs. Throttling the enqueue rate would artificially delay requests and could cause the dispatcher to miss tighter deadlines that haven't been enqueued yet.

4. **No wasted round-trips.** With direct dispatch, a 429 response wastes a full HTTP round-trip and consumes gateway resources (connection handling, flow control evaluation). With queue-based dispatch, the request sits in Redis until the dispatcher determines it's safe to forward.

### Message Format

Request messages follow the wire format defined in the [llm-d-async README — Request Messages and Consumption](https://github.com/llm-d/llm-d-async/blob/main/README.md#request-messages-and-consumption). The `metadata` field carries batch-processor correlation data (`job_id`, `request_index`) that the dispatcher passes through opaquely and returns in the result. The `headers` field can carry HTTP headers that the dispatcher forwards to the inference gateway (e.g., fairness/SLO headers that the current executor attaches directly).

The sorted-set score is the request's SLO deadline (Unix timestamp), so earliest-deadline requests are dispatched first across all jobs sharing the same pool — providing cross-job deadline-aware scheduling.

**Example:**

```json
{
  "id": "batch_req_xyz",
  "created": 1700000000,
  "deadline": 1700086400,
  "payload": {
    "model": "Qwen/Qwen3-0.6B",
    "messages": [{"role": "user", "content": "What is 2+2?"}],
    "max_tokens": 128
  },
  "metadata": {
    "job_id": "batch_abc123",
    "request_index": "42"
  }
}
```

### Producer (Batch-Processor)

Enqueuing is the job of the **`AsyncDispatcher`**, the leaf stage of the execution pipeline in async mode (see [Batch Processor Design — Execution](batch_processor_architecture.md#execution)). The `RequestSource` reads each plan entry and its input line and emits a `RequestItem` (with request payload, SLO deadline header, and any pass-through fairness/SLO headers); the `AsyncDispatcher` consumes each item and, for each:

1. Resolves the shared async client for the request's model → pool.
2. Records the request in a per-job `PendingRequests` map, keyed by request ID, so the collector can match the result later.
3. Submits it to the request queue (e.g., `llm-d-async:requests:{pool_name}`) via the llm-d-async producer — **fire-and-forget**: the submit returns immediately and the result arrives asynchronously.

As described above, the producer does not need to throttle enqueue operations. In async mode there are **no per-endpoint or global semaphores** in the batch-processor (the pipeline runs without the `AIMDDispatcher` stage) — the dispatcher's dispatch budget handles flow control downstream. In sync mode, the AIMD + semaphore flow is retained, confined to the `AIMDDispatcher` stage.

### Dispatcher (reads from request queue)

The dispatcher ([llm-d-async](https://github.com/llm-d/llm-d-async)) periodically polls the sorted set. On each cycle, it computes the budget on its "Dispatch Gate" to determine the current dispatch budget $D$. If $D > B$ (the reserved baseline), it pops up to `max_SYS × budget` requests (lowest score = earliest deadline first) and forwards them to the inference gateway via HTTP. See [Dispatch Budget](https://github.com/llm-d/llm-d-async/blob/main/docs/dispatch-budget.md) for the full gating logic.

The dispatcher requires no changes to support this integration, it already implements the Redis sorted-set flow with dispatch budget gating. Only the queue names need to match the naming convention.

---

## Result Queue

The result queue holds completed inference responses. The dispatcher writes results into this queue after receiving responses from the inference gateway; the batch-processor **consumes** results and routes them back to the appropriate job's output writer.

Note: the result queue is currently implemented by a Redis list.

### Message Format

Result messages follow the format defined in the [llm-d-async README — Results](https://github.com/llm-d/llm-d-async/blob/main/README.md#results). The `metadata` from the original request (containing `job_id` and `request_index`) is passed through by the dispatcher, allowing the consumer to route results back to the correct job.

**Example:**

```json
{
  "id": "batch_req_xyz",
  "payload": "{\"id\":\"chatcmpl-...\",\"object\":\"chat.completion\",\"model\":\"Qwen/Qwen3-0.6B\",\"choices\":[...]}",
  "metadata": {
    "job_id": "batch_abc123",
    "request_index": "42"
  }
}
```

### Dispatcher (writes to result queue)

After the dispatcher receives a response from the inference gateway (success or failure), it writes the result to the replica-specific result queue carried on the request (e.g., `llm-d-async:results:{pool_name}:{processor_id}`).

### Consumer (Batch-Processor)

Result consumption is split between a **long-lived broadcaster** (one per model/pool, shared across all jobs) and the **per-job `ResultCollector`** (part of each job's execution pipeline).

#### ResultBroadcaster (watches the result queue)

Because a job's pipeline is short-lived but the result queue is long-lived and shared by every job targeting the same pool, the batch-processor does **not** poll the result queue directly from the collector. Instead, when async inference is enabled, the `Processor` starts a `broadcasterRegistry` at startup with one `ResultBroadcaster` per model/pool. Each broadcaster:

1. Runs the shared async client's `GetResult()` loop against its pool's result queue (with retry/backoff on transient errors).
2. Converts each raw result into a `ResultItem` (preserving the HTTP status, or mapping non-HTTP failures to an error result).
3. Fans that `ResultItem` out to **every currently-subscribed result channel** — it does not itself filter by job or model.

Broadcasters outlive individual jobs, so a single `GetResult()` consumer per pool and Processor replica keeps draining that replica's result queue even as jobs start and finish.

#### ResultCollector (per-job)

When a job starts, its `AsyncDispatcher` **subscribes** the pipeline's `resultCh` to the broadcasters for the job's models; when the job ends it **unsubscribes**. The `ResultCollector` drains `resultCh` and, for each result, calls `PendingRequests.Resolve()` to match it back to a request this job submitted (keyed by request ID). Matched results are enriched with the original `custom_id`, model, and submit timestamp, then written to `output.jsonl` (success) or `error.jsonl` (failure), and progress/metrics are recorded.

Because broadcasters fan *all* of a pool's results to *all* subscribed jobs, the collector must filter:

- **Result for another job**: multiple jobs may share a pool; a result whose request ID is not in this job's `PendingRequests` map is silently dropped (`Resolve()` returns `false`).
- **Duplicate results**: `Resolve()` uses `LoadAndDelete`, so once a request ID is matched and removed, a repeat delivery no longer matches and is dropped.
- **Ordering**: results arrive out of order (the dispatcher processes requests concurrently). This is fine — the collector writes results in arrival order.

#### Cancellation / SLO expiry

After the submit phase, the `AsyncDispatcher` waits for all pending requests to be resolved (or for the context to be cancelled). On cancellation or SLO expiry it best-effort **cancels** the still-pending request IDs on the queue (so the dispatcher skips them) and **drains** any submitted-but-uncollected requests as `batch_expired` error results, so that `output_lines + error_lines == total_requests`.

---

## Job Lifecycle Impact

- **Ingestion**: Unchanged — the preprocessor still builds per-model plan files.
- **Execution**: The executor enqueues all plan entries into the request queue and then waits for results via the consumer. A job is "execution complete" when all expected results have been received (or the SLO/cancel deadline fires).
- **Finalization**: Unchanged — output files are uploaded to shared storage.
- **Cancellation / SLO expiry**: When a job is cancelled or expires, the batch-processor must remove any pending (not-yet-dispatched) requests from the request queue. This requires tracking which requests have been enqueued but not yet completed.

### Concurrency Control Interaction

With the dispatcher handling flow control, the batch-processor's concurrency model simplifies:

| Concern | Sync mode (direct dispatch) | Async mode (dispatcher) |
|---------|--------------------------|-----------------|
| Inference pool saturation | AIMD on 429/5xx (reactive) | Dispatch budget (proactive) |
| Per-endpoint concurrency | Adaptive semaphore | Dispatcher gates per pool |
| Global concurrency | Fixed semaphore | Not needed — queue is a passive buffer |
| Cross-processor coordination | None | Shared queue + single dispatcher |

In async mode, the AIMD controller and semaphores are not used — the dispatcher gates requests before they reach the inference gateway, and the batch-processor's role is "enqueuer + result collector." In sync mode, the existing concurrency model (AIMD + semaphores + direct HTTP dispatch) is retained; these semaphores live **solely in the `AIMDDispatcher` stage** of the execution pipeline (no other pipeline stage holds semaphores). The two modes select different leaf dispatchers in the same pipeline and are mutually exclusive at config level (`dispatch_mode: sync | async`).

---

## Open Questions

**Request cancellation**: When a job is cancelled, how do we efficiently remove its pending requests from the sorted set? Options:
   - Scan and delete by `job_id` (requires iterating the set — O(n)).
   - Let the dispatcher skip expired/cancelled requests (lazy cleanup) — simpler but wastes dispatch budget on dead requests.
   - Use a per-job cancellation flag that the dispatcher checks before forwarding.
