# Consistency Simulation Harness

- **Revision**: 1
- **Status**: Proposal

---

## Problem

The consistency review (2026-08-18) identified seven classes of cross-store crash windows (F1 through F7) arising from non-atomic write sequences across Redis, Postgres, S3, and local disk. Before rearchitecting, we need an executable proof that these bugs exist today, and a regression gate that prevents them from returning once fixed.

These windows come from the topology itself: three stores, no cross-store
transactions. That was a reasonable choice for the first version, and any
system built this way has the same windows. The harness makes each one
reproducible so the rearchitecture can fix them one at a time; the ratchet
tracks which are fixed. A finding that is already a known, accepted
limitation should be marked as such during review.

The existing test tiers cannot do this. Unit and integration tests run against in-memory mocks, so there are no cross-store windows to hit. E2E tests run the happy path against a Kind cluster with no fault injection and no way to stop a process at a specific instruction.

## Goals

1. Deterministically reproduce each finding from the review as a named, runnable scenario.
2. Check global invariants against the real stores, not against what any one component believes.
3. Act as a ratchet: each scenario is tracked as `broken` or `fixed`; the fixed set only grows.
4. Exercise realistic inference timing without GPUs, via vllm-vcr.

Non-goals: performance benchmarking, Kubernetes-level chaos (node eviction is modeled as SIGKILL of a container).

## Topology

One compose file (`test/simulation/compose.yaml`) running real components against real stores:

```
                    ┌──────────┐
   sim runner ──────► toxiproxy ├──┬──► postgres
   (Go test)        └──────────┘  ├──► redis
        │                         └──► minio
        │  failpoint HTTP
        ▼
  apiserver · batch-processor · batch-gc     (failpoints build)
        │
        ▼
  vllm frontend ──► vllm-vcr play            (simulated engine-core)
```

- **Stores**: postgres, redis, minio. Real images, fresh volumes per run.
- **Gateway binaries**: built with `-tags failpoints` (see below). All store connections routed through toxiproxy so the runner can inject network faults per component per store.
- **Inference**: a vLLM frontend container with `vllm-vcr play` as its engine-core. The latency model is configured so each request takes 2 to 5 seconds, holding jobs in `in_progress` long enough to kill processes mid-execution deterministically. No model weights, no GPU.
- **Time compression**: config overrides only, no code changes. Reconciler interval 5s, collector interval 5s, heartbeat 1s, poll interval 500ms, completion windows of tens of seconds.

## Fault injection

Two mechanisms, chosen per scenario:

### Failpoints (crash exactly here)

A small in-repo package (`internal/util/failpoint`) instruments the identified windows. It was chosen over gofail after weighing the tradeoff: gofail's comment-driven code generation rewrites source in place at build time, which complicates the Makefile and the diff, while the in-repo version is one explicit call per window, compiled to an inlined empty function unless the binary is built with `-tags failpoints`. Failpoints are armed via the `FAILPOINTS` environment variable at process start (`name=exit`, `name=exit(code)`, `name=sleep(ms)`, `;`-separated). Each is named for the window it sits in:

| Failpoint | Location | Finding |
|---|---|---|
| `apiserver/after-batch-dbstore` | between DBStore and PQEnqueue | F1a |
| `apiserver/after-cancel-pqdelete` | between PQDelete and DBUpdate | F2a |
| `apiserver/after-cancel-dbupdate` | between DBUpdate and event send | F2b |
| `processor/after-dequeue` | between PQDequeue and InFlightSet | F4 |
| `processor/after-blob-store` | between S3 Store and file DBStore | F5 |
| `processor/after-file-records` | between file records and terminal write | F6 |
| `processor/before-terminal-write` | before UpdatePersistentStatus | F3 |

The armed action is `panic` or `os.Exit(137)`, simulating OOM kill at that instruction. Sleep actions are also available to hold a window open while another actor races.

### Toxiproxy (false failures)

Failpoints cannot produce false failures, where an operation lands server-side but the client sees an error. Toxiproxy can, by cutting the response path after the request is delivered:

- F1b: PQEnqueue succeeds in Redis, client sees timeout, compensating DBDelete runs.
- F1c: enqueue fails and the compensating DBDelete also fails (full partition).
- F4: the window between PQDequeue and InFlightSet leaves a dequeued job invisible; the reconciler re-enqueues it and the job runs twice.

Every component-store connection is routed through toxiproxy on its own proxy (`config/toxiproxy.json`, nine proxies), so a toxic partitions exactly one edge: apiserver↔redis can lie while processor↔redis stays healthy. Toxics are applied and removed by the runner per scenario through the control API; harness cleanup heals all proxies. Compose only; network-fault scenarios skip on the kind backend.

## Invariant checker

A `simcheck` package asserted by every scenario, with two modes.

**Trace invariants** (continuous): an observer goroutine polls batch status via the API and directly from Postgres at 100ms, recording per-batch status timelines. Asserted against the legal transition graph from the processor design doc:

- No transition out of a terminal state (catches F3: `failed` then `completed`).
- No regression `cancelling → in_progress` (catches the blind read-modify-write).
- Timestamps monotonic and preserved across transitions.

**End-state invariants** (after quiesce plus grace period):

| Invariant | Statement | Catches |
|---|---|---|
| Liveness | every batch is terminal, queued, or owned by a fresh lease/heartbeat | F1a, F2a, F6 |
| Conservation | `completed + failed + rejected == total` for every terminal batch | F4 double-execution |
| Referential (record → blob) | every `output_file_id` / `error_file_id` resolves to a file record and an S3 object | F6 |
| Referential (blob → record) | every S3 object older than the grace period has a file record | F5 |
| Cancel honored | a batch whose cancel was acked 200 ends in `cancelled` (or `completed` only if finalizing had begun) | F2 |
| API honesty | a create that returned 5xx never produces a batch that runs | F1b, F1c |
| Single execution | total inference requests observed by vllm-vcr for a batch ≤ line count | F4 |

The last invariant uses vllm-vcr's request log as a witness: the simulated backend counts every request it serves, so duplicate execution is measured at the only place it cannot be hidden.

## Scenarios

Each finding maps to one Go test under `test/simulation/` with build tag `simulation`. Shape:

1. Reset stores, start topology, arm failpoint or toxic.
2. Drive the API (create batch with N lines, optionally cancel at a phase).
3. Trigger the fault, then disarm and let recovery mechanisms run at compressed intervals.
4. Quiesce, run `simcheck`, compare against the ratchet manifest.

A seeded chaos mode complements the deterministic scenarios: random SIGKILL and partition schedules over a stream of batches, followed by the same invariant check. Failures replay from the seed. Chaos mode finds windows the review missed; anything it finds gets promoted to a named scenario.

## Ratchet mechanism

`test/simulation/ratchet.yaml` records the expected outcome per scenario:

```yaml
scenarios:
  F1a_create_crash_before_enqueue: broken   # invariant violation expected and asserted
  F3_terminal_overwrite:           broken
  F5_orphaned_blob:                broken
  # flipped to fixed as rearchitecture phases land
```

Semantics enforced in CI:

- A scenario marked `broken` must produce its expected invariant violation. This proves the harness still detects the bug and the reproduction has not rotted.
- A scenario marked `fixed` must produce no violations. A regression fails CI.
- A `broken` scenario that passes cleanly fails CI with "promote to fixed", so the manifest tracks reality.
- Flipping `fixed` back to `broken` is a reviewable diff, never a silent change.

The rearchitecture is done when the manifest is all `fixed` and chaos mode runs clean for a configured budget.

## Status (2026-08-18)

Implemented and green: topology (compose + host-vcr fallback), failpoints at
seven windows, timeline observer with legal-transition checking, progress
count recording, per-run JSONL event logs and harvested Tempo traces, the
ratchet manifest, and seven scenarios that each reproduce their finding:

| Scenario | Invariant violated | Mechanism |
|---|---|---|
| F1a_create_crash_before_enqueue | API honesty | crash between DBStore and PQEnqueue |
| F2a_cancel_reverted | cancel effectiveness | crash between PQDelete and DBUpdate |
| F2b_cancel_event_lost | legal transitions | crash between DBUpdate and cancel event |
| F3_terminal_overwrite | terminal immutability | sleep before CAS-less terminal write |
| F4a_worker_crash_strands_job | work conservation | SIGKILL + pod replacement |
| F5_orphaned_blob | blob referential | crash between S3 Store and file record |
| F6_finalization_strand | results reachability | crash between file records and completed write |
| F1b_enqueue_false_failure | API honesty | enqueue lands, response blackholed; compensation deletes the row under a running job |
| F1c_create_compensation_partition | API honesty | partition after DBStore; enqueue and compensating delete both fail |
| F4b_duplicate_execution | single execution | dequeue held past staleness; reconciler re-enqueues; both copies run |

The last three (PR 2) need the network to lie: store connections run through
per-component toxiproxy proxies, and vllm-vcr's request log is the witness
(the engine counts every request it serves, so phantom and duplicate
execution are measured where they cannot be hidden). F4b found that the
dequeue-time runnable gate accepts `in_progress`, so a re-enqueued duplicate
launches as long as the first execution is still running; and that requests
already sent keep executing after the losing worker's heartbeat abort.

Incidental fixes landed while building, each upstreamable as its own PR
independent of the harness:

- schema-apply retry on concurrent `CREATE TABLE IF NOT EXISTS` (SQLSTATE
  23505), a real cold-start crash when the three binaries boot together
- OTel tracer init for batch-gc plus a `reconciler.triage` span, making the
  reconciler's interventions visible in traces
- `GO_BUILD_TAGS` build arg in the three Dockerfiles

Harness-side hardening: short-timeout observer polls (a killed apiserver's
dead port proxy would otherwise hang a poll through the interesting window).

## Follow-up plan

**PR 3 — chaos mode.** Seeded random SIGKILL and partition schedules over a
stream of batches, then the full invariant sweep; failures replay from the
seed. New windows it finds get promoted to named scenarios. Also worth a
named scenario from this session's accidents: a version-mismatched inference
backend makes every request fail instantly with 500s and batches "complete"
full of error bodies — completion should be distinguishable from
all-requests-failed.

**Fidelity: the kind backend.** The harness supports two stack backends
(`SIM_BACKEND`): the default compose topology, and a dev-deploy'd Kind
cluster (`make sim-kind-deploy`). Kind buys what compose cannot fake: real
pod replacement on kill, `ENABLE_GIE=true` putting the EPP ext-proc on the
inference path so AIMD sees genuine 429/5xx backpressure, and
`kubectl scale` for the multi-replica async scenarios. Scenario knobs
translate per backend (failpoints via `kubectl set env`, the
stale-heartbeat variant via a helm value); traces come from the cluster's
Jaeger instead of Tempo. Compose remains the fast inner loop. On the vcr
side, prefer the slim image (neuralmagic/vllm-vcr#86) over the host-vcr
fallback once published; engine-side spans (neuralmagic/vllm-vcr#85) extend
the trace lane to the backend.

**CI.** Run `make test-simulation` in a workflow (linux runners can build the
gateway images; inference needs the slim vcr image or the release binaries).
Gate merges on the ratchet: `fixed` scenarios must pass, `broken` scenarios
must still reproduce.

**Async dispatch mode (llm-d-async).** Async mode adds a distributed state
surface of its own: requests submitted fire-and-forget to the llm-d-async
request queue with an in-memory `PendingRequests` map as the only record,
results popped destructively from a shared per-pool result queue by
long-lived `ResultBroadcaster`s. The async queues are plain Redis structures
on the stack's existing Redis, so the topology is a processor config
(`processor-async.yaml`) plus a harness-run queue consumer
(`asyncbridge.go`) that plays the worker fleet: pop request, forward to vcr,
push result. Implemented and reproducing: A1_async_result_destruction —
processor killed after submission, pending map gone, replacement's
broadcasters pop and discard every returning result, reconciler terminalizes
the orphan as failed. Remaining async scenarios: broadcaster restart losing
queued results, duplicate submission on re-run, multi-replica destruction
(`kubectl scale` on the kind backend). The single-store design must answer
how the pending set becomes durable in async mode.

**Then the rearchitecture** (see the consistency review): CAS everywhere,
Postgres queue with leases, cancel via status + LISTEN/NOTIFY, blob
mark-and-sweep, Redis retirement. Each phase flips its scenarios to `fixed`;
the ratchet report tracks the burndown.

## Dependencies

- `internal/util/failpoint`: in-repo failpoints (see Fault injection above). Zero cost when not built with the tag; no external dependency.
- `github.com/Shopify/toxiproxy/v2` client: network fault injection, maintained by Shopify (PR 2).
- vllm-vcr: simulated inference backend. Single image bundling the vLLM Rust frontend and the mock engine-core; latency via `MOCK_TTFT_MS` / `MOCK_ITL_MS` env vars.
