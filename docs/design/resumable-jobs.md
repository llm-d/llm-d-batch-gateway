# Resumable Batch Jobs Design

- **Revision**: 1
- **Last Updated**: 2026-10-14
- **Issue**: [#645](https://github.com/llm-d/llm-d-batch-gateway/issues/645)

---

## Problem

A batch processor holds a batch's in-flight results in pod-local memory and
pod-local output files. Losing the pod mid-batch — crash, OOM kill, node
eviction, scale-in — loses all results fetched since the last progress flush,
and the recovered batch restarts inference from scratch. Batches of tens of
thousands of requests could burn hours of inference and lose it all.

---

## Design Overview

Every batch runs on the **resumable pipeline**: per-request results are
checkpointed to object storage as they arrive, ownership of an in-flight batch
is a lease in PostgreSQL fenced by an epoch counter, and finalization streams
the checkpointed objects directly into the output artifact. A replacement
processor picks up a disrupted batch from its checkpoints and only re-runs the
requests whose results were never committed.

`dispatch_mode` selects only the request transport — `sync` dispatches
directly over HTTP with AIMD-controlled concurrency, `async` submits to
llm-d-async queues — and is orthogonal to durability: the checkpointing,
ownership, and finalization machinery is identical.

```
apiserver:  create batch (validating, resumable = FALSE)

processor:  PQDequeue claims fresh rows (resumable = FALSE) and requeued rows
            (resumable = TRUE, takes the ownership lease)
            preprocess: input file → request manifest (ordinal, stable IDs)
            ActivateResumableBatch: batch_items → in_progress, resumable = TRUE,
              lease set on the claimed processor; batch_entries seeded
              (metadata only)
            execution: for each non-terminal entry —
              dispatch (beforeSubmit → MarkEntryInFlight, fenced)
              result arrives → checkpoint result object to
                results/<batchID>/<requestID>.jsonl → fenced entry commit
            finalization: stream entry objects in ordinal order (size + SHA-256
              verified) → multipart upload → fenced terminal transition

disruption: pod death or lease expiry → batch-gc reconciler requeues the row
            (fenced, epoch+1, ownership cleared) → any processor re-dequeues it,
            re-derives the manifest, skips terminal entries, re-submits the rest
```

### Ownership and fencing

- The **owner** of an activated batch is the processor pod's **hostname**
  (matching the reconciler's pod-liveness model and the async consumer ID).
  Two incarnations of the same hostname are distinguished by the **epoch**.
  Ownership rides on `processor_id`, set at dequeue for every claimed row; the
  lease set at activation distinguishes a resumable owner from a mere queue
  claim.
- The owner holds a **lease** (`resumable.lease_duration`, default 5m, renewed
  at half the interval until `runJob` returns — starting at the pickup, before
  the terminal-bound routing, so an artifact assembly bounded by
  `finalizeTimeout` cannot outlive the lease). A batch becomes reclaimable
  when the owner is absent from the reconciler's live-pod set or the lease
  has expired.
- Every state write is a compare-and-swap on `(status, epoch)`. A fenced-out
  **zombie** — a former owner still executing after ownership moved — can
  checkpoint result objects (they are content-addressed and idempotent) but can
  never commit entry state or finalize the batch.

### Result checkpointing

Each request's result is written to
`results/<batchID>/<requestID>.jsonl` in the batch's folder. Keys are plain and
deterministic, never epoch-scoped: the entries row stores the winning object's
key, so committed work survives any reclaim. Nothing is ever overwritten — on a
key collision with mismatched bytes (a concurrent duplicate inference), the
writer commits a unique fallback key
`results/<batchID>/<requestID>.<uuid>.jsonl` instead. Garbage objects (orphans
from a crash between PUT and commit, losing fallbacks) are never referenced by
entries and are removed with the prefix cleanup described below.

### Finalization

Finalization keyset-paginates `batch_entries` in ordinal order, fetches objects
with bounded concurrency (`resumable.finalize_concurrency`, default 16),
verifies each object's size and SHA-256, and streams the lines into a
multipart upload — there are no pod-local output files. Artifact routing
matches the sync pipeline's classification: a line carrying an error object
goes to the error file, while a failed request whose result preserved a
non-2xx HTTP response goes to the output file (the entries row keeps no error
code for those). Artifact keys are scoped by the ownership epoch, so a
re-finalization after a requeue (which bumps the epoch) writes its own keys:
it is never blocked by — and never clobbers — an artifact committed by an
earlier generation whose terminal write failed, even when the replacement
resolved additional entries in between and derives different bytes. Within
one generation the entry set is frozen once finalization began, so a
same-generation retry re-derives the same key and verifies the committed
bytes. The winning file IDs are published solely through the fenced terminal
transition; artifacts committed by losing generations are never referenced
by a terminal document, and their file records expire and are removed by the
file GC. The terminal transition is fenced and idempotent, so a crash
mid-finalization is safely retried by whoever owns the batch next.

---

## Semantics

The pipeline provides **at-least-once** request execution with exactly-once
result accounting:

- A request is re-submitted after a disruption unless its result was already
  committed to an entries row. Committed results are never re-run and appear
  exactly once in the output artifact (strict ordinal order, verified hashes).
- **Async results are consumed durably** (llm-d-async ≥ v1.0.0): each result
  is claimed under a lease and acknowledged only after its durable
  checkpoint, so a crash between the delivery and the checkpoint redelivers
  the completed result instead of repeating its inference. A result whose
  checkpoint fails (or whose ownership was lost) is left unacked for exactly
  that redelivery; a duplicate is dropped by the first-wins entry commit.
  Delivery is routed per request — only the job whose pending set owns the
  request receives it — so exactly one consumer can release each
  acknowledgement, and residue is acknowledged by the broadcaster only
  after a durable check (`HasOpenEntryByRequest`) proves no open entry can
  still account for it: a delivery for a terminal or deleted entry is
  discarded, while one for an open entry stays unacked so its lease
  redelivers it to whichever processor recovers its batch.
- **Stranded deliveries are recovered across processors**: result routes
  are per-processor (`results/<pool>/<consumer>`), and a request submitted by
  processor A has its result delivered on A's route even after A dies. The
  entry therefore records its **submitter** (`batch_entries.submitted_by`,
  stamped at `MarkEntryInFlight` and backfilled at orphan requeue), and a
  processor that reclaims a batch consumes each previous submitter's route
  through a refcounted recovery broadcaster for the batch's models. An
  in-flight entry is not re-submitted immediately: the resuming owner
  pre-stores it in its pending set and defers re-submission for a recovery
  window (`resumable.recovery_window`, default the 5m claim lease), during
  which the stranded delivery — completed, failed, or deadline-exceeded —
  is checkpointed through the recovery route; only an entry with no delivery
  by then falls back to submission. The window ends early once every
  deferred entry's delivery has been checkpointed (its pending-set entry
  resolved), so a fully recovered batch does not wait it out. Recovering
  entries are pre-stored before the recovery broadcasters start claiming
  (a delivery claimed before its entry is pre-stored routes to no owner and
  bounces on its lease), and recovery claims hold the route's short lease
  (60s, not the 5m crash-detection window): two processors recovering
  different batches from the same dead submitter occasionally claim each
  other's deliveries, and a mis-claimed foreign delivery — held unacked by
  the residue gate until its lease lapses — must cost one brief bounce, not
  a full window. A takeover therefore re-runs inference only for results
  genuinely lost by the queue, not for results stranded on a dead owner's
  route.
- **Duplicate inference** is therefore possible only when a claimed delivery
  is lost outright — a result fetched by a dead or fenced-out owner that was
  never committed, stranded on its route, AND not recovered within the
  recovery window is re-inferred by the new owner; a committed or recovered
  result is not. The worst case is one duplicate dispatch per non-terminal
  request per takeover, not per retry tick.
- A disrupted incarnation's queued submissions are **never cancelled**: the
  queue reports a cancelled request as a `CANCELLED` result, and the next
  incarnation — which re-submits the same stable request IDs — would
  checkpoint that stale result as a request failure. Residue drains through
  the queue and its duplicate results are dropped on commit; the queue is
  cancelled only for terminal aborts (user cancel, SLO expiry), where no
  resumption follows.
- Final counts are computed from `COUNT(*) FILTER` over `batch_entries` at
  terminalization, so progress is exact even across takeovers.

---

## Topology rules

- The resumable pipeline is the only pipeline; there is no
  `resumable.enabled` flag. A processor without a resumable store fails
  construction.
- `dispatch_mode` is pure transport selection and may be set per deployment
  or flipped in a rolling upgrade: any processor can claim and resume any
  batch either way.
- PostgreSQL is the only supported DB backend.
- No restrictions on processor replicas or `numWorkers`: ownership is leased
  and fenced per batch, so any replica count works.
- Rows from before the unified pipeline (non-resumable rows in an execution
  status) have no entries and cannot be resumed: startup recovery fails them
  closed. Drain in-flight batches before upgrading.

---

## Operational notes

- **Identity**: the owner is the pod hostname. The chart deploys the processor
  as a StatefulSet, so identities are stable and debuggable in logs and DB
  rows; the epoch makes a reused hostname (pod replacement) safe.
- **`results/` prefix lifecycle**: result objects live under
  `results/<batchID>/` in the batch's folder. They are intermediate state
  while the batch is in flight and garbage after finalization. The batch-gc
  purges the whole prefix (`DeletePrefix`) before deleting the batch row, so
  entries, row, and objects disappear together.
- **Incomplete multipart uploads**: a finalizer that crashes mid-stream leaves
  an incomplete multipart upload that is invisible to listing. Configure an S3
  bucket lifecycle rule (`AbortIncompleteMultipartUpload`, e.g. after 1 day) to
  sweep them; complete uploads are unaffected because
  `CompleteMultipartUpload` is atomic.
- **GC behavior**: the batch-gc reconciler requeues orphaned resumable rows
  (fenced `epoch+1`, ownership cleared) when the owner pod is gone or the lease
  has expired; it never finalizes them — artifact assembly is processor work.
  `cancelling` and SLO-expired orphans are requeued too and finalized by
  whichever processor picks them up.
- **Tunables** (chart values under `processor.config.resumable`):
  `leaseDuration` (default `5m`), `finalizeConcurrency` (default `16`),
  `finalizeTimeout` (default `30m`). All must be positive; validation fails at
  startup otherwise.
