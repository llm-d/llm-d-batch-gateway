-- Copyright 2026 The llm-d Authors
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at

-- http://www.apache.org/licenses/LICENSE-2.0

-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE TABLE IF NOT EXISTS batch_items (
    id            TEXT PRIMARY KEY,
    tenant_id     TEXT NOT NULL,
    expiry        BIGINT,
    tags          JSONB,
    spec          JSONB,
    status        JSONB,
    processor_id  TEXT,
    priority      BIGINT,
    epoch         BIGINT NOT NULL DEFAULT 0,
    recovery_attempts BIGINT NOT NULL DEFAULT 0,
    resumable     BOOLEAN NOT NULL DEFAULT FALSE
);

-- Schema migration for existing tables from previous versions.
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS processor_id TEXT;
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS priority BIGINT;
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS recovery_attempts BIGINT NOT NULL DEFAULT 0;
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS resumable BOOLEAN NOT NULL DEFAULT FALSE;

-- Rows written before the queue columns existed carry the SLO in a tag and
-- have no owner. Restore the queue order from the tag and hand in-flight rows
-- to a sentinel owner so the reconciler reclaims them on its first cycle.
-- Resumable rows are excluded: a requeued row legitimately has no owner
-- (processor_id IS NULL, non-terminal) while it waits for the next dequeue,
-- and stamping it would make it unclaimable until the reconciler's next
-- cycle. Legacy rows predate the resumable flag and are always FALSE.
UPDATE batch_items
   SET priority = (tags->>'slo_unix_micro')::bigint
 WHERE priority IS NULL AND tags ? 'slo_unix_micro';
UPDATE batch_items
   SET processor_id = 'pre-migration'
 WHERE processor_id IS NULL
   AND resumable = FALSE
   AND status::jsonb->>'status' IN ('in_progress', 'finalizing', 'cancelling');

CREATE INDEX IF NOT EXISTS idx_batch_items_tenant_id ON batch_items(tenant_id);
CREATE INDEX IF NOT EXISTS idx_batch_items_expiry ON batch_items(expiry) WHERE expiry IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_batch_items_tags ON batch_items USING GIN (tags) WHERE tags IS NOT NULL;

-- Queue index: unclaimed, non-resumable jobs ordered by priority. Use a new
-- name so existing installations do not retain the pre-resumable predicate
-- through CREATE INDEX IF NOT EXISTS. Create the replacement before removing
-- the old index so an upgrade never leaves the queue without an index.
CREATE INDEX IF NOT EXISTS idx_batch_items_queue_non_resumable
    ON batch_items (priority ASC)
    WHERE processor_id IS NULL
      AND resumable = FALSE
      AND status IS NOT NULL
      AND status::jsonb->>'status' = 'validating';
DROP INDEX IF EXISTS idx_batch_items_queue;

-- Processor ownership index: find jobs owned by a specific processor for crash recovery.
CREATE INDEX IF NOT EXISTS idx_batch_items_processor
    ON batch_items (processor_id)
    WHERE processor_id IS NOT NULL;

-- Durable-until-consumed events (BatchEventChannelClient).
-- id BIGSERIAL gives FIFO ordering. event_type = int(BatchEventType). expires_at = unix seconds.
-- Rows are the source of truth (durable, late-attach-safe); NOTIFY is a latency hint.
CREATE TABLE IF NOT EXISTS batch_events (
    id         BIGSERIAL PRIMARY KEY,
    job_id     TEXT      NOT NULL,
    event_type INTEGER   NOT NULL,
    expires_at BIGINT    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_batch_events_job_id ON batch_events (job_id, id);
CREATE INDEX IF NOT EXISTS idx_batch_events_expires_at ON batch_events (expires_at);

-- Resumable ownership rides on processor_id (set at dequeue for every
-- claimed row); the lease set at activation/reclaim distinguishes a resumable
-- owner from a mere queue claim, so entry writes fence on the pair.
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS owner_lease_expires_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_batch_items_resumable_lease
    ON batch_items (owner_lease_expires_at) WHERE resumable = TRUE;
-- Requeued resumable rows waiting to be re-claimed by PQDequeue.
CREATE INDEX IF NOT EXISTS idx_batch_items_queue_resumable
    ON batch_items (priority ASC)
    WHERE processor_id IS NULL AND resumable = TRUE;

-- One metadata row per request of a resumable batch. No payloads: bodies and
-- results live only in the file store.
CREATE TABLE IF NOT EXISTS batch_entries (
    batch_id      TEXT        NOT NULL,
    request_id    TEXT        NOT NULL,
    ordinal       BIGINT      NOT NULL,
    custom_id     TEXT        NOT NULL,
    status        TEXT        NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending','in_flight','completed','failed')),
    attempts      BIGINT      NOT NULL DEFAULT 0,
    submitted_by  TEXT,
    result_key    TEXT,
    result_bytes  BIGINT,
    result_sha256 TEXT,
    error_code    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at  TIMESTAMPTZ,
    PRIMARY KEY (batch_id, request_id),
    CONSTRAINT fk_batch_entries_batch FOREIGN KEY (batch_id)
        REFERENCES batch_items(id) ON DELETE CASCADE
);
-- Schema migration for entries written before submitter tracking existed.
ALTER TABLE batch_entries ADD COLUMN IF NOT EXISTS submitted_by TEXT;
CREATE INDEX IF NOT EXISTS idx_batch_entries_ordinal ON batch_entries (batch_id, ordinal);
CREATE INDEX IF NOT EXISTS idx_batch_entries_open
    ON batch_entries (batch_id) WHERE status IN ('pending','in_flight');
-- Global lookup by request ID: request IDs are stable and batch-scoped
-- (SHA-1 of batch ID + custom ID), so the residue check can resolve an
-- unrouted delivery to its entry without knowing the batch. Not UNIQUE:
-- creation is best-effort on upgrade; the residue check treats any open
-- entry as "not safe to acknowledge", which is conservative under duplicates.
CREATE INDEX IF NOT EXISTS idx_batch_entries_request_id ON batch_entries (request_id);
