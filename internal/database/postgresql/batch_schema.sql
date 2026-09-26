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

CREATE TABLE IF NOT EXISTS batch_manifests (
    batch_id      TEXT PRIMARY KEY REFERENCES batch_items(id) ON DELETE CASCADE,
    version       INTEGER NOT NULL,
    owner_epoch   BIGINT NOT NULL,
    entries       JSONB NOT NULL,
    completed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (jsonb_typeof(entries) = 'array')
);

CREATE TABLE IF NOT EXISTS batch_request_attempts (
    batch_id      TEXT NOT NULL REFERENCES batch_items(id) ON DELETE CASCADE,
    request_id    TEXT NOT NULL,
    attempt       INTEGER NOT NULL,
    owner_epoch   BIGINT NOT NULL,
    submitted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, request_id, attempt)
);

CREATE TABLE IF NOT EXISTS batch_result_checkpoints (
    batch_id       TEXT NOT NULL REFERENCES batch_items(id) ON DELETE CASCADE,
    request_id     TEXT NOT NULL,
    owner_epoch    BIGINT NOT NULL,
    result         JSONB NOT NULL,
    checkpointed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (batch_id, request_id)
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
UPDATE batch_items
   SET priority = (tags->>'slo_unix_micro')::bigint
 WHERE priority IS NULL AND tags ? 'slo_unix_micro';
UPDATE batch_items
   SET processor_id = 'pre-migration'
 WHERE processor_id IS NULL
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
