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
    recovery_attempts BIGINT NOT NULL DEFAULT 0
);

-- Schema migration for existing tables from previous versions.
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS processor_id TEXT;
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS priority BIGINT;
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS epoch BIGINT NOT NULL DEFAULT 0;
ALTER TABLE batch_items ADD COLUMN IF NOT EXISTS recovery_attempts BIGINT NOT NULL DEFAULT 0;

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

-- Queue index: unclaimed jobs ordered by priority (SLO deadline, earliest first).
CREATE INDEX IF NOT EXISTS idx_batch_items_queue
    ON batch_items (priority ASC)
    WHERE processor_id IS NULL
      AND status IS NOT NULL
      AND status::jsonb->>'status' = 'validating';

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
