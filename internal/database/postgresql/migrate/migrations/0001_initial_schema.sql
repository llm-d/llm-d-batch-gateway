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
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    expiry     BIGINT,
    tags       JSONB,
    spec       JSONB,
    status     JSONB
);

CREATE INDEX IF NOT EXISTS idx_batch_items_tenant_id ON batch_items(tenant_id);
CREATE INDEX IF NOT EXISTS idx_batch_items_expiry ON batch_items(expiry) WHERE expiry IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_batch_items_tags ON batch_items USING GIN (tags) WHERE tags IS NOT NULL;

CREATE TABLE IF NOT EXISTS file_items (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    expiry     BIGINT,
    tags       JSONB,
    purpose    TEXT,
    spec       JSONB,
    status     JSONB
);

CREATE INDEX IF NOT EXISTS idx_file_items_tenant_id ON file_items(tenant_id);
CREATE INDEX IF NOT EXISTS idx_file_items_expiry ON file_items(expiry) WHERE expiry IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_file_items_purpose ON file_items(purpose) WHERE purpose IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_file_items_tags ON file_items USING GIN (tags) WHERE tags IS NOT NULL;
