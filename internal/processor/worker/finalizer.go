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

package worker

import (
	"context"
	"fmt"
	"strconv"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

// finalizationTimeout bounds how long finalizeJob waits for file uploads and DB
// writes before giving up. Storage and DB clients have their own timeouts, but
// this provides an explicit application-level bound — same pattern as
// panicRecoveryTimeout. Declared as var (not const) so tests can shorten it.
var finalizationTimeout = 5 * time.Minute

// storeFileRecord creates a file metadata record in the database.
// Used for both output and error files.
// If the batch has a user-provided output_expires_after_seconds tag, it takes
// precedence over the config default (DefaultOutputExpirationSeconds).
func (p *Processor) storeFileRecord(
	ctx context.Context,
	fileID, fileName, tenantID string,
	size int64,
	batchTags db.Tags,
) error {
	now := time.Now().Unix()

	expiresAt := p.resolveOutputExpiration(now, batchTags)
	var expiresAtPtr *int64
	if expiresAt > 0 {
		expiresAtPtr = &expiresAt
	}

	fileObj := &openai.FileObject{
		ID:        fileID,
		Bytes:     size,
		CreatedAt: now,
		ExpiresAt: expiresAtPtr,
		Filename:  fileName,
		Object:    "file",
		Purpose:   openai.FileObjectPurposeBatchOutput,
		Status:    openai.FileObjectStatusProcessed,
	}
	fileItem, err := converter.FileToDBItem(fileObj, tenantID, db.Tags{})
	if err != nil {
		return fmt.Errorf("failed to convert file to db item: %w", err)
	}

	if err := p.files.db.DBStore(ctx, fileItem); err != nil {
		return fmt.Errorf("failed to store file record: %w", err)
	}
	return nil
}

// ensureFileRecord stores a file record, treating a store failure as success
// when a matching record already exists for the same deterministic file ID: a
// re-finalization re-derives the ID, so an earlier incarnation may have
// committed the record between its artifact commit and its terminal write
// (a duplicate insert errors in Postgres). Only identity-relevant fields are
// compared — CreatedAt/ExpiresAt legitimately drift between incarnations.
func (p *Processor) ensureFileRecord(
	ctx context.Context,
	fileID, fileName, tenantID string,
	size int64,
	batchTags db.Tags,
) error {
	storeErr := p.storeFileRecord(ctx, fileID, fileName, tenantID, size, batchTags)
	if storeErr == nil {
		return nil
	}

	items, _, _, err := p.files.db.DBGet(ctx, &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{fileID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		return storeErr
	}
	item := items[0]
	fileObj, err := converter.DBItemToFile(item)
	if err != nil {
		return storeErr
	}
	if item.TenantID == tenantID && item.ID == fileID &&
		item.Purpose == string(openai.FileObjectPurposeBatchOutput) &&
		fileObj.Filename == fileName && fileObj.Bytes == size {
		return nil
	}
	return storeErr
}

// resolveOutputExpiration returns the ExpiresAt timestamp for an output or error file.
// Priority: user-provided output_expires_after_seconds tag > config default.
// Returns 0 (no expiration) if neither is set.
func (p *Processor) resolveOutputExpiration(now int64, batchTags db.Tags) int64 {
	if s, ok := batchTags[batch_types.TagOutputExpiresAfterSeconds]; ok {
		if ttl, err := strconv.ParseInt(s, 10, 64); err == nil && ttl > 0 {
			return now + ttl
		}
	}
	if ttl := p.cfg.DefaultOutputExpirationSeconds; ttl > 0 {
		return now + ttl
	}
	return 0
}
