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

// Package collector implements the garbage collection logic for expired batch jobs and files.
package collector

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"golang.org/x/sync/errgroup"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	fs "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

// GarbageCollector handles garbage collection of expired batch jobs and files.
type GarbageCollector struct {
	batchDBClient   db.BatchDBClient
	fileDBClient    db.FileDBClient
	filesClient     fs.BatchFilesClient
	dryRun          bool
	interval        time.Duration
	maxConcurrency  int
	onCycleComplete func(*Result)
}

// NewGarbageCollector creates a new garbage collector.
// maxConcurrency bounds how many items are deleted in parallel within a single page.
// onCycleComplete, if non-nil, is called after each GC cycle with the result.
func NewGarbageCollector(batchDBClient db.BatchDBClient, fileDBClient db.FileDBClient, filesClient fs.BatchFilesClient, dryRun bool, interval time.Duration, maxConcurrency int, onCycleComplete func(*Result)) *GarbageCollector {
	return &GarbageCollector{
		batchDBClient:   batchDBClient,
		fileDBClient:    fileDBClient,
		filesClient:     filesClient,
		dryRun:          dryRun,
		interval:        interval,
		maxConcurrency:  maxConcurrency,
		onCycleComplete: onCycleComplete,
	}
}

// Result contains the results of a garbage collection run.
type Result struct {
	BatchesDeleted int
	FilesDeleted   int
	BatchesFailed  int
	FilesFailed    int
}

// pageSize is the number of expired items to fetch per database query.
const pageSize = 100

// collectBatchJobs fetches and processes expired batch jobs.
func (c *GarbageCollector) collectBatchJobs(ctx context.Context, result *Result) error {
	logger := logr.FromContextOrDiscard(ctx)
	query := &db.BatchQuery{BaseQuery: db.BaseQuery{Expired: true}}

	cursor := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batches, nextCursor, expectMore, err := c.batchDBClient.DBGet(ctx, query, false, cursor, pageSize)
		if err != nil {
			return fmt.Errorf("failed to query expired batch jobs: %w", err)
		}

		var mu sync.Mutex
		var grp errgroup.Group
		grp.SetLimit(c.maxConcurrency)

		for _, batch := range batches {
			grp.Go(func() error {
				deleted, err := c.processBatch(ctx, batch)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					result.BatchesFailed++
					logger.Error(err, "Failed to process batch job", "jobID", batch.ID)
				} else if deleted {
					result.BatchesDeleted++
				}
				return nil // don't short-circuit other deletions
			})
		}
		_ = grp.Wait()

		if !expectMore {
			break
		}
		cursor = nextCursor
	}

	return nil
}

// collectFiles fetches and processes expired files.
func (c *GarbageCollector) collectFiles(ctx context.Context, result *Result) error {
	logger := logr.FromContextOrDiscard(ctx)

	// includeStatic=true to retrieve Spec (contains the filename needed for physical file deletion).
	query := &db.FileQuery{
		BaseQuery: db.BaseQuery{Expired: true},
	}

	cursor := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		files, nextCursor, expectMore, err := c.fileDBClient.DBGet(ctx, query, true, cursor, pageSize)
		if err != nil {
			return fmt.Errorf("failed to query expired files: %w", err)
		}

		var mu sync.Mutex
		var grp errgroup.Group
		grp.SetLimit(c.maxConcurrency)

		for _, file := range files {
			grp.Go(func() error {
				deleted, err := c.processFile(ctx, file)
				mu.Lock()
				defer mu.Unlock()
				if err != nil {
					result.FilesFailed++
					logger.Error(err, "Failed to process file", "fileID", file.ID)
				} else if deleted {
					result.FilesDeleted++
				}
				return nil // don't short-circuit other deletions
			})
		}
		_ = grp.Wait()

		if !expectMore {
			break
		}
		cursor = nextCursor
	}

	return nil
}

// RunLoop runs the garbage collector in a continuous loop at the configured interval.
// It blocks until the context is cancelled, executing a GC cycle immediately on start
// and then on every tick of the interval.
func (c *GarbageCollector) RunLoop(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("Starting garbage collection loop", "interval", c.interval)

	// Run immediately on startup before waiting for the first tick.
	c.run(ctx)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Garbage collection loop stopped")
			return ctx.Err()
		case <-ticker.C:
			c.run(ctx)
		}
	}
}

// run executes a single GC cycle and logs the result.
func (c *GarbageCollector) run(ctx context.Context) *Result {
	logger := logr.FromContextOrDiscard(ctx)

	result := &Result{}

	if err := c.collectBatchJobs(ctx, result); err != nil {
		logger.Error(err, "Failed to collect batch jobs")
	}
	if err := c.collectFiles(ctx, result); err != nil {
		logger.Error(err, "Failed to collect files")
	}

	logger.Info("Garbage collection cycle completed",
		"batchesDeleted", result.BatchesDeleted,
		"batchesFailed", result.BatchesFailed,
		"filesDeleted", result.FilesDeleted,
		"filesFailed", result.FilesFailed,
	)

	if c.onCycleComplete != nil {
		c.onCycleComplete(result)
	}

	return result
}

// processBatch deletes an expired batch job. Returns true if the job was deleted.
func (c *GarbageCollector) processBatch(ctx context.Context, job *db.BatchItem) (bool, error) {
	logger := logr.FromContextOrDiscard(ctx)

	if c.dryRun {
		logger.V(logging.DEBUG).Info("Expired job found", "jobID", job.ID, "expiry", job.Expiry, "action", "dry-run")
		return false, nil
	}

	// Purge the resumable result objects before the row goes away: the
	// entries cascade with the row, so afterwards nobody can derive the
	// object keys anymore. Best-effort — for non-resumable jobs the prefix
	// simply holds no objects, and a missing tenant only skips the purge.
	if folderName, err := ucom.GetFolderNameByTenantID(job.TenantID); err != nil {
		logger.Error(err, "Failed to derive folder name; skipping result prefix purge",
			"jobID", job.ID, "tenantID", job.TenantID)
	} else if _, err := c.filesClient.DeletePrefix(ctx, folderName, "results/"+job.ID+"/"); err != nil {
		logger.Error(err, "Failed to purge resumable result objects",
			"jobID", job.ID, "folderName", folderName)
	}

	deletedIDs, err := c.batchDBClient.DBDelete(ctx, []string{job.ID})
	if err != nil {
		return false, err
	}

	if len(deletedIDs) > 0 {
		logger.V(logging.DEBUG).Info("Expired job deleted", "jobID", job.ID, "expiry", job.Expiry)
		return true, nil
	}

	return false, nil
}

// processFile deletes an expired file's physical storage and DB metadata. Returns true if the file was deleted.
func (c *GarbageCollector) processFile(ctx context.Context, file *db.FileItem) (bool, error) {
	logger := logr.FromContextOrDiscard(ctx)

	if c.dryRun {
		logger.V(logging.DEBUG).Info("Expired file found", "fileID", file.ID, "expiry", file.Expiry, "action", "dry-run")
		return false, nil
	}

	fileObject, err := converter.DBItemToFile(file)
	if err != nil {
		return false, fmt.Errorf("failed to convert DBItem to FileObject: %w", err)
	}

	folderName, err := ucom.GetFolderNameByTenantID(file.TenantID)
	if err != nil {
		return false, fmt.Errorf("failed to get folder name for tenant %q: %w", file.TenantID, err)
	}

	// Delete the physical file first, then the DB metadata. If the file is already
	// gone (e.g. from a previous partial GC cycle), proceed to delete the metadata.
	storageName := ucom.FileStorageName(fileObject.ID, fileObject.Filename)
	if err := c.filesClient.Delete(ctx, storageName, folderName); err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error(err, "Failed to delete physical file", "fileID", file.ID, "storageName", storageName, "folderName", folderName)
		return false, fmt.Errorf("failed to delete physical file with ID %v : %w", file.ID, err)
	}

	deletedIDs, err := c.fileDBClient.DBDelete(ctx, []string{file.ID})
	if err != nil {
		return false, err
	}

	if len(deletedIDs) > 0 {
		logger.V(logging.DEBUG).Info("Expired file deleted", "fileID", file.ID, "expiry", file.Expiry)
		return true, nil
	}

	return false, nil
}
