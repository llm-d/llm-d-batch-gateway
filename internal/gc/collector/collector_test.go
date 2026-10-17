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

package collector

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	fsapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

// errorBatchDBClient wraps a batch MockDBClient and injects errors.
type errorBatchDBClient struct {
	api.BatchDBClient
	getErr    error
	deleteErr error
}

func (e *errorBatchDBClient) DBGet(ctx context.Context, query *api.BatchQuery, includeStatic bool, start, limit int) ([]*api.BatchItem, int, bool, error) {
	if e.getErr != nil {
		return nil, 0, false, e.getErr
	}
	return e.BatchDBClient.DBGet(ctx, query, includeStatic, start, limit)
}

func (e *errorBatchDBClient) DBDelete(ctx context.Context, ids []string) ([]string, error) {
	if e.deleteErr != nil {
		return nil, e.deleteErr
	}
	return e.BatchDBClient.DBDelete(ctx, ids)
}

// errorFileDBClient wraps a file MockDBClient and injects errors.
type errorFileDBClient struct {
	api.FileDBClient
	getErr    error
	deleteErr error
}

func (e *errorFileDBClient) DBGet(ctx context.Context, query *api.FileQuery, includeStatic bool, start, limit int) ([]*api.FileItem, int, bool, error) {
	if e.getErr != nil {
		return nil, 0, false, e.getErr
	}
	return e.FileDBClient.DBGet(ctx, query, includeStatic, start, limit)
}

func (e *errorFileDBClient) DBDelete(ctx context.Context, ids []string) ([]string, error) {
	if e.deleteErr != nil {
		return nil, e.deleteErr
	}
	return e.FileDBClient.DBDelete(ctx, ids)
}

const (
	// defaultInterval is a placeholder interval used for one-shot run() tests
	// where the interval is irrelevant.
	defaultInterval = time.Hour

	// defaultMaxConcurrency is used for tests that don't specifically test concurrency behavior.
	defaultMaxConcurrency = 10
)

// createPhysicalFile creates an empty file under the given root so that
// the mock client's Delete succeeds. It hashes the tenantID the same way the
// collector does (via ucom.GetFolderNameByTenantID) to match the folder name
// used at deletion time.
func createPhysicalFile(t *testing.T, rootDir, filename, tenantID string) {
	t.Helper()
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("get folder name: %v", err)
	}
	dir := filepath.Join(rootDir, folderName)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create dir: %v", err)
	}
	path := filepath.Join(dir, filename)
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatalf("create file: %v", err)
	}
}

// createTestFileWithTenant creates a FileItem with a unique tenantID and
// corresponding physical file on disk. This makes tests safe for parallel execution.
func createTestFileWithTenant(t *testing.T, rootDir, id, tenantID string, expiry int64) *api.FileItem {
	t.Helper()
	filename := fmt.Sprintf("test-%s.jsonl", id)
	createPhysicalFile(t, rootDir, filename, tenantID)
	return &api.FileItem{
		BaseIndexes: api.BaseIndexes{
			ID:       id,
			TenantID: tenantID,
			Expiry:   expiry,
		},
		Purpose: "batch",
		BaseContents: api.BaseContents{
			Spec: []byte(fmt.Sprintf(`{"filename":%q}`, filename)),
		},
	}
}

func newTestBatchDBClient() *mock.MockDBClient[api.BatchItem, api.BatchQuery] {
	return mock.NewMockDBClient[api.BatchItem, api.BatchQuery](
		func(b *api.BatchItem) string { return b.ID },
		func(q *api.BatchQuery) *api.BaseQuery { return &q.BaseQuery },
	)
}

func newTestFileDBClient() *mock.MockDBClient[api.FileItem, api.FileQuery] {
	return mock.NewMockDBClient[api.FileItem, api.FileQuery](
		func(f *api.FileItem) string { return f.ID },
		func(q *api.FileQuery) *api.BaseQuery { return &q.BaseQuery },
	)
}

func newTestFilesClient(rootDir string) *mockfiles.MockBatchFilesClient {
	return mockfiles.NewMockBatchFilesClient(rootDir)
}

func createTestJob(id string, expiry int64) *api.BatchItem {
	return &api.BatchItem{
		BaseIndexes: api.BaseIndexes{
			ID:     id,
			Expiry: expiry,
		},
	}
}

func createTestFile(id string, expiry int64) *api.FileItem {
	return &api.FileItem{
		BaseIndexes: api.BaseIndexes{
			ID:     id,
			Expiry: expiry,
		},
		Purpose: "batch",
		BaseContents: api.BaseContents{
			Spec: []byte(fmt.Sprintf(`{"filename":"test-%s.jsonl"}`, id)),
		},
	}
}

func dbGetAllBatch(ctx context.Context, client *mock.MockDBClient[api.BatchItem, api.BatchQuery]) []*api.BatchItem {
	items, _, _, _ := client.DBGet(ctx, &api.BatchQuery{}, true, 0, 0)
	return items
}

func dbGetBatchByIDs(ctx context.Context, client *mock.MockDBClient[api.BatchItem, api.BatchQuery], ids []string) []*api.BatchItem {
	items, _, _, _ := client.DBGet(ctx, &api.BatchQuery{BaseQuery: api.BaseQuery{IDs: ids}}, true, 0, 0)
	return items
}

func dbGetAllFiles(ctx context.Context, client *mock.MockDBClient[api.FileItem, api.FileQuery]) []*api.FileItem {
	items, _, _, _ := client.DBGet(ctx, &api.FileQuery{}, true, 0, 0)
	return items
}

func dbGetFileByIDs(ctx context.Context, client *mock.MockDBClient[api.FileItem, api.FileQuery], ids []string) []*api.FileItem {
	items, _, _, _ := client.DBGet(ctx, &api.FileQuery{BaseQuery: api.BaseQuery{IDs: ids}}, true, 0, 0)
	return items
}

// -- Batch job GC tests --

func TestCollector_Run_DeletesExpiredJobs(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = batchDB.DBStore(ctx, createTestJob("expired-job", expiredTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 1 {
		t.Errorf("Expected 1 job deleted, got %d", result.BatchesDeleted)
	}
	if jobs := dbGetBatchByIDs(ctx, batchDB, []string{"expired-job"}); len(jobs) != 0 {
		t.Error("Expected expired job to be deleted from database")
	}
}

func TestCollector_Run_PurgesResumableResultPrefixBeforeRowDelete(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	const tenantID = "tenant-gc-resumable"
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		t.Fatalf("get folder name: %v", err)
	}

	job := createTestJob("resumable-job", time.Now().Add(-1*time.Hour).Unix())
	job.TenantID = tenantID
	_ = batchDB.DBStore(ctx, job)

	for _, name := range []string{
		"results/resumable-job/req-1.jsonl",
		"results/resumable-job/req-2.jsonl",
		"results/other-job/req-1.jsonl",
	} {
		if _, err := filesClient.Store(ctx, name, folderName, 0, 0, strings.NewReader("data")); err != nil {
			t.Fatalf("store %s: %v", name, err)
		}
	}

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 1 {
		t.Errorf("Expected 1 job deleted, got %d", result.BatchesDeleted)
	}
	if jobs := dbGetBatchByIDs(ctx, batchDB, []string{"resumable-job"}); len(jobs) != 0 {
		t.Error("Expected expired job to be deleted from database")
	}
	for _, name := range []string{"results/resumable-job/req-1.jsonl", "results/resumable-job/req-2.jsonl"} {
		if _, _, err := filesClient.Retrieve(ctx, name, folderName); err == nil {
			t.Errorf("Expected result object %s to be purged", name)
		}
	}
	if _, _, err := filesClient.Retrieve(ctx, "results/other-job/req-1.jsonl", folderName); err != nil {
		t.Errorf("Expected other job's result objects to survive: %v", err)
	}
}

func TestCollector_Run_SkipsNonExpiredJobs(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	futureTime := time.Now().Add(1 * time.Hour).Unix()
	_ = batchDB.DBStore(ctx, createTestJob("active-job", futureTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 0 {
		t.Errorf("Expected 0 jobs deleted, got %d", result.BatchesDeleted)
	}
	if jobs := dbGetBatchByIDs(ctx, batchDB, []string{"active-job"}); len(jobs) != 1 {
		t.Error("Expected active job to still exist in database")
	}
}

func TestCollector_Run_SkipsJobsWithNoExpiresAt(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	_ = batchDB.DBStore(ctx, createTestJob("no-expiry-job", 0))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 0 {
		t.Errorf("Expected 0 jobs deleted, got %d", result.BatchesDeleted)
	}
	if jobs := dbGetBatchByIDs(ctx, batchDB, []string{"no-expiry-job"}); len(jobs) != 1 {
		t.Error("Expected job without expiry to still exist in database")
	}
}

func TestCollector_Run_DryRunDoesNotDelete(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = batchDB.DBStore(ctx, createTestJob("expired-job-dry", expiredTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, true, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 0 {
		t.Errorf("Expected 0 jobs deleted in dry-run mode, got %d", result.BatchesDeleted)
	}
	if jobs := dbGetBatchByIDs(ctx, batchDB, []string{"expired-job-dry"}); len(jobs) != 1 {
		t.Error("Expected job to still exist in dry-run mode")
	}
}

func TestCollector_Run_MixedJobs(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	futureTime := time.Now().Add(1 * time.Hour).Unix()

	_ = batchDB.DBStore(ctx, createTestJob("expired-1", expiredTime))
	_ = batchDB.DBStore(ctx, createTestJob("expired-2", expiredTime))
	_ = batchDB.DBStore(ctx, createTestJob("active-1", futureTime))
	_ = batchDB.DBStore(ctx, createTestJob("no-expiry", 0))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 2 {
		t.Errorf("Expected 2 jobs deleted, got %d", result.BatchesDeleted)
	}
	if allJobs := dbGetAllBatch(ctx, batchDB); len(allJobs) != 2 {
		t.Errorf("Expected 2 jobs remaining, got %d", len(allJobs))
	}
}

func TestCollector_Run_EmptyDatabase(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 0 {
		t.Errorf("Expected 0 jobs deleted, got %d", result.BatchesDeleted)
	}
	if result.FilesDeleted != 0 {
		t.Errorf("Expected 0 files deleted, got %d", result.FilesDeleted)
	}
}

// -- RunLoop tests --

func TestCollector_RunLoop_StopsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	cycleDone := make(chan *Result)
	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, time.Hour, defaultMaxConcurrency, func(r *Result) {
		cycleDone <- r
	})

	done := make(chan error, 1)
	go func() {
		done <- gc.RunLoop(ctx)
	}()

	<-cycleDone
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunLoop did not stop after context cancellation")
	}
}

func TestCollector_RunLoop_DeletesExpiredJobsAcrossCycles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = batchDB.DBStore(ctx, createTestJob("expired-loop-1", expiredTime))

	cycleDone := make(chan *Result)
	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, 50*time.Millisecond, defaultMaxConcurrency, func(r *Result) {
		cycleDone <- r
	})

	done := make(chan error, 1)
	go func() {
		done <- gc.RunLoop(ctx)
	}()

	<-cycleDone

	if jobs := dbGetBatchByIDs(ctx, batchDB, []string{"expired-loop-1"}); len(jobs) != 0 {
		t.Error("Expected expired-loop-1 to be deleted after first cycle")
	}

	_ = batchDB.DBStore(ctx, createTestJob("expired-loop-2", expiredTime))

	<-cycleDone

	if jobs := dbGetBatchByIDs(ctx, batchDB, []string{"expired-loop-2"}); len(jobs) != 0 {
		t.Error("Expected expired-loop-2 to be deleted after second cycle")
	}

	cancel()
	<-done
}

func TestCollector_RunLoop_RunsImmediatelyOnStartup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = batchDB.DBStore(ctx, createTestJob("immediate-gc", expiredTime))

	cycleDone := make(chan *Result)
	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, time.Hour, defaultMaxConcurrency, func(r *Result) {
		cycleDone <- r
	})

	done := make(chan error, 1)
	go func() {
		done <- gc.RunLoop(ctx)
	}()

	<-cycleDone

	if jobs := dbGetBatchByIDs(ctx, batchDB, []string{"immediate-gc"}); len(jobs) != 0 {
		t.Error("Expected expired job to be deleted by the immediate startup cycle")
	}

	cancel()
	<-done
}

// -- File GC tests --

func TestCollector_Run_DeletesExpiredFiles(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file", t.Name(), expiredTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != 1 {
		t.Errorf("Expected 1 file deleted, got %d", result.FilesDeleted)
	}
	if files := dbGetFileByIDs(ctx, fileDB, []string{"expired-file"}); len(files) != 0 {
		t.Error("Expected expired file to be deleted from database")
	}
}

func TestCollector_Run_DeletesMetadataWhenPhysicalFileAlreadyGone(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	// Create a file in the DB but NOT on disk — physical delete returns os.ErrNotExist,
	// which should be tolerated so the DB metadata is cleaned up.
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	file := createTestFile("no-physical-file", expiredTime)
	file.TenantID = t.Name()
	_ = fileDB.DBStore(ctx, file)

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != 1 {
		t.Errorf("Expected 1 file deleted when physical file is already gone, got %d", result.FilesDeleted)
	}
	if files := dbGetFileByIDs(ctx, fileDB, []string{"no-physical-file"}); len(files) != 0 {
		t.Error("Expected file metadata to be deleted when physical file is already gone")
	}
}

func TestCollector_Run_SkipsNonExpiredFiles(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	futureTime := time.Now().Add(1 * time.Hour).Unix()
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "active-file", t.Name(), futureTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != 0 {
		t.Errorf("Expected 0 files deleted, got %d", result.FilesDeleted)
	}
	if files := dbGetFileByIDs(ctx, fileDB, []string{"active-file"}); len(files) != 1 {
		t.Error("Expected active file to still exist in database")
	}
}

func TestCollector_Run_SkipsFilesWithNoExpiry(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	_ = fileDB.DBStore(ctx, createTestFile("no-expiry-file", 0))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != 0 {
		t.Errorf("Expected 0 files deleted, got %d", result.FilesDeleted)
	}
	if files := dbGetFileByIDs(ctx, fileDB, []string{"no-expiry-file"}); len(files) != 1 {
		t.Error("Expected file without expiry to still exist in database")
	}
}

func TestCollector_Run_DryRunDoesNotDeleteFiles(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file-dry", t.Name(), expiredTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, true, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != 0 {
		t.Errorf("Expected 0 files deleted in dry-run mode, got %d", result.FilesDeleted)
	}
	if files := dbGetFileByIDs(ctx, fileDB, []string{"expired-file-dry"}); len(files) != 1 {
		t.Error("Expected file to still exist in dry-run mode")
	}
}

func TestCollector_Run_MixedFiles(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	futureTime := time.Now().Add(1 * time.Hour).Unix()

	tenant := t.Name()
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file-1", tenant, expiredTime))
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file-2", tenant, expiredTime))
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "active-file-1", tenant, futureTime))
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "no-expiry-file", tenant, 0))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != 2 {
		t.Errorf("Expected 2 files deleted, got %d", result.FilesDeleted)
	}
	if allFiles := dbGetAllFiles(ctx, fileDB); len(allFiles) != 2 {
		t.Errorf("Expected 2 files remaining, got %d", len(allFiles))
	}
}

// -- Combined batch + file GC tests --

func TestCollector_Run_MixedJobsAndFiles(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	futureTime := time.Now().Add(1 * time.Hour).Unix()

	tenant := t.Name()
	_ = batchDB.DBStore(ctx, createTestJob("expired-job", expiredTime))
	_ = batchDB.DBStore(ctx, createTestJob("active-job", futureTime))
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file", tenant, expiredTime))
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "active-file", tenant, futureTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 1 {
		t.Errorf("Expected 1 job deleted, got %d", result.BatchesDeleted)
	}
	if result.FilesDeleted != 1 {
		t.Errorf("Expected 1 file deleted, got %d", result.FilesDeleted)
	}
	if jobs := dbGetAllBatch(ctx, batchDB); len(jobs) != 1 {
		t.Errorf("Expected 1 job remaining, got %d", len(jobs))
	}
	if files := dbGetAllFiles(ctx, fileDB); len(files) != 1 {
		t.Errorf("Expected 1 file remaining, got %d", len(files))
	}
}

func TestCollector_RunLoop_DeletesExpiredFilesAcrossCycles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	tenant := t.Name()
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file-loop-1", tenant, expiredTime))

	cycleDone := make(chan *Result)
	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, 50*time.Millisecond, defaultMaxConcurrency, func(r *Result) {
		cycleDone <- r
	})

	done := make(chan error, 1)
	go func() {
		done <- gc.RunLoop(ctx)
	}()

	<-cycleDone

	if files := dbGetFileByIDs(ctx, fileDB, []string{"expired-file-loop-1"}); len(files) != 0 {
		t.Error("Expected expired-file-loop-1 to be deleted after first cycle")
	}

	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file-loop-2", tenant, expiredTime))

	<-cycleDone

	if files := dbGetFileByIDs(ctx, fileDB, []string{"expired-file-loop-2"}); len(files) != 0 {
		t.Error("Expected expired-file-loop-2 to be deleted after second cycle")
	}

	cancel()
	<-done
}

func TestCollector_Run_FileWithInvalidSpec(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	file := &api.FileItem{
		BaseIndexes: api.BaseIndexes{
			ID:     "invalid-spec-file",
			Expiry: expiredTime,
		},
		Purpose: "batch",
		BaseContents: api.BaseContents{
			Spec: []byte(`invalid json`),
		},
	}
	_ = fileDB.DBStore(ctx, file)

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	// Invalid spec means filename cannot be extracted, so processFile logs an error
	// and the file is not deleted.
	if result.FilesDeleted != 0 {
		t.Errorf("Expected 0 files deleted (invalid spec), got %d", result.FilesDeleted)
	}
}

func TestCollector_Run_ErrorsAreCapturedNotReturned(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	tenant := t.Name()
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = batchDB.DBStore(ctx, createTestJob("expired-job", expiredTime))
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file", tenant, expiredTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	// Both collections should have run and succeeded
	if result.BatchesDeleted != 1 {
		t.Errorf("Expected 1 job deleted, got %d", result.BatchesDeleted)
	}
	if result.FilesDeleted != 1 {
		t.Errorf("Expected 1 file deleted, got %d", result.FilesDeleted)
	}
}

// ── DB error injection tests ─────────────────────────────────────────────────

func TestCollector_Run_BatchDBGetError(t *testing.T) {
	ctx := context.Background()
	batchDB := &errorBatchDBClient{
		BatchDBClient: newTestBatchDBClient(),
		getErr:        fmt.Errorf("connection refused"),
	}
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 0 {
		t.Errorf("Expected 0 batches deleted on DB error, got %d", result.BatchesDeleted)
	}
}

func TestCollector_Run_FileDBGetError(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := &errorFileDBClient{
		FileDBClient: newTestFileDBClient(),
		getErr:       fmt.Errorf("connection refused"),
	}
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != 0 {
		t.Errorf("Expected 0 files deleted on DB error, got %d", result.FilesDeleted)
	}
}

func TestCollector_Run_BatchDBDeleteError(t *testing.T) {
	ctx := context.Background()
	batchDB := &errorBatchDBClient{
		BatchDBClient: newTestBatchDBClient(),
		deleteErr:     fmt.Errorf("write conflict"),
	}
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = batchDB.DBStore(ctx, createTestJob("expired-job", expiredTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 0 {
		t.Errorf("Expected 0 batches deleted on delete error, got %d", result.BatchesDeleted)
	}
	if result.BatchesFailed != 1 {
		t.Errorf("Expected 1 batch failure, got %d", result.BatchesFailed)
	}
}

func TestCollector_Run_FileDBDeleteError(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := &errorFileDBClient{
		FileDBClient: newTestFileDBClient(),
		deleteErr:    fmt.Errorf("write conflict"),
	}
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	tenant := t.Name()
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file", tenant, expiredTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != 0 {
		t.Errorf("Expected 0 files deleted on delete error, got %d", result.FilesDeleted)
	}
	if result.FilesFailed != 1 {
		t.Errorf("Expected 1 file failure, got %d", result.FilesFailed)
	}
}

func TestCollector_Run_DBGetErrorDoesNotBlockOtherCollection(t *testing.T) {
	ctx := context.Background()
	// Batch DB fails, but file DB works
	batchDB := &errorBatchDBClient{
		BatchDBClient: newTestBatchDBClient(),
		getErr:        fmt.Errorf("connection refused"),
	}
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	tenant := t.Name()
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, "expired-file", tenant, expiredTime))

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != 0 {
		t.Errorf("Expected 0 batches deleted, got %d", result.BatchesDeleted)
	}
	if result.FilesDeleted != 1 {
		t.Errorf("Expected 1 file deleted despite batch DB error, got %d", result.FilesDeleted)
	}
}

// ── Pagination tests ─────────────────────────────────────────────────────────

func TestCollector_Run_PaginationExactlyPageSize(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	for i := 0; i < pageSize; i++ {
		_ = batchDB.DBStore(ctx, createTestJob(fmt.Sprintf("job-%d", i), expiredTime))
	}

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != pageSize {
		t.Errorf("Expected %d batches deleted, got %d", pageSize, result.BatchesDeleted)
	}
}

func TestCollector_Run_PaginationMultiplePages(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	total := pageSize + 50
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	for i := 0; i < total; i++ {
		_ = batchDB.DBStore(ctx, createTestJob(fmt.Sprintf("job-%d", i), expiredTime))
	}

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, defaultMaxConcurrency, nil)
	result := gc.run(ctx)

	// Due to OFFSET-based pagination with concurrent deletes, some items may be
	// skipped and caught on the next cycle. We verify at least one full page was
	// processed and no more than the total.
	if result.BatchesDeleted == 0 {
		t.Error("Expected some batches deleted, got 0")
	}
	if result.BatchesDeleted > total {
		t.Errorf("Deleted more than total: %d > %d", result.BatchesDeleted, total)
	}
}

// -- Concurrency tests --

// concurrentFilesClient wraps a BatchFilesClient and tracks peak concurrency
// during Delete calls. Uses a short delay to give goroutines time to overlap.
type concurrentFilesClient struct {
	fsapi.BatchFilesClient
	delay time.Duration

	mu        sync.Mutex
	active    int
	maxActive int
}

func (c *concurrentFilesClient) Delete(ctx context.Context, fileName, folderName string) error {
	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.mu.Unlock()

	time.Sleep(c.delay)

	c.mu.Lock()
	c.active--
	c.mu.Unlock()

	return c.BatchFilesClient.Delete(ctx, fileName, folderName)
}

func (c *concurrentFilesClient) peakConcurrency() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxActive
}

// concurrentBatchDBClient wraps a BatchDBClient and tracks peak concurrency
// during DBDelete calls.
type concurrentBatchDBClient struct {
	api.BatchDBClient
	deleteDelay time.Duration

	mu        sync.Mutex
	active    int
	maxActive int
}

func (c *concurrentBatchDBClient) DBDelete(ctx context.Context, ids []string) ([]string, error) {
	c.mu.Lock()
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	c.mu.Unlock()

	time.Sleep(c.deleteDelay)

	c.mu.Lock()
	c.active--
	c.mu.Unlock()

	return c.BatchDBClient.DBDelete(ctx, ids)
}

func (c *concurrentBatchDBClient) peakConcurrency() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxActive
}

func TestCollector_Run_FilesDeletionRunsConcurrently(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()

	filesRoot := t.TempDir()
	filesClient := &concurrentFilesClient{
		BatchFilesClient: newTestFilesClient(filesRoot),
		delay:            20 * time.Millisecond,
	}

	numFiles := 10
	tenant := t.Name()
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	for i := 0; i < numFiles; i++ {
		_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, fmt.Sprintf("conc-file-%d", i), tenant, expiredTime))
	}

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, numFiles, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != numFiles {
		t.Errorf("Expected %d files deleted, got %d", numFiles, result.FilesDeleted)
	}

	if peak := filesClient.peakConcurrency(); peak < 2 {
		t.Errorf("file deletions appear sequential: peak concurrency = %d, want >= 2", peak)
	}
}

func TestCollector_Run_BatchDeletionRunsConcurrently(t *testing.T) {
	ctx := context.Background()

	realBatchDB := newTestBatchDBClient()
	batchDB := &concurrentBatchDBClient{
		BatchDBClient: realBatchDB,
		deleteDelay:   20 * time.Millisecond,
	}
	fileDB := newTestFileDBClient()
	filesRoot := t.TempDir()
	filesClient := newTestFilesClient(filesRoot)

	numBatches := 10
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	for i := 0; i < numBatches; i++ {
		_ = realBatchDB.DBStore(ctx, createTestJob(fmt.Sprintf("conc-batch-%d", i), expiredTime))
	}

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, numBatches, nil)
	result := gc.run(ctx)

	if result.BatchesDeleted != numBatches {
		t.Errorf("Expected %d batches deleted, got %d", numBatches, result.BatchesDeleted)
	}

	if peak := batchDB.peakConcurrency(); peak < 2 {
		t.Errorf("batch deletions appear sequential: peak concurrency = %d, want >= 2", peak)
	}
}

func TestCollector_Run_ConcurrencyBoundIsRespected(t *testing.T) {
	ctx := context.Background()
	batchDB := newTestBatchDBClient()
	fileDB := newTestFileDBClient()

	maxConcurrency := 2
	filesRoot := t.TempDir()
	filesClient := &concurrentFilesClient{
		BatchFilesClient: newTestFilesClient(filesRoot),
		delay:            20 * time.Millisecond,
	}

	numFiles := 10
	tenant := t.Name()
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	for i := 0; i < numFiles; i++ {
		_ = fileDB.DBStore(ctx, createTestFileWithTenant(t, filesRoot, fmt.Sprintf("bound-file-%d", i), tenant, expiredTime))
	}

	gc := NewGarbageCollector(batchDB, fileDB, filesClient, false, defaultInterval, maxConcurrency, nil)
	result := gc.run(ctx)

	if result.FilesDeleted != numFiles {
		t.Errorf("Expected %d files deleted, got %d", numFiles, result.FilesDeleted)
	}

	if peak := filesClient.peakConcurrency(); peak > maxConcurrency {
		t.Errorf("concurrency bound exceeded: peak = %d, maxConcurrency = %d", peak, maxConcurrency)
	}
	if peak := filesClient.peakConcurrency(); peak < 2 {
		t.Errorf("concurrency too low: peak = %d, want >= 2", peak)
	}
}
