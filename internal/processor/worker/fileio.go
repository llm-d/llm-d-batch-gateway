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
	"io"
	"os"
	"path/filepath"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	// folder names
	jobsDirName  = "jobs"
	plansDirName = "plans"

	// local job artifact file names
	inputFileName  = "input.jsonl"
	outputFileName = "output.jsonl"
	errorFileName  = "error.jsonl"

	// remote storage file name format strings
	outputStorageNameFmt = "batch_output_%s.jsonl"
	errorStorageNameFmt  = "batch_error_%s.jsonl"
)

func (p *Processor) jobRootDir(jobID, tenantID string) (string, error) {
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		return "", fmt.Errorf("failed to sanitize tenant id for job path: %w", err)
	}
	return filepath.Join(p.cfg.WorkDir, folderName, jobsDirName, jobID), nil
}

func (p *Processor) jobInputFilePath(jobID, tenantID string) (string, error) {
	jobRootDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		return "", err
	}
	return filepath.Join(jobRootDir, inputFileName), nil
}

// jobOutputStorageName returns the filename used when uploading the output file to shared storage.
func jobOutputStorageName(jobID string) string {
	return fmt.Sprintf(outputStorageNameFmt, jobID)
}

// jobErrorStorageName returns the filename used when uploading the error file to shared storage.
func jobErrorStorageName(jobID string) string {
	return fmt.Sprintf(errorStorageNameFmt, jobID)
}

func (p *Processor) jobPlansDir(jobID, tenantID string) (string, error) {
	jobRootDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		return "", err
	}
	return filepath.Join(jobRootDir, plansDirName), nil
}

// createLocalInputFile creates or truncates the local input file for a job.
func (p *Processor) createLocalInputFile(jobID, tenantID string) (*os.File, string, error) {
	localInputFilePath, err := p.jobInputFilePath(jobID, tenantID)
	if err != nil {
		return nil, "", err
	}

	localInputFile, err := os.OpenFile(localInputFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, localInputFilePath, fmt.Errorf("failed to create local input file: %w", err)
	}
	return localInputFile, localInputFilePath, nil
}

// cleanupJobArtifacts removes the local job artifacts directory as best-effort.
func (p *Processor) cleanupJobArtifacts(ctx context.Context, jobID, tenantID string) {
	logger := logr.FromContextOrDiscard(ctx)
	jobDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		logger.Error(err, "Failed to resolve job directory for cleanup")
		return
	}

	if err := os.RemoveAll(jobDir); err != nil {
		// keep going: cleanup failure should not block status transitions.
		logger.Error(err, "Failed to remove job directory", "path", jobDir)
		return
	}
	logger.V(logging.INFO).Info("Removed job directory", "path", jobDir)
}

// openInputFileStream opens the input file stream
func (p *Processor) openInputFileStream(ctx context.Context, inputFileID string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	// get file metadata from database
	items, _, _, err := p.files.db.DBGet(ctx, &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{inputFileID}}}, true, 0, 1)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get input file metadata: %w", err)
	}
	if len(items) == 0 {
		return nil, nil, fmt.Errorf("input file %q not found in db", inputFileID)
	}

	// convert file db item to file object
	fileItem := items[0]
	fileObj, err := converter.DBItemToFile(fileItem)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert file db item to file: %w", err)
	}

	// retrieve input jsonl file from storage
	folderName, err := ucom.GetFolderNameByTenantID(fileItem.TenantID)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get folder name by tenant id: %w", err)
	}
	storageName := ucom.FileStorageName(fileItem.ID, fileObj.Filename)
	reader, metadata, err := p.files.storage.Retrieve(ctx, storageName, folderName)
	if err != nil {
		return nil, metadata, fmt.Errorf("failed to open input file stream: %w", err)
	}
	return reader, metadata, nil
}
