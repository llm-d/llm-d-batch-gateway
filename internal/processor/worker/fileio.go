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
	outputFileName = "output.jsonl"
	errorFileName  = "error.jsonl"

	// remote storage file name format strings
	outputStorageNameFmt = "batch_output_%s.jsonl"
	errorStorageNameFmt  = "batch_error_%s.jsonl"
)

// inputFileRef identifies an input file in shared storage.
// Resolved once per job from the input file's DB record.
type inputFileRef struct {
	storageName string
	folderName  string
}

func (p *Processor) jobRootDir(jobID, tenantID string) (string, error) {
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		return "", fmt.Errorf("failed to sanitize tenant id for job path: %w", err)
	}
	return filepath.Join(p.cfg.WorkDir, folderName, jobsDirName, jobID), nil
}

func (p *Processor) jobOutputFilePath(jobID, tenantID string) (string, error) {
	jobRootDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		return "", err
	}
	return filepath.Join(jobRootDir, outputFileName), nil
}

func (p *Processor) jobErrorFilePath(jobID, tenantID string) (string, error) {
	jobRootDir, err := p.jobRootDir(jobID, tenantID)
	if err != nil {
		return "", err
	}
	return filepath.Join(jobRootDir, errorFileName), nil
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

// resolveInputFileCoords looks up the storage name and folder for an input file ID.
func (p *Processor) resolveInputFileCoords(ctx context.Context, inputFileID string) (*inputFileRef, error) {
	items, _, _, err := p.files.db.DBGet(ctx, &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{inputFileID}}}, true, 0, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to get input file metadata: %w", err)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("input file %q not found in db", inputFileID)
	}

	fileItem := items[0]
	fileObj, err := converter.DBItemToFile(fileItem)
	if err != nil {
		return nil, fmt.Errorf("failed to convert file db item to file: %w", err)
	}

	folderName, err := ucom.GetFolderNameByTenantID(fileItem.TenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to get folder name by tenant id: %w", err)
	}

	return &inputFileRef{
		storageName: ucom.FileStorageName(fileItem.ID, fileObj.Filename),
		folderName:  folderName,
	}, nil
}

// openInputFileStream opens the input file stream from shared storage.
func (p *Processor) openInputFileStream(ctx context.Context, inputFileID string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
	ref, err := p.resolveInputFileCoords(ctx, inputFileID)
	if err != nil {
		return nil, nil, err
	}
	reader, metadata, err := p.files.storage.Retrieve(ctx, ref.storageName, ref.folderName)
	if err != nil {
		return nil, metadata, fmt.Errorf("failed to open input file stream: %w", err)
	}
	return reader, metadata, nil
}
