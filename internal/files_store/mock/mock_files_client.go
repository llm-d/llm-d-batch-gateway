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

// Package mock provides mock implementations for testing.
package mock

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
)

// Compile-time check: MockBatchFilesClient implements api.BatchFilesClient.
var _ api.BatchFilesClient = (*MockBatchFilesClient)(nil)

// MockBatchFilesClient is a mock implementation of the BatchFilesClient interface.
type MockBatchFilesClient struct {
	rootDir string
}

// NewMockBatchFilesClient creates a new mock client that stores files under rootDir.
func NewMockBatchFilesClient(rootDir string) *MockBatchFilesClient {
	return &MockBatchFilesClient{rootDir: rootDir}
}

// Store stores a file in the files storage.
func (m *MockBatchFilesClient) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (*api.BatchFileMetadata, error) {
	rootDir := m.rootDir

	fullDir := filepath.Join(rootDir, folderName)
	if err := os.MkdirAll(fullDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// create and write file
	filePath := filepath.Join(fullDir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	var totalBytes int64
	var lineCount int64
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		lineLen := int64(len(line))

		if fileSizeLimit > 0 && totalBytes+lineLen+1 > fileSizeLimit {
			_ = os.Remove(filePath)
			return nil, fmt.Errorf("file size exceeds limit of %d bytes", fileSizeLimit)
		}

		// write line to file
		if _, err := file.Write(line); err != nil {
			_ = os.Remove(filePath)
			return nil, fmt.Errorf("failed to write file: %w", err)
		}
		if _, err := file.Write([]byte("\n")); err != nil {
			_ = os.Remove(filePath)
			return nil, fmt.Errorf("failed to write file: %w", err)
		}

		totalBytes += lineLen + 1 // +1 for newline
		lineCount++

		// check line count limit
		if lineNumLimit > 0 && lineCount > lineNumLimit {
			_ = os.Remove(filePath)
			return nil, fmt.Errorf("file line count exceeds limit of %d lines", lineNumLimit)
		}
	}

	if err := scanner.Err(); err != nil {
		_ = os.Remove(filePath)
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	// construct file metadata
	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	// Return relative location (relative to rootDir)
	relativeLocation := filepath.Join(folderName, fileName)

	return &api.BatchFileMetadata{
		Location:    relativeLocation,
		Size:        totalBytes,
		LinesNumber: lineCount,
		ModTime:     fileInfo.ModTime(),
	}, nil
}

// Retrieve retrieves a file from the files storage.
func (m *MockBatchFilesClient) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	// Use /tmp as root folder
	rootDir := m.rootDir
	location := filepath.Join(folderName, fileName)
	filePath := filepath.Join(rootDir, location)

	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file info
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, fmt.Errorf("failed to get file info: %w", err)
	}

	return file, &api.BatchFileMetadata{
		Location: location,
		Size:     fileInfo.Size(),
		ModTime:  fileInfo.ModTime(),
	}, nil
}

// RetrieveRange retrieves a byte range [offset, offset+length) from a file.
func (m *MockBatchFilesClient) RetrieveRange(ctx context.Context, fileName, folderName string, offset int64, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	filePath := filepath.Join(m.rootDir, folderName, fileName)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	buf := make([]byte, length)
	n, err := file.ReadAt(buf, offset)
	if err != nil && (err != io.EOF || int64(n) != length) {
		return nil, fmt.Errorf("failed to read range at offset %d length %d: %w", offset, length, err)
	}
	buf = buf[:n]

	return io.NopCloser(bytes.NewReader(buf)), nil
}

// List lists the files in the specified location.
func (m *MockBatchFilesClient) List(ctx context.Context, location string) ([]api.BatchFileMetadata, error) {
	// Use /tmp as root folder
	rootDir := m.rootDir
	dirPath := filepath.Join(rootDir, location)

	// Read directory
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []api.BatchFileMetadata{}, nil
		}
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// Build metadata list
	var files []api.BatchFileMetadata
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		files = append(files, api.BatchFileMetadata{
			Location: filepath.Join(location, entry.Name()),
			Size:     info.Size(),
			ModTime:  info.ModTime(),
		})
	}

	return files, nil
}

// Delete deletes the file in the specified location.
func (m *MockBatchFilesClient) Delete(ctx context.Context, fileName, folderName string) error {
	// Use /tmp as root folder
	rootDir := m.rootDir
	location := filepath.Join(folderName, fileName)
	filePath := filepath.Join(rootDir, location)

	// Delete file
	if err := os.Remove(filePath); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}

	return nil
}

// GetContext returns a derived context for a call.
func (m *MockBatchFilesClient) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	if timeLimit > 0 {
		return context.WithTimeout(parentCtx, timeLimit)
	}
	return context.WithCancel(parentCtx)
}

// Close closes the client.
func (m *MockBatchFilesClient) Close() error {
	return nil
}
