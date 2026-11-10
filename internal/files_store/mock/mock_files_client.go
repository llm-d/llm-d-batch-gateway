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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
)

// Compile-time check: MockBatchFilesClient implements filesapi.BatchFilesClient.
var _ filesapi.BatchFilesClient = (*MockBatchFilesClient)(nil)

// MockBatchFilesClient is a mock implementation of the BatchFilesClient interface.
type MockBatchFilesClient struct {
	rootDir string
}

// NewMockBatchFilesClient creates a new mock client that stores files under rootDir.
func NewMockBatchFilesClient(rootDir string) *MockBatchFilesClient {
	return &MockBatchFilesClient{rootDir: rootDir}
}

// Store stores a file in the files storage.
func (m *MockBatchFilesClient) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (*filesapi.BatchFileMetadata, error) {
	rootDir := m.rootDir
	location := filepath.Join(folderName, fileName)
	filePath := filepath.Join(rootDir, location)

	if _, err := os.Stat(filePath); err == nil {
		return nil, fmt.Errorf("%w: %s", filesapi.ErrFileExists, location)
	}

	fullDir := filepath.Dir(filePath)
	if err := os.MkdirAll(fullDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Write to a temp file and rename it into place as the last operation:
	// like the fs and S3 clients, an object only becomes visible once fully
	// written, so a concurrent duplicate never observes partial content.
	tmp, err := os.CreateTemp(fullDir, ".store-*")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}()

	var totalBytes int64
	var lineCount int64
	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		lineLen := int64(len(line))

		if fileSizeLimit > 0 && totalBytes+lineLen+1 > fileSizeLimit {
			return nil, fmt.Errorf("file size exceeds limit of %d bytes", fileSizeLimit)
		}

		// write line to file
		if _, err := tmp.Write(line); err != nil {
			return nil, fmt.Errorf("failed to write file: %w", err)
		}
		if _, err := tmp.Write([]byte("\n")); err != nil {
			return nil, fmt.Errorf("failed to write file: %w", err)
		}

		totalBytes += lineLen + 1 // +1 for newline
		lineCount++

		// check line count limit
		if lineNumLimit > 0 && lineCount > lineNumLimit {
			return nil, fmt.Errorf("file line count exceeds limit of %d lines", lineNumLimit)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	// construct file metadata
	fileInfo, err := tmp.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	// Link the temp file into place as the last operation: unlike rename,
	// a hard link fails when the destination exists, so the create-only
	// contract holds even when two writers race.
	if err := os.Link(tmpName, filePath); err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%w: %s", filesapi.ErrFileExists, location)
		}
		return nil, fmt.Errorf("failed to store file: %w", err)
	}

	// Return relative location (relative to rootDir)
	return &filesapi.BatchFileMetadata{
		Location:    location,
		Size:        totalBytes,
		LinesNumber: lineCount,
		ModTime:     fileInfo.ModTime(),
	}, nil
}

// OpenStream opens a streaming write buffered in memory. The file is
// written only on Commit.
func (m *MockBatchFilesClient) OpenStream(ctx context.Context, fileName, folderName string) (filesapi.StreamWriter, error) {
	location := filepath.Join(folderName, fileName)
	filePath := filepath.Join(m.rootDir, location)

	if _, err := os.Stat(filePath); err == nil {
		return nil, fmt.Errorf("%w: %s", filesapi.ErrFileExists, location)
	}

	return &mockStreamWriter{
		rootDir:    m.rootDir,
		fileName:   fileName,
		folderName: folderName,
	}, nil
}

// mockStreamWriter buffers a streaming write in memory.
type mockStreamWriter struct {
	rootDir    string
	fileName   string
	folderName string
	buf        bytes.Buffer
}

var _ filesapi.StreamWriter = (*mockStreamWriter)(nil)

func (w *mockStreamWriter) Write(p []byte) (int, error) {
	return w.buf.Write(p)
}

func (w *mockStreamWriter) Commit(ctx context.Context) (*filesapi.BatchFileMetadata, error) {
	filePath := filepath.Join(w.rootDir, w.folderName, w.fileName)
	fullDir := filepath.Dir(filePath)
	if err := os.MkdirAll(fullDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	// Create-only: the commit fails when another writer published the
	// destination while this stream was open, mirroring the fs and s3
	// adapters' conditional publication.
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("%w: %s", filesapi.ErrFileExists, filepath.Join(w.folderName, w.fileName))
		}
		return nil, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(w.buf.Bytes()); err != nil {
		_ = os.Remove(filePath)
		return nil, fmt.Errorf("failed to write file: %w", err)
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	return &filesapi.BatchFileMetadata{
		Location: filepath.Join(w.folderName, w.fileName),
		Size:     fileInfo.Size(),
		ModTime:  fileInfo.ModTime(),
	}, nil
}

func (w *mockStreamWriter) Abort(ctx context.Context) error {
	return nil
}

// Retrieve retrieves a file from the files storage.
func (m *MockBatchFilesClient) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *filesapi.BatchFileMetadata, error) {
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

	return file, &filesapi.BatchFileMetadata{
		Location: location,
		Size:     fileInfo.Size(),
		ModTime:  fileInfo.ModTime(),
	}, nil
}

// List lists the files in the specified location.
func (m *MockBatchFilesClient) List(ctx context.Context, location string) ([]filesapi.BatchFileMetadata, error) {
	// Use /tmp as root folder
	rootDir := m.rootDir
	dirPath := filepath.Join(rootDir, location)

	// Read directory
	entries, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []filesapi.BatchFileMetadata{}, nil
		}
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// Build metadata list
	var files []filesapi.BatchFileMetadata
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		files = append(files, filesapi.BatchFileMetadata{
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

// DeletePrefix deletes every file under folderName/prefix and returns the
// number of deleted files. A prefix with no stored files is a no-op.
func (m *MockBatchFilesClient) DeletePrefix(ctx context.Context, folderName, prefix string) (int, error) {
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	dirPath := filepath.Join(m.rootDir, folderName, prefix)

	var deleted int
	err := filepath.WalkDir(dirPath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		deleted++
		return nil
	})
	if err != nil {
		return deleted, fmt.Errorf("failed to delete prefix: %w", err)
	}

	// Best-effort cleanup of the now-empty directory, like Delete.
	_ = os.RemoveAll(dirPath)

	return deleted, nil
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
