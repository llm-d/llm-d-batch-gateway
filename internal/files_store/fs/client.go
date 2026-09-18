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

// Package fs provides a filesystem-based implementation of the BatchFilesClient interface.
package fs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	fsio "github.com/llm-d/llm-d-batch-gateway/internal/files_store/io"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	// defaultTimeout is the default timeout for filesystem operations.
	defaultTimeout = 30 * time.Second
	// defaultDirPerm is the default permission for created directories.
	defaultDirPerm = 0o755
)

// Config holds configuration for the filesystem client.
type Config struct {
	BasePath string `yaml:"base_path"`
}

// Validate checks that all required fields are set.
func (c *Config) Validate() error {
	if c.BasePath == "" {
		return fmt.Errorf("fs.base_path cannot be empty")
	}
	return nil
}

// Client implements api.BatchFilesClient using local filesystem storage.
type Client struct {
	root *os.Root
}

// Compile-time check that Client implements api.BatchFilesClient.
var _ api.BatchFilesClient = (*Client)(nil)

var tmpCounter atomic.Int64

// New creates a new filesystem-based BatchFilesClient.
func New(basePath string) (*Client, error) {
	absPath, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	if err := os.MkdirAll(absPath, defaultDirPerm); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	root, err := os.OpenRoot(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open root directory: %w", err)
	}

	return &Client{
		root: root,
	}, nil
}

// resolvePath returns the relative path within the root.
// Both folderName and fileName must be non-empty to prevent operations
// directly on the root directory.
func (c *Client) resolvePath(folderName, fileName string) (string, error) {
	if folderName == "" {
		return "", fmt.Errorf("folderName must not be empty")
	}
	if fileName == "" {
		return "", fmt.Errorf("fileName must not be empty")
	}
	return filepath.Join(folderName, fileName), nil
}

// Store stores a file in the filesystem.
func (c *Client) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (
	*api.BatchFileMetadata, error,
) {
	relPath, err := c.resolvePath(folderName, fileName)
	if err != nil {
		return nil, err
	}

	if _, err := c.root.Stat(relPath); err == nil {
		return nil, fmt.Errorf("%w: %s", api.ErrFileExists, relPath)
	}

	dir := filepath.Dir(relPath)
	if err := c.root.MkdirAll(dir, defaultDirPerm); err != nil {
		return nil, err
	}

	// Create temp file name within the target directory.
	var tmpFile *os.File
	var tmpName string
	for range 3 {
		tmpName = filepath.Join(dir, fmt.Sprintf(".tmp-%d-%d", time.Now().UnixNano(), tmpCounter.Add(1)))
		tmpFile, err = c.root.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}
	if tmpFile == nil {
		return nil, fmt.Errorf("failed to create unique temp file in %s after retries", dir)
	}
	defer func() {
		// Clean up on error.
		_ = c.root.Remove(tmpName)
	}()

	countingReader := &fsio.LimitedCountingReader{
		Reader:    reader,
		SizeLimit: fileSizeLimit,
		LineLimit: lineNumLimit,
	}

	_, err = io.Copy(tmpFile, countingReader)
	if err != nil {
		_ = tmpFile.Close()
		switch {
		case errors.Is(err, api.ErrFileTooLarge):
			return nil, api.ErrFileTooLarge
		case errors.Is(err, api.ErrTooManyLines):
			return nil, api.ErrTooManyLines
		default:
			return nil, err
		}
	}

	if err := tmpFile.Close(); err != nil {
		return nil, err
	}

	info, err := c.root.Stat(tmpName)
	if err != nil {
		return nil, err
	}

	metadata := &api.BatchFileMetadata{
		Location:    relPath,
		Size:        info.Size(),
		LinesNumber: countingReader.LineCount,
		ModTime:     info.ModTime(),
	}

	// Rename temp file to final destination as the last operation.
	if err := c.root.Rename(tmpName, relPath); err != nil {
		return nil, err
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File stored successfully",
		"path", relPath, "size", metadata.Size, "lines", metadata.LinesNumber)

	return metadata, nil
}

// Retrieve retrieves a file from the filesystem.
// The returned io.Reader is also an io.ReadCloser; callers should close it when done.
func (c *Client) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	relPath, err := c.resolvePath(folderName, fileName)
	if err != nil {
		return nil, nil, err
	}

	file, err := c.root.Open(relPath)
	if err != nil {
		return nil, nil, err
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}

	metadata := &api.BatchFileMetadata{
		Location: relPath,
		Size:     info.Size(),
		ModTime:  info.ModTime(),
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File retrieved successfully",
		"path", relPath, "size", metadata.Size)

	return file, metadata, nil
}

// RetrieveRange retrieves a byte range [offset, offset+length) from a file in the filesystem.
func (c *Client) RetrieveRange(ctx context.Context, fileName, folderName string, offset int64, length int64) (io.ReadCloser, error) {
	relPath, err := c.resolvePath(folderName, fileName)
	if err != nil {
		return nil, err
	}

	file, err := c.root.Open(relPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	buf := make([]byte, length)
	n, err := file.ReadAt(buf, offset)
	if err != nil && (err != io.EOF || int64(n) != length) {
		return nil, fmt.Errorf("read range at offset %d length %d: %w", offset, length, err)
	}
	buf = buf[:n]

	logr.FromContextOrDiscard(ctx).V(logging.TRACE).Info("Range retrieved",
		"path", relPath, "offset", offset, "length", length)

	return io.NopCloser(bytes.NewReader(buf)), nil
}

// Delete deletes a file from the filesystem.
// After removing the file, it attempts to remove the parent directory if empty.
// The directory cleanup is best-effort and does not fail the operation. (e.g. if the directory is not empty, it will not be removed)
func (c *Client) Delete(ctx context.Context, fileName, folderName string) error {
	relPath, err := c.resolvePath(folderName, fileName)
	if err != nil {
		return err
	}

	if err := c.root.Remove(relPath); err != nil {
		return err
	}

	logger := logr.FromContextOrDiscard(ctx).V(logging.INFO)
	logger.Info("File deleted successfully", "path", relPath)

	if err := c.root.Remove(folderName); err == nil {
		logger.Info("Empty directory removed", "dir", folderName)
	}

	return nil
}

// GetContext returns a derived context with a timeout.
func (c *Client) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	if timeLimit == 0 {
		timeLimit = defaultTimeout
	}
	return context.WithTimeout(parentCtx, timeLimit)
}

// Close closes the client and releases the root directory handle.
func (c *Client) Close() error {
	return c.root.Close()
}
