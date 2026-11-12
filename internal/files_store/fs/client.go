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
	"context"
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"path/filepath"
	"strings"
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

	// Create temp file within the target directory.
	tmpFile, tmpName, err := c.createTempFile(dir)
	if err != nil {
		return nil, err
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

	// Link the temp file into place as the last operation: unlike rename,
	// a hard link fails when the destination exists, so the create-only
	// contract holds even when the earlier Stat raced another writer.
	if err := c.root.Link(tmpName, relPath); err != nil {
		if errors.Is(err, iofs.ErrExist) {
			return nil, fmt.Errorf("%w: %s", api.ErrFileExists, relPath)
		}
		return nil, err
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File stored successfully",
		"path", relPath, "size", metadata.Size, "lines", metadata.LinesNumber)

	return metadata, nil
}

// createTempFile creates a uniquely named temporary file inside dir.
func (c *Client) createTempFile(dir string) (*os.File, string, error) {
	for range 3 {
		tmpName := filepath.Join(dir, fmt.Sprintf(".tmp-%d-%d", time.Now().UnixNano(), tmpCounter.Add(1)))
		tmpFile, err := c.root.OpenFile(tmpName, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return tmpFile, tmpName, nil
		}
		if !errors.Is(err, os.ErrExist) {
			return nil, "", err
		}
	}
	return nil, "", fmt.Errorf("failed to create unique temp file in %s after retries", dir)
}

// OpenStream opens a streaming write: data is buffered in a temp file and
// becomes visible only when the stream is committed (atomic rename).
func (c *Client) OpenStream(ctx context.Context, fileName, folderName string) (api.StreamWriter, error) {
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

	tmpFile, tmpName, err := c.createTempFile(dir)
	if err != nil {
		return nil, err
	}

	return &fsStreamWriter{
		root:    c.root,
		file:    tmpFile,
		tmpName: tmpName,
		dstPath: relPath,
	}, nil
}

// fsStreamWriter streams into a temp file, linking it into place on
// Commit. The link (unlike a rename) fails when the destination exists,
// so the commit is create-only: a writer that opened its stream before the
// current owner published cannot replace the winner's object.
type fsStreamWriter struct {
	root    *os.Root
	file    *os.File
	tmpName string
	dstPath string
}

var _ api.StreamWriter = (*fsStreamWriter)(nil)

func (w *fsStreamWriter) Write(p []byte) (int, error) {
	return w.file.Write(p)
}

func (w *fsStreamWriter) Commit(ctx context.Context) (*api.BatchFileMetadata, error) {
	info, err := w.file.Stat()
	if err == nil {
		err = w.file.Close()
	}
	if err == nil {
		// Link the temp file into place as the last operation, like Store:
		// unlike rename, a hard link fails when the destination exists, so
		// the create-only contract holds even when the open-time Stat raced
		// another writer — a stale owner cannot replace the current owner's
		// object.
		err = w.root.Link(w.tmpName, w.dstPath)
		if errors.Is(err, iofs.ErrExist) {
			err = fmt.Errorf("%w: %s", api.ErrFileExists, w.dstPath)
		}
	}
	if err != nil {
		_ = w.root.Remove(w.tmpName)
		return nil, err
	}
	// The destination owns the bytes now; the temp name is an alias.
	_ = w.root.Remove(w.tmpName)

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File streamed successfully",
		"path", w.dstPath, "size", info.Size())

	return &api.BatchFileMetadata{
		Location: w.dstPath,
		Size:     info.Size(),
		ModTime:  info.ModTime(),
	}, nil
}

func (w *fsStreamWriter) Abort(ctx context.Context) error {
	_ = w.file.Close()
	if err := w.root.Remove(w.tmpName); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
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

// DeletePrefix deletes every file under folderName/prefix and returns the
// number of deleted files. A prefix with no stored files is a no-op.
func (c *Client) DeletePrefix(ctx context.Context, folderName, prefix string) (int, error) {
	if folderName == "" {
		return 0, fmt.Errorf("folderName must not be empty")
	}
	if prefix == "" {
		return 0, fmt.Errorf("prefix must not be empty")
	}
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	dir := filepath.Join(folderName, prefix)

	var deleted int
	err := iofs.WalkDir(c.root.FS(), dir, func(path string, d iofs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if err := c.root.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		deleted++
		return nil
	})
	if err != nil {
		return deleted, err
	}

	logger := logr.FromContextOrDiscard(ctx).V(logging.INFO)
	logger.Info("Prefix deleted", "path", dir, "files", deleted)
	// Best-effort cleanup of the now-empty directory, like Delete.
	if err := c.root.Remove(dir); err == nil {
		logger.Info("Empty directory removed", "dir", dir)
	}

	return deleted, nil
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
