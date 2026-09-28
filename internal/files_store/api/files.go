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

// Package api provides interfaces for batch file storage operations.
package api

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/store"
)

var (
	// ErrFileTooLarge is returned when the file size exceeds the limit.
	ErrFileTooLarge = errors.New("file size exceeds limit")
	// ErrTooManyLines is returned when the file exceeds the line limit.
	ErrTooManyLines = errors.New("file exceeds line limit")
	// ErrFileExists is returned when attempting to store a file that already exists.
	ErrFileExists = errors.New("file already exists")
)

type BatchFileMetadata struct {
	Location    string    // Absolute location of the file.
	Size        int64     // The size of the file in bytes.
	LinesNumber int64     // The size of the file in lines.
	ModTime     time.Time // Modification time.
}

type BatchFilesClient interface {
	store.BatchClientAdmin

	// Store stores a file in the files storage.
	Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (
		fileMd *BatchFileMetadata, err error)

	// Retrieve retrieves a file from the files storage.
	Retrieve(ctx context.Context, fileName, folderName string) (reader io.ReadCloser, fileMd *BatchFileMetadata, err error)

	// RetrieveRange retrieves a byte range [offset, offset+length) from a file in storage.
	RetrieveRange(ctx context.Context, fileName, folderName string, offset int64, length int64) (io.ReadCloser, error)

	// Delete deletes the file in the specified location.
	Delete(ctx context.Context, fileName, folderName string) (err error)
}
