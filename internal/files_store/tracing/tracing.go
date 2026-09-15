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

// Package tracing provides an OpenTelemetry tracing wrapper for BatchFilesClient.
package tracing

import (
	"context"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

// Client wraps a BatchFilesClient and adds OpenTelemetry tracing spans
// to all storage operations.
type Client struct {
	inner   api.BatchFilesClient
	backend attribute.KeyValue
}

// Wrap returns a new BatchFilesClient that adds tracing spans around
// every call to the underlying client. The backend parameter (e.g. "fs",
// "s3") is recorded as a storage.backend attribute on every span.
func Wrap(inner api.BatchFilesClient, backend string) api.BatchFilesClient {
	return &Client{
		inner:   inner,
		backend: attribute.String("storage.backend", backend),
	}
}

func (c *Client) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (*api.BatchFileMetadata, error) {
	ctx, span := uotel.StartSpan(ctx, "storage.Store")
	defer span.End()
	span.SetAttributes(
		c.backend,
		attribute.String("storage.file_name", fileName),
		attribute.String("storage.folder", folderName),
	)

	md, err := c.inner.Store(ctx, fileName, folderName, fileSizeLimit, lineNumLimit, reader)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "store failed")
		return md, err
	}
	span.SetAttributes(attribute.Int64("storage.file_size", md.Size))
	return md, nil
}

func (c *Client) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	_, span := uotel.StartSpan(ctx, "storage.Retrieve")
	span.SetAttributes(
		c.backend,
		attribute.String("storage.file_name", fileName),
		attribute.String("storage.folder", folderName),
	)

	reader, md, err := c.inner.Retrieve(ctx, fileName, folderName)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "retrieve failed")
		span.End()
		return nil, nil, err
	}
	if md != nil {
		span.SetAttributes(attribute.Int64("storage.file_size", md.Size))
	}
	// The span is kept open and ended when the caller closes the reader,
	// so that streaming download time (especially from S3) is captured.
	return &tracedReadCloser{ReadCloser: reader, span: span}, md, nil
}

// tracedReadCloser wraps an io.ReadCloser and ends the associated span
// when Close is called, capturing the full streaming duration.
type tracedReadCloser struct {
	io.ReadCloser
	span trace.Span
}

func (r *tracedReadCloser) Close() error {
	defer r.span.End()
	err := r.ReadCloser.Close()
	if err != nil {
		r.span.RecordError(err)
		r.span.SetStatus(codes.Error, "close failed")
	}
	return err
}

func (c *Client) RetrieveRange(ctx context.Context, fileName, folderName string, offset int64, length int64) (io.ReadCloser, error) {
	_, span := uotel.StartSpan(ctx, "storage.RetrieveRange")
	span.SetAttributes(
		c.backend,
		attribute.String("storage.file_name", fileName),
		attribute.String("storage.folder", folderName),
		attribute.Int64("storage.offset", offset),
		attribute.Int64("storage.length", length),
	)

	reader, err := c.inner.RetrieveRange(ctx, fileName, folderName, offset, length)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "retrieve range failed")
		span.End()
		return nil, err
	}
	return &tracedReadCloser{ReadCloser: reader, span: span}, nil
}

func (c *Client) Delete(ctx context.Context, fileName, folderName string) error {
	ctx, span := uotel.StartSpan(ctx, "storage.Delete")
	defer span.End()
	span.SetAttributes(
		c.backend,
		attribute.String("storage.file_name", fileName),
		attribute.String("storage.folder", folderName),
	)

	err := c.inner.Delete(ctx, fileName, folderName)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "delete failed")
	}
	return err
}

func (c *Client) Close() error {
	return c.inner.Close()
}
