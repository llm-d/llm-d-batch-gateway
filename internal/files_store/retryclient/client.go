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

// Package retryclient wraps a BatchFilesClient with retry logic.
package retryclient

import (
	"context"
	"fmt"
	"io"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/retry"
)

// Client wraps a BatchFilesClient and retries transient errors.
type Client struct {
	inner     api.BatchFilesClient
	cfg       retry.Config
	component ucom.Component
}

var _ api.BatchFilesClient = (*Client)(nil)

// New creates a retry-wrapping Client.
// component identifies the caller (e.g. "processor", "apiserver", "garbage-collector") for metrics.
// Callers should only wrap when cfg.MaxRetries > 0; with MaxRetries == 0
// retry.Do still calls fn exactly once, so there is no functional difference
// but an unnecessary layer of indirection.
func New(inner api.BatchFilesClient, cfg retry.Config, component ucom.Component) *Client {
	InitMetrics()
	return &Client{inner: inner, cfg: cfg, component: component}
}

func (c *Client) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (
	*api.BatchFileMetadata, error,
) {
	// All current callers pass seekable readers (os.File, multipart.File),
	// but we guard against non-seekable ones for forward compatibility.
	// Without Seek, the reader cannot be rewound after a failed attempt,
	// so retrying would upload partial/empty data.
	rs, seekable := reader.(io.ReadSeeker)
	if !seekable {
		return c.inner.Store(ctx, fileName, folderName, fileSizeLimit, lineNumLimit, reader)
	}

	var meta *api.BatchFileMetadata
	attempts, err := retry.Do(ctx, &c.cfg, func(attempt int) error {
		if attempt > 1 {
			recordRetry("store", c.component)
			logr.FromContextOrDiscard(ctx).Info("Retrying file store",
				"file", fileName, "attempt", attempt, "maxRetries", c.cfg.MaxRetries)
			// Seek failure means the reader cannot be rewound, so subsequent
			// Store attempts would send partial/empty data. Abort immediately.
			if _, seekErr := rs.Seek(0, io.SeekStart); seekErr != nil {
				return retry.Permanent(fmt.Errorf("seek failed: %w", seekErr))
			}
		}

		var storeErr error
		meta, storeErr = c.inner.Store(ctx, fileName, folderName, fileSizeLimit, lineNumLimit, rs)
		return storeErr
	})
	if err != nil {
		if attempts > c.cfg.MaxRetries {
			recordExhausted("store", c.component)
		}
	} else {
		recordSuccess("store", c.component)
	}
	return meta, err
}

func (c *Client) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	var (
		rc   io.ReadCloser
		meta *api.BatchFileMetadata
	)
	attempts, err := retry.Do(ctx, &c.cfg, func(attempt int) error {
		if attempt > 1 {
			if rc != nil {
				_ = rc.Close()
			}
			recordRetry("retrieve", c.component)
			logr.FromContextOrDiscard(ctx).Info("Retrying file retrieve",
				"file", fileName, "attempt", attempt, "maxRetries", c.cfg.MaxRetries)
		}

		var retrieveErr error
		rc, meta, retrieveErr = c.inner.Retrieve(ctx, fileName, folderName)
		return retrieveErr
	})
	if err != nil {
		if attempts > c.cfg.MaxRetries {
			recordExhausted("retrieve", c.component)
		}
	} else {
		recordSuccess("retrieve", c.component)
	}
	return rc, meta, err
}

func (c *Client) RetrieveRange(ctx context.Context, fileName, folderName string, offset int64, length int64) (io.ReadCloser, error) {
	var rc io.ReadCloser
	attempts, err := retry.Do(ctx, &c.cfg, func(attempt int) error {
		if attempt > 1 {
			if rc != nil {
				_ = rc.Close()
			}
			recordRetry("retrieve_range", c.component)
			logr.FromContextOrDiscard(ctx).Info("Retrying file retrieve range",
				"file", fileName, "offset", offset, "length", length,
				"attempt", attempt, "maxRetries", c.cfg.MaxRetries)
		}

		var retrieveErr error
		rc, retrieveErr = c.inner.RetrieveRange(ctx, fileName, folderName, offset, length)
		return retrieveErr
	})
	if err != nil {
		if attempts > c.cfg.MaxRetries {
			recordExhausted("retrieve_range", c.component)
		}
	} else {
		recordSuccess("retrieve_range", c.component)
	}
	return rc, err
}

func (c *Client) Delete(ctx context.Context, fileName, folderName string) error {
	attempts, err := retry.Do(ctx, &c.cfg, func(attempt int) error {
		if attempt > 1 {
			recordRetry("delete", c.component)
			logr.FromContextOrDiscard(ctx).Info("Retrying file delete",
				"file", fileName, "attempt", attempt, "maxRetries", c.cfg.MaxRetries)
		}
		return c.inner.Delete(ctx, fileName, folderName)
	})
	if err != nil {
		if attempts > c.cfg.MaxRetries {
			recordExhausted("delete", c.component)
		}
	} else {
		recordSuccess("delete", c.component)
	}
	return err
}

func (c *Client) Close() error {
	return c.inner.Close()
}
