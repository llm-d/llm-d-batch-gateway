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

// Package gcs provides a Google Cloud Storage implementation of the BatchFilesClient interface.
//
// Authentication uses Application Default Credentials (ADC); in Kubernetes this is
// typically GKE Workload Identity bound to the component's Kubernetes ServiceAccount.
// No secret key is read from the mounted app secret — the caller is expected to
// arrange for GCP credentials via the pod environment.
package gcs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	fsio "github.com/llm-d/llm-d-batch-gateway/internal/files_store/io"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
)

const (
	defaultTimeout = 30 * time.Second

	// deletePrefixConcurrency bounds the parallel deletes in DeletePrefix.
	// GCS has no multi-object delete, so every object is its own request.
	deletePrefixConcurrency = 16
)

// errStreamFinished is returned by a StreamWriter used after Commit or Abort.
var errStreamFinished = errors.New("stream already committed or aborted")

// bucketHandle is the subset of *storage.BucketHandle used by the client.
// It exists so unit tests can substitute an in-memory implementation.
// Object access is wired via newWriter/newReader/deleteObject/statObject/
// listObjects closures instead, so this interface intentionally omits
// BucketHandle.Object.
type bucketHandle interface {
	Attrs(ctx context.Context) (*storage.BucketAttrs, error)
	Create(ctx context.Context, projectID string, attrs *storage.BucketAttrs) error
}

// writer is the subset of *storage.Writer we need. *storage.Writer satisfies it.
type writer interface {
	io.WriteCloser
}

// reader is a *storage.Reader along with its readily-available attrs, exposed
// as an io.ReadCloser for the BatchFilesClient contract.
type reader interface {
	io.ReadCloser
	Size() int64
	ModTime() time.Time
}

// Client implements api.BatchFilesClient using Google Cloud Storage.
// All objects are stored in a single configured bucket. The folderName
// parameter in Store/Retrieve/Delete is used as part of the object name for
// tenant isolation, matching the S3 client's single-bucket-with-prefix model.
type Client struct {
	bucket bucketHandle
	// newWriter opens a create-only writer for key. sizeHint is the object's
	// size when known up front, or -1.
	newWriter    func(ctx context.Context, key string, sizeHint int64) writer
	newReader    func(ctx context.Context, key string) (reader, error)
	deleteObject func(ctx context.Context, key string) error
	// statObject returns nil when the object exists and
	// storage.ErrObjectNotExist when it does not.
	statObject func(ctx context.Context, key string) error
	// listObjects calls fn with the name of every object under prefix and
	// stops at fn's first error.
	listObjects func(ctx context.Context, prefix string, fn func(key string) error) error
	prefix      string
	bucketName  string
	closer      io.Closer // the underlying *storage.Client; nil when constructed with a test bucket
}

// Compile-time check that Client implements api.BatchFilesClient.
var _ api.BatchFilesClient = (*Client)(nil)

// Config holds configuration for the GCS client. Credentials are sourced from
// Application Default Credentials by the storage client; nothing here is secret.
type Config struct {
	Bucket           string `yaml:"bucket"`
	Prefix           string `yaml:"prefix"`
	ProjectID        string `yaml:"project_id"`
	AutoCreateBucket bool   `yaml:"auto_create_bucket"`
	// VerifyBucketOnStart controls whether New runs a HeadBucket (bucket.Attrs) at
	// startup. Set to false to skip the startup check — required when the workload
	// service account lacks roles/storage.bucketViewer (e.g. GKE Workload Identity
	// granted only objectUser), since the HeadBucket otherwise fails with 403 and
	// prevents the pod from starting. When skipped, an unreachable or non-existent
	// bucket surfaces as an error on the first object operation instead.
	// The Helm chart ships this as true by default (see verifyBucketOnStart in the
	// chart values), so chart users retain the startup check unless they opt out.
	// Note: auto_create_bucket only runs at startup under this flag; the
	// combination auto_create_bucket=true with verify_bucket_on_start=false
	// is rejected by Validate (auto-create would never run and the flag would
	// be silently dropped).
	VerifyBucketOnStart bool `yaml:"verify_bucket_on_start"`
}

// Validate checks that all required fields are set.
func (c *Config) Validate() error {
	if c.Bucket == "" {
		return fmt.Errorf("gcs.bucket cannot be empty")
	}
	if c.AutoCreateBucket && c.ProjectID == "" {
		return fmt.Errorf("gcs.project_id is required when auto_create_bucket is true")
	}
	// AutoCreateBucket only runs from the startup HeadBucket path (ensureBucket
	// under VerifyBucketOnStart). Wiring it into the verify=false path would
	// defeat that path's purpose (skip all bucket-level calls for SAs that
	// can't HeadBucket — typically objectUser-only, which also can't create
	// buckets). Reject the combination so a misconfigured chart fails at
	// startup instead of silently never creating the bucket.
	if c.AutoCreateBucket && !c.VerifyBucketOnStart {
		return fmt.Errorf("gcs.auto_create_bucket requires verify_bucket_on_start=true (auto-create only runs on the startup HeadBucket path)")
	}
	return nil
}

// New creates a new GCS-based BatchFilesClient.
//
// It creates a *storage.Client using Application Default Credentials and the
// provided context, then verifies (or, when AutoCreateBucket is set, creates)
// the configured bucket before returning.
func New(ctx context.Context, cfg Config) (*Client, error) {
	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	c, err := newFromClient(ctx, cfg, gcsClient)
	if err != nil {
		_ = gcsClient.Close()
		return nil, err
	}
	c.closer = gcsClient
	return c, nil
}

// newFromClient wires a Client from an already-created *storage.Client. Shared
// by New (production) and tests (which pass a fake client).
func newFromClient(ctx context.Context, cfg Config, gcsClient *storage.Client) (*Client, error) {
	bucket := gcsClient.Bucket(cfg.Bucket)
	c := &Client{
		bucket:     bucket,
		prefix:     cfg.Prefix,
		bucketName: cfg.Bucket,
		newWriter: func(ctx context.Context, key string, sizeHint int64) writer {
			// IfDoesNotExist makes the create atomic: GCS rejects the write with
			// 412 Precondition Failed if the object already exists, eliminating the
			// HeadObject-then-Put TOCTOU window. Losers of a concurrent-write race
			// fail without overwriting the winner's object.
			w := bucket.Object(key).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
			w.ChunkSize = chunkSizeFor(sizeHint)
			return w
		},
		newReader: func(ctx context.Context, key string) (reader, error) {
			r, err := bucket.Object(key).NewReader(ctx)
			if err != nil {
				return nil, err
			}
			return &gcsReader{Reader: r}, nil
		},
		deleteObject: func(ctx context.Context, key string) error {
			return bucket.Object(key).Delete(ctx)
		},
		statObject: func(ctx context.Context, key string) error {
			_, err := bucket.Object(key).Attrs(ctx)
			return err
		},
		listObjects: func(ctx context.Context, prefix string, fn func(key string) error) error {
			q := &storage.Query{Prefix: prefix}
			if err := q.SetAttrSelection([]string{"Name"}); err != nil {
				return err
			}
			it := bucket.Objects(ctx, q)
			for {
				attrs, err := it.Next()
				if errors.Is(err, iterator.Done) {
					return nil
				}
				if err != nil {
					return err
				}
				if err := fn(attrs.Name); err != nil {
					return err
				}
			}
		},
	}

	if cfg.VerifyBucketOnStart {
		if err := c.ensureBucket(ctx, cfg.AutoCreateBucket, cfg.ProjectID); err != nil {
			return nil, fmt.Errorf("ensure bucket %s: %w", cfg.Bucket, err)
		}
	}
	return c, nil
}

// chunkSizeFor returns the Writer.ChunkSize for an upload of sizeHint bytes.
// The SDK allocates a ChunkSize buffer for every writer, 16 MiB by default,
// and the resumable pipeline stores one small result object per request, so
// a known size below the default gets a buffer just larger than the object
// (the SDK rounds it up to 256 KiB). The upload stays a single request and
// keeps its retries; a ChunkSize of zero would disable both the buffer and
// the retries.
func chunkSizeFor(sizeHint int64) int {
	if sizeHint < 0 || sizeHint >= googleapi.DefaultUploadChunkSize {
		return googleapi.DefaultUploadChunkSize
	}
	return int(sizeHint) + 1
}

// buildKey constructs the full GCS object name from the folder name and file name.
func (c *Client) buildKey(folderName, fileName string) string {
	key := folderName + "/" + fileName
	if c.prefix != "" {
		key = c.prefix + "/" + key
	}
	return key
}

// ensureBucket checks that the configured bucket exists. When autoCreate is
// true, it creates the bucket if it does not exist; otherwise it returns an
// error. Called once at startup.
func (c *Client) ensureBucket(ctx context.Context, autoCreate bool, projectID string) error {
	if _, err := c.bucket.Attrs(ctx); err == nil {
		return nil
	} else if !errors.Is(err, storage.ErrBucketNotExist) {
		return fmt.Errorf("head bucket %s: %w", c.bucketName, err)
	}

	if !autoCreate {
		return fmt.Errorf("bucket %s does not exist", c.bucketName)
	}

	if err := c.bucket.Create(ctx, projectID, nil); err != nil {
		// Bucket names are globally unique; a concurrent create race where the
		// bucket already exists (and is owned by this caller) surfaces as a 409
		// Conflict. That race is benign — the desired end state (bucket exists)
		// is reached — so tolerate it. Other create errors still fail construction.
		var gErr *googleapi.Error
		if errors.As(err, &gErr) && gErr.Code == http.StatusConflict {
			return nil
		}
		return fmt.Errorf("create bucket %s: %w", c.bucketName, err)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("Bucket created", "bucket", c.bucketName)
	return nil
}

// Store stores a file in GCS.
// The folderName parameter is used as an object-name prefix for tenant isolation.
//
// Writes are made atomic via an IfDoesNotExist precondition: the GCS SDK enforces
// that the object must not already exist, so a concurrent writer that wins the
// race causes losers to fail without overwriting the existing object. A failed
// precondition surfaces as a 412 Precondition Failed and is mapped to
// api.ErrFileExists. This removes the HeadObject-then-Put TOCTOU window the S3
// backend still has; S3 retains its two-step pattern since its SDK offers no
// equivalent conditional put that returns a distinguishable error.
//
// A Store rejected mid-stream for ErrFileTooLarge or ErrTooManyLines aborts the
// upload instead of committing it: the writer's context is canceled and Close is
// never called, so the SDK's resumable session is discarded and no object — not
// even a partial one — exists at the key afterwards. A retry under the same
// (fileName, folderName) therefore starts from a clean not-exists rather than
// tripping the IfDoesNotExist precondition on a truncated object.
func (c *Client) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (
	*api.BatchFileMetadata, error,
) {
	key := c.buildKey(folderName, fileName)

	countingReader := &fsio.LimitedCountingReader{
		Reader:    reader,
		SizeLimit: fileSizeLimit,
		LineLimit: lineNumLimit,
	}

	// wctx is bound to this object's upload: canceling it aborts the SDK's resumable
	// upload. Close, by contrast, finalizes the session and commits whatever bytes
	// were streamed — so canceling is the only way to reject a mid-stream limit
	// without leaving a partial object behind.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sizeHint := int64(-1)
	if l, ok := reader.(interface{ Len() int }); ok {
		sizeHint = int64(l.Len())
	}
	w := c.newWriter(wctx, key, sizeHint)
	// Copy to the writer. A write error is surfaced by Close (see Writer.Close docs),
	// so we Close and prefer its errors over a mid-stream copy error — except when a
	// size/line limit trips, where we cancel (abort) instead of committing partials.
	_, copyErr := io.Copy(w, countingReader)
	switch {
	case errors.Is(copyErr, api.ErrFileTooLarge):
		// The limit tripped mid-copy. Abort rather than commit the bytes received so
		// far: cancel wctx (never Close — Close would finalize the session and commit
		// a partial object under the new key).
		cancel()
		return nil, api.ErrFileTooLarge
	case errors.Is(copyErr, api.ErrTooManyLines):
		cancel()
		return nil, api.ErrTooManyLines
	}
	closeErr := w.Close()
	if err := errors.Join(copyErr, closeErr); err != nil {
		switch {
		case errors.Is(err, api.ErrFileTooLarge):
			return nil, api.ErrFileTooLarge
		case errors.Is(err, api.ErrTooManyLines):
			return nil, api.ErrTooManyLines
		default:
			// A 412 Precondition Failed is returned by the IfDoesNotExist
			// conditional write when the object already exists on the server. Map
			// it to the same ErrFileExists sentinel the S3/fs backends use. We use
			// *googleapi.Error (the JSON/REST error surface used by
			// cloud.google.com/go/storage); if a future SDK switches to
			// gRPC transport this branch may need a status.Status case as well.
			var gErr *googleapi.Error
			if errors.As(err, &gErr) && gErr.Code == http.StatusPreconditionFailed {
				return nil, fmt.Errorf("%w: %s", api.ErrFileExists, key)
			}
			return nil, err
		}
	}

	metadata := &api.BatchFileMetadata{
		Location:    key,
		Size:        countingReader.BytesRead,
		LinesNumber: countingReader.LineCount,
		ModTime:     time.Now(),
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File stored successfully",
		"bucket", c.bucketName, "key", key, "size", metadata.Size, "lines", metadata.LinesNumber)

	return metadata, nil
}

// Retrieve retrieves a file from GCS.
// The folderName parameter is used as an object-name prefix for tenant isolation.
func (c *Client) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	key := c.buildKey(folderName, fileName)

	r, err := c.newReader(ctx, key)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil, nil, os.ErrNotExist
		}
		return nil, nil, err
	}

	metadata := &api.BatchFileMetadata{
		Location: key,
		Size:     r.Size(),
		ModTime:  r.ModTime(),
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File retrieved successfully",
		"bucket", c.bucketName, "key", key, "size", metadata.Size)

	return r, metadata, nil
}

// Delete deletes a file from GCS.
// The folderName parameter is used as an object-name prefix for tenant isolation.
//
// Deleting a non-existent object maps to os.ErrNotExist, matching the fs
// backend; callers that want idempotent delete (e.g. the apiserver cleanup
// path) ignore ErrNotExist, while the gc reconciler explicitly tolerates it
// (internal/gc/collector/collector.go).
func (c *Client) Delete(ctx context.Context, fileName, folderName string) error {
	key := c.buildKey(folderName, fileName)

	if err := c.deleteObject(ctx, key); err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return os.ErrNotExist
		}
		return err
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File deleted successfully",
		"bucket", c.bucketName, "key", key)

	return nil
}

// DeletePrefix deletes every object under folderName/prefix and returns the
// number of deleted objects. A prefix with no stored objects is a no-op. An
// object that disappears between the listing and its delete is not counted
// and is not an error, so concurrent purges of the same prefix are safe.
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
	keyPrefix := c.buildKey(folderName, prefix)

	var deleted atomic.Int64
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(deletePrefixConcurrency)
	listErr := c.listObjects(gctx, keyPrefix, func(key string) error {
		g.Go(func() error {
			if err := c.deleteObject(gctx, key); err != nil {
				if errors.Is(err, storage.ErrObjectNotExist) {
					return nil
				}
				return fmt.Errorf("delete %s: %w", key, err)
			}
			deleted.Add(1)
			return nil
		})
		return nil
	})
	// A failed delete cancels gctx, which also fails the listing; report the
	// delete error, which is the cause.
	if err := g.Wait(); err != nil {
		return int(deleted.Load()), err
	}
	if listErr != nil {
		return int(deleted.Load()), fmt.Errorf("list %s: %w", keyPrefix, listErr)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("Prefix deleted",
		"bucket", c.bucketName, "prefix", keyPrefix, "objects", deleted.Load())
	return int(deleted.Load()), nil
}

// OpenStream opens a streaming write to GCS. The upload runs under the
// context passed here: cancelling it aborts the upload. The bytes go straight
// to the destination through the same create-only writer as Store. GCS makes
// an object visible only when its upload is finalized, so nothing is readable
// before Commit, and Abort leaves no object behind.
func (c *Client) OpenStream(ctx context.Context, fileName, folderName string) (api.StreamWriter, error) {
	key := c.buildKey(folderName, fileName)

	err := c.statObject(ctx, key)
	if err == nil {
		return nil, fmt.Errorf("%w: %s", api.ErrFileExists, key)
	}
	if !errors.Is(err, storage.ErrObjectNotExist) {
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)
	return &gcsStreamWriter{
		w:          c.newWriter(wctx, key, -1),
		cancel:     cancel,
		bucketName: c.bucketName,
		key:        key,
	}, nil
}

// gcsStreamWriter streams into a create-only upload. Commit finalizes the
// upload; Abort cancels its context, which discards the resumable session
// without committing, as in Store's limit-exceeded path.
type gcsStreamWriter struct {
	w          writer
	cancel     context.CancelFunc
	bucketName string
	key        string
	bytes      int64
	finished   bool
}

var _ api.StreamWriter = (*gcsStreamWriter)(nil)

func (w *gcsStreamWriter) Write(p []byte) (int, error) {
	if w.finished {
		return 0, errStreamFinished
	}
	n, err := w.w.Write(p)
	w.bytes += int64(n)
	return n, err
}

// Commit finalizes the upload. It returns api.ErrFileExists when another
// writer published the object after this stream was opened; the winner's
// object stands.
func (w *gcsStreamWriter) Commit(ctx context.Context) (*api.BatchFileMetadata, error) {
	if w.finished {
		return nil, errStreamFinished
	}
	w.finished = true
	defer w.cancel()

	if err := w.w.Close(); err != nil {
		var gErr *googleapi.Error
		if errors.As(err, &gErr) && gErr.Code == http.StatusPreconditionFailed {
			return nil, fmt.Errorf("%w: %s", api.ErrFileExists, w.key)
		}
		return nil, err
	}

	md := &api.BatchFileMetadata{
		Location: w.key,
		Size:     w.bytes,
		ModTime:  time.Now(),
	}
	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File streamed successfully",
		"bucket", w.bucketName, "key", w.key, "size", md.Size)
	return md, nil
}

// Abort discards the upload. It is a no-op after Commit.
func (w *gcsStreamWriter) Abort(_ context.Context) error {
	if w.finished {
		return nil
	}
	w.finished = true
	w.cancel()
	return nil
}

// GetContext returns a derived context with a timeout.
func (c *Client) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	if timeLimit == 0 {
		timeLimit = defaultTimeout
	}
	return context.WithTimeout(parentCtx, timeLimit)
}

// Close closes the underlying storage client.
func (c *Client) Close() error {
	if c.closer != nil {
		return c.closer.Close()
	}
	return nil
}

// gcsReader adapts *storage.Reader to the internal reader interface by
// exposing Size/ModTime as methods. *storage.Reader exposes Attrs as a field
// (not a method); this wrapper caches the values at construction time.
type gcsReader struct {
	*storage.Reader
}

func (r *gcsReader) Size() int64        { return r.Attrs.Size }
func (r *gcsReader) ModTime() time.Time { return r.Attrs.LastModified }
