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

// Package s3 provides an S3-based implementation of the BatchFilesClient interface.
package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/go-logr/logr"
	"github.com/google/uuid"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	fsio "github.com/llm-d/llm-d-batch-gateway/internal/files_store/io"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	defaultTimeout = 30 * time.Second
)

type s3API interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	DeleteObjects(ctx context.Context, params *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error)
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CreateBucket(ctx context.Context, params *s3.CreateBucketInput, optFns ...func(*s3.Options)) (*s3.CreateBucketOutput, error)
	HeadBucket(ctx context.Context, params *s3.HeadBucketInput, optFns ...func(*s3.Options)) (*s3.HeadBucketOutput, error)
}

type uploaderAPI interface {
	Upload(ctx context.Context, input *s3.PutObjectInput, opts ...func(*manager.Uploader)) (*manager.UploadOutput, error) //nolint:staticcheck // TODO: migrate to feature/s3/transfermanager
}

// Client implements api.BatchFilesClient using S3 storage.
// All objects are stored in a single configured bucket. The folderName parameter
// in Store/Retrieve/Delete is used as part of the S3 key for tenant isolation.
type Client struct {
	s3Client s3API
	uploader uploaderAPI
	prefix   string
	bucket   string
}

var _ api.BatchFilesClient = (*Client)(nil)

// Config holds configuration for the S3 client.
type Config struct {
	Region           string `yaml:"region"`
	Bucket           string `yaml:"bucket"`
	Endpoint         string `yaml:"endpoint"`
	AccessKeyID      string `yaml:"access_key_id"`
	SecretAccessKey  string `yaml:"-"`
	Prefix           string `yaml:"prefix"`
	UsePathStyle     bool   `yaml:"use_path_style"`
	AutoCreateBucket bool   `yaml:"auto_create_bucket"`
}

// Validate checks that all required fields are set.
func (c *Config) Validate() error {
	if c.Region == "" {
		return fmt.Errorf("s3.region cannot be empty")
	}
	if c.Bucket == "" {
		return fmt.Errorf("s3.bucket cannot be empty")
	}
	return nil
}

// New creates a new S3-based BatchFilesClient.
func New(ctx context.Context, cfg Config) (*Client, error) {
	var opts []func(*config.LoadOptions) error
	opts = append(opts, config.WithRegion(cfg.Region))

	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, "")))
	}

	awsCfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		})
	}
	if cfg.UsePathStyle {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.UsePathStyle = true
		})
	}

	s3Client := s3.NewFromConfig(awsCfg, s3Opts...)

	c := &Client{
		s3Client: s3Client,
		uploader: manager.NewUploader(s3Client), //nolint:staticcheck // TODO: migrate to feature/s3/transfermanager
		prefix:   cfg.Prefix,
		bucket:   cfg.Bucket,
	}

	if err := c.ensureBucket(ctx, cfg.AutoCreateBucket); err != nil {
		return nil, fmt.Errorf("ensure bucket %s: %w", cfg.Bucket, err)
	}

	return c, nil
}

// buildKey constructs the full S3 key from the folder name and file name.
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
func (c *Client) ensureBucket(ctx context.Context, autoCreate bool) error {
	_, err := c.s3Client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err == nil {
		return nil
	}

	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		var noSuchBucket *types.NoSuchBucket
		if !errors.As(err, &noSuchBucket) {
			return fmt.Errorf("head bucket %s: %w", c.bucket, err)
		}
	}

	if !autoCreate {
		return fmt.Errorf("bucket %s does not exist", c.bucket)
	}

	_, err = c.s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(c.bucket),
	})
	if err != nil {
		var alreadyOwned *types.BucketAlreadyOwnedByYou
		if errors.As(err, &alreadyOwned) {
			return nil
		}
		return fmt.Errorf("create bucket %s: %w", c.bucket, err)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("Bucket created", "bucket", c.bucket)
	return nil
}

// Store stores a file in S3.
// The folderName parameter is used as a key prefix for tenant isolation.
func (c *Client) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (
	*api.BatchFileMetadata, error,
) {
	key := c.buildKey(folderName, fileName)

	_, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return nil, fmt.Errorf("%w: %s", api.ErrFileExists, key)
	}

	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		return nil, err
	}

	countingReader := &fsio.LimitedCountingReader{
		Reader:    reader,
		SizeLimit: fileSizeLimit,
		LineLimit: lineNumLimit,
	}

	_, err = c.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
		Body:   countingReader,
		// Conditional create: fail instead of overwriting an object that
		// appeared after the HeadObject check, so the create-only contract
		// holds even when two writers race. (Multipart uploads do not
		// enforce the condition, but the objects written through this path
		// are single JSONL lines, far below the manager's part size.)
		IfNoneMatch: aws.String("*"),
	})
	if err != nil {
		// This SDK version surfaces the 412 only as an error code, not a
		// typed error.
		var apiErr smithy.APIError
		switch {
		case errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed":
			return nil, fmt.Errorf("%w: %s", api.ErrFileExists, key)
		case errors.Is(err, api.ErrFileTooLarge):
			return nil, api.ErrFileTooLarge
		case errors.Is(err, api.ErrTooManyLines):
			return nil, api.ErrTooManyLines
		default:
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
		"bucket", c.bucket, "key", key, "size", metadata.Size, "lines", metadata.LinesNumber)

	return metadata, nil
}

// errStreamAborted is the body-read error that aborts an in-flight upload.
var errStreamAborted = errors.New("stream aborted")

// OpenStream opens a streaming write to S3. The upload runs under the
// context passed here: cancelling it fails subsequent writes. The bytes go
// to a unique staging key and become visible at the destination only when
// the returned StreamWriter is committed — the commit publishes with a
// create-only conditional write, which the multipart upload of a large body
// cannot carry itself.
func (c *Client) OpenStream(ctx context.Context, fileName, folderName string) (api.StreamWriter, error) {
	key := c.buildKey(folderName, fileName)

	_, err := c.s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err == nil {
		return nil, fmt.Errorf("%w: %s", api.ErrFileExists, key)
	}

	var notFound *types.NotFound
	if !errors.As(err, &notFound) {
		return nil, err
	}

	stagingKey := key + ".staging." + uuid.NewString()

	pr, pw := io.Pipe()
	w := &s3StreamWriter{
		pw:         pw,
		s3Client:   c.s3Client,
		done:       make(chan error, 1),
		bucket:     c.bucket,
		key:        key,
		stagingKey: stagingKey,
	}
	go func() {
		_, uploadErr := c.uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(c.bucket),
			Key:    aws.String(stagingKey),
			Body:   pr,
		})
		// Unblock a writer stuck in Write now that the upload stopped
		// reading (e.g. the context was cancelled).
		_ = pr.CloseWithError(uploadErr)
		w.done <- uploadErr
	}()

	return w, nil
}

// s3StreamWriter pipes writes into an S3 upload under a unique staging key;
// Commit publishes the staged bytes to the destination with a create-only
// conditional write. The manager.Uploader switches to multipart for large
// bodies and aborts its parts when the body read fails, so an Abort leaves
// no partial object behind — at worst an unreferenced staging object, which
// Abort removes best-effort.
type s3StreamWriter struct {
	pw         *io.PipeWriter
	done       chan error
	mu         sync.Mutex
	s3Client   s3API
	bucket     string
	key        string
	stagingKey string
	bytes      int64
}

var _ api.StreamWriter = (*s3StreamWriter)(nil)

func (w *s3StreamWriter) Write(p []byte) (int, error) {
	n, err := w.pw.Write(p)
	w.bytes += int64(n)
	return n, err
}

func (w *s3StreamWriter) Commit(ctx context.Context) (*api.BatchFileMetadata, error) {
	if err := w.pw.Close(); err != nil {
		return nil, err
	}
	if err := w.result(); err != nil {
		return nil, err
	}

	md, err := w.publish(ctx)
	if err != nil {
		return nil, err
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File streamed successfully",
		"bucket", w.bucket, "key", w.key, "size", md.Size)

	return md, nil
}

// publish moves the staged object to its destination with an If-None-Match
// conditional write, making the commit create-only: a writer that opened
// its stream before the current owner published gets ErrFileExists instead
// of replacing the winner's object. The staged bytes are buffered first:
// the SDK's S3 payload-hash middleware reads the PUT body to compute
// x-amz-content-sha256 and rewinds it, so the body must be seekable — a
// streaming GetObject body cannot be rewound, and plain-HTTP S3-compatible
// endpoints (no unsigned-payload escape) cannot sign a streaming body.
// Store publishes seekable bodies the same way.
func (w *s3StreamWriter) publish(ctx context.Context) (*api.BatchFileMetadata, error) {
	staged, err := w.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.stagingKey),
	})
	if err != nil {
		return nil, fmt.Errorf("open staged object %s: %w", w.stagingKey, err)
	}
	buf, readErr := io.ReadAll(staged.Body)
	closeErr := staged.Body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read staged object %s: %w", w.stagingKey, readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("close staged object %s: %w", w.stagingKey, closeErr)
	}
	size := int64(len(buf))

	_, err = w.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(w.bucket),
		Key:           aws.String(w.key),
		Body:          bytes.NewReader(buf),
		ContentLength: aws.Int64(size),
		IfNoneMatch:   aws.String("*"),
	})
	if err != nil {
		// This SDK version surfaces the 412 only as an error code, not a
		// typed error.
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) && apiErr.ErrorCode() == "PreconditionFailed" {
			// Another writer published first: the winner's object stands, and
			// this stream's staged bytes are garbage.
			if _, delErr := w.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket: aws.String(w.bucket),
				Key:    aws.String(w.stagingKey),
			}); delErr != nil {
				logr.FromContextOrDiscard(ctx).Error(delErr, "Failed to delete staged object", "key", w.stagingKey)
			}
			return nil, fmt.Errorf("%w: %s", api.ErrFileExists, w.key)
		}
		return nil, err
	}

	// The destination owns the bytes now; drop the staging object.
	if _, err := w.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(w.bucket),
		Key:    aws.String(w.stagingKey),
	}); err != nil {
		logr.FromContextOrDiscard(ctx).Error(err, "Failed to delete staged object", "key", w.stagingKey)
	}

	modTime := time.Now()
	if staged.LastModified != nil {
		modTime = *staged.LastModified
	}
	return &api.BatchFileMetadata{
		Location: w.key,
		Size:     size,
		ModTime:  modTime,
	}, nil
}

func (w *s3StreamWriter) Abort(ctx context.Context) error {
	// Failing the body read aborts the upload.
	_ = w.pw.CloseWithError(errStreamAborted)
	if err := w.result(); err == nil {
		// The staged object was fully uploaded but never published; remove
		// it so the abort leaves nothing behind.
		if _, delErr := w.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(w.bucket),
			Key:    aws.String(w.stagingKey),
		}); delErr != nil {
			logr.FromContextOrDiscard(ctx).Error(delErr, "Failed to delete staged object after abort", "key", w.stagingKey)
		}
		return fmt.Errorf("stream aborted but staged object %s was already uploaded", w.stagingKey)
	}
	return nil
}

// result consumes the upload outcome exactly once: the goroutine sends a
// single value and never closes the channel, so a second receive — Commit
// followed by the caller's cleanup Abort — would block forever.
func (w *s3StreamWriter) result() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done == nil {
		return errStreamAborted
	}
	err := <-w.done
	w.done = nil
	return err
}

// Retrieve retrieves a file from S3.
// The folderName parameter is used as a key prefix for tenant isolation.
func (c *Client) Retrieve(ctx context.Context, fileName, folderName string) (io.ReadCloser, *api.BatchFileMetadata, error) {
	key := c.buildKey(folderName, fileName)

	out, err := c.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		var noSuchBucket *types.NoSuchBucket
		if errors.As(err, &noSuchKey) || errors.As(err, &noSuchBucket) {
			return nil, nil, os.ErrNotExist
		}
		return nil, nil, err
	}

	var size int64
	if out.ContentLength != nil {
		size = *out.ContentLength
	}

	modTime := time.Now()
	if out.LastModified != nil {
		modTime = *out.LastModified
	}

	metadata := &api.BatchFileMetadata{
		Location: key,
		Size:     size,
		ModTime:  modTime,
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File retrieved successfully",
		"bucket", c.bucket, "key", key, "size", metadata.Size)

	return out.Body, metadata, nil
}

// Delete deletes a file from S3.
// The folderName parameter is used as a key prefix for tenant isolation.
func (c *Client) Delete(ctx context.Context, fileName, folderName string) error {
	key := c.buildKey(folderName, fileName)

	_, err := c.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noSuchBucket *types.NoSuchBucket
		if errors.As(err, &noSuchBucket) {
			return os.ErrNotExist
		}
		return err
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("File deleted successfully",
		"bucket", c.bucket, "key", key)

	return nil
}

// deleteObjectsBatchLimit is S3's maximum number of keys per DeleteObjects
// call.
const deleteObjectsBatchLimit = 1000

// DeletePrefix deletes every object under folderName/prefix and returns the
// number of deleted objects. A prefix with no stored objects is a no-op.
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

	logger := logr.FromContextOrDiscard(ctx).V(logging.INFO)
	var deleted int
	var token *string
	for {
		out, err := c.s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(c.bucket),
			Prefix:            aws.String(keyPrefix),
			ContinuationToken: token,
		})
		if err != nil {
			return deleted, err
		}

		objects := make([]types.ObjectIdentifier, 0, len(out.Contents))
		for _, obj := range out.Contents {
			if obj.Key != nil {
				objects = append(objects, types.ObjectIdentifier{Key: obj.Key})
			}
		}
		for start := 0; start < len(objects); start += deleteObjectsBatchLimit {
			end := min(start+deleteObjectsBatchLimit, len(objects))
			del, err := c.s3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
				Bucket: aws.String(c.bucket),
				Delete: &types.Delete{Objects: objects[start:end], Quiet: aws.Bool(true)},
			})
			if err != nil {
				return deleted, err
			}
			for _, e := range del.Errors {
				logger.Info("Failed to delete object", "key", *e.Key, "message", *e.Message)
			}
			deleted += end - start - len(del.Errors)
		}

		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		token = out.NextContinuationToken
	}

	logger.Info("Prefix deleted", "bucket", c.bucket, "prefix", keyPrefix, "objects", deleted)
	return deleted, nil
}

// GetContext returns a derived context with a timeout.
func (c *Client) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	if timeLimit == 0 {
		timeLimit = defaultTimeout
	}
	return context.WithTimeout(parentCtx, timeLimit)
}

// Close closes the client.
func (c *Client) Close() error {
	return nil
}
