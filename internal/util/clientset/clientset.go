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

// Package clientset provides factory functions for creating all external clients
// used by the batch gateway apiserver and processor. Centralising client
// construction here ensures both processes use identical setup logic.
package clientset

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"

	"github.com/go-logr/logr"
	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	fsapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	fsclient "github.com/llm-d/llm-d-batch-gateway/internal/files_store/fs"
	"github.com/llm-d/llm-d-batch-gateway/internal/files_store/retryclient"
	s3client "github.com/llm-d/llm-d-batch-gateway/internal/files_store/s3"
	fstracing "github.com/llm-d/llm-d-batch-gateway/internal/files_store/tracing"
	sharedcfg "github.com/llm-d/llm-d-batch-gateway/internal/shared/config"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// Clientset holds all clients.
type Clientset struct {
	File           fsapi.BatchFilesClient
	BatchDB        dbapi.BatchProgressDBClient
	FileDB         dbapi.FileDBClient
	Queue          dbapi.BatchPriorityQueueClient
	Event          dbapi.BatchEventChannelClient
	Inference      *inference.GatewayResolver
	AsyncInference *inference.AsyncGatewayResolver
}

// NewFSFileClient creates a filesystem-based file storage client.
func NewFSFileClient(ctx context.Context, cfg *fsclient.Config) (fsapi.BatchFilesClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("fs config cannot be nil")
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid fs config: %w", err)
	}
	c, err := fsclient.New(cfg.BasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create fs file client: %w", err)
	}
	logr.FromContextOrDiscard(ctx).Info("Filesystem-based file client created", "base_path", cfg.BasePath)
	return c, nil
}

// NewS3FileClient creates an S3-based file storage client.
// It reads the secret access key from the mounted secrets when not set in the config.
func NewS3FileClient(ctx context.Context, cfg *s3client.Config) (fsapi.BatchFilesClient, error) {
	if cfg == nil {
		return nil, fmt.Errorf("s3 config cannot be nil")
	}
	if cfg.SecretAccessKey == "" {
		s3SecretAccessKey, err := ucom.ReadSecretFile(ucom.SecretKeyS3SecretAccessKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read S3 secret access key: %w", err)
		}
		cfg.SecretAccessKey = s3SecretAccessKey
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid s3 config: %w", err)
	}
	c, err := s3client.New(ctx, *cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create s3 file client: %w", err)
	}
	logr.FromContextOrDiscard(ctx).Info("S3 file client created", "region", cfg.Region, "endpoint", cfg.Endpoint)
	return c, nil
}

// NewPostgreSQLDBClients creates PostgreSQL-backed batch and file database clients.
// It reads the URL from the mounted secrets when not set in the config.
func NewPostgreSQLDBClients(ctx context.Context, cfg *postgresql.PostgreSQLConfig) (dbapi.BatchProgressDBClient, dbapi.FileDBClient, error) {
	if cfg == nil {
		return nil, nil, fmt.Errorf("postgresql config cannot be nil")
	}
	if cfg.Url == "" {
		postgreSQLURL, err := ucom.ReadSecretFile(ucom.SecretKeyPostgreSQLURL)
		if err != nil {
			return nil, nil, err
		}
		cfg.Url = postgreSQLURL
	}
	batchDB, err := postgresql.NewPostgresBatchDBClient(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create postgresql batch-db client: %w", err)
	}
	fileDB, err := postgresql.NewPostgresFileDBClient(ctx, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create postgresql file-db client: %w", err)
	}
	logr.FromContextOrDiscard(ctx).Info("PostgreSQL-based database client created")
	return batchDB, fileDB, nil
}

// Option configures which clients NewClientset creates.
type Option func(*clientsetConfig)

type clientsetConfig struct {
	dbCfg             *sharedcfg.DBClientConfig
	fileCfg           *sharedcfg.FileClientConfig
	inferenceGlobal   *inference.GatewayClientConfig
	inferencePerModel map[string]inference.GatewayClientConfig
	asyncInference    *inference.AsyncClientConfig
}

// WithDB enables creation of batch and file database clients.
func WithDB(cfg sharedcfg.DBClientConfig) Option {
	cfg = cfg.DeepCopy()
	return func(c *clientsetConfig) { c.dbCfg = &cfg }
}

// WithFile enables creation of the file storage client.
func WithFile(cfg sharedcfg.FileClientConfig) Option {
	return func(c *clientsetConfig) { c.fileCfg = &cfg }
}

// WithGlobalInference enables creation of a global inference client.
func WithGlobalInference(cfg inference.GatewayClientConfig) Option {
	return func(c *clientsetConfig) { c.inferenceGlobal = &cfg }
}

// WithPerModelInference enables creation of per-model inference clients.
func WithPerModelInference(cfgs map[string]inference.GatewayClientConfig) Option {
	copied := make(map[string]inference.GatewayClientConfig, len(cfgs))
	for k, v := range cfgs {
		copied[k] = v
	}
	return func(c *clientsetConfig) { c.inferencePerModel = copied }
}

// WithAsyncInference enables async dispatch via llm-d-async queues.
func WithAsyncInference(cfg inference.AsyncClientConfig) Option {
	copied := cfg
	copied.Models = maps.Clone(cfg.Models)
	return func(c *clientsetConfig) { c.asyncInference = &copied }
}

// NewClientset creates the clients specified by the given options.
// On error, any clients already created are closed before returning.
func NewClientset(ctx context.Context, component ucom.Component, opts ...Option) (_ *Clientset, retErr error) {
	logger := logr.FromContextOrDiscard(ctx)

	cfg := &clientsetConfig{}
	for _, o := range opts {
		o(cfg)
	}

	cs := &Clientset{}
	defer func() {
		if retErr != nil {
			if closeErr := cs.Close(); closeErr != nil {
				logger.Error(closeErr, "failed to close partially constructed clientset")
			}
		}
	}()

	// build file store client
	if cfg.fileCfg != nil {
		switch cfg.fileCfg.Type {
		case sharedcfg.FileTypeFS:
			c, err := NewFSFileClient(ctx, &cfg.fileCfg.FSConfig)
			if err != nil {
				return nil, err
			}
			cs.File = fstracing.Wrap(c, sharedcfg.FileTypeFS)
		case sharedcfg.FileTypeS3:
			c, err := NewS3FileClient(ctx, &cfg.fileCfg.S3Config)
			if err != nil {
				return nil, err
			}
			cs.File = fstracing.Wrap(c, sharedcfg.FileTypeS3)
		default:
			return nil, fmt.Errorf("unsupported file_client.type: %s (supported values: fs, s3)", cfg.fileCfg.Type)
		}
		if cfg.fileCfg.Retry.MaxRetries > 0 {
			cs.File = retryclient.New(cs.File, cfg.fileCfg.Retry, component)
			logger.Info("File client wrapped with retry", "maxRetries", cfg.fileCfg.Retry.MaxRetries)
		}
	}

	// build database client
	if cfg.dbCfg != nil {
		switch cfg.dbCfg.Type {
		case sharedcfg.DBTypePostgreSQL:
			batchDB, fileDB, err := NewPostgreSQLDBClients(ctx, &cfg.dbCfg.PostgreSQLCfg)
			if err != nil {
				return nil, err
			}
			cs.BatchDB = batchDB
			cs.FileDB = fileDB

			var processorID string
			if component == ucom.ComponentProcessor {
				processorID, err = os.Hostname()
				if err != nil {
					return nil, fmt.Errorf("failed to get hostname for processor ID: %w", err)
				}
			}
			queueClient, err := postgresql.NewPostgresBatchQueueClient(ctx, &cfg.dbCfg.PostgreSQLCfg, processorID)
			if err != nil {
				return nil, fmt.Errorf("failed to create postgres queue client: %w", err)
			}
			cs.Queue = queueClient

			eventClient, err := postgresql.NewPostgresBatchEventClient(ctx, &cfg.dbCfg.PostgreSQLCfg, logger)
			if err != nil {
				return nil, fmt.Errorf("failed to create postgres event client: %w", err)
			}
			cs.Event = eventClient
		default:
			return nil, fmt.Errorf("unsupported database.type: %s (supported values: postgresql)", cfg.dbCfg.Type)
		}
	}

	// build inference client(s)
	switch {
	case cfg.asyncInference != nil:
		if cfg.asyncInference.RedisURL == "" {
			redisURL, err := ucom.ReadSecretFile(ucom.SecretKeyRedisURL)
			if err != nil {
				return nil, fmt.Errorf("async inference requires a Redis URL (set RedisURL or configure secret %s): %w", ucom.SecretKeyRedisURL, err)
			}
			cfg.asyncInference.RedisURL = redisURL
		}
		resolver, err := inference.NewAsyncResolver(*cfg.asyncInference, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create async inference clients: %w", err)
		}
		logger.Info("Async inference clients created", "count", len(cfg.asyncInference.Models))
		cs.AsyncInference = resolver
	case cfg.inferenceGlobal != nil:
		resolver, err := inference.NewGlobalResolver(*cfg.inferenceGlobal, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create global inference client: %w", err)
		}
		logger.Info("Global inference client created")
		cs.Inference = resolver
	case len(cfg.inferencePerModel) > 0:
		resolver, err := inference.NewPerModelResolver(cfg.inferencePerModel, logger)
		if err != nil {
			return nil, fmt.Errorf("failed to create per-model inference clients: %w", err)
		}
		logger.Info("Per-model inference clients created", "count", len(cfg.inferencePerModel))
		cs.Inference = resolver
	}

	return cs, nil
}

func (cs *Clientset) Close() error {
	var errs []error
	if cs.Queue != nil {
		if err := cs.Queue.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if cs.Event != nil {
		if err := cs.Event.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if cs.BatchDB != nil {
		if err := cs.BatchDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if cs.FileDB != nil {
		if err := cs.FileDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if cs.File != nil {
		if err := cs.File.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if cs.Inference != nil {
		if err := cs.Inference.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if cs.AsyncInference != nil {
		if err := cs.AsyncInference.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
