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

// Package config provides shared configuration types used by apiserver, processor, and gc.
package config

import (
	"fmt"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	fsclient "github.com/llm-d/llm-d-batch-gateway/internal/files_store/fs"
	gcsclient "github.com/llm-d/llm-d-batch-gateway/internal/files_store/gcs"
	s3client "github.com/llm-d/llm-d-batch-gateway/internal/files_store/s3"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/retry"
)

// Database backend types.
const (
	DBTypePostgreSQL = "postgresql"
	DBTypeMock       = "mock"
)

// File storage backend types.
const (
	FileTypeFS   = "fs"
	FileTypeS3   = "s3"
	FileTypeGCS  = "gcs"
	FileTypeMock = "mock"
)

// DBClientConfig holds database client configuration shared by all components.
type DBClientConfig struct {
	// Type specifies the database backend: DBTypePostgreSQL.
	Type string `yaml:"type"`
	// PostgreSQLCfg holds PostgreSQL connection settings (used when Type is "postgresql").
	PostgreSQLCfg postgresql.PostgreSQLConfig `yaml:"postgresql"`
}

// DeepCopy returns a copy of the config.
func (c DBClientConfig) DeepCopy() DBClientConfig {
	return c
}

// FileClientConfig holds file storage client configuration shared by all components.
type FileClientConfig struct {
	Type      string           `yaml:"type"`
	FSConfig  fsclient.Config  `yaml:"fs"`
	S3Config  s3client.Config  `yaml:"s3"`
	GCSConfig gcsclient.Config `yaml:"gcs"`
	Retry     retry.Config     `yaml:"retry"`
}

// ValidateType validates the Type field. It accepts fs, s3, gcs, and mock.
// Components that don't support mock (currently only gc) must reject it
// themselves on top of this call, so the fs/s3/gcs set has a single source of
// truth here and callers don't each maintain a copy of the switch. Returns nil
// for a valid type; returns an error for the empty string or an unsupported
// value, naming the accepted values so the message is actionable.
func (c FileClientConfig) ValidateType() error {
	switch c.Type {
	case FileTypeFS, FileTypeS3, FileTypeGCS, FileTypeMock:
		return nil
	case "":
		return fmt.Errorf("file_client.type is required (must be %q, %q, %q, or %q)",
			FileTypeFS, FileTypeS3, FileTypeGCS, FileTypeMock)
	default:
		return fmt.Errorf("file_client.type must be %q, %q, %q, or %q, got %q",
			FileTypeFS, FileTypeS3, FileTypeGCS, FileTypeMock, c.Type)
	}
}

// OTelConfig holds OpenTelemetry-related settings shared by apiserver and processor.
type OTelConfig struct {
	PostgresqlTracing bool `yaml:"postgresql_tracing"`
}
