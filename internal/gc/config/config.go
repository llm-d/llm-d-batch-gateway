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

// Package config provides configuration loading and validation for the batch garbage collector.
package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"

	sharedcfg "github.com/llm-d/llm-d-batch-gateway/internal/shared/config"
)

const (
	// DefaultMaxConcurrency is the default number of concurrent item deletions per GC cycle.
	DefaultMaxConcurrency = 10

	// DefaultReconcilerInterval is the default interval between orphan reconciler cycles.
	// It also serves as the staleness threshold for in-flight entries.
	DefaultReconcilerInterval = 60 * time.Minute
)

// ReconcilerConfig holds the orphan reconciler configuration.
type ReconcilerConfig struct {
	Enabled                bool          `yaml:"enabled"`
	Interval               time.Duration `yaml:"interval"`
	ProcessorLabelSelector string        `yaml:"processor_label_selector"`
	ProcessorStatefulSet   string        `yaml:"processor_statefulset"`
}

// CollectorConfig holds collector-specific settings (interval and concurrency).
type CollectorConfig struct {
	Interval       time.Duration `yaml:"interval"`
	MaxConcurrency int           `yaml:"max_concurrency"`
}

// DefaultMetricsAddr is the default listen address for the metrics HTTP server.
const DefaultMetricsAddr = ":9091"

// Config holds the garbage collector configuration.
type Config struct {
	DryRun      bool   `yaml:"dry_run"`
	MetricsAddr string `yaml:"metrics_addr"`

	// Collector holds the collector-specific configuration (interval, concurrency).
	Collector CollectorConfig `yaml:"collector"`

	// Reconciler configures the orphan reconciler that detects and recovers
	// batch jobs stuck in non-terminal states.
	Reconciler ReconcilerConfig `yaml:"reconciler"`

	// DB client configuration
	DBClientCfg sharedcfg.DBClientConfig `yaml:"db_client"`

	// FileClientCfg holds the file storage backend configuration.
	FileClientCfg sharedcfg.FileClientConfig `yaml:"file_client"`

	// OTelCfg holds OpenTelemetry-related settings.
	OTelCfg sharedcfg.OTelConfig `yaml:"otel"`
}

// Load reads and validates a Config from the given YAML file path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := &Config{
		Collector: CollectorConfig{
			Interval:       1 * time.Hour,
			MaxConcurrency: DefaultMaxConcurrency,
		},
		MetricsAddr: DefaultMetricsAddr,
		Reconciler: ReconcilerConfig{
			Enabled:  true,
			Interval: DefaultReconcilerInterval,
		},
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	if cfg.Collector.MaxConcurrency <= 0 {
		return nil, fmt.Errorf("collector.max_concurrency must be positive, got %d", cfg.Collector.MaxConcurrency)
	}

	if cfg.Collector.Interval <= 0 {
		return nil, fmt.Errorf("collector.interval must be positive, got %v", cfg.Collector.Interval)
	}

	if cfg.Reconciler.Enabled && cfg.Reconciler.Interval <= 0 {
		return nil, fmt.Errorf("reconciler.interval must be positive when enabled, got %v", cfg.Reconciler.Interval)
	}

	if err := validateMetricsAddr(cfg.MetricsAddr); err != nil {
		return nil, fmt.Errorf("metrics_addr: %w", err)
	}

	switch cfg.DBClientCfg.Type {
	case sharedcfg.DBTypePostgreSQL:
		// valid
	case "":
		return nil, fmt.Errorf("db_client.type is required (must be \"postgresql\")")
	default:
		return nil, fmt.Errorf("db_client.type must be \"postgresql\", got %q", cfg.DBClientCfg.Type)
	}

	// Validate via the shared helper (single source of truth for fs/s3/gcs/mock),
	// then reject mock: gc only supports the real object stores. Keep this local
	// rejection so the shared helper can stay generic (apiserver/processor accept mock).
	if err := cfg.FileClientCfg.ValidateType(); err != nil {
		return nil, err
	}
	if cfg.FileClientCfg.Type == sharedcfg.FileTypeMock {
		return nil, fmt.Errorf("file_client.type %q is not supported by gc (use %q, %q, or %q)",
			sharedcfg.FileTypeMock, sharedcfg.FileTypeFS, sharedcfg.FileTypeS3, sharedcfg.FileTypeGCS)
	}

	if err := cfg.FileClientCfg.Retry.Validate(); err != nil {
		return nil, fmt.Errorf("file_client.retry: %w", err)
	}

	return cfg, nil
}

// validateMetricsAddr ensures addr is in ":PORT" format (e.g. ":9091").
// The Helm chart derives the containerPort by trimming the leading colon,
// so host:port forms like "0.0.0.0:9091" are intentionally rejected.
func validateMetricsAddr(addr string) error {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("must be :PORT format (e.g. \":9091\"), got %q", addr)
	}
	if host != "" {
		return fmt.Errorf("must be :PORT format (e.g. \":9091\"), got %q; host part is not supported", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("port must be 1-65535, got %q", portStr)
	}
	return nil
}
