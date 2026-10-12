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

package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoad_FilesystemBackend(t *testing.T) {
	path := writeTempConfig(t, `
dry_run: true
collector:
  interval: 30m
db_client:
  type: "postgresql"
file_client:
  type: "fs"
  fs:
    base_path: "/data/files"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.DryRun {
		t.Error("expected dry_run to be true")
	}
	if cfg.Collector.Interval != 30*time.Minute {
		t.Errorf("expected interval 30m, got %v", cfg.Collector.Interval)
	}
	if cfg.FileClientCfg.Type != "fs" {
		t.Errorf("expected file_client.type fs, got %s", cfg.FileClientCfg.Type)
	}
	if cfg.FileClientCfg.FSConfig.BasePath != "/data/files" {
		t.Errorf("expected base_path /data/files, got %s", cfg.FileClientCfg.FSConfig.BasePath)
	}
}

func TestLoad_S3Backend(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
file_client:
  type: "s3"
  s3:
    region: "us-east-1"
    endpoint: "https://s3.example.com"
    access_key_id: "AKID"
    prefix: "batch/"
    use_path_style: true
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FileClientCfg.Type != "s3" {
		t.Errorf("expected file_client.type s3, got %s", cfg.FileClientCfg.Type)
	}
	if cfg.FileClientCfg.S3Config.Region != "us-east-1" {
		t.Errorf("expected region us-east-1, got %s", cfg.FileClientCfg.S3Config.Region)
	}
	if cfg.FileClientCfg.S3Config.Endpoint != "https://s3.example.com" {
		t.Errorf("expected endpoint https://s3.example.com, got %s", cfg.FileClientCfg.S3Config.Endpoint)
	}
	if cfg.FileClientCfg.S3Config.AccessKeyID != "AKID" {
		t.Errorf("expected access_key_id AKID, got %s", cfg.FileClientCfg.S3Config.AccessKeyID)
	}
	if cfg.FileClientCfg.S3Config.Prefix != "batch/" {
		t.Errorf("expected prefix batch/, got %s", cfg.FileClientCfg.S3Config.Prefix)
	}
	if !cfg.FileClientCfg.S3Config.UsePathStyle {
		t.Error("expected use_path_style to be true")
	}
}

func TestLoad_PostgreSQLDatabase(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DBClientCfg.Type != "postgresql" {
		t.Errorf("expected db_client.type postgresql, got %s", cfg.DBClientCfg.Type)
	}
}

func TestLoad_Defaults(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DryRun {
		t.Error("expected dry_run to default to false")
	}
	if cfg.Collector.Interval != 1*time.Hour {
		t.Errorf("expected default interval 1h, got %v", cfg.Collector.Interval)
	}
	if cfg.Collector.MaxConcurrency != DefaultMaxConcurrency {
		t.Errorf("expected default max_concurrency %d, got %d", DefaultMaxConcurrency, cfg.Collector.MaxConcurrency)
	}
}

func TestLoad_CustomMaxConcurrency(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
collector:
  max_concurrency: 20
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Collector.MaxConcurrency != 20 {
		t.Errorf("expected max_concurrency 20, got %d", cfg.Collector.MaxConcurrency)
	}
}

func TestLoad_ErrorZeroMaxConcurrency(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
collector:
  max_concurrency: 0
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for zero max_concurrency")
	}
}

func TestLoad_ErrorNegativeMaxConcurrency(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
collector:
  max_concurrency: -5
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for negative max_concurrency")
	}
}

func TestLoad_ErrorNoDBClientType(t *testing.T) {
	path := writeTempConfig(t, `
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when db_client.type is missing")
	}
}

func TestLoad_ErrorInvalidDBClientType(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "mysql"
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid db_client.type")
	}
}

func TestLoad_ErrorNoFileClientType(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error when file_client.type is missing")
	}
}

func TestLoad_ErrorInvalidFileClientType(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
file_client:
  type: "gcs"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid file_client.type")
	}
}

func TestLoad_ErrorMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoad_ErrorInvalidYAML(t *testing.T) {
	path := writeTempConfig(t, `{{{invalid`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoad_ErrorZeroInterval(t *testing.T) {
	path := writeTempConfig(t, `
collector:
  interval: 0s
db_client:
  type: "postgresql"
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for zero interval")
	}
}

func TestLoad_ErrorNegativeInterval(t *testing.T) {
	path := writeTempConfig(t, `
collector:
  interval: -5m
db_client:
  type: "postgresql"
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for negative interval")
	}
}

func TestLoad_ReconcilerDefaults(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.Reconciler.Enabled {
		t.Error("expected reconciler to be enabled by default")
	}
	if cfg.Reconciler.Interval != DefaultReconcilerInterval {
		t.Errorf("expected default reconciler interval %v, got %v", DefaultReconcilerInterval, cfg.Reconciler.Interval)
	}
}

func TestLoad_ReconcilerCustomInterval(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
reconciler:
  enabled: true
  interval: 30m
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.Reconciler.Enabled {
		t.Error("expected reconciler to be enabled")
	}
	if cfg.Reconciler.Interval != 30*time.Minute {
		t.Errorf("expected reconciler interval 30m, got %v", cfg.Reconciler.Interval)
	}
}

func TestLoad_ReconcilerDisabled(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
reconciler:
  enabled: false
  interval: 0s
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Reconciler.Enabled {
		t.Error("expected reconciler to be disabled")
	}
}

func TestLoad_ErrorReconcilerZeroInterval(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
reconciler:
  enabled: true
  interval: 0s
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for zero reconciler interval when enabled")
	}
}

func TestLoad_ErrorReconcilerNegativeInterval(t *testing.T) {
	path := writeTempConfig(t, `
db_client:
  type: "postgresql"
reconciler:
  enabled: true
  interval: -10m
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for negative reconciler interval when enabled")
	}
}

func TestLoad_MetricsAddr(t *testing.T) {
	valid := []string{":9091", ":8080", ":1", ":65535"}
	for _, addr := range valid {
		t.Run("valid_"+addr, func(t *testing.T) {
			path := writeTempConfig(t, `
metrics_addr: "`+addr+`"
db_client:
  type: "postgresql"
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
			cfg, err := Load(path)
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", addr, err)
			}
			if cfg.MetricsAddr != addr {
				t.Fatalf("metrics_addr=%q, want %q", cfg.MetricsAddr, addr)
			}
		})
	}

	invalid := []string{"9091", "0.0.0.0:9091", "localhost:9091", "example.com:9091", ":0", ":99999"}
	for _, addr := range invalid {
		t.Run("invalid_"+addr, func(t *testing.T) {
			path := writeTempConfig(t, `
metrics_addr: "`+addr+`"
db_client:
  type: "postgresql"
file_client:
  type: "fs"
  fs:
    base_path: "/tmp/files"
`)
			_, err := Load(path)
			if err == nil {
				t.Fatalf("expected error for metrics_addr %q", addr)
			}
		})
	}
}
