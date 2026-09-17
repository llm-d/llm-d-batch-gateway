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
	"strings"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/ptr"
)

// validPerModelConfig returns a minimal valid per-model gateway config for tests.
func validPerModelConfig() map[string]ModelGatewayConfig {
	return map[string]ModelGatewayConfig{
		"llama-3": {
			URL:            "http://llama-gw:8000",
			RequestTimeout: ptr.To(5 * time.Minute),
			MaxRetries:     ptr.To(3),
			InitialBackoff: ptr.To(1 * time.Second),
			MaxBackoff:     ptr.To(60 * time.Second),
		},
	}
}

// validGlobalConfig returns a minimal valid global gateway config for tests.
func validGlobalConfig() *ModelGatewayConfig {
	return &ModelGatewayConfig{
		URL:            "http://global-gw:8000",
		RequestTimeout: ptr.To(5 * time.Minute),
		MaxRetries:     ptr.To(3),
		InitialBackoff: ptr.To(1 * time.Second),
		MaxBackoff:     ptr.To(60 * time.Second),
	}
}

func TestNewConfig_Defaults(t *testing.T) {
	c := NewConfig()
	if c == nil {
		t.Fatalf("NewConfig returned nil")
		return
	}

	if c.PollInterval != 5*time.Second {
		t.Fatalf("PollInterval = %v, want %v", c.PollInterval, 5*time.Second)
	}
	if c.TaskWaitTime != 1*time.Second {
		t.Fatalf("TaskWaitTime = %v, want %v", c.TaskWaitTime, 1*time.Second)
	}
	if c.NumWorkers != DefaultNumWorkers {
		t.Fatalf("NumWorkers = %d, want %d", c.NumWorkers, DefaultNumWorkers)
	}
	if c.WorkDirSizeLimit != DefaultWorkDirSizeLimit {
		t.Fatalf("WorkDirSizeLimit = %q, want %q", c.WorkDirSizeLimit, DefaultWorkDirSizeLimit)
	}
	if c.InputFileDiskBudgetPercent != DefaultInputFileDiskBudgetPercent {
		t.Fatalf("InputFileDiskBudgetPercent = %d, want %d", c.InputFileDiskBudgetPercent, DefaultInputFileDiskBudgetPercent)
	}
	if c.MaxInputFileSizeBytes != DefaultMaxInputFileSizeBytes {
		t.Fatalf("MaxInputFileSizeBytes = %d, want %d", c.MaxInputFileSizeBytes, DefaultMaxInputFileSizeBytes)
	}
	if c.Concurrency.Global != 100 {
		t.Fatalf("Concurrency.Global = %d, want %d", c.Concurrency.Global, 100)
	}
	if c.Concurrency.PerEndpoint != 10 {
		t.Fatalf("Concurrency.PerEndpoint = %d, want %d", c.Concurrency.PerEndpoint, 10)
	}
	if c.WorkDir == "" {
		t.Fatalf("WorkDir should not be empty")
	}
	if c.DBClientCfg.Type != "postgresql" {
		t.Fatalf("DBClientCfg.Type = %q, want %q", c.DBClientCfg.Type, "postgresql")
	}
	if c.Concurrency.Recovery != 5 {
		t.Fatalf("Concurrency.Recovery = %d, want %d", c.Concurrency.Recovery, 5)
	}
	if c.ModelGateways != nil {
		t.Fatalf("ModelGateways should be nil by default, got %v", c.ModelGateways)
	}
	if c.GlobalInferenceGateway != nil {
		t.Fatalf("GlobalInferenceGateway should be nil by default")
	}
	if c.SendFairnessHeader {
		t.Fatalf("SendFairnessHeader = true, want false by default")
	}
	if c.RouteKeyMethod != RouteKeyMethodBare {
		t.Fatalf("RouteKeyMethod = %q, want empty (bare) by default", c.RouteKeyMethod)
	}

	want90Days := int64(90 * 24 * 60 * 60)
	if c.DefaultOutputExpirationSeconds != want90Days {
		t.Fatalf("DefaultOutputExpirationSeconds = %d, want %d", c.DefaultOutputExpirationSeconds, want90Days)
	}
	if c.ProgressTTLSeconds != 86400 {
		t.Fatalf("ProgressTTLSeconds = %d, want %d", c.ProgressTTLSeconds, 86400)
	}
	if c.DispatchMode != DispatchModeSync {
		t.Fatalf("DispatchMode = %q, want %q", c.DispatchMode, DispatchModeSync)
	}
	if c.AsyncDispatchConfig.ResultPollTimeout != 5*time.Second {
		t.Fatalf("AsyncDispatchConfig.ResultPollTimeout = %v, want %v", c.AsyncDispatchConfig.ResultPollTimeout, 5*time.Second)
	}
}

func TestProcessorConfig_EffectiveNumWorkers(t *testing.T) {
	tests := []struct {
		name      string
		workers   int
		sizeLimit string
		percent   int
		fileSize  int64
		want      int
		wantErr   bool
	}{
		{name: "default chart capacity caps twenty workers at five", workers: 20, sizeLimit: "10Gi", percent: 10, want: 5},
		{name: "configured workers remain the lower cap", workers: 4, sizeLimit: "10Gi", percent: 10, want: 4},
		{name: "larger input budget allows all configured workers", workers: 20, sizeLimit: "10Gi", percent: 50, want: 20},
		{name: "larger allowed input reduces the cap", workers: 20, sizeLimit: "10Gi", percent: 10, fileSize: 512 << 20, want: 2},
		{name: "budget too small for one file is rejected", workers: 20, sizeLimit: "1Gi", percent: 10, wantErr: true},
		{name: "invalid percentage is rejected", workers: 20, sizeLimit: "10Gi", percent: 0, wantErr: true},
		{name: "invalid size is rejected", workers: 20, sizeLimit: "not-a-size", percent: 10, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			cfg.NumWorkers = tt.workers
			cfg.WorkDirSizeLimit = tt.sizeLimit
			cfg.InputFileDiskBudgetPercent = tt.percent
			if tt.fileSize != 0 {
				cfg.MaxInputFileSizeBytes = tt.fileSize
			}

			got, err := cfg.EffectiveNumWorkers()
			if tt.wantErr {
				if err == nil {
					t.Fatal("EffectiveNumWorkers() error = nil, want error")
				}
				return
			}
			if err != nil {
				t.Fatalf("EffectiveNumWorkers() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("EffectiveNumWorkers() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestProcessorConfig_Validate_WorkDirEmpty(t *testing.T) {
	c := NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = validPerModelConfig()
	c.WorkDir = ""
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error for empty WorkDir, got nil")
	}
}

func TestProcessorConfig_Validate_TaskWaitTimeMustBeShorterThanPollInterval(t *testing.T) {
	c := NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = validPerModelConfig()
	c.DBClientCfg.Type = "mock"
	c.PollInterval = 1 * time.Second
	c.TaskWaitTime = 1 * time.Second
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error when task_wait_time >= poll_interval, got nil")
	}

	c.TaskWaitTime = 500 * time.Millisecond
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error when task_wait_time < poll_interval: %v", err)
	}
}

func TestProcessorConfig_LoadFromYAML_ExplicitZeroMaxRetries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")

	yamlData := []byte(`
dispatch_mode: sync
poll_interval: 5s
task_wait_time: 1s
num_workers: 1
concurrency:
  global: 100
  per_endpoint: 10
  recovery: 5
  aimd:
    enabled: true
    min: 5
    backoff_factor: 0.5
    additive_increase: 1
work_dir: "` + dir + `/work"
addr: ":9090"
shutdown_timeout: 30s
queue_time_bucket:
  start: 0.1
  factor: 2
  count: 10
process_time_bucket:
  start: 0.1
  factor: 2
  count: 15
e2e_latency_bucket:
  start: 1
  factor: 3
  count: 12
model_gateways:
  "llama-3":
    url: "http://llama-gw:8000"
    inference_objective: "batch-sheddable-a"
    request_timeout: 5m
    max_retries: 3
    initial_backoff: 1s
    max_backoff: 60s
  "no-retry-model":
    url: "http://no-retry-gw:8000"
    request_timeout: 5m
    max_retries: 0
    initial_backoff: 1s
    max_backoff: 60s
progress_ttl_seconds: 86400
`)

	if err := os.WriteFile(path, yamlData, 0o600); err != nil {
		t.Fatalf("failed to write yaml: %v", err)
	}

	c := NewConfig()
	if err := c.LoadFromYAML(path); err != nil {
		t.Fatalf("LoadFromYAML() error: %v", err)
	}

	noRetry, ok := c.ModelGateways["no-retry-model"]
	if !ok {
		t.Fatal("ModelGateways missing no-retry-model")
	}
	if noRetry.MaxRetries == nil {
		t.Fatal("no-retry-model MaxRetries should not be nil after YAML parse")
	}
	if *noRetry.MaxRetries != 0 {
		t.Fatalf("no-retry-model MaxRetries = %d, want 0 (explicit zero must not be overwritten by default)", *noRetry.MaxRetries)
	}

	llama, ok := c.ModelGateways["llama-3"]
	if !ok {
		t.Fatal("ModelGateways missing llama-3")
	}
	if llama.InferenceObjective != "batch-sheddable-a" {
		t.Fatalf("llama-3 InferenceObjective = %q, want %q", llama.InferenceObjective, "batch-sheddable-a")
	}
	if noRetry.InferenceObjective != "" {
		t.Fatalf("no-retry-model InferenceObjective = %q, want empty", noRetry.InferenceObjective)
	}

	if c.InferenceObjectiveFor("llama-3") != "batch-sheddable-a" {
		t.Fatalf("InferenceObjectiveFor(llama-3) = %q, want per-model override", c.InferenceObjectiveFor("llama-3"))
	}
	if c.InferenceObjectiveFor("no-retry-model") != "" {
		t.Fatalf("InferenceObjectiveFor(no-retry-model) = %q, want empty", c.InferenceObjectiveFor("no-retry-model"))
	}

	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() should pass with explicit max_retries=0: %v", err)
	}
}

func TestProcessorConfig_Validate_NeitherGlobalNorPerModel(t *testing.T) {
	c := NewConfig()
	c.DispatchMode = DispatchModeSync
	if err := c.Validate(); err == nil {
		t.Fatal("Validate() expected error when neither global nor per-model is configured")
	}
}

func TestProcessorConfig_Validate_GlobalOnly(t *testing.T) {
	c := NewConfig()
	c.DispatchMode = DispatchModeSync
	c.GlobalInferenceGateway = validGlobalConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error with global-only config: %v", err)
	}
}

func TestProcessorConfig_Validate_PerModelWithoutDefault(t *testing.T) {
	c := NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = validPerModelConfig()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error with per-model-only config: %v", err)
	}
}

func TestProcessorConfig_Validate_GlobalAndPerModelMutuallyExclusive(t *testing.T) {
	c := NewConfig()
	c.DispatchMode = DispatchModeSync
	c.GlobalInferenceGateway = validGlobalConfig()
	c.ModelGateways = validPerModelConfig()
	if err := c.Validate(); err == nil {
		t.Fatal("Validate() expected error when both global and per-model are set")
	}
}

func TestProcessorConfig_Validate_APIKeyFile(t *testing.T) {
	t.Run("name_and_file_mutually_exclusive", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = map[string]ModelGatewayConfig{
			"llama-3": {
				URL:            "http://gw:8000",
				RequestTimeout: ptr.To(5 * time.Minute),
				MaxRetries:     ptr.To(3),
				InitialBackoff: ptr.To(1 * time.Second),
				MaxBackoff:     ptr.To(60 * time.Second),
				APIKeyName:     "my-key",
				APIKeyFile:     "/some/file",
			},
		}
		if err := c.Validate(); err == nil {
			t.Fatal("Validate() expected error when both api_key_name and api_key_file are set, got nil")
		}
	})

	t.Run("file_not_found", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = map[string]ModelGatewayConfig{
			"llama-3": {
				URL:            "http://gw:8000",
				RequestTimeout: ptr.To(5 * time.Minute),
				MaxRetries:     ptr.To(3),
				InitialBackoff: ptr.To(1 * time.Second),
				MaxBackoff:     ptr.To(60 * time.Second),
				APIKeyFile:     "/nonexistent/path/to/key",
			},
		}
		if err := c.Validate(); err == nil {
			t.Fatal("Validate() expected error when api_key_file does not exist, got nil")
		}
	})

	t.Run("valid_file", func(t *testing.T) {
		dir := t.TempDir()
		keyFile := filepath.Join(dir, "token")
		if err := os.WriteFile(keyFile, []byte("my-secret-token"), 0o600); err != nil {
			t.Fatalf("failed to write key file: %v", err)
		}

		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = map[string]ModelGatewayConfig{
			"llama-3": {
				URL:            "http://gw:8000",
				RequestTimeout: ptr.To(5 * time.Minute),
				MaxRetries:     ptr.To(3),
				InitialBackoff: ptr.To(1 * time.Second),
				MaxBackoff:     ptr.To(60 * time.Second),
				APIKeyFile:     keyFile,
			},
		}
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate() unexpected error with valid api_key_file: %v", err)
		}
	})

	t.Run("resolve_reads_and_trims_file", func(t *testing.T) {
		dir := t.TempDir()
		keyFile := filepath.Join(dir, "token")
		if err := os.WriteFile(keyFile, []byte("  file-based-token  \n"), 0o600); err != nil {
			t.Fatalf("failed to write key file: %v", err)
		}

		cfg := NewConfig()
		cfg.DispatchMode = DispatchModeSync
		cfg.ModelGateways = map[string]ModelGatewayConfig{
			"llama-3": {
				URL:            "http://gateway:8000",
				APIKeyFile:     keyFile,
				RequestTimeout: ptr.To(5 * time.Minute),
				MaxRetries:     ptr.To(3),
				InitialBackoff: ptr.To(1 * time.Second),
				MaxBackoff:     ptr.To(60 * time.Second),
			},
		}

		resolved, err := ResolveModelGateways(cfg)
		if err != nil {
			t.Fatalf("ResolveModelGateways() error: %v", err)
		}

		got := resolved.PerModel["llama-3"].APIKey
		if got != "file-based-token" {
			t.Fatalf("APIKey = %q, want %q", got, "file-based-token")
		}
	})
}

func TestProcessorConfig_Validate_GatewayTLSPartialConfigRejected(t *testing.T) {
	c := NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = map[string]ModelGatewayConfig{
		"llama-3": {
			URL:               "http://gw:8000",
			RequestTimeout:    ptr.To(5 * time.Minute),
			MaxRetries:        ptr.To(3),
			InitialBackoff:    ptr.To(1 * time.Second),
			MaxBackoff:        ptr.To(60 * time.Second),
			TLSClientCertFile: "/tmp/client-cert.pem",
		},
	}
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error when only tls_client_cert_file is set, got nil")
	}

	c.ModelGateways = map[string]ModelGatewayConfig{
		"llama-3": {
			URL:              "http://gw:8000",
			RequestTimeout:   ptr.To(5 * time.Minute),
			MaxRetries:       ptr.To(3),
			InitialBackoff:   ptr.To(1 * time.Second),
			MaxBackoff:       ptr.To(60 * time.Second),
			TLSClientKeyFile: "/tmp/client-key.pem",
		},
	}
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error when only tls_client_key_file is set, got nil")
	}
}

func TestProcessorConfig_Validate_MinimumValueChecks(t *testing.T) {
	c := NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = validPerModelConfig()
	c.NumWorkers = 0
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error for num_workers <= 0, got nil")
	}

	c = NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = validPerModelConfig()
	c.Concurrency.Global = 0
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error for concurrency.global <= 0, got nil")
	}

	c = NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = validPerModelConfig()
	c.Concurrency.PerEndpoint = 0
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error for concurrency.per_endpoint <= 0, got nil")
	}

	c = NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = validPerModelConfig()
	c.ShutdownTimeout = 0
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error for shutdown_timeout <= 0, got nil")
	}

	c = NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = validPerModelConfig()
	c.Concurrency.Recovery = 0
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error for concurrency.recovery <= 0, got nil")
	}

	c = NewConfig()
	c.DispatchMode = DispatchModeSync
	c.ModelGateways = map[string]ModelGatewayConfig{
		"llama-3": {
			URL:            "http://gw:8000",
			RequestTimeout: nil,
			MaxRetries:     ptr.To(3),
			InitialBackoff: ptr.To(1 * time.Second),
			MaxBackoff:     ptr.To(60 * time.Second),
		},
	}
	if err := c.Validate(); err == nil {
		t.Fatalf("Validate() expected error for nil request_timeout, got nil")
	}
}

func TestProcessorConfig_Validate_ConcurrencyAIMD(t *testing.T) {
	t.Run("default config passes", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = validPerModelConfig()
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate() error: %v", err)
		}
	})

	t.Run("backoff factor out of range", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = validPerModelConfig()
		c.Concurrency.AIMD.BackoffFactor = 1.5
		if err := c.Validate(); err == nil {
			t.Fatal("expected error for backoff_factor >= 1")
		}
	})

	t.Run("min > per_endpoint", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.Concurrency.Global = 10
		c.Concurrency.PerEndpoint = 5
		c.Concurrency.AIMD.Min = 20
		c.ModelGateways = validPerModelConfig()
		if err := c.Validate(); err == nil {
			t.Fatal("expected error for aimd.min > per_endpoint")
		}
	})

	t.Run("min > per_endpoint second case", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.Concurrency.Global = 100
		c.Concurrency.PerEndpoint = 5
		c.Concurrency.AIMD.Min = 10
		c.ModelGateways = validPerModelConfig()
		if err := c.Validate(); err == nil {
			t.Fatal("expected error for aimd.min > per_endpoint")
		}
	})

	t.Run("fixed limit when min equals per_endpoint", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.Concurrency.Global = 50
		c.Concurrency.PerEndpoint = 10
		c.Concurrency.AIMD.Min = 10
		c.ModelGateways = validPerModelConfig()
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate() error: %v", err)
		}
	})

	t.Run("NewConfig provides AIMD defaults", func(t *testing.T) {
		c := NewConfig()
		if c.Concurrency.AIMD.Min != 5 {
			t.Fatalf("AIMD.Min = %d, want 5", c.Concurrency.AIMD.Min)
		}
		if c.Concurrency.AIMD.BackoffFactor != 0.5 {
			t.Fatalf("AIMD.BackoffFactor = %f, want 0.5", c.Concurrency.AIMD.BackoffFactor)
		}
		if c.Concurrency.AIMD.AdditiveIncrease != 1 {
			t.Fatalf("AIMD.AdditiveIncrease = %d, want 1", c.Concurrency.AIMD.AdditiveIncrease)
		}
		if !c.Concurrency.AIMD.Enabled {
			t.Fatal("AIMD.Enabled should be true by default")
		}
	})

	t.Run("zero backoff_factor rejected by Validate", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = validPerModelConfig()
		c.Concurrency.AIMD.BackoffFactor = 0
		if err := c.Validate(); err == nil {
			t.Fatal("expected error for backoff_factor = 0")
		}
	})

	t.Run("AIMD disabled skips AIMD validation", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = validPerModelConfig()
		c.Concurrency.AIMD.Enabled = false
		c.Concurrency.AIMD.BackoffFactor = 0
		c.Concurrency.AIMD.Min = 0
		if err := c.Validate(); err != nil {
			t.Fatalf("Validate() should pass with AIMD disabled, got: %v", err)
		}
	})

	t.Run("zero min rejected", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = validPerModelConfig()
		c.Concurrency.AIMD.Min = 0
		if err := c.Validate(); err == nil {
			t.Fatal("expected error for aimd.min = 0")
		}
	})

	t.Run("zero additive_increase rejected", func(t *testing.T) {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = validPerModelConfig()
		c.Concurrency.AIMD.AdditiveIncrease = 0
		if err := c.Validate(); err == nil {
			t.Fatal("expected error for aimd.additive_increase = 0")
		}
	})
}

func TestProcessorConfig_LoadFromYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.yaml")

	yamlData := []byte(`
dispatch_mode: sync
poll_interval: 2s
task_wait_time: 500ms
num_workers: 3
concurrency:
  global: 50
  per_endpoint: 5
  recovery: 3
  aimd:
    enabled: true
    min: 2
    backoff_factor: 0.7
    additive_increase: 2
work_dir: "` + dir + `/work"
addr: ":1234"
model_gateways:
  "llama-3":
    url: "http://example:8000"
    request_timeout: 30s
    max_retries: 9
    initial_backoff: 250ms
    max_backoff: 10s
    tls_insecure_skip_verify: true
default_output_expiration_seconds: 86400
progress_ttl_seconds: 3600
send_fairness_header: true
route_key_method: tenant
`)

	if err := os.WriteFile(path, yamlData, 0o600); err != nil {
		t.Fatalf("failed to write yaml: %v", err)
	}

	c := NewConfig()
	if err := c.LoadFromYAML(path); err != nil {
		t.Fatalf("LoadFromYAML() error: %v", err)
	}

	if c.PollInterval != 2*time.Second {
		t.Fatalf("PollInterval = %v, want %v", c.PollInterval, 2*time.Second)
	}
	if c.TaskWaitTime != 500*time.Millisecond {
		t.Fatalf("TaskWaitTime = %v, want %v", c.TaskWaitTime, 500*time.Millisecond)
	}
	if c.NumWorkers != 3 {
		t.Fatalf("NumWorkers = %d, want %d", c.NumWorkers, 3)
	}
	if c.Concurrency.Global != 50 {
		t.Fatalf("Concurrency.Global = %d, want %d", c.Concurrency.Global, 50)
	}
	if c.Concurrency.PerEndpoint != 5 {
		t.Fatalf("Concurrency.PerEndpoint = %d, want %d", c.Concurrency.PerEndpoint, 5)
	}
	if c.Concurrency.Recovery != 3 {
		t.Fatalf("Concurrency.Recovery = %d, want %d", c.Concurrency.Recovery, 3)
	}
	if c.Concurrency.AIMD.Min != 2 {
		t.Fatalf("Concurrency.AIMD.Min = %d, want %d", c.Concurrency.AIMD.Min, 2)
	}
	if c.Concurrency.AIMD.BackoffFactor != 0.7 {
		t.Fatalf("Concurrency.AIMD.BackoffFactor = %f, want %f", c.Concurrency.AIMD.BackoffFactor, 0.7)
	}
	if c.Concurrency.AIMD.AdditiveIncrease != 2 {
		t.Fatalf("Concurrency.AIMD.AdditiveIncrease = %d, want %d", c.Concurrency.AIMD.AdditiveIncrease, 2)
	}
	if c.WorkDir != filepath.Join(dir, "work") {
		t.Fatalf("WorkDir = %q, want %q", c.WorkDir, filepath.Join(dir, "work"))
	}
	if c.Addr != ":1234" {
		t.Fatalf("Addr = %q, want %q", c.Addr, ":1234")
	}

	gw, ok := c.ModelGateways["llama-3"]
	if !ok {
		t.Fatalf("ModelGateways missing %q key after YAML load", "llama-3")
	}
	if gw.URL != "http://example:8000" {
		t.Fatalf("llama-3 URL = %q, want %q", gw.URL, "http://example:8000")
	}
	if gw.RequestTimeout == nil || *gw.RequestTimeout != 30*time.Second {
		t.Fatalf("llama-3 RequestTimeout = %v, want 30s", gw.RequestTimeout)
	}
	if gw.MaxRetries == nil || *gw.MaxRetries != 9 {
		t.Fatalf("llama-3 MaxRetries = %v, want 9", gw.MaxRetries)
	}
	if gw.InitialBackoff == nil || *gw.InitialBackoff != 250*time.Millisecond {
		t.Fatalf("llama-3 InitialBackoff = %v, want 250ms", gw.InitialBackoff)
	}
	if gw.MaxBackoff == nil || *gw.MaxBackoff != 10*time.Second {
		t.Fatalf("llama-3 MaxBackoff = %v, want 10s", gw.MaxBackoff)
	}
	if !gw.TLSInsecureSkipVerify {
		t.Fatalf("llama-3 TLSInsecureSkipVerify = false, want true")
	}

	if c.DefaultOutputExpirationSeconds != 86400 {
		t.Fatalf("DefaultOutputExpirationSeconds = %d, want %d", c.DefaultOutputExpirationSeconds, 86400)
	}
	if c.ProgressTTLSeconds != 3600 {
		t.Fatalf("ProgressTTLSeconds = %d, want %d", c.ProgressTTLSeconds, 3600)
	}
	if !c.SendFairnessHeader {
		t.Fatalf("SendFairnessHeader = false, want true")
	}
	if c.RouteKeyMethod != RouteKeyMethodTenant {
		t.Fatalf("RouteKeyMethod = %q, want %q", c.RouteKeyMethod, RouteKeyMethodTenant)
	}
}

func TestValidate_RouteKeyMethod(t *testing.T) {
	base := func() *ProcessorConfig {
		c := NewConfig()
		c.DispatchMode = DispatchModeSync
		c.ModelGateways = validPerModelConfig()
		return c
	}

	c := base()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error for default route_key_method: %v", err)
	}

	c = base()
	c.RouteKeyMethod = RouteKeyMethodTenant
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate() unexpected error for tenant route_key_method: %v", err)
	}

	c = base()
	c.RouteKeyMethod = RouteKeyMethod("geography")
	if err := c.Validate(); err == nil {
		t.Fatal("Validate() expected error for unknown route_key_method, got nil")
	}
}

func TestProcessorConfig_Validate_AsyncDispatch(t *testing.T) {
	validAsyncConfig := func() *ProcessorConfig {
		c := NewConfig()
		c.DispatchMode = DispatchModeAsync
		c.AsyncDispatchConfig = AsyncDispatchConfig{
			ResultPollTimeout: 5 * time.Second,
			Models: map[string]AsyncModelConfig{
				"llama-3": {InferencePoolName: "pool-a"},
			},
		}
		return c
	}

	tests := []struct {
		name    string
		mutate  func(*ProcessorConfig)
		wantErr bool
	}{
		{
			name:    "valid async config",
			mutate:  func(_ *ProcessorConfig) {},
			wantErr: false,
		},
		{
			name: "sync mode ignores async fields",
			mutate: func(c *ProcessorConfig) {
				c.DispatchMode = DispatchModeSync
				c.ModelGateways = validPerModelConfig()
				c.AsyncDispatchConfig = AsyncDispatchConfig{}
			},
			wantErr: false,
		},
		{
			name: "empty dispatch_mode treated as sync",
			mutate: func(c *ProcessorConfig) {
				c.DispatchMode = DispatchMode("")
				c.ModelGateways = validPerModelConfig()
			},
			wantErr: false,
		},
		{
			name:    "invalid dispatch_mode rejected",
			mutate:  func(c *ProcessorConfig) { c.DispatchMode = DispatchMode("invalid") },
			wantErr: true,
		},
		{
			name:    "async zero result_poll_timeout",
			mutate:  func(c *ProcessorConfig) { c.AsyncDispatchConfig.ResultPollTimeout = 0 },
			wantErr: true,
		},
		{
			name: "async missing inference_pool_name",
			mutate: func(c *ProcessorConfig) {
				c.AsyncDispatchConfig.Models = map[string]AsyncModelConfig{
					"llama-3": {InferencePoolName: ""},
				}
			},
			wantErr: true,
		},
		{
			name: "async global gateway rejected",
			mutate: func(c *ProcessorConfig) {
				c.GlobalInferenceGateway = &ModelGatewayConfig{URL: "http://gw:8000"}
			},
			wantErr: true,
		},
		{
			name: "async no models configured",
			mutate: func(c *ProcessorConfig) {
				c.AsyncDispatchConfig.Models = nil
			},
			wantErr: true,
		},
		{
			name: "sync mode does not require async_dispatch.models",
			mutate: func(c *ProcessorConfig) {
				c.DispatchMode = DispatchModeSync
				c.ModelGateways = validPerModelConfig()
				c.AsyncDispatchConfig.Models = nil
			},
			wantErr: false,
		},
		{
			name:    "async negative result_poll_timeout",
			mutate:  func(c *ProcessorConfig) { c.AsyncDispatchConfig.ResultPollTimeout = -1 * time.Second },
			wantErr: true,
		},
		{
			name: "async multiple models all valid",
			mutate: func(c *ProcessorConfig) {
				c.AsyncDispatchConfig.Models = map[string]AsyncModelConfig{
					"llama-3": {InferencePoolName: "pool-a"},
					"mistral": {InferencePoolName: "pool-b"},
				}
			},
			wantErr: false,
		},
		{
			name: "async one model missing inference_pool_name among multiple",
			mutate: func(c *ProcessorConfig) {
				c.AsyncDispatchConfig.Models = map[string]AsyncModelConfig{
					"llama-3": {InferencePoolName: "pool-a"},
					"mistral": {InferencePoolName: ""},
				}
			},
			wantErr: true,
		},
		{
			name: "async explicit queue names accepted",
			mutate: func(c *ProcessorConfig) {
				c.AsyncDispatchConfig.Models = map[string]AsyncModelConfig{
					"llama-3": {
						InferencePoolName: "pool-a",
						RequestQueueName:  "custom:req:a",
						ResultQueueName:   "custom:res:a",
					},
				}
			},
			wantErr: false,
		},
		{
			name: "async partial queue names rejected",
			mutate: func(c *ProcessorConfig) {
				c.AsyncDispatchConfig.Models = map[string]AsyncModelConfig{
					"llama-3": {
						InferencePoolName: "pool-a",
						RequestQueueName:  "custom:req:a",
					},
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := validAsyncConfig()
			tt.mutate(c)
			err := c.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestProcessorConfig_DispatchMode(t *testing.T) {
	modes := []struct {
		name string
		yaml string
		want DispatchMode
	}{
		{name: "default", yaml: "{}", want: DispatchModeSync},
		{name: "empty", yaml: "dispatch_mode: \"\"", want: DispatchModeSync},
		{name: "explicit async", yaml: "dispatch_mode: async", want: DispatchModeAsync},
		{name: "explicit sync", yaml: "dispatch_mode: sync", want: DispatchModeSync},
	}
	targets := []struct {
		name     string
		async    bool
		global   bool
		perModel bool
		asyncErr string
		syncErr  string
	}{
		{
			name:     "no targets",
			asyncErr: "async_dispatch.models must be configured",
			syncErr:  "either global_inference_gateway or model_gateways must be configured",
		},
		{
			name:    "async mappings",
			async:   true,
			syncErr: "either global_inference_gateway or model_gateways must be configured",
		},
		{
			name:     "global gateway",
			global:   true,
			asyncErr: "global_inference_gateway is not supported",
		},
		{
			name:     "per-model gateways",
			perModel: true,
			asyncErr: "async_dispatch.models must be configured",
		},
		{
			name:     "async mappings and per-model gateways",
			async:    true,
			perModel: true,
		},
	}
	for _, mode := range modes {
		t.Run(mode.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(mode.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			for _, target := range targets {
				t.Run(target.name, func(t *testing.T) {
					c := NewConfig()
					if err := c.LoadFromYAML(path); err != nil {
						t.Fatal(err)
					}
					if target.async {
						c.AsyncDispatchConfig.Models = map[string]AsyncModelConfig{
							"llama-3": {InferencePoolName: "pool-a"},
						}
					}
					if target.global {
						c.GlobalInferenceGateway = validGlobalConfig()
					}
					if target.perModel {
						c.ModelGateways = validPerModelConfig()
					}
					wantErr := target.asyncErr
					if mode.want == DispatchModeSync {
						wantErr = target.syncErr
					}
					err := c.Validate()
					if c.DispatchMode != mode.want {
						t.Fatalf("DispatchMode = %q after Validate(), want %q", c.DispatchMode, mode.want)
					}
					if wantErr != "" {
						if err == nil || !strings.Contains(err.Error(), wantErr) {
							t.Fatalf("Validate() error = %v, want %q", err, wantErr)
						}
						if mode.want == DispatchModeAsync {
							for _, hint := range []string{"async_dispatch.models", "explicitly set dispatch_mode: sync"} {
								if !strings.Contains(err.Error(), hint) {
									t.Errorf("Validate() error = %v, missing guidance %q", err, hint)
								}
							}
						}
						return
					}
					if err != nil {
						t.Fatalf("Validate() unexpected error: %v", err)
					}
					resolved, err := ResolveModelGateways(c)
					if err != nil {
						t.Fatalf("ResolveModelGateways() error: %v", err)
					}
					if mode.want == DispatchModeAsync {
						if resolved.Async == nil || resolved.Global != nil || resolved.PerModel != nil {
							t.Fatalf("expected only async targets, got %+v", resolved)
						}
						if resolved.Async.Models["llama-3"].PoolName != "pool-a" {
							t.Fatalf("unexpected async models: %v", resolved.Async.Models)
						}
						return
					}
					if resolved.Async != nil || (resolved.Global != nil) != target.global || (resolved.PerModel != nil) != target.perModel {
						t.Fatalf("expected explicit sync targets, got %+v", resolved)
					}
					if target.global && resolved.Global.URL != c.GlobalInferenceGateway.URL {
						t.Errorf("global URL = %q, want %q", resolved.Global.URL, c.GlobalInferenceGateway.URL)
					}
					if target.perModel && resolved.PerModel["llama-3"].URL != c.ModelGateways["llama-3"].URL {
						t.Errorf("per-model URL = %q, want %q", resolved.PerModel["llama-3"].URL, c.ModelGateways["llama-3"].URL)
					}
				})
			}
		})
	}
}

func TestProcessorConfig_IsAsync(t *testing.T) {
	c := NewConfig()
	if c.IsAsync() {
		t.Fatal("IsAsync() = true for default config, want false")
	}
	c.DispatchMode = DispatchModeAsync
	if !c.IsAsync() {
		t.Fatal("IsAsync() = false when DispatchMode is async, want true")
	}
}

func TestProcessorConfig_InferenceObjectiveFor(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ProcessorConfig
		modelID string
		want    string
	}{
		{
			name:    "no objective configured anywhere",
			cfg:     ProcessorConfig{DispatchMode: DispatchModeSync, ModelGateways: validPerModelConfig()},
			modelID: "llama-3",
			want:    "",
		},
		{
			name: "per-model objective set",
			cfg: ProcessorConfig{
				DispatchMode: DispatchModeSync,
				ModelGateways: map[string]ModelGatewayConfig{
					"llama-3": {
						URL:                "http://gw:8000",
						InferenceObjective: "batch-sheddable-a",
						RequestTimeout:     ptr.To(5 * time.Minute),
						MaxRetries:         ptr.To(3),
						InitialBackoff:     ptr.To(1 * time.Second),
						MaxBackoff:         ptr.To(60 * time.Second),
					},
				},
			},
			modelID: "llama-3",
			want:    "batch-sheddable-a",
		},
		{
			name: "unlisted model returns empty",
			cfg: ProcessorConfig{
				DispatchMode: DispatchModeSync,
				ModelGateways: map[string]ModelGatewayConfig{
					"llama-3": {
						URL:                "http://gw:8000",
						InferenceObjective: "batch-sheddable-a",
						RequestTimeout:     ptr.To(5 * time.Minute),
						MaxRetries:         ptr.To(3),
						InitialBackoff:     ptr.To(1 * time.Second),
						MaxBackoff:         ptr.To(60 * time.Second),
					},
				},
			},
			modelID: "mistral",
			want:    "",
		},
		{
			name: "per-model empty returns empty",
			cfg: ProcessorConfig{
				DispatchMode: DispatchModeSync,
				ModelGateways: map[string]ModelGatewayConfig{
					"llama-3": {
						URL:            "http://gw:8000",
						RequestTimeout: ptr.To(5 * time.Minute),
						MaxRetries:     ptr.To(3),
						InitialBackoff: ptr.To(1 * time.Second),
						MaxBackoff:     ptr.To(60 * time.Second),
					},
				},
			},
			modelID: "llama-3",
			want:    "",
		},
		{
			name: "global gateway with objective",
			cfg: ProcessorConfig{
				DispatchMode: DispatchModeSync,
				GlobalInferenceGateway: &ModelGatewayConfig{
					URL:                "http://global-gw:8000",
					InferenceObjective: "batch-sheddable-global",
					RequestTimeout:     ptr.To(5 * time.Minute),
					MaxRetries:         ptr.To(3),
					InitialBackoff:     ptr.To(1 * time.Second),
					MaxBackoff:         ptr.To(60 * time.Second),
				},
			},
			modelID: "any-model",
			want:    "batch-sheddable-global",
		},
		{
			name: "global gateway without objective returns empty",
			cfg: ProcessorConfig{
				DispatchMode: DispatchModeSync,
				GlobalInferenceGateway: &ModelGatewayConfig{
					URL:            "http://global-gw:8000",
					RequestTimeout: ptr.To(5 * time.Minute),
					MaxRetries:     ptr.To(3),
					InitialBackoff: ptr.To(1 * time.Second),
					MaxBackoff:     ptr.To(60 * time.Second),
				},
			},
			modelID: "any-model",
			want:    "",
		},
		{
			name: "async model objective set",
			cfg: ProcessorConfig{
				DispatchMode: DispatchModeAsync,
				AsyncDispatchConfig: AsyncDispatchConfig{
					Models: map[string]AsyncModelConfig{
						"llama-3": {
							InferencePoolName:  "pool-a",
							InferenceObjective: "batch-sheddable-a",
						},
					},
				},
			},
			modelID: "llama-3",
			want:    "batch-sheddable-a",
		},
		{
			name: "async model without objective returns empty",
			cfg: ProcessorConfig{
				DispatchMode: DispatchModeAsync,
				AsyncDispatchConfig: AsyncDispatchConfig{
					Models: map[string]AsyncModelConfig{
						"llama-3": {InferencePoolName: "pool-a"},
					},
				},
			},
			modelID: "llama-3",
			want:    "",
		},
		{
			name: "sync mode ignores leftover async config",
			cfg: ProcessorConfig{
				DispatchMode: DispatchModeSync,
				ModelGateways: map[string]ModelGatewayConfig{
					"llama-3": {
						URL:                "http://gw:8000",
						InferenceObjective: "sync-objective",
						RequestTimeout:     ptr.To(5 * time.Minute),
						MaxRetries:         ptr.To(3),
						InitialBackoff:     ptr.To(1 * time.Second),
						MaxBackoff:         ptr.To(60 * time.Second),
					},
				},
				AsyncDispatchConfig: AsyncDispatchConfig{
					Models: map[string]AsyncModelConfig{
						"llama-3": {
							InferencePoolName:  "pool-a",
							InferenceObjective: "stale-async-objective",
						},
					},
				},
			},
			modelID: "llama-3",
			want:    "sync-objective",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cfg.InferenceObjectiveFor(tt.modelID)
			if got != tt.want {
				t.Fatalf("InferenceObjectiveFor(%q) = %q, want %q", tt.modelID, got, tt.want)
			}
		})
	}
}

func TestResolveModelGateways_Async(t *testing.T) {
	t.Run("populates Async field", func(t *testing.T) {
		cfg := NewConfig()
		cfg.DispatchMode = DispatchModeAsync
		cfg.AsyncDispatchConfig = AsyncDispatchConfig{
			ResultPollTimeout: 10 * time.Second,
			Models: map[string]AsyncModelConfig{
				"model-a": {InferencePoolName: "pool-a"},
				"model-b": {InferencePoolName: "pool-b"},
			},
		}

		resolved, err := ResolveModelGateways(cfg)
		if err != nil {
			t.Fatalf("ResolveModelGateways() error: %v", err)
		}

		if resolved.Async == nil {
			t.Fatal("expected Async to be set")
		}
		if resolved.Global != nil {
			t.Error("expected Global to be nil in async mode")
		}
		if resolved.PerModel != nil {
			t.Error("expected PerModel to be nil in async mode")
		}
		if len(resolved.Async.Models) != 2 {
			t.Fatalf("Models count = %d, want 2", len(resolved.Async.Models))
		}
		if resolved.Async.Models["model-a"].PoolName != "pool-a" {
			t.Errorf("Models[model-a].PoolName = %q, want %q", resolved.Async.Models["model-a"].PoolName, "pool-a")
		}
		if resolved.Async.Models["model-b"].PoolName != "pool-b" {
			t.Errorf("Models[model-b].PoolName = %q, want %q", resolved.Async.Models["model-b"].PoolName, "pool-b")
		}
		if resolved.Async.ResultPollTimeout != 10*time.Second {
			t.Errorf("ResultPollTimeout = %v, want %v", resolved.Async.ResultPollTimeout, 10*time.Second)
		}
	})

	t.Run("passes explicit queue names through", func(t *testing.T) {
		cfg := NewConfig()
		cfg.DispatchMode = DispatchModeAsync
		cfg.AsyncDispatchConfig = AsyncDispatchConfig{
			ResultPollTimeout: 10 * time.Second,
			Models: map[string]AsyncModelConfig{
				"model-a": {
					InferencePoolName: "pool-a",
					RequestQueueName:  "custom:req:a",
					ResultQueueName:   "custom:res:a",
				},
			},
		}

		resolved, err := ResolveModelGateways(cfg)
		if err != nil {
			t.Fatalf("ResolveModelGateways() error: %v", err)
		}

		m := resolved.Async.Models["model-a"]
		if m.RequestQueueName != "custom:req:a" {
			t.Errorf("RequestQueueName = %q, want %q", m.RequestQueueName, "custom:req:a")
		}
		if m.ResultQueueName != "custom:res:a" {
			t.Errorf("ResultQueueName = %q, want %q", m.ResultQueueName, "custom:res:a")
		}
	})

	t.Run("sync mode does not populate Async", func(t *testing.T) {
		cfg := NewConfig()
		cfg.DispatchMode = DispatchModeSync
		cfg.ModelGateways = validPerModelConfig()

		resolved, err := ResolveModelGateways(cfg)
		if err != nil {
			t.Fatalf("ResolveModelGateways() error: %v", err)
		}

		if resolved.Async != nil {
			t.Error("expected Async to be nil in sync mode")
		}
		if len(resolved.PerModel) == 0 {
			t.Error("expected PerModel to be populated in sync mode")
		}
	})
}
