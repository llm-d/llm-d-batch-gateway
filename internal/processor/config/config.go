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

// The processor's configuration definitions.

package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	sharedcfg "github.com/llm-d/llm-d-batch-gateway/internal/shared/config"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/ptr"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/retry"
	inference "github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
	"gopkg.in/yaml.v3"
)

// ConcurrencyConfig groups all dispatch-rate and concurrency control knobs.
type ConcurrencyConfig struct {
	// Global limits total in-flight inference requests across all workers.
	// Acts as a fixed ceiling — the sum of all per-endpoint concurrency
	// is bounded by this value.
	Global int `yaml:"global"`

	// PerEndpoint is the initial and maximum concurrency per inference endpoint.
	// Models sharing the same gateway endpoint share one semaphore controlled
	// by this value.
	PerEndpoint int `yaml:"per_endpoint"`

	// Recovery limits concurrent job recoveries during startup.
	// Each recovery can involve DB lookups, S3 uploads, and status updates.
	Recovery int `yaml:"recovery"`

	// AIMD holds adaptive concurrency control parameters.
	AIMD AIMDConfig `yaml:"aimd"`
}

// AIMDConfig holds parameters for Additive Increase / Multiplicative Decrease
// concurrency control per inference endpoint.
type AIMDConfig struct {
	// Enabled controls whether AIMD adaptive concurrency is active.
	// When false, per-endpoint concurrency is fixed at ConcurrencyConfig.PerEndpoint.
	Enabled bool `yaml:"enabled"`

	// Min is the floor for per-endpoint adaptive concurrency.
	// AIMD will never reduce a single endpoint's effective limit below this value.
	Min int `yaml:"min"`

	// BackoffFactor is the multiplicative decrease applied to the per-endpoint
	// concurrency limit when the endpoint signals overload (429/5xx). Must be
	// in (0, 1). Default: 0.5.
	BackoffFactor float64 `yaml:"backoff_factor"`

	// AdditiveIncrease is the number of concurrency slots added after a full
	// window of consecutive successes per endpoint. Default: 1.
	AdditiveIncrease int `yaml:"additive_increase"`
}

// DispatchMode selects the inference dispatch backend.
type DispatchMode string

const (
	DispatchModeSync  DispatchMode = "sync"
	DispatchModeAsync DispatchMode = "async"
)

// RouteKeyMethod selects how model_gateways lookup keys are derived from a
// request model ID.
type RouteKeyMethod string

const (
	// RouteKeyMethodBare (default, empty) keys lookups by the bare model ID.
	RouteKeyMethodBare RouteKeyMethod = ""
	// RouteKeyMethodTenant scopes lookups as "<tenantID>/<modelID>".
	RouteKeyMethodTenant RouteKeyMethod = "tenant"
)

// AsyncModelConfig describes the async dispatch target for a single model.
type AsyncModelConfig struct {
	// InferencePoolName identifies the async dispatch pool for this model.
	InferencePoolName string `yaml:"inference_pool_name"`

	// InferenceObjective is the name of a GIE InferenceObjective CRD sent in
	// the x-gateway-inference-objective header on inference requests.
	// When empty, the header is not sent.
	InferenceObjective string `yaml:"inference_objective"`

	// RequestQueueName overrides the Redis sorted-set queue name used to
	// submit inference requests. When empty, the name is derived from
	// InferencePoolName as "llm-d-async:requests:<pool>".
	// Deprecated fallback: the derived naming convention will be removed in
	// a future release. Set explicit queue names for new deployments.
	RequestQueueName string `yaml:"request_queue_name"`

	// ResultQueueName overrides the Redis list base name used to read inference
	// results. The Processor appends its replica identity. When empty, the base
	// name is derived from InferencePoolName as "llm-d-async:results:<pool>".
	// Deprecated fallback: the derived naming convention will be removed in
	// a future release. Set an explicit queue base name for new deployments.
	ResultQueueName string `yaml:"result_queue_name"`
}

// AsyncDispatchConfig holds configuration for the llm-d-async dispatch backend.
// Only used when DispatchMode == "async".
type AsyncDispatchConfig struct {
	// RedisURL is the full Redis connection URL (e.g. "redis://host:6379",
	// "rediss://user:pass@host:6379" for TLS). Read from the mounted secret
	// at runtime (SecretKeyRedisURL)
	RedisURL string `yaml:"-"`

	// ResultPollTimeout is the timeout per GetResult poll cycle.
	// Controls how long each blocking poll waits before retrying.
	ResultPollTimeout time.Duration `yaml:"result_poll_timeout"`

	// Models maps model names to their async dispatch targets.
	// Required when DispatchMode == "async".
	Models map[string]AsyncModelConfig `yaml:"models"`
}

type ProcessorConfig struct {
	// TaskWaitTime is the timeout parameter used when dequeueing from the priority queue
	// This should be shorter than PollInterval
	TaskWaitTime time.Duration `yaml:"task_wait_time"`

	// NumWorkers is the fixed number of worker goroutines spawned to process jobs
	NumWorkers int `yaml:"num_workers"`

	// Concurrency groups all dispatch-rate and concurrency control settings.
	Concurrency ConcurrencyConfig `yaml:"concurrency"`

	// PollInterval defines how frequently the processor checks the database for new jobs
	PollInterval time.Duration `yaml:"poll_interval"`

	// QueueTimeBucket defines exponential bucket configs for queue wait time metric
	QueueTimeBucket BucketConfig `yaml:"queue_time_bucket"`

	// ProcessTimeBucket defines exponential bucket configs for process time metric
	ProcessTimeBucket BucketConfig `yaml:"process_time_bucket"`

	// E2ELatencyBucket defines exponential bucket configs for end-to-end job latency metric.
	// Covers the full lifecycle from submission to terminal state, which can span hours for large jobs.
	E2ELatencyBucket BucketConfig `yaml:"e2e_latency_bucket"`

	// DB client configuration
	DBClientCfg sharedcfg.DBClientConfig `yaml:"db_client"`

	Addr string `yaml:"addr"`
	// TerminateOnObservabilityFailure controls whether observability server failures should terminate the processor.
	// false: best-effort (default), true: fatal.
	TerminateOnObservabilityFailure bool `yaml:"terminate_on_observability_failure"`

	// ShutdownTimeout is the timeout for shutting down the processor
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`

	// WorkDir is the work directory for processor
	WorkDir string `yaml:"work_dir"`

	// GlobalInferenceGateway, when set, routes all inference requests to a
	// single endpoint regardless of model name. Per-model entries in
	// ModelGateways are ignored for routing when this is set.
	// Use this for MaaS / multi-model platforms or LoRA adapter deployments
	// where many model names share one inference endpoint.
	GlobalInferenceGateway *ModelGatewayConfig `yaml:"global_inference_gateway,omitempty"`

	// ModelGateways maps model names to gateway/inference settings.
	// Only models listed here are routed; requests for unlisted models
	// receive a request-level error.
	// Ignored when GlobalInferenceGateway is set.
	ModelGateways map[string]ModelGatewayConfig `yaml:"model_gateways"`

	// RouteKeyMethod selects how model_gateways lookup keys are derived.
	// "tenant" scopes lookups by the job's tenant ID: requests resolve
	// gateways under "<tenantID>/<modelID>", so identically-named models of
	// different tenants route to their own backends on a shared apiserver.
	// The forwarded request body is left untouched (the runtime still sees
	// the bare model name). Empty (default) keeps bare-model lookups.
	RouteKeyMethod RouteKeyMethod `yaml:"route_key_method"`

	// DefaultOutputExpirationSeconds is the default TTL for batch output/error files in seconds.
	// Used as fallback when the user does not provide output_expires_after in POST /v1/batches.
	// 0 means no expiration (keep until explicitly deleted).
	DefaultOutputExpirationSeconds int64 `yaml:"default_output_expiration_seconds"`

	// ProgressUpdateInterval is the throttling interval between intermediate progress updates to the DB.
	ProgressUpdateInterval time.Duration `yaml:"progress_update_interval"`

	// SendFairnessHeader controls whether the processor sends
	// x-gateway-inference-fairness-id on inference requests.
	// false (default) omits the fairness header.
	SendFairnessHeader bool `yaml:"send_fairness_header"`

	// EnablePprof enables pprof profiling endpoints on the observability server.
	EnablePprof bool `yaml:"enable_pprof"`

	// OTelCfg holds OpenTelemetry-related settings.
	OTelCfg sharedcfg.OTelConfig `yaml:"otel"`

	// FileClient holds configuration for the shared file storage client (fs or s3).
	FileClientCfg sharedcfg.FileClientConfig `yaml:"file_client"`

	// DispatchMode selects the inference dispatch backend.
	// "sync" (default): direct HTTP via InferenceClient.
	// "async": submit via llm-d-async producer, collect from result queue.
	DispatchMode DispatchMode `yaml:"dispatch_mode"`

	// AsyncDispatchConfig holds llm-d-async dispatch settings. Only used when DispatchMode == "async".
	AsyncDispatchConfig AsyncDispatchConfig `yaml:"async_dispatch"`
}

// ModelGatewayConfig describes the full gateway and HTTP/TLS settings for one
// model or the global inference gateway. Each per-model entry must be fully
// specified — there is no inheritance between entries.
//
// HTTP fields use pointers so that nil (unset) is distinguishable from explicit
// zero values (e.g. MaxRetries=0 means "no retries", RequestTimeout=0 means
// "no timeout").
//
// API key resolution (mutually exclusive, first match wins):
//   - api_key_file: read the token/key from an arbitrary file path
//     (e.g. /var/run/secrets/kubernetes.io/serviceaccount/token).
//   - api_key_name: key name under /etc/.secrets/ (mounted Kubernetes secret).
//   - (neither set): no API key is sent. For global gateway, the mounted
//     inference-api-key secret is tried as a best-effort fallback.
type ModelGatewayConfig struct {
	URL        string `yaml:"url"`
	APIKeyName string `yaml:"api_key_name"`
	APIKeyFile string `yaml:"api_key_file"`

	// InferenceObjective is the name of a GIE InferenceObjective CRD sent in
	// the x-gateway-inference-objective header on inference requests. Use this
	// to target per-model InferencePools in multi-pool GIE deployments.
	// When empty, the header is not sent.
	InferenceObjective string `yaml:"inference_objective"`

	RequestTimeout *time.Duration `yaml:"request_timeout"`
	MaxRetries     *int           `yaml:"max_retries"`
	InitialBackoff *time.Duration `yaml:"initial_backoff"`
	MaxBackoff     *time.Duration `yaml:"max_backoff"`

	TLSInsecureSkipVerify bool   `yaml:"tls_insecure_skip_verify"`
	TLSCACertFile         string `yaml:"tls_ca_cert_file,omitempty"`
	TLSClientCertFile     string `yaml:"tls_client_cert_file,omitempty"`
	TLSClientKeyFile      string `yaml:"tls_client_key_file,omitempty"`

	// Deprecated: use AsyncDispatchConfig.Models instead.
	// InferencePoolName was used to identify the async dispatch pool for this model.
	InferencePoolName string `yaml:"inference_pool_name"`
}

type BucketConfig struct {
	BucketStart  float64 `yaml:"start"`
	BucketFactor float64 `yaml:"factor"`
	BucketCount  int     `yaml:"count"`
}

// IsAsync returns true when the processor is configured for async dispatch.
func (c *ProcessorConfig) IsAsync() bool {
	return c.DispatchMode == DispatchModeAsync
}

// InferenceObjectiveFor returns the inference objective configured on the
// gateway that will handle requests for modelID.
// Returns "" when no objective is configured, which means the header is not sent.
func (c *ProcessorConfig) InferenceObjectiveFor(modelID string) string {
	if c.IsAsync() {
		if m, ok := c.AsyncDispatchConfig.Models[modelID]; ok {
			return m.InferenceObjective
		}
		return ""
	}
	if c.GlobalInferenceGateway != nil {
		return c.GlobalInferenceGateway.InferenceObjective
	}
	if gw, ok := c.ModelGateways[modelID]; ok {
		return gw.InferenceObjective
	}
	return ""
}

// LoadFromYAML loads the configuration from a YAML file.
// Callers should start from NewConfig() so that concurrency/AIMD defaults
// are already set; YAML values then override only what is explicitly specified.
func (pc *ProcessorConfig) LoadFromYAML(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	return decoder.Decode(pc)
}

// NewConfig returns a new ProcessorConfig with default values.
// Gateway fields (GlobalInferenceGateway, ModelGateways) are intentionally
// left nil — the user must configure exactly one via YAML or env.
// TaskWaitTime has to be shorter than poll interval.
func NewConfig() *ProcessorConfig {
	return &ProcessorConfig{
		PollInterval: 5 * time.Second,
		TaskWaitTime: 1 * time.Second,
		ProcessTimeBucket: BucketConfig{
			BucketStart:  0.1,
			BucketFactor: 2,
			BucketCount:  15,
		},
		QueueTimeBucket: BucketConfig{
			BucketStart:  0.1,
			BucketFactor: 2,
			BucketCount:  10,
		},
		E2ELatencyBucket: BucketConfig{
			BucketStart:  1,
			BucketFactor: 3,
			BucketCount:  12,
		},

		Concurrency: ConcurrencyConfig{
			Global:      100,
			PerEndpoint: 10,
			Recovery:    5,
			AIMD: AIMDConfig{
				Enabled:          true,
				Min:              5,
				BackoffFactor:    0.5,
				AdditiveIncrease: 1,
			},
		},
		NumWorkers: 1,
		Addr:       ":9090",
		// Keep observability as best-effort by default.
		TerminateOnObservabilityFailure: false,
		ShutdownTimeout:                 30 * time.Second,
		WorkDir:                         "/var/lib/batch-gateway/processor",
		DBClientCfg: sharedcfg.DBClientConfig{
			Type: sharedcfg.DBTypePostgreSQL,
		},
		FileClientCfg: sharedcfg.FileClientConfig{
			Type: sharedcfg.FileTypeMock,
			Retry: retry.Config{
				MaxRetries:     3,
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     10 * time.Second,
			},
		},
		DefaultOutputExpirationSeconds: 90 * 24 * 60 * 60, // 90 days
		ProgressUpdateInterval:         15 * time.Second,  // 15 seconds

		DispatchMode: DispatchModeSync,
		AsyncDispatchConfig: AsyncDispatchConfig{
			ResultPollTimeout: 5 * time.Second,
		},
	}
}

func (c *ProcessorConfig) Validate() error {
	if c.PollInterval <= 0 {
		return fmt.Errorf("poll_interval must be > 0")
	}
	if c.TaskWaitTime <= 0 {
		return fmt.Errorf("task_wait_time must be > 0")
	}
	if c.TaskWaitTime >= c.PollInterval {
		return fmt.Errorf("task_wait_time must be shorter than poll_interval")
	}
	if c.NumWorkers <= 0 {
		return fmt.Errorf("num_workers must be > 0")
	}

	if err := c.Concurrency.validate(); err != nil {
		return err
	}

	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("shutdown_timeout must be > 0")
	}
	if c.Addr == "" {
		return fmt.Errorf("addr cannot be empty")
	}
	if c.WorkDir == "" {
		return fmt.Errorf("work_dir cannot be empty")
	}

	if c.QueueTimeBucket.BucketStart <= 0 || c.QueueTimeBucket.BucketFactor <= 1 || c.QueueTimeBucket.BucketCount <= 0 {
		return fmt.Errorf("queue_time_bucket must satisfy: start > 0, factor > 1, count > 0")
	}
	if c.ProcessTimeBucket.BucketStart <= 0 || c.ProcessTimeBucket.BucketFactor <= 1 || c.ProcessTimeBucket.BucketCount <= 0 {
		return fmt.Errorf("process_time_bucket must satisfy: start > 0, factor > 1, count > 0")
	}
	if c.E2ELatencyBucket.BucketStart <= 0 || c.E2ELatencyBucket.BucketFactor <= 1 || c.E2ELatencyBucket.BucketCount <= 0 {
		return fmt.Errorf("e2e_latency_bucket must satisfy: start > 0, factor > 1, count > 0")
	}

	if err := c.FileClientCfg.Retry.Validate(); err != nil {
		return fmt.Errorf("file_client.retry: %w", err)
	}

	switch c.RouteKeyMethod {
	case RouteKeyMethodBare, RouteKeyMethodTenant:
	default:
		return fmt.Errorf("route_key_method must be empty or %q, got %q", RouteKeyMethodTenant, c.RouteKeyMethod)
	}

	if err := c.validateGateways(); err != nil {
		return err
	}

	return nil
}

func (c *ProcessorConfig) validateGateways() error {
	switch c.DispatchMode {
	case DispatchModeSync, DispatchMode(""):
		c.DispatchMode = DispatchModeSync
		if c.GlobalInferenceGateway == nil && len(c.ModelGateways) == 0 {
			return fmt.Errorf("either global_inference_gateway or model_gateways must be configured")
		}
		if c.GlobalInferenceGateway != nil && len(c.ModelGateways) > 0 {
			return fmt.Errorf("global_inference_gateway and model_gateways are mutually exclusive")
		}
		return c.validateSyncDispatchConfig()
	case DispatchModeAsync:
		return c.validateAsyncDispatchConfig()
	default:
		return fmt.Errorf("dispatch_mode must be %q or %q, got %q", DispatchModeSync, DispatchModeAsync, c.DispatchMode)
	}
}

func (c *ProcessorConfig) validateSyncDispatchConfig() error {
	if c.GlobalInferenceGateway != nil {
		if err := validateGatewayConfig("global_inference_gateway", *c.GlobalInferenceGateway); err != nil {
			return err
		}
	}
	for model, gw := range c.ModelGateways {
		if err := validateGatewayConfig(fmt.Sprintf("model_gateways[%s]", model), gw); err != nil {
			return err
		}
	}
	return nil
}

func (c *ProcessorConfig) validateAsyncDispatchConfig() error {
	if c.AsyncDispatchConfig.ResultPollTimeout <= 0 {
		return fmt.Errorf("async_dispatch.result_poll_timeout must be > 0")
	}
	if c.GlobalInferenceGateway != nil {
		return fmt.Errorf("global_inference_gateway is not supported with dispatch_mode %q; use async_dispatch.models", DispatchModeAsync)
	}
	if len(c.AsyncDispatchConfig.Models) == 0 {
		return fmt.Errorf("async_dispatch.models must be configured when dispatch_mode is %q", DispatchModeAsync)
	}
	for model, m := range c.AsyncDispatchConfig.Models {
		if m.InferencePoolName == "" {
			return fmt.Errorf("async_dispatch.models[%s].inference_pool_name must be set", model)
		}
		if (m.RequestQueueName == "") != (m.ResultQueueName == "") {
			return fmt.Errorf("async_dispatch.models[%s]: request_queue_name and result_queue_name must both be set or both be empty", model)
		}
	}
	return nil
}

func (cc *ConcurrencyConfig) validate() error {
	if cc.Global <= 0 {
		return fmt.Errorf("concurrency.global must be > 0")
	}
	if cc.PerEndpoint <= 0 {
		return fmt.Errorf("concurrency.per_endpoint must be > 0")
	}
	if cc.PerEndpoint > cc.Global {
		return fmt.Errorf("concurrency.per_endpoint (%d) must be <= concurrency.global (%d)", cc.PerEndpoint, cc.Global)
	}
	if cc.Recovery <= 0 {
		return fmt.Errorf("concurrency.recovery must be > 0")
	}
	if cc.AIMD.Enabled {
		if cc.AIMD.Min <= 0 {
			return fmt.Errorf("concurrency.aimd.min must be > 0")
		}
		if cc.AIMD.BackoffFactor <= 0 || cc.AIMD.BackoffFactor >= 1 {
			return fmt.Errorf("concurrency.aimd.backoff_factor must be in (0, 1), got %f", cc.AIMD.BackoffFactor)
		}
		if cc.AIMD.AdditiveIncrease <= 0 {
			return fmt.Errorf("concurrency.aimd.additive_increase must be > 0")
		}
		if cc.AIMD.Min > cc.PerEndpoint {
			return fmt.Errorf("concurrency.aimd.min (%d) must be <= concurrency.per_endpoint (%d)", cc.AIMD.Min, cc.PerEndpoint)
		}
	}
	return nil
}

func validateGatewayConfig(prefix string, gw ModelGatewayConfig) error {
	if gw.URL == "" {
		return fmt.Errorf("%s.url cannot be empty", prefix)
	}
	if gw.RequestTimeout == nil {
		return fmt.Errorf("%s.request_timeout must be set", prefix)
	}
	if *gw.RequestTimeout < 0 {
		return fmt.Errorf("%s.request_timeout must be >= 0", prefix)
	}
	if gw.MaxRetries == nil {
		return fmt.Errorf("%s.max_retries must be set", prefix)
	}
	if *gw.MaxRetries < 0 {
		return fmt.Errorf("%s.max_retries must be >= 0", prefix)
	}
	if gw.InitialBackoff == nil {
		return fmt.Errorf("%s.initial_backoff must be set", prefix)
	}
	if *gw.InitialBackoff < 0 {
		return fmt.Errorf("%s.initial_backoff must be >= 0", prefix)
	}
	if gw.MaxBackoff == nil {
		return fmt.Errorf("%s.max_backoff must be set", prefix)
	}
	if *gw.MaxBackoff < 0 {
		return fmt.Errorf("%s.max_backoff must be >= 0", prefix)
	}
	if *gw.MaxBackoff < *gw.InitialBackoff {
		return fmt.Errorf("%s.max_backoff must be >= initial_backoff", prefix)
	}
	if gw.APIKeyName != "" && gw.APIKeyFile != "" {
		return fmt.Errorf("%s: api_key_name and api_key_file are mutually exclusive", prefix)
	}
	if gw.APIKeyFile != "" {
		if _, err := os.Stat(gw.APIKeyFile); err != nil {
			return fmt.Errorf("%s.api_key_file: %w", prefix, err)
		}
	}
	if (gw.TLSClientCertFile == "") != (gw.TLSClientKeyFile == "") {
		return fmt.Errorf("%s: tls_client_cert_file and tls_client_key_file must both be set or both be empty", prefix)
	}
	if gw.TLSCACertFile != "" {
		if _, err := os.Stat(gw.TLSCACertFile); err != nil {
			return fmt.Errorf("%s.tls_ca_cert_file: %w", prefix, err)
		}
	}
	if gw.TLSClientCertFile != "" {
		if _, err := os.Stat(gw.TLSClientCertFile); err != nil {
			return fmt.Errorf("%s.tls_client_cert_file: %w", prefix, err)
		}
		if _, err := os.Stat(gw.TLSClientKeyFile); err != nil {
			return fmt.Errorf("%s.tls_client_key_file: %w", prefix, err)
		}
	}
	return nil
}

// resolveGatewayAPIKey resolves the API key for a single gateway config entry.
func resolveGatewayAPIKey(name string, gw ModelGatewayConfig) (string, error) {
	switch {
	case gw.APIKeyFile != "":
		data, err := os.ReadFile(gw.APIKeyFile)
		if err != nil {
			return "", fmt.Errorf("read API key file for %q: %w", name, err)
		}
		return strings.TrimSpace(string(data)), nil
	case gw.APIKeyName != "":
		key, err := ucom.ReadSecretFile(gw.APIKeyName)
		if err != nil {
			return "", fmt.Errorf("read API key for %q: %w", name, err)
		}
		return key, nil
	default:
		return "", nil
	}
}

func toGatewayClientConfig(gw ModelGatewayConfig, apiKey string) inference.GatewayClientConfig {
	return inference.GatewayClientConfig{
		URL:                   gw.URL,
		APIKey:                apiKey,
		Timeout:               ptr.Deref(gw.RequestTimeout),
		MaxRetries:            ptr.Deref(gw.MaxRetries),
		InitialBackoff:        ptr.Deref(gw.InitialBackoff),
		MaxBackoff:            ptr.Deref(gw.MaxBackoff),
		TLSInsecureSkipVerify: gw.TLSInsecureSkipVerify,
		TLSCACertFile:         gw.TLSCACertFile,
		TLSClientCertFile:     gw.TLSClientCertFile,
		TLSClientKeyFile:      gw.TLSClientKeyFile,
	}
}

// ResolvedGateways holds the fully-resolved gateway configs ready for client construction.
type ResolvedGateways struct {
	Global   *inference.GatewayClientConfig
	PerModel map[string]inference.GatewayClientConfig
	Async    *inference.AsyncClientConfig
}

// ResolveModelGateways resolves API keys for all configured gateways and returns
// a ResolvedGateways ready to pass to the inference client resolver.
// Validate() ensures exactly one of GlobalInferenceGateway or ModelGateways is set.
func ResolveModelGateways(cfg *ProcessorConfig) (*ResolvedGateways, error) {
	result := &ResolvedGateways{}

	if cfg.IsAsync() {
		models := make(map[string]inference.AsyncModelPoolConfig, len(cfg.AsyncDispatchConfig.Models))
		for model, m := range cfg.AsyncDispatchConfig.Models {
			models[model] = inference.AsyncModelPoolConfig{
				PoolName:         m.InferencePoolName,
				RequestQueueName: m.RequestQueueName,
				ResultQueueName:  m.ResultQueueName,
			}
		}
		result.Async = &inference.AsyncClientConfig{
			Models:            models,
			ResultPollTimeout: cfg.AsyncDispatchConfig.ResultPollTimeout,
		}
		return result, nil
	}

	if cfg.GlobalInferenceGateway != nil {
		apiKey, err := resolveGatewayAPIKey("global_inference_gateway", *cfg.GlobalInferenceGateway)
		if err != nil {
			return nil, err
		}
		if apiKey == "" {
			if key, err := ucom.ReadSecretFile(ucom.SecretKeyInferenceAPI); err == nil {
				apiKey = key
			}
		}
		gc := toGatewayClientConfig(*cfg.GlobalInferenceGateway, apiKey)
		result.Global = &gc
	}

	if len(cfg.ModelGateways) > 0 {
		resolved := make(map[string]inference.GatewayClientConfig, len(cfg.ModelGateways))
		for model, gw := range cfg.ModelGateways {
			apiKey, err := resolveGatewayAPIKey(model, gw)
			if err != nil {
				return nil, err
			}
			resolved[model] = toGatewayClientConfig(gw, apiKey)
		}
		result.PerModel = resolved
	}

	return result, nil
}
