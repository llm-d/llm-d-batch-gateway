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

package http

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	cbackoff "github.com/cenkalti/backoff/v5"
	"github.com/go-logr/logr"
	"github.com/go-resty/resty/v2"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

const (
	HeaderNameReqID       string = "X-Request-ID"
	HeaderNameContentType string = "Content-Type"
	HeaderDroppedReason   string = "x-llm-d-request-dropped-reason"

	DroppedReasonTTLExpired = "rejected-ttl-expired"

	// rateLimitBackoffMultiplier scales the initial backoff for 429 responses.
	// A 429 means the server explicitly asks the client to slow down, so we
	// back off harder than for transient 5xx errors.
	rateLimitBackoffMultiplier = 3
)

type capacityRetryKeyType struct{}

var capacityRetryCtxKey capacityRetryKeyType

// NewCapacityRetryContext returns a child context that tracks whether any HTTP
// retry was triggered by a capacity-related response (429 or 5xx). Call the
// returned function after the request completes to read the result.
// This is opt-in: if the caller does not wrap the context, retries are not tracked.
func NewCapacityRetryContext(ctx context.Context) (context.Context, func() bool) {
	var flag atomic.Bool
	return context.WithValue(ctx, capacityRetryCtxKey, &flag), flag.Load
}

type droppedReasonKeyType struct{}

var droppedReasonCtxKey droppedReasonKeyType

// NewDroppedReasonContext returns a child context that captures the value of the
// x-llm-d-request-dropped-reason response header from the final HTTP response.
// Call the returned function after the request completes to read the result.
func NewDroppedReasonContext(ctx context.Context) (context.Context, func() string) {
	var reason atomic.Value
	return context.WithValue(ctx, droppedReasonCtxKey, &reason), func() string {
		v, _ := reason.Load().(string)
		return v
	}
}

// HTTPClient implements HTTP client with retry, TLS, and observability support
type HTTPClient struct {
	client    *resty.Client
	transport *http.Transport // underlying transport (before OTel wrapping)
	closeOnce sync.Once
}

// Config holds configuration for the HTTP client
type Config struct {
	BaseURL         string        // Base URL of the HTTP server (e.g., "http://localhost:8000")
	Timeout         time.Duration // Request timeout (default: 5 minutes)
	MaxIdleConns    int           // Maximum idle connections (default: 100)
	IdleConnTimeout time.Duration // Idle connection timeout (default: 90 seconds)
	APIKey          string        // Optional API key for authentication

	// TLS configuration (optional)
	TLSInsecureSkipVerify bool   // Skip TLS certificate verification (default: false - INSECURE, only for testing)
	TLSCACertFile         string // Path to custom CA certificate file (for private CAs)
	TLSClientCertFile     string // Path to client certificate file (for mTLS)
	TLSClientKeyFile      string // Path to client private key file (for mTLS)
	TLSMinVersion         uint16 // Minimum TLS version (default: TLS 1.2). Use tls.VersionTLS12, tls.VersionTLS13
	TLSMaxVersion         uint16 // Maximum TLS version (default: 0 = no max, use latest)

	// Retry configuration (optional, set MaxRetries > 0 to enable)
	// Uses resty's built-in exponential backoff with jitter
	MaxRetries     int           // Maximum number of retry attempts (default: 0 = disabled)
	InitialBackoff time.Duration // Initial/minimum retry wait time (default: 1 second)
	MaxBackoff     time.Duration // Maximum retry wait time (default: 60 seconds)
}

// NewHTTPClient creates a new HTTP client
func NewHTTPClient(config Config, logger logr.Logger) (*HTTPClient, error) {
	// Set defaults for HTTP client
	if config.Timeout == 0 {
		config.Timeout = 5 * time.Minute
	}
	if config.MaxIdleConns == 0 {
		config.MaxIdleConns = 100
	}
	if config.IdleConnTimeout == 0 {
		config.IdleConnTimeout = 90 * time.Second
	}

	// Set defaults for retry configuration
	if config.MaxRetries > 0 {
		if config.InitialBackoff == 0 {
			config.InitialBackoff = 1 * time.Second
		}
		if config.MaxBackoff == 0 {
			config.MaxBackoff = 60 * time.Second
		}
	}

	// Create resty client
	client := resty.New().
		SetBaseURL(config.BaseURL).
		SetTimeout(config.Timeout).
		SetHeader(HeaderNameContentType, "application/json")

	// Set auth token if provided (adds "Authorization: Bearer <token>" to all requests)
	if config.APIKey != "" {
		client.SetAuthToken(config.APIKey)
	}

	// Configure transport - start with Go's secure defaults (http.DefaultTransport)
	// This gives us: TLS 1.2+, system root CAs, certificate verification, proper timeouts
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, fmt.Errorf("unexpected default transport type: %T", http.DefaultTransport)
	}
	transport := defaultTransport.Clone()

	// Override only the settings we need to customize for batch processing
	transport.MaxIdleConns = config.MaxIdleConns
	transport.MaxIdleConnsPerHost = config.MaxIdleConns // Higher than default (17) for batch workloads
	transport.IdleConnTimeout = config.IdleConnTimeout
	transport.ResponseHeaderTimeout = config.Timeout // Use the same timeout as the request

	// Configure custom TLS if needed
	tlsConfig, err := BuildTLSConfig(&config, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to build TLS config: %w", err)
	}
	if tlsConfig != nil {
		transport.TLSClientConfig = tlsConfig
	}
	// Otherwise, TLSClientConfig stays nil = Go uses system root CAs + TLS 1.2+ defaults

	client.SetTransport(otelhttp.NewTransport(transport,
		otelhttp.WithSpanNameFormatter(func(_ string, _ *http.Request) string {
			return "http-request"
		}),
	))

	// Configure retry only if enabled
	if config.MaxRetries > 0 {
		client.SetRetryCount(config.MaxRetries).
			SetRetryWaitTime(config.InitialBackoff).                                   // Min wait time between retries
			SetRetryMaxWaitTime(config.MaxBackoff).                                    // Max wait time between retries
			SetRetryAfter(newRetryAfterFunc(config.InitialBackoff, config.MaxBackoff)) // Status-code-aware backoff

		// Retry condition: retry on server errors, rate limits, and network errors.
		// Exception: 429/503 with x-llm-d-request-dropped-reason: rejected-ttl-expired
		// is not retried because the SLO already expired on the server side.
		client.AddRetryCondition(func(r *resty.Response, err error) bool {
			if err != nil {
				return true // Retry on network errors
			}

			statusCode := r.StatusCode()
			if statusCode == http.StatusTooManyRequests || statusCode == http.StatusServiceUnavailable {
				return r.Header().Get(HeaderDroppedReason) != DroppedReasonTTLExpired
			}
			return statusCode >= http.StatusInternalServerError
		})

		// Add retry hook for logging and capacity-retry tracking
		client.AddRetryHook(func(resp *resty.Response, err error) {
			if resp != nil && err == nil {
				sc := resp.StatusCode()
				if sc == http.StatusTooManyRequests || sc >= http.StatusInternalServerError {
					if flag, ok := resp.Request.Context().Value(capacityRetryCtxKey).(*atomic.Bool); ok {
						flag.Store(true)
					}
				}
			}
			if resp != nil {
				if reqID := resp.Request.Header.Get(HeaderNameReqID); reqID != "" {
					logger := logr.FromContextOrDiscard(resp.Request.Context())
					logger.V(logging.DEBUG).Info("Retrying request", "request_id", reqID,
						"attempt", resp.Request.Attempt, "max_retries", config.MaxRetries)
				}
			}
		})
	}

	return &HTTPClient{
		client:    client,
		transport: transport,
	}, nil
}

// Close releases resources held by the client by closing idle connections in the
// underlying transport. In-flight requests are not interrupted.
//
// Close is idempotent and safe to call from multiple goroutines.
func (c *HTTPClient) Close() error {
	if c == nil {
		return nil
	}
	c.closeOnce.Do(func() {
		if c.transport != nil {
			c.transport.CloseIdleConnections()
		}
	})
	return nil
}

// Post makes an HTTP POST request with automatic retry logic.
// Returns the response body, status code, and any error.
func (c *HTTPClient) Post(ctx context.Context, endpoint string, body interface{}, headers map[string]string, requestID string) ([]byte, int, error) {
	logger := logr.FromContextOrDiscard(ctx)

	// Create resty request with context
	restyReq := c.client.R().SetContext(ctx)

	// Set request ID header if provided
	if requestID != "" {
		restyReq.SetHeader(HeaderNameReqID, requestID)
	}

	// Set pass-through headers
	for k, v := range headers {
		restyReq.SetHeader(k, v)
	}

	// Set request body (resty handles JSON marshaling)
	restyReq.SetBody(body)

	// Execute request (resty handles retries automatically)
	resp, err := restyReq.Post(endpoint)

	// Handle request-level errors (network, timeout, etc.)
	if err != nil {
		return nil, 0, err
	}

	if reason := resp.Header().Get(HeaderDroppedReason); reason != "" {
		if ptr, ok := ctx.Value(droppedReasonCtxKey).(*atomic.Value); ok {
			ptr.Store(reason)
		}
	}

	retries := resp.Request.Attempt - 1

	// Log success with retry info
	if retries > 0 {
		logger.V(logging.DEBUG).Info("Request succeeded after retries", "retries", retries, "request_id", requestID)
	}

	return resp.Body(), resp.StatusCode(), nil
}

// HandleErrorResponse parses error response and maps to Error
func (c *HTTPClient) HandleErrorResponse(ctx context.Context, statusCode int, body []byte) *ClientError {
	logger := logr.FromContextOrDiscard(ctx)

	// Try to parse OpenAI-style error response
	var errorResp struct {
		Error struct {
			Code    string `json:"code"`
			Type    string `json:"type"`
			Message string `json:"message"`
			Param   string `json:"param"`
		} `json:"error"`
	}

	message := string(body)
	if err := json.Unmarshal(body, &errorResp); err == nil && errorResp.Error.Message != "" {
		message = errorResp.Error.Message
	}

	// Map HTTP status codes to error categories
	category := MapStatusCodeToCategory(statusCode)

	logger.V(logging.DEBUG).Info("HTTP request failed", "status", statusCode, "category", category, "message", message)

	return &ClientError{
		Category:     category,
		Message:      fmt.Sprintf("HTTP %d: %s", statusCode, message),
		RawError:     fmt.Errorf("status code: %d, body: %s", statusCode, string(body)),
		StatusCode:   statusCode,
		ResponseBody: body,
	}
}

// MapStatusCodeToCategory maps HTTP status codes to error categories
func MapStatusCodeToCategory(statusCode int) ErrorCategory {
	switch statusCode {
	case http.StatusBadRequest: // 400
		return ErrCategoryInvalidReq
	case http.StatusUnauthorized, http.StatusForbidden: // 401, 403
		return ErrCategoryAuth
	case http.StatusTooManyRequests: // 429
		return ErrCategoryRateLimit
	case http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout: // 500, 502, 503, 504
		return ErrCategoryServer
	default:
		if statusCode >= http.StatusInternalServerError {
			return ErrCategoryServer
		}
		return ErrCategoryUnknown
	}
}

// newRetryAfterFunc returns a resty RetryAfterFunc that differentiates retry
// delays by HTTP status code:
//   - 429 or 503 with Retry-After header: use the server-specified delay (0 retries as fast as InitialBackoff allows)
//   - 429 without Retry-After: use a longer backoff (rateLimitBackoffMultiplier × initialBackoff)
//   - 502/503/504 without Retry-After: transient failures, faster retry targeting ~maxBackoff/2 (jitter may exceed)
//   - other 5xx: standard exponential backoff
//
// RFC 9110 defines Retry-After on 503 the same way RFC 6585 does on 429: the
// server tells the client how long it expects to be unavailable. Ignoring it
// on a 503 would spend every retry inside that window and fail a request the
// server asked us to resend later.
//
// Resty clamps the returned delay to [InitialBackoff, MaxBackoff].
func newRetryAfterFunc(initialBackoff, maxBackoff time.Duration) func(*resty.Client, *resty.Response) (time.Duration, error) {
	return func(_ *resty.Client, resp *resty.Response) (time.Duration, error) {
		if resp == nil {
			return computeBackoff(1, initialBackoff, maxBackoff), nil
		}

		switch resp.StatusCode() {
		case http.StatusTooManyRequests: // 429
			if d, ok := serverRetryAfter(resp); ok {
				return d, nil
			}
			// No usable header — use longer backoff
			return computeBackoff(resp.Request.Attempt, rateLimitBackoffMultiplier*initialBackoff, maxBackoff), nil

		case http.StatusServiceUnavailable: // 503
			if d, ok := serverRetryAfter(resp); ok {
				return d, nil
			}
			// No usable header: transient failure, retry faster
			return computeBackoff(resp.Request.Attempt, initialBackoff, maxBackoff/2), nil

		case http.StatusBadGateway, http.StatusGatewayTimeout: // 502, 504
			// Transient failures — retry faster
			return computeBackoff(resp.Request.Attempt, initialBackoff, maxBackoff/2), nil

		default:
			return computeBackoff(resp.Request.Attempt, initialBackoff, maxBackoff), nil
		}
	}
}

// serverRetryAfter returns the delay from the response's Retry-After header
// and true when the header is present and parseable. Resty treats a returned
// 0 as "use default algorithm", so Retry-After: 0 maps to 1ns, which resty
// clamps to InitialBackoff.
func serverRetryAfter(resp *resty.Response) (time.Duration, bool) {
	ra := resp.Header().Get("Retry-After")
	if ra == "" {
		return 0, false
	}
	d, ok := parseRetryAfter(ra)
	if !ok {
		return 0, false
	}
	if d == 0 {
		d = 1 * time.Nanosecond
	}
	return d, true
}

// computeBackoff uses cenkalti/backoff to calculate exponential backoff with
// jitter for the given attempt number.
func computeBackoff(attempt int, base, max time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	b := &cbackoff.ExponentialBackOff{
		InitialInterval:     base,
		MaxInterval:         max,
		Multiplier:          2,
		RandomizationFactor: 0.5,
	}
	b.Reset()
	var d time.Duration
	for range attempt {
		d = b.NextBackOff()
	}
	return d
}

// parseRetryAfter parses a Retry-After header value, which can be either
// a number of seconds (e.g. "120") or an HTTP-date (e.g. "Thu, 01 Dec 1994 16:00:00 GMT").
// Returns the parsed duration and true if successful, or (0, false) if unparsable.
// A value of 0 seconds is valid and means "retry as soon as possible" (subject to
// resty's InitialBackoff floor).
func parseRetryAfter(value string) (time.Duration, bool) {
	// Try integer seconds first
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second, true
	}
	// Try HTTP-date format
	if t, err := time.Parse(http.TimeFormat, value); err == nil {
		d := time.Until(t)
		if d < 0 {
			d = 0
		}
		return d, true
	}
	return 0, false
}

// BuildTLSConfig constructs a custom TLS configuration based on provided options
// Returns nil if no custom TLS config is needed (use system defaults)
func BuildTLSConfig(config *Config, logger logr.Logger) (*tls.Config, error) {
	if !config.TLSInsecureSkipVerify &&
		config.TLSCACertFile == "" &&
		config.TLSClientCertFile == "" &&
		config.TLSClientKeyFile == "" &&
		config.TLSMinVersion == 0 &&
		config.TLSMaxVersion == 0 {
		return nil, nil
	}

	// At least one custom TLS option is set - build custom config
	tlsConfig := &tls.Config{}

	// 1. InsecureSkipVerify (testing only)
	if config.TLSInsecureSkipVerify {
		tlsConfig.InsecureSkipVerify = true
		logger.Info("WARNING: TLS certificate verification is disabled - this is insecure and should only be used for testing")
	}

	// 2. Custom CA certificate (for private CAs)
	if config.TLSCACertFile != "" {
		caCert, err := os.ReadFile(config.TLSCACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate file %s: %w", config.TLSCACertFile, err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", config.TLSCACertFile)
		}

		tlsConfig.RootCAs = caCertPool
		logger.V(logging.INFO).Info("Loaded custom CA certificate", "file", config.TLSCACertFile)
	}

	// 3. Client certificate (for mTLS)
	if config.TLSClientCertFile != "" && config.TLSClientKeyFile != "" {
		clientCert, err := tls.LoadX509KeyPair(config.TLSClientCertFile, config.TLSClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key pair: %w", err)
		}

		tlsConfig.Certificates = []tls.Certificate{clientCert}
		logger.V(logging.INFO).Info("Loaded client certificate", "file", config.TLSClientCertFile)
	} else if config.TLSClientCertFile != "" || config.TLSClientKeyFile != "" {
		return nil, fmt.Errorf("both TLSClientCertFile and TLSClientKeyFile must be specified for mTLS")
	}

	// 4. TLS version constraints
	if config.TLSMinVersion != 0 {
		tlsConfig.MinVersion = config.TLSMinVersion
		logger.V(logging.INFO).Info("Set minimum TLS version", "version", fmt.Sprintf("0x%04x", config.TLSMinVersion))
	}
	if config.TLSMaxVersion != 0 {
		tlsConfig.MaxVersion = config.TLSMaxVersion
		logger.V(logging.INFO).Info("Set maximum TLS version", "version", fmt.Sprintf("0x%04x", config.TLSMaxVersion))
	}

	return tlsConfig, nil
}
