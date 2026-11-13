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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/go-resty/resty/v2"
)

func testLogger(t testing.TB) logr.Logger {
	return testr.NewWithInterface(t, testr.Options{})
}

// TestNewHTTPClient_Defaults tests that NewHTTPClient sets proper defaults
func TestNewHTTPClient_Defaults(t *testing.T) {
	config := Config{
		BaseURL: "http://localhost:8000",
	}

	client, err := NewHTTPClient(config, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	if client == nil {
		t.Fatal("Expected non-nil client")
		return
	}

	// Verify transport settings (defaults should be applied)
	if client.transport == nil {
		t.Fatal("Expected non-nil Transport")
	}
	if client.transport.MaxIdleConns != 100 {
		t.Errorf("Expected Transport.MaxIdleConns=100, got %d", client.transport.MaxIdleConns)
	}
	if client.transport.MaxIdleConnsPerHost != 100 {
		t.Errorf("Expected Transport.MaxIdleConnsPerHost=100, got %d", client.transport.MaxIdleConnsPerHost)
	}
	if client.transport.ResponseHeaderTimeout != 5*time.Minute {
		t.Errorf("Expected Transport.ResponseHeaderTimeout=5m (same as default Timeout), got %v", client.transport.ResponseHeaderTimeout)
	}

	if err := client.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestHTTPClient_Close_nilReceiver(t *testing.T) {
	var c *HTTPClient
	if err := c.Close(); err != nil {
		t.Fatalf("Close on nil: %v", err)
	}
}

// TestNewHTTPClient_CustomConfig tests NewHTTPClient with custom configuration
func TestNewHTTPClient_CustomConfig(t *testing.T) {
	config := Config{
		BaseURL:         "http://example.com",
		Timeout:         10 * time.Second,
		MaxIdleConns:    50,
		IdleConnTimeout: 60 * time.Second,
		APIKey:          "test-api-key",
	}

	client, err := NewHTTPClient(config, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	if client == nil {
		t.Fatal("Expected non-nil client")
	}

	// Client created successfully with custom settings
}

// TestNewHTTPClient_RetryDefaults tests retry configuration defaults
func TestNewHTTPClient_RetryDefaults(t *testing.T) {
	config := Config{
		BaseURL:    "http://localhost:8000",
		MaxRetries: 3,
	}

	client, err := NewHTTPClient(config, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	if client == nil {
		t.Fatal("Expected non-nil client")
	}

	// Client created successfully with retry enabled (defaults applied internally)
}

// TestNewHTTPClient_RetryCustom tests custom retry configuration
func TestNewHTTPClient_RetryCustom(t *testing.T) {
	config := Config{
		BaseURL:        "http://localhost:8000",
		MaxRetries:     5,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
	}

	client, err := NewHTTPClient(config, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	if client == nil {
		t.Fatal("Expected non-nil client")
	}

	// Client created successfully with custom retry settings
}

// TestPost_Success tests successful POST request
func TestPost_Success(t *testing.T) {
	requestBody := map[string]string{"key": "value"}
	expectedResponse := map[string]string{"result": "success"}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("Expected POST method, got %s", r.Method)
		}

		// Verify headers
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Expected Content-Type=application/json, got %s", r.Header.Get("Content-Type"))
		}

		// Verify request body
		var body map[string]string
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("Failed to decode request body: %v", err)
		}
		if body["key"] != "value" {
			t.Errorf("Expected body key=value, got %v", body)
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(expectedResponse)
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{BaseURL: server.URL}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	respBody, statusCode, err := client.Post(context.Background(), "/test", requestBody, nil, "")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}

	var response map[string]string
	if err := json.Unmarshal(respBody, &response); err != nil {
		t.Fatalf("Failed to unmarshal response: %v", err)
	}

	if response["result"] != "success" {
		t.Errorf("Expected result=success, got %v", response)
	}
}

// TestPost_WithRequestID tests POST request with request ID header
func TestPost_WithRequestID(t *testing.T) {
	expectedRequestID := "test-request-123"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestID := r.Header.Get("X-Request-ID")
		if requestID != expectedRequestID {
			t.Errorf("Expected X-Request-ID=%s, got %s", expectedRequestID, requestID)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{BaseURL: server.URL}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, expectedRequestID)
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}
}

// TestPost_WithCustomHeaders tests POST request with custom headers
func TestPost_WithCustomHeaders(t *testing.T) {
	customHeaders := map[string]string{
		"X-Custom-Header": "custom-value",
		"X-Another":       "another-value",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for k, v := range customHeaders {
			if r.Header.Get(k) != v {
				t.Errorf("Expected header %s=%s, got %s", k, v, r.Header.Get(k))
			}
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{BaseURL: server.URL}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, customHeaders, "")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}
}

// TestPost_WithAPIKey tests that API key is included as Bearer token
func TestPost_WithAPIKey(t *testing.T) {
	expectedAPIKey := "test-api-key-123"

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		expectedAuth := "Bearer " + expectedAPIKey
		if authHeader != expectedAuth {
			t.Errorf("Expected Authorization=%s, got %s", expectedAuth, authHeader)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{
		BaseURL: server.URL,
		APIKey:  expectedAPIKey,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}
}

// TestPost_ContextCancellation tests POST request with cancelled context
func TestPost_ContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{BaseURL: server.URL}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	_, _, err = client.Post(ctx, "/test", nil, nil, "")
	if err == nil {
		t.Fatal("Expected error for cancelled context, got nil")
	}
}

// TestPost_ContextTimeout tests POST request with timeout
func TestPost_ContextTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{BaseURL: server.URL}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, _, err = client.Post(ctx, "/test", nil, nil, "")
	if err == nil {
		t.Fatal("Expected timeout error, got nil")
	}
}

// TestPost_RetryOn500 tests that 500 errors are retried
func TestPost_RetryOn500(t *testing.T) {
	var attemptCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attemptCount.Add(1)
		if count < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(`{"error": {"message": "server error"}}`))
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result": "success"}`))
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{
		BaseURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "test-retry")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}

	if attemptCount.Load() != 3 {
		t.Errorf("Expected 3 attempts, got %d", attemptCount.Load())
	}

}

// TestPost_RetryOn429 tests that 429 (rate limit) errors are retried
func TestPost_RetryOn429(t *testing.T) {
	var attemptCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attemptCount.Add(1)
		if count < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": {"message": "rate limit exceeded"}}`))
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result": "success"}`))
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{
		BaseURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "test-retry-429")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}

	if attemptCount.Load() != 2 {
		t.Errorf("Expected 2 attempts, got %d", attemptCount.Load())
	}

}

// TestPost_NoRetryOn400 tests that 400 errors are not retried
func TestPost_NoRetryOn400(t *testing.T) {
	var attemptCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attemptCount.Add(1)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error": {"message": "bad request"}}`))
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{
		BaseURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "test-no-retry")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", statusCode)
	}

	if attemptCount.Load() != 1 {
		t.Errorf("Expected 1 attempt (no retry), got %d", attemptCount.Load())
	}

}

// TestRetryAfter_429WithRetryAfterHeader tests that 429 responses with Retry-After header
// are retried after the server-specified delay.
func TestRetryAfter_429WithRetryAfterHeader(t *testing.T) {
	tests := []struct {
		name         string
		retryAfter   string
		minElapsed   time.Duration
		maxElapsed   time.Duration
		assertTiming bool
	}{
		{
			name:         "seconds format",
			retryAfter:   "0",
			maxElapsed:   80 * time.Millisecond,
			assertTiming: true,
		},
		{
			name:         "HTTP-date format",
			retryAfter:   time.Now().Add(120 * time.Millisecond).UTC().Format(http.TimeFormat),
			assertTiming: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var attemptCount atomic.Int32

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				count := attemptCount.Add(1)
				if count < 2 {
					w.Header().Set("Retry-After", tt.retryAfter)
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = w.Write([]byte(`{"error": {"message": "rate limited"}}`))
				} else {
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"result": "success"}`))
				}
			}))
			defer server.Close()

			client, err := NewHTTPClient(Config{
				BaseURL:        server.URL,
				MaxRetries:     3,
				InitialBackoff: 10 * time.Millisecond,
				MaxBackoff:     100 * time.Millisecond,
			}, testLogger(t))
			if err != nil {
				t.Fatalf("NewHTTPClient failed: %v", err)
			}

			start := time.Now()
			_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "test-retry-after")
			elapsed := time.Since(start)
			if err != nil {
				t.Fatalf("Post failed: %v", err)
			}

			if statusCode != http.StatusOK {
				t.Errorf("Expected status 200, got %d", statusCode)
			}

			if attemptCount.Load() != 2 {
				t.Errorf("Expected 2 attempts, got %d", attemptCount.Load())
			}
			if tt.assertTiming {
				if tt.minElapsed > 0 && elapsed < tt.minElapsed {
					t.Errorf("Expected elapsed >= %v, got %v", tt.minElapsed, elapsed)
				}
				if tt.maxElapsed > 0 && elapsed > tt.maxElapsed {
					t.Errorf("Expected elapsed <= %v, got %v", tt.maxElapsed, elapsed)
				}
			}
		})
	}
}

// TestRetryAfter_429WithoutHeader tests that 429 responses without Retry-After header
// still retry with the 429-specific longer backoff.
func TestRetryAfter_429WithoutHeader(t *testing.T) {
	var attemptCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count := attemptCount.Add(1)
		if count < 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error": {"message": "rate limited"}}`))
		} else {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result": "success"}`))
		}
	}))
	defer server.Close()

	client, err := NewHTTPClient(Config{
		BaseURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "test-429-no-header")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}

	if attemptCount.Load() != 2 {
		t.Errorf("Expected 2 attempts, got %d", attemptCount.Load())
	}
}

// TestRetryAfter_5xxUsesFasterBackoff verifies that the retryAfter function
// uses a lower max-backoff cap for transient 502/503/504 (maxBackoff/2) than
// for other 5xx errors (maxBackoff).
func TestRetryAfter_5xxUsesFasterBackoff(t *testing.T) {
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 60 * time.Second
	)

	retryAfter := newRetryAfterFunc(initialBackoff, maxBackoff)

	makeResp := func(statusCode, attempt int) *resty.Response {
		return &resty.Response{
			Request:     &resty.Request{Attempt: attempt},
			RawResponse: &http.Response{StatusCode: statusCode, Header: http.Header{}},
		}
	}

	transientCodes := []int{
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout,
	}

	// The jitter factor is 0.5, so the max returned value for a capped
	// interval is max * (1 + 0.5) = max * 1.5.
	transientCeiling := maxBackoff / 2 * 3 / 2 // maxBackoff/2 with +50% jitter
	standardCeiling := maxBackoff * 3 / 2      // maxBackoff with +50% jitter

	for _, code := range transientCodes {
		t.Run(fmt.Sprintf("status_%d_capped_at_half_max", code), func(t *testing.T) {
			for attempt := 1; attempt <= 10; attempt++ {
				d, _ := retryAfter(nil, makeResp(code, attempt))
				if d > transientCeiling {
					t.Errorf("attempt %d: %d backoff %v exceeds transient ceiling %v",
						attempt, code, d, transientCeiling)
				}
			}
		})
	}

	t.Run("status_500_reaches_higher_cap", func(t *testing.T) {
		// At high attempt counts, 500 should sometimes exceed the transient
		// ceiling since its cap is maxBackoff (not maxBackoff/2).
		// Run enough iterations to observe this with high probability.
		var exceeded bool
		for range 200 {
			d, _ := retryAfter(nil, makeResp(http.StatusInternalServerError, 10))
			if d > transientCeiling {
				exceeded = true
				break
			}
		}
		if !exceeded {
			t.Errorf("500 backoff at high attempt never exceeded transient ceiling %v; "+
				"expected it to reach up to %v", transientCeiling, standardCeiling)
		}
	})
}

// TestRetryAfter_503HonorsRetryAfterHeader verifies that a 503 carrying a
// Retry-After header waits the server-specified delay instead of the fast
// transient backoff, while 502/504 keep the fast backoff.
func TestRetryAfter_503HonorsRetryAfterHeader(t *testing.T) {
	const (
		initialBackoff = 1 * time.Second
		maxBackoff     = 60 * time.Second
	)

	retryAfter := newRetryAfterFunc(initialBackoff, maxBackoff)

	makeResp := func(statusCode int, retryAfterHeader string) *resty.Response {
		h := http.Header{}
		if retryAfterHeader != "" {
			h.Set("Retry-After", retryAfterHeader)
		}
		return &resty.Response{
			Request:     &resty.Request{Attempt: 1},
			RawResponse: &http.Response{StatusCode: statusCode, Header: h},
		}
	}

	t.Run("503 with seconds", func(t *testing.T) {
		d, err := retryAfter(nil, makeResp(http.StatusServiceUnavailable, "45"))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d != 45*time.Second {
			t.Errorf("expected 45s, got %v", d)
		}
	})

	t.Run("503 with HTTP-date", func(t *testing.T) {
		at := time.Now().Add(40 * time.Second).UTC().Format(http.TimeFormat)
		d, _ := retryAfter(nil, makeResp(http.StatusServiceUnavailable, at))
		// HTTP-date has one-second resolution.
		if d < 38*time.Second || d > 41*time.Second {
			t.Errorf("expected about 40s, got %v", d)
		}
	})

	t.Run("503 with zero maps to the minimum", func(t *testing.T) {
		d, _ := retryAfter(nil, makeResp(http.StatusServiceUnavailable, "0"))
		if d != time.Nanosecond {
			t.Errorf("expected 1ns (clamped by resty to InitialBackoff), got %v", d)
		}
	})

	// The transient ceiling with +50% jitter, as in TestRetryAfter_5xxUsesFasterBackoff.
	transientCeiling := maxBackoff / 2 * 3 / 2

	t.Run("503 with invalid header falls back to transient backoff", func(t *testing.T) {
		d, _ := retryAfter(nil, makeResp(http.StatusServiceUnavailable, "soon"))
		if d <= 0 || d > transientCeiling {
			t.Errorf("expected transient backoff in (0, %v], got %v", transientCeiling, d)
		}
	})

	for _, code := range []int{http.StatusBadGateway, http.StatusGatewayTimeout} {
		t.Run(fmt.Sprintf("status_%d_ignores_header", code), func(t *testing.T) {
			// A 1s initial backoff at attempt 1 is at most 1.5s with jitter,
			// far below the 45s the header asks for.
			d, _ := retryAfter(nil, makeResp(code, "45"))
			if d <= 0 || d > initialBackoff*3/2 {
				t.Errorf("expected first-attempt transient backoff, got %v", d)
			}
		})
	}
}

// TestPost_RetryOn503WaitsForRetryAfter verifies end to end that the client
// waits out a 503's Retry-After before resending.
func TestPost_RetryOn503WaitsForRetryAfter(t *testing.T) {
	var attemptCount atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attemptCount.Add(1) < 2 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error": {"message": "unavailable"}}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result": "success"}`))
	}))
	defer server.Close()

	// Without the header the first retry would wait at most 15ms.
	client, err := NewHTTPClient(Config{
		BaseURL:        server.URL,
		MaxRetries:     3,
		InitialBackoff: 10 * time.Millisecond,
		MaxBackoff:     5 * time.Second,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	start := time.Now()
	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "test-503-retry-after")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}
	if attemptCount.Load() != 2 {
		t.Errorf("Expected 2 attempts, got %d", attemptCount.Load())
	}
	if elapsed < 900*time.Millisecond {
		t.Errorf("Expected the retry to wait about 1s for Retry-After, got %v", elapsed)
	}
}

// TestParseRetryAfter tests the Retry-After header parsing function.
func TestParseRetryAfter(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		wantOK   bool
		wantZero bool // if wantOK, whether the duration should be zero
	}{
		{name: "integer seconds", value: "120", wantOK: true, wantZero: false},
		{name: "zero seconds", value: "0", wantOK: true, wantZero: true},
		{name: "HTTP-date future", value: time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat), wantOK: true, wantZero: false},
		{name: "HTTP-date past", value: time.Now().Add(-10 * time.Second).UTC().Format(http.TimeFormat), wantOK: true, wantZero: true},
		{name: "invalid value", value: "abc", wantOK: false},
		{name: "empty string", value: "", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, ok := parseRetryAfter(tt.value)
			if ok != tt.wantOK {
				t.Fatalf("parseRetryAfter(%q): ok = %v, want %v", tt.value, ok, tt.wantOK)
			}
			if !tt.wantOK {
				return
			}
			if tt.wantZero && d != 0 {
				t.Errorf("Expected zero duration, got %v", d)
			}
			if !tt.wantZero && d <= 0 {
				t.Errorf("Expected positive duration, got %v", d)
			}
		})
	}
}

// TestHandleErrorResponse_OpenAIFormat tests parsing OpenAI-style error response
func TestHandleErrorResponse_OpenAIFormat(t *testing.T) {
	body := []byte(`{"error": {"message": "Invalid API key", "type": "invalid_request_error", "code": "invalid_api_key"}}`)

	client, _ := NewHTTPClient(Config{BaseURL: "http://localhost"}, testLogger(t))
	clientErr := client.HandleErrorResponse(context.Background(), http.StatusUnauthorized, body)

	if clientErr == nil {
		t.Fatal("Expected non-nil error")
		return
	}

	if clientErr.Category != ErrCategoryAuth {
		t.Errorf("Expected category AUTH_ERROR, got %s", clientErr.Category)
	}

	if clientErr.Message != "HTTP 401: Invalid API key" {
		t.Errorf("Expected message 'HTTP 401: Invalid API key', got %s", clientErr.Message)
	}

	if clientErr.StatusCode != http.StatusUnauthorized {
		t.Errorf("Expected StatusCode 401, got %d", clientErr.StatusCode)
	}

	if string(clientErr.ResponseBody) != string(body) {
		t.Errorf("Expected ResponseBody to be preserved, got %s", clientErr.ResponseBody)
	}
}

// TestHandleErrorResponse_PlainText tests parsing plain text error response
func TestHandleErrorResponse_PlainText(t *testing.T) {
	body := []byte("Internal Server Error")

	client, _ := NewHTTPClient(Config{BaseURL: "http://localhost"}, testLogger(t))
	clientErr := client.HandleErrorResponse(context.Background(), http.StatusInternalServerError, body)

	if clientErr == nil {
		t.Fatal("Expected non-nil error")
		return
	}

	if clientErr.Category != ErrCategoryServer {
		t.Errorf("Expected category SERVER_ERROR, got %s", clientErr.Category)
	}

	if clientErr.Message != "HTTP 500: Internal Server Error" {
		t.Errorf("Expected message 'HTTP 500: Internal Server Error', got %s", clientErr.Message)
	}
}

// TestHandleErrorResponse_EmptyBody tests handling empty error response
func TestHandleErrorResponse_EmptyBody(t *testing.T) {
	body := []byte("")

	client, _ := NewHTTPClient(Config{BaseURL: "http://localhost"}, testLogger(t))
	clientErr := client.HandleErrorResponse(context.Background(), http.StatusBadGateway, body)

	if clientErr == nil {
		t.Fatal("Expected non-nil error")
		return
	}

	if clientErr.Category != ErrCategoryServer {
		t.Errorf("Expected category SERVER_ERROR, got %s", clientErr.Category)
	}

	if clientErr.Message != "HTTP 502: " {
		t.Errorf("Expected message 'HTTP 502: ', got %s", clientErr.Message)
	}
}

// TestMapStatusCodeToCategory tests all status code mappings
func TestMapStatusCodeToCategory(t *testing.T) {
	tests := []struct {
		statusCode int
		expected   ErrorCategory
	}{
		{http.StatusBadRequest, ErrCategoryInvalidReq},          // 400
		{http.StatusUnauthorized, ErrCategoryAuth},              // 401
		{http.StatusForbidden, ErrCategoryAuth},                 // 403
		{http.StatusTooManyRequests, ErrCategoryRateLimit},      // 429
		{http.StatusInternalServerError, ErrCategoryServer},     // 500
		{http.StatusBadGateway, ErrCategoryServer},              // 502
		{http.StatusServiceUnavailable, ErrCategoryServer},      // 503
		{http.StatusGatewayTimeout, ErrCategoryServer},          // 504
		{http.StatusHTTPVersionNotSupported, ErrCategoryServer}, // 505 (other 5xx)
		{http.StatusNotFound, ErrCategoryUnknown},               // 404 (unmapped 4xx)
		{http.StatusTeapot, ErrCategoryUnknown},                 // 418 (unmapped)
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("status_%d", tt.statusCode), func(t *testing.T) {
			category := MapStatusCodeToCategory(tt.statusCode)
			if category != tt.expected {
				t.Errorf("For status %d, expected category %s, got %s", tt.statusCode, tt.expected, category)
			}
		})
	}
}

func TestClientError_OpenAIErrorType(t *testing.T) {
	tests := []struct {
		name     string
		category ErrorCategory
		want     string
	}{
		{name: "invalid request", category: ErrCategoryInvalidReq, want: "invalid_request_error"},
		{name: "auth", category: ErrCategoryAuth, want: "authentication_error"},
		{name: "rate limit", category: ErrCategoryRateLimit, want: "rate_limit_error"},
		{name: "server", category: ErrCategoryServer, want: "server_error"},
		{name: "parse", category: ErrCategoryParse, want: "invalid_response"},
		{name: "unknown", category: ErrCategoryUnknown, want: "unknown_error"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := (&ClientError{Category: tt.category}).OpenAIErrorType()
			if got != tt.want {
				t.Fatalf("OpenAIErrorType() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestBuildTLSConfig_Nil tests that BuildTLSConfig returns nil for default config
func TestBuildTLSConfig_Nil(t *testing.T) {
	config := Config{}
	tlsConfig, err := BuildTLSConfig(&config, testLogger(t))

	if err != nil {
		t.Fatalf("BuildTLSConfig failed: %v", err)
	}

	if tlsConfig != nil {
		t.Error("Expected nil TLS config for default settings")
	}
}

// TestBuildTLSConfig_InsecureSkipVerify tests InsecureSkipVerify option
func TestBuildTLSConfig_InsecureSkipVerify(t *testing.T) {
	config := Config{
		TLSInsecureSkipVerify: true,
	}
	tlsConfig, err := BuildTLSConfig(&config, testLogger(t))

	if err != nil {
		t.Fatalf("BuildTLSConfig failed: %v", err)
	}

	if tlsConfig == nil {
		t.Fatal("Expected non-nil TLS config")
		return
	}

	if !tlsConfig.InsecureSkipVerify {
		t.Error("Expected InsecureSkipVerify=true")
	}
}

// TestBuildTLSConfig_CustomCA tests custom CA certificate
func TestBuildTLSConfig_CustomCA(t *testing.T) {
	// Create a temporary CA cert file
	tmpDir := t.TempDir()
	caCertFile := filepath.Join(tmpDir, "ca.crt")

	// Generate a valid self-signed certificate
	caCertPEM := generateTestCertificate(t)
	if err := os.WriteFile(caCertFile, caCertPEM, 0644); err != nil {
		t.Fatalf("Failed to write CA cert file: %v", err)
	}

	config := Config{
		TLSCACertFile: caCertFile,
	}
	tlsConfig, err := BuildTLSConfig(&config, testLogger(t))

	if err != nil {
		t.Fatalf("BuildTLSConfig failed: %v", err)
	}

	if tlsConfig == nil {
		t.Fatal("Expected non-nil TLS config")
		return
	}

	if tlsConfig.RootCAs == nil {
		t.Error("Expected non-nil RootCAs")
	}
}

// TestBuildTLSConfig_CustomCA_FileNotFound tests error when CA cert file doesn't exist
func TestBuildTLSConfig_CustomCA_FileNotFound(t *testing.T) {
	config := Config{
		TLSCACertFile: "/nonexistent/ca.crt",
	}
	_, err := BuildTLSConfig(&config, testLogger(t))

	if err == nil {
		t.Fatal("Expected error for missing CA cert file")
	}
}

// TestBuildTLSConfig_CustomCA_InvalidPEM tests error when CA cert is invalid
func TestBuildTLSConfig_CustomCA_InvalidPEM(t *testing.T) {
	tmpDir := t.TempDir()
	caCertFile := filepath.Join(tmpDir, "ca.crt")

	if err := os.WriteFile(caCertFile, []byte("not a valid PEM certificate"), 0644); err != nil {
		t.Fatalf("Failed to write invalid CA cert file: %v", err)
	}

	config := Config{
		TLSCACertFile: caCertFile,
	}
	_, err := BuildTLSConfig(&config, testLogger(t))

	if err == nil {
		t.Fatal("Expected error for invalid PEM certificate")
	}
}

// TestBuildTLSConfig_ClientCert_BothRequired tests that both cert and key are required for mTLS
func TestBuildTLSConfig_ClientCert_BothRequired(t *testing.T) {
	tests := []struct {
		name     string
		certFile string
		keyFile  string
	}{
		{"only cert", "/path/to/cert.pem", ""},
		{"only key", "", "/path/to/key.pem"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				TLSClientCertFile: tt.certFile,
				TLSClientKeyFile:  tt.keyFile,
			}
			_, err := BuildTLSConfig(&config, testLogger(t))

			if err == nil {
				t.Fatal("Expected error when only one of cert/key is specified")
			}
		})
	}
}

// TestBuildTLSConfig_TLSVersions tests TLS version constraints
func TestBuildTLSConfig_TLSVersions(t *testing.T) {
	config := Config{
		TLSMinVersion: tls.VersionTLS12,
		TLSMaxVersion: tls.VersionTLS13,
	}
	tlsConfig, err := BuildTLSConfig(&config, testLogger(t))

	if err != nil {
		t.Fatalf("BuildTLSConfig failed: %v", err)
	}

	if tlsConfig == nil {
		t.Fatal("Expected non-nil TLS config")
		return
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("Expected MinVersion=TLS1.2, got 0x%04x", tlsConfig.MinVersion)
	}

	if tlsConfig.MaxVersion != tls.VersionTLS13 {
		t.Errorf("Expected MaxVersion=TLS1.3, got 0x%04x", tlsConfig.MaxVersion)
	}
}

// TestBuildTLSConfig_CombinedOptions tests combination of TLS options
func TestBuildTLSConfig_CombinedOptions(t *testing.T) {
	tmpDir := t.TempDir()
	caCertFile := filepath.Join(tmpDir, "ca.crt")

	// Generate a valid self-signed certificate
	caCertPEM := generateTestCertificate(t)
	if err := os.WriteFile(caCertFile, caCertPEM, 0644); err != nil {
		t.Fatalf("Failed to write CA cert file: %v", err)
	}

	config := Config{
		TLSCACertFile: caCertFile,
		TLSMinVersion: tls.VersionTLS12,
	}
	tlsConfig, err := BuildTLSConfig(&config, testLogger(t))

	if err != nil {
		t.Fatalf("BuildTLSConfig failed: %v", err)
	}

	if tlsConfig == nil {
		t.Fatal("Expected non-nil TLS config")
		return
	}

	if tlsConfig.RootCAs == nil {
		t.Error("Expected non-nil RootCAs")
	}

	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("Expected MinVersion=TLS1.2, got 0x%04x", tlsConfig.MinVersion)
	}
}

// TestClientError_Error tests ClientError Error() method
func TestClientError_Error(t *testing.T) {
	err := &ClientError{
		Category: ErrCategoryAuth,
		Message:  "authentication failed",
		RawError: fmt.Errorf("invalid token"),
	}

	if err.Error() != "authentication failed" {
		t.Errorf("Expected error message 'authentication failed', got %s", err.Error())
	}
}

// TestClientError_IsRetryable tests ClientError IsRetryable() method
func TestClientError_IsRetryable(t *testing.T) {
	tests := []struct {
		category ErrorCategory
		expected bool
	}{
		{ErrCategoryRateLimit, true},
		{ErrCategoryServer, true},
		{ErrCategoryInvalidReq, false},
		{ErrCategoryAuth, false},
		{ErrCategoryParse, false},
		{ErrCategoryUnknown, false},
	}

	for _, tt := range tests {
		t.Run(string(tt.category), func(t *testing.T) {
			err := &ClientError{Category: tt.category}
			if err.IsRetryable() != tt.expected {
				t.Errorf("For category %s, expected IsRetryable=%v, got %v", tt.category, tt.expected, err.IsRetryable())
			}
		})
	}
}

// TestNewHTTPClient_TLSInsecureSkipVerify_Integration tests insecure TLS in integration
func TestNewHTTPClient_TLSInsecureSkipVerify_Integration(t *testing.T) {
	// Create HTTPS test server with self-signed cert
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result": "success"}`))
	}))
	defer server.Close()

	// Client with InsecureSkipVerify should work
	client, err := NewHTTPClient(Config{
		BaseURL:               server.URL,
		TLSInsecureSkipVerify: true,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}
}

// TestNewHTTPClient_TLSVerifyFails tests that TLS verification fails without InsecureSkipVerify
func TestNewHTTPClient_TLSVerifyFails(t *testing.T) {
	// Create HTTPS test server with self-signed cert
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result": "success"}`))
	}))
	defer server.Close()

	// Client without InsecureSkipVerify should fail on self-signed cert
	client, err := NewHTTPClient(Config{
		BaseURL: server.URL,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, _, err = client.Post(context.Background(), "/test", nil, nil, "")
	if err == nil {
		t.Fatal("Expected TLS verification error, got nil")
	}

	// Verify it's a certificate verification error
	if !isTLSError(err) {
		t.Errorf("Expected TLS error, got: %v", err)
	}
}

// TestNewHTTPClient_WithCustomCA_Integration tests custom CA certificate
func TestNewHTTPClient_WithCustomCA_Integration(t *testing.T) {
	// Create HTTPS test server
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result": "success"}`))
	}))
	defer server.Close()

	// Extract the server's CA cert
	tmpDir := t.TempDir()
	caCertFile := filepath.Join(tmpDir, "ca.crt")

	serverCert := server.Certificate()
	caCertPool := x509.NewCertPool()
	caCertPool.AddCert(serverCert)

	// Write server cert as CA cert
	certPEM := pemEncodeCert(serverCert.Raw)
	if err := os.WriteFile(caCertFile, certPEM, 0644); err != nil {
		t.Fatalf("Failed to write CA cert: %v", err)
	}

	// Client with custom CA should work
	client, err := NewHTTPClient(Config{
		BaseURL:       server.URL,
		TLSCACertFile: caCertFile,
	}, testLogger(t))
	if err != nil {
		t.Fatalf("NewHTTPClient failed: %v", err)
	}

	_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "")
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	if statusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", statusCode)
	}
}

// Helper functions

// generateTestCertificate generates a valid self-signed certificate for testing
func generateTestCertificate(t *testing.T) []byte {
	t.Helper()

	// Generate a private key
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	// Create certificate template
	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("Failed to generate serial number: %v", err)
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	// Create self-signed certificate
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("Failed to create certificate: %v", err)
	}

	// Encode to PEM
	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: derBytes,
	})

	return certPEM
}

func isTLSError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return containsString(errStr, "tls") ||
		containsString(errStr, "certificate") ||
		containsString(errStr, "x509")
}

func containsString(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	if len(s) < len(substr) {
		return false
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestCapacityRetryTracking(t *testing.T) {
	t.Run("429 retry sets capacity flag", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		ctx, hadCapacity := NewCapacityRetryContext(context.Background())
		_, statusCode, err := client.Post(ctx, "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if statusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", statusCode)
		}
		if !hadCapacity() {
			t.Fatal("expected HadCapacityRetry=true after 429 retry")
		}
	})

	t.Run("5xx retry sets capacity flag", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		ctx, hadCapacity := NewCapacityRetryContext(context.Background())
		_, statusCode, err := client.Post(ctx, "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if statusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", statusCode)
		}
		if !hadCapacity() {
			t.Fatal("expected HadCapacityRetry=true after 502 retry")
		}
	})

	t.Run("network-only retry does not set capacity flag", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		ctx, hadCapacity := NewCapacityRetryContext(context.Background())
		_, statusCode, err := client.Post(ctx, "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if statusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", statusCode)
		}
		if hadCapacity() {
			t.Fatal("expected HadCapacityRetry=false when no retries occurred")
		}
	})

	t.Run("no tracking context is safe", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     2,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		// No capacity tracking context — should not panic
		_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if statusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", statusCode)
		}
	})
}

func TestDroppedReasonHandling(t *testing.T) {
	t.Run("503 with rejected-ttl-expired is not retried", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			w.Header().Set(HeaderDroppedReason, DroppedReasonTTLExpired)
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"ttl expired"}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if statusCode != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want 503", statusCode)
		}
		if attempts.Load() != 1 {
			t.Fatalf("expected 1 attempt (no retries), got %d", attempts.Load())
		}
	})

	t.Run("429 with rejected-ttl-expired is not retried", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			attempts.Add(1)
			w.Header().Set(HeaderDroppedReason, DroppedReasonTTLExpired)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"ttl expired"}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if statusCode != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want 429", statusCode)
		}
		if attempts.Load() != 1 {
			t.Fatalf("expected 1 attempt (no retries), got %d", attempts.Load())
		}
	})

	t.Run("429 with other dropped reason is retried", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			count := attempts.Add(1)
			if count < 3 {
				w.Header().Set(HeaderDroppedReason, "rejected-saturated")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if statusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", statusCode)
		}
		if attempts.Load() != 3 {
			t.Fatalf("expected 3 attempts, got %d", attempts.Load())
		}
	})

	t.Run("503 with other dropped reason is retried", func(t *testing.T) {
		var attempts atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			count := attempts.Add(1)
			if count < 3 {
				w.Header().Set(HeaderDroppedReason, "rejected-saturated")
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     3,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		_, statusCode, err := client.Post(context.Background(), "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if statusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", statusCode)
		}
		if attempts.Load() != 3 {
			t.Fatalf("expected 3 attempts, got %d", attempts.Load())
		}
	})

	t.Run("dropped reason captured via context", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(HeaderDroppedReason, DroppedReasonTTLExpired)
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":"ttl expired"}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL:        server.URL,
			MaxRetries:     1,
			InitialBackoff: time.Millisecond,
			MaxBackoff:     10 * time.Millisecond,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		ctx, droppedReason := NewDroppedReasonContext(context.Background())
		_, _, err = client.Post(ctx, "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if droppedReason() != DroppedReasonTTLExpired {
			t.Fatalf("droppedReason = %q, want %q", droppedReason(), DroppedReasonTTLExpired)
		}
	})

	t.Run("no dropped reason header leaves context empty", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer server.Close()

		client, err := NewHTTPClient(Config{
			BaseURL: server.URL,
		}, testLogger(t))
		if err != nil {
			t.Fatal(err)
		}

		ctx, droppedReason := NewDroppedReasonContext(context.Background())
		_, _, err = client.Post(ctx, "/test", nil, nil, "")
		if err != nil {
			t.Fatal(err)
		}
		if droppedReason() != "" {
			t.Fatalf("droppedReason = %q, want empty", droppedReason())
		}
	})
}

func pemEncodeCert(derBytes []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: derBytes,
	})
}
