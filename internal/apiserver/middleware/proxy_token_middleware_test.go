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

// The file contains unit tests for the proxy token middleware.
package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

func TestProxyTokenMiddleware(t *testing.T) {
	const proxyToken = "proxy-secret"

	tests := []struct {
		name           string
		configToken    string
		allowAnonymous bool
		tokens         []string // values to add for the proxy token header
		wantCalled     bool
	}{
		{
			name:        "right token reaches the handler",
			configToken: proxyToken,
			tokens:      []string{proxyToken},
			wantCalled:  true,
		},
		{
			name:        "missing token is rejected",
			configToken: proxyToken,
		},
		{
			name:        "wrong token is rejected",
			configToken: proxyToken,
			tokens:      []string{"guess"},
		},
		{
			// Envoy ext_authz appends the proxy's value after the client's.
			name:        "last token value is used",
			configToken: proxyToken,
			tokens:      []string{"guess", proxyToken},
			wantCalled:  true,
		},
		{
			name:        "client token before a wrong last value is rejected",
			configToken: proxyToken,
			tokens:      []string{proxyToken, "guess"},
		},
		{
			name:   "empty configured token rejects an empty header",
			tokens: []string{""},
		},
		{
			name:           "anonymous tenant skips the check",
			configToken:    proxyToken,
			allowAnonymous: true,
			tokens:         []string{"guess"},
			wantCalled:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := newTestConfig("X-MaaS-Username")
			config.ProxyToken = tt.configToken
			config.AllowAnonymousTenant = tt.allowAnonymous

			var (
				called     bool
				tokenInReq []string
			)
			handler := NewProxyTokenMiddleware(config)(batchesRoute, func(w http.ResponseWriter, r *http.Request) {
				called = true
				tokenInReq = r.Header.Values(common.DefaultProxyTokenHeader)
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
			for _, v := range tt.tokens {
				req.Header.Add(common.DefaultProxyTokenHeader, v)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if called != tt.wantCalled {
				t.Fatalf("expected handler called=%t, got %t", tt.wantCalled, called)
			}
			if tt.wantCalled {
				if len(tokenInReq) != 0 {
					t.Errorf("proxy token header reached the handler: %q", tokenInReq)
				}
				return
			}
			if w.Code != http.StatusUnauthorized {
				t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, w.Code)
			}
			var resp openai.ErrorResponse
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatalf("failed to decode error response: %v", err)
			}
			if resp.Error.Type != "authentication_error" {
				t.Errorf("expected error type %q, got %q", "authentication_error", resp.Error.Type)
			}
		})
	}
}

// TestProxyTokenMiddleware_MiddlewareChain runs the check in the order the API
// server registers it: NewRequestMiddleware, SecurityHeaders, proxy token, RequireTenant.
func TestProxyTokenMiddleware_MiddlewareChain(t *testing.T) {
	const (
		tenantHeader = "X-MaaS-Username"
		proxyToken   = "proxy-secret"
	)

	tests := []struct {
		name       string
		method     string
		token      string // proxy token header value; empty sends no header
		wantStatus int
	}{
		{
			name:       "request with the token reaches the handler",
			method:     http.MethodGet,
			token:      proxyToken,
			wantStatus: http.StatusOK,
		},
		{
			name:       "request without the token is rejected",
			method:     http.MethodGet,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "CORS preflight without the token is answered",
			method:     http.MethodOptions,
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := newTestConfig(tenantHeader)
			config.ProxyToken = proxyToken

			handler := RequireTenant(batchesRoute, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
			})
			handler = NewProxyTokenMiddleware(config)(batchesRoute, handler)
			handler = SecurityHeaders(batchesRoute, handler)
			handler = wrapWithRequestMiddleware(config, handler)

			req := httptest.NewRequest(tt.method, "/v1/batches", nil)
			req.Header.Set(tenantHeader, "real-user")
			if tt.token != "" {
				req.Header.Set(common.DefaultProxyTokenHeader, tt.token)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, w.Code)
			}
			if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("expected security headers on the response, got X-Content-Type-Options=%q", got)
			}
		})
	}
}
