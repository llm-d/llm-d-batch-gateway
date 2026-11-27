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

// The file contains unit tests for the request middleware, focusing on tenant
// header extraction and the last-value workaround for ext_authz header append behavior.
package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
)

// newTestConfig returns a minimal ServerConfig suitable for middleware tests.
func newTestConfig(tenantHeader string) *common.ServerConfig {
	return &common.ServerConfig{
		Port: "8080",
		InputHeaders: map[string]string{
			common.InputHeaderKeyTenant: tenantHeader,
		},
	}
}

var batchesRoute = common.Route{Method: http.MethodGet, Pattern: "/v1/batches"}

// wrapWithRequestMiddleware creates a handler wrapped by NewRequestMiddleware for testing.
func wrapWithRequestMiddleware(config *common.ServerConfig, inner http.HandlerFunc) http.HandlerFunc {
	mw := NewRequestMiddleware(config)
	return mw(batchesRoute, inner)
}

func TestRequestMiddleware_TenantHeader(t *testing.T) {
	const tenantHeader = "X-MaaS-Username"

	tests := []struct {
		name           string
		headerValues   []string // values to add for the tenant header
		allowAnonymous bool
		expectedTenant string
	}{
		{
			name:           "single tenant header uses that value",
			headerValues:   []string{"real-user"},
			expectedTenant: "real-user",
		},
		{
			name:           "no tenant header leaves the tenant empty",
			headerValues:   nil,
			expectedTenant: "",
		},
		{
			name:           "no tenant header falls back to default when anonymous tenants are allowed",
			headerValues:   nil,
			allowAnonymous: true,
			expectedTenant: common.DefaultTenantID,
		},
		{
			name:           "tenant header wins when anonymous tenants are allowed",
			headerValues:   []string{"real-user"},
			allowAnonymous: true,
			expectedTenant: "real-user",
		},
		{
			// Workaround for ext_authz append behavior: when a client sends a
			// spoofed header and the auth service appends the real value, the
			// middleware must use the last value (the auth-injected one).
			name:           "multiple tenant headers takes last value (ext_authz workaround)",
			headerValues:   []string{"attacker", "real-user"},
			expectedTenant: "real-user",
		},
		{
			name:           "three tenant headers takes last value",
			headerValues:   []string{"first", "second", "third"},
			expectedTenant: "third",
		},
		{
			name:           "empty single header leaves the tenant empty",
			headerValues:   []string{""},
			expectedTenant: "",
		},
		{
			name:           "empty single header falls back to default when anonymous tenants are allowed",
			headerValues:   []string{""},
			allowAnonymous: true,
			expectedTenant: common.DefaultTenantID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := newTestConfig(tenantHeader)
			config.AllowAnonymousTenant = tt.allowAnonymous
			captured := "unset"
			handler := wrapWithRequestMiddleware(config, func(w http.ResponseWriter, r *http.Request) {
				if v, ok := r.Context().Value(common.TenantIDKey).(string); ok {
					captured = v
				}
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
			for _, v := range tt.headerValues {
				req.Header.Add(tenantHeader, v)
			}

			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if captured != tt.expectedTenant {
				t.Errorf("expected tenant %q, got %q", tt.expectedTenant, captured)
			}
		})
	}
}

func TestRequestMiddleware_RequestID(t *testing.T) {
	config := newTestConfig("X-MaaS-Username")

	t.Run("preserves existing request ID", func(t *testing.T) {
		var captured string
		handler := wrapWithRequestMiddleware(config, func(w http.ResponseWriter, r *http.Request) {
			if v, ok := r.Context().Value(common.RequestIDKey).(string); ok {
				captured = v
			}
			w.WriteHeader(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
		req.Header.Set(RequestIdHeaderKey, "my-request-id")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if captured != "my-request-id" {
			t.Errorf("expected request ID %q, got %q", "my-request-id", captured)
		}
		if w.Header().Get(RequestIdHeaderKey) != "my-request-id" {
			t.Errorf("expected response header %q, got %q", "my-request-id", w.Header().Get(RequestIdHeaderKey))
		}
	})

	t.Run("generates request ID when absent", func(t *testing.T) {
		var captured string
		handler := wrapWithRequestMiddleware(config, func(w http.ResponseWriter, r *http.Request) {
			if v, ok := r.Context().Value(common.RequestIDKey).(string); ok {
				captured = v
			}
			w.WriteHeader(http.StatusOK)
		})

		req := httptest.NewRequest(http.MethodGet, "/v1/batches", nil)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)

		if captured == "" {
			t.Error("expected generated request ID, got empty string")
		}
		headerID := w.Header().Get(RequestIdHeaderKey)
		if headerID == "" {
			t.Error("expected response header to have generated request ID")
		}
		if captured != headerID {
			t.Errorf("expected context request ID %q to match response header %q", captured, headerID)
		}
	})
}
