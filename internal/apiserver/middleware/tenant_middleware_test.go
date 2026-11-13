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

// The file contains unit tests for the tenant requirement middleware.
package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// assertMissingTenantError checks that w holds the 401 written for a request without a tenant.
func assertMissingTenantError(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()

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
}

func TestRequireTenant(t *testing.T) {
	tests := []struct {
		name       string
		ctx        context.Context
		wantCalled bool
	}{
		{
			name:       "tenant in context reaches the handler",
			ctx:        context.WithValue(context.Background(), common.TenantIDKey, "real-user"),
			wantCalled: true,
		},
		{
			name: "empty tenant is rejected",
			ctx:  context.WithValue(context.Background(), common.TenantIDKey, ""),
		},
		{
			name: "context without a tenant is rejected",
			ctx:  context.Background(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			called := false
			handler := RequireTenant(dummyRoute, func(w http.ResponseWriter, r *http.Request) {
				called = true
				w.WriteHeader(http.StatusOK)
			})

			req := httptest.NewRequest(http.MethodGet, "/test", nil).WithContext(tt.ctx)
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if called != tt.wantCalled {
				t.Errorf("expected handler called=%t, got %t", tt.wantCalled, called)
			}
			if tt.wantCalled {
				if w.Code != http.StatusOK {
					t.Errorf("expected status %d, got %d", http.StatusOK, w.Code)
				}
				return
			}
			assertMissingTenantError(t, w)
		})
	}
}

// TestRequireTenant_MiddlewareChain runs RequireTenant in the order the API
// server registers it: NewRequestMiddleware, SecurityHeaders, RequireTenant.
func TestRequireTenant_MiddlewareChain(t *testing.T) {
	const tenantHeader = "X-MaaS-Username"

	tests := []struct {
		name           string
		method         string
		tenant         string // tenant header value; empty sends no header
		allowAnonymous bool
		wantStatus     int
		wantTenant     string // tenant the handler sees; empty means the handler must not run
	}{
		{
			name:       "request with tenant header reaches the handler",
			method:     http.MethodGet,
			tenant:     "real-user",
			wantStatus: http.StatusOK,
			wantTenant: "real-user",
		},
		{
			name:       "request without tenant header is rejected",
			method:     http.MethodGet,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:           "request without tenant header is served as default when anonymous tenants are allowed",
			method:         http.MethodGet,
			allowAnonymous: true,
			wantStatus:     http.StatusOK,
			wantTenant:     common.DefaultTenantID,
		},
		{
			name:       "CORS preflight without tenant header is answered",
			method:     http.MethodOptions,
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := newTestConfig(tenantHeader)
			config.AllowAnonymousTenant = tt.allowAnonymous

			var gotTenant string
			handler := RequireTenant(batchesRoute, func(w http.ResponseWriter, r *http.Request) {
				gotTenant = common.GetTenantIDFromContext(r.Context())
				w.WriteHeader(http.StatusOK)
			})
			handler = SecurityHeaders(batchesRoute, handler)
			handler = wrapWithRequestMiddleware(config, handler)

			req := httptest.NewRequest(tt.method, "/v1/batches", nil)
			if tt.tenant != "" {
				req.Header.Set(tenantHeader, tt.tenant)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)

			if w.Code != tt.wantStatus {
				t.Errorf("expected status %d, got %d", tt.wantStatus, w.Code)
			}
			if gotTenant != tt.wantTenant {
				t.Errorf("expected handler tenant %q, got %q", tt.wantTenant, gotTenant)
			}
			if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("expected security headers on the response, got X-Content-Type-Options=%q", got)
			}
			if tt.wantStatus == http.StatusUnauthorized {
				assertMissingTenantError(t, w)
			}
		})
	}
}
