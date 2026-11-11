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

// The file implements the middleware that rejects requests without a tenant.
package middleware

import (
	"net/http"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// RequireTenant returns a RouteMiddleware that rejects requests whose context
// carries no tenant ID with 401. It runs inside NewRequestMiddleware, which
// resolves the tenant ID, and inside SecurityHeaders, so the rejection carries
// the security headers and CORS preflight requests are answered before it.
func RequireTenant(_ common.Route, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if tenantID, ok := r.Context().Value(common.TenantIDKey).(string); !ok || tenantID == "" {
			oaiErr := openai.NewAPIError(http.StatusUnauthorized, "", "Missing tenant identity", nil)
			common.WriteAPIError(w, r, oaiErr)
			return
		}

		next(w, r)
	}
}
