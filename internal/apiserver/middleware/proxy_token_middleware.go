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

// The file implements the middleware that checks the auth proxy's shared token.
package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// NewProxyTokenMiddleware returns a RouteMiddleware that rejects with 401 any
// request whose proxy token header does not hold config.ProxyToken, so only
// requests that passed through the auth proxy are served. Like the tenant
// header, the last value is used, since Envoy ext_authz appends after
// client-supplied values. The header is removed before the handler runs, so it
// is never forwarded as a pass-through header. With config.AllowAnonymousTenant
// the token is not checked.
//
// It runs inside SecurityHeaders, so the rejection carries the security headers
// and CORS preflight requests are answered before it.
func NewProxyTokenMiddleware(config *common.ServerConfig) common.RouteMiddleware {
	tokenHeader := config.GetProxyTokenHeader()
	want := []byte(config.ProxyToken)
	return func(_ common.Route, next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			got := common.LastHeaderValue(r, tokenHeader, "")
			r.Header.Del(tokenHeader)

			if !config.AllowAnonymousTenant {
				if len(want) == 0 || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
					oaiErr := openai.NewAPIError(http.StatusUnauthorized, "", "Missing or invalid proxy token", nil)
					common.WriteAPIError(w, r, oaiErr)
					return
				}
			}

			next(w, r)
		}
	}
}
