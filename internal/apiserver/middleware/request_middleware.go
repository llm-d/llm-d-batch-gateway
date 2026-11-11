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

// The file implements request-level observability middleware:
// request ID generation, tenant extraction, structured logging, metrics, and OTel tracing.
package middleware

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/metrics"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

const (
	RequestIdHeaderKey = "x-request-id"
)

// NewRequestMiddleware returns a RouteMiddleware that adds request ID generation,
// tenant extraction, structured logging, metrics recording, and OTel tracing.
// It uses route.Pattern (e.g. "/v1/batches/{batch_id}") as the metric path label,
// avoiding high-cardinality issues from raw paths with resource IDs.
// OTel spans are created when route.SpanName is non-empty.
func NewRequestMiddleware(config *common.ServerConfig) common.RouteMiddleware {
	return func(route common.Route, next http.HandlerFunc) http.HandlerFunc {
		metricsPath := route.Pattern
		spanName := route.SpanName
		return func(w http.ResponseWriter, r *http.Request) {
			// Extract request ID from header
			requestID := r.Header.Get(RequestIdHeaderKey)
			if requestID == "" {
				requestID = uuid.NewString()
			}
			w.Header().Set(RequestIdHeaderKey, requestID)

			// Extract tenant ID from header. Without the header it stays empty and
			// RequireTenant rejects the request, unless anonymous tenants are allowed.
			tenantID := common.LastHeaderValue(r, config.GetTenantHeader(), "")
			if tenantID == "" && config.AllowAnonymousTenant {
				tenantID = common.DefaultTenantID
			}

			// Extract file ID and batch ID from path values for logging
			fileID := r.PathValue(common.PathParamFileID)
			batchID := r.PathValue(common.PathParamBatchID)

			// Create request logger with request ID and tenant ID
			logger := logr.FromContextOrDiscard(r.Context())
			logger = logger.WithValues("requestID", requestID)
			logger = logger.WithValues("tenantID", tenantID)
			if fileID != "" {
				logger = logger.WithValues("fileID", fileID)
			}
			if batchID != "" {
				logger = logger.WithValues("batchID", batchID)
			}

			ctx := logr.NewContext(r.Context(), logger)
			ctx = context.WithValue(ctx, common.RequestIDKey, requestID)
			ctx = context.WithValue(ctx, common.TenantIDKey, tenantID)

			// Log incoming request
			logger.V(logging.DEBUG).Info("incoming request",
				"method", r.Method,
				"path", r.URL.Path,
				"remoteAddr", r.RemoteAddr,
			)

			rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			// Metrics
			start := time.Now()
			metrics.RecordRequestStart(r.Method, metricsPath)
			defer func() {
				duration := time.Since(start).Seconds()
				status := strconv.Itoa(rw.statusCode)
				metrics.RecordRequestFinish(r.Method, metricsPath, status, duration)
			}()

			// OTel tracing (skipped when route has no SpanName)
			if spanName != "" {
				var span trace.Span
				ctx, span = uotel.StartSpan(ctx, spanName)
				span.SetAttributes(attribute.String(uotel.AttrTenantID, tenantID))
				if fileID != "" {
					span.SetAttributes(attribute.String(uotel.AttrInputFileID, fileID))
				}
				if batchID != "" {
					span.SetAttributes(attribute.String(uotel.AttrBatchID, batchID))
				}
				defer func() {
					span.SetAttributes(attribute.Int("http.status_code", rw.statusCode))
					if rw.statusCode >= 500 {
						span.SetStatus(codes.Error, http.StatusText(rw.statusCode))
					}
					span.End()
				}()
			}

			next(rw, r.WithContext(ctx))
		}
	}
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) StatusCode() int {
	return rw.statusCode
}
