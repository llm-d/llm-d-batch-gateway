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

package otel

import (
	"context"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-logr/logr"
)

const defaultServiceName = "batch-gateway"

// The exporter types OTEL_TRACES_EXPORTER selects between.
const (
	exporterTypeOTLP    = "otlp"
	exporterTypeConsole = "console"
	exporterTypeNone    = "none"

	defaultExporterType = exporterTypeOTLP
)

// Span attribute keys for batch-gateway resources.
const (
	AttrBatchID      = "batch.id"
	AttrInputFileID  = "batch.input_file.id"
	AttrOutputFileID = "batch.output_file.id"
	AttrErrorFileID  = "batch.error_file.id"
	AttrTenantID     = "tenant.id"
	// AttrPassThroughHeaders lists the names of the client headers forwarded to
	// inference for this batch.
	AttrPassThroughHeaders = "batch.pass_through_headers"
	// Job-level request counts as span attributes for persistent trace-based analysis.
	// These complement the in-DB progress updates (UpdateProgressCounts),
	// which are used for real-time status polling only.
	AttrRequestTotal     = "batch.request.total"
	AttrRequestCompleted = "batch.request.completed"
	AttrRequestFailed    = "batch.request.failed"
	AttrModelCount       = "batch.model.count"
	AttrRequestCount     = "batch.request.count"
	AttrInputLineCount   = "batch.input.line_count"
	AttrRejectedCount    = "batch.input.rejected_count"
	AttrSizeBucket       = "batch.size_bucket"
)

// baseLoggerKey stores the logger captured before the first trace enrichment.
// Nested StartSpan calls enrich from this base rather than from the
// accumulated context logger, preventing duplicate trace_id/span_id fields.
type baseLoggerKey struct{}

// StartSpan creates a new span using the batch-gateway tracer.
// When the span carries a valid trace context, the logger in the returned
// context is enriched with trace_id and span_id so that all downstream
// log lines emitted via logr.FromContextOrDiscard(ctx) are automatically
// correlated with the active trace.
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	ctx, span := otel.Tracer(defaultServiceName).Start(ctx, name, opts...)
	if sc := span.SpanContext(); sc.IsValid() {
		base, ok := ctx.Value(baseLoggerKey{}).(logr.Logger)
		if !ok {
			base = logr.FromContextOrDiscard(ctx)
			ctx = context.WithValue(ctx, baseLoggerKey{}, base)
		}
		logger := base.WithValues(
			"trace_id", sc.TraceID().String(),
			"span_id", sc.SpanID().String(),
		)
		ctx = logr.NewContext(ctx, logger)
	}
	return ctx, span
}

// SetAttr sets attributes on the span in the given context.
func SetAttr(ctx context.Context, attrs ...attribute.KeyValue) {
	trace.SpanFromContext(ctx).SetAttributes(attrs...)
}

// DetachedContext returns a new background context that carries a span linked to
// the span in the original context. Use this when the original context is cancelled
// (e.g. pod shutdown) but you still need to perform traced operations (e.g. re-enqueue).
// The linked span appears in Jaeger as a separate trace with a link back to the original,
// avoiding orphan spans with no connection to the parent trace.
func DetachedContext(ctx context.Context, name string) (context.Context, trace.Span) {
	var links []trace.Link
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
		links = append(links, trace.Link{SpanContext: sc})
	}
	bgCtx := logr.NewContext(context.Background(), logr.FromContextOrDiscard(ctx))
	if base, ok := ctx.Value(baseLoggerKey{}).(logr.Logger); ok {
		bgCtx = context.WithValue(bgCtx, baseLoggerKey{}, base)
	}
	return StartSpan(bgCtx, name, trace.WithLinks(links...))
}

// InitTracer sets up an OpenTelemetry TracerProvider.
// Configuration is done via environment variables:
// - OTEL_TRACES_EXPORTER: Span exporter, "otlp", "console" or "none" (default: otlp)
// - OTEL_EXPORTER_OTLP_ENDPOINT: OTLP collector endpoint, consumed by the otlp exporter
// - OTEL_SERVICE_NAME: Service name (default: "batch-gateway")
//
// The propagator is installed unconditionally, before the exporter type is resolved, so
// trace context received by this process is still forwarded downstream even when
// OTEL_TRACES_EXPORTER=none.
func InitTracer(ctx context.Context) (shutdown func(context.Context) error, err error) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	exporterType := traceExporterType(ctx)

	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = defaultServiceName
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	opt := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
	}

	// "none" registers no span processor at all. Spans are still created and
	// propagated, so instrumented code and context propagation are unaffected.
	if exporterType != exporterTypeNone {
		exporter, err := newSpanExporter(ctx, exporterType)
		if err != nil {
			return nil, err
		}
		opt = append(opt, sdktrace.WithBatcher(exporter))
	}

	tp := sdktrace.NewTracerProvider(opt...)
	otel.SetTracerProvider(tp)

	logr.FromContextOrDiscard(ctx).Info("OpenTelemetry tracing initialized",
		"exporter", exporterType,
		"service", serviceName,
		"endpoint", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))

	return tp.Shutdown, nil
}

// traceExporterType resolves OTEL_TRACES_EXPORTER to one of the types
// newSpanExporter builds:
//
//   - otlp: export spans through gRPC to an opentelemetry collector
//   - console: pretty print spans on stdout, for development
//   - none: create spans but export nothing
//
// An unrecognised value falls back to otlp with a logged warning rather than
// failing startup.
func traceExporterType(ctx context.Context) string {
	exporterType := os.Getenv("OTEL_TRACES_EXPORTER")
	if exporterType == "" {
		return defaultExporterType
	}

	switch exporterType {
	case exporterTypeOTLP, exporterTypeConsole, exporterTypeNone:
		return exporterType
	default:
		logr.FromContextOrDiscard(ctx).Info("unsupported OTEL_TRACES_EXPORTER, falling back to otlp", "value", exporterType)
		return defaultExporterType
	}
}

// newSpanExporter builds the exporter named by exporterType, which traceExporterType
// has already narrowed to otlp or console; "none" is handled by the caller and never
// reaches here. The otlp exporter respects the standard OTel env vars
// (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_EXPORTER_OTLP_INSECURE, etc.) automatically.
func newSpanExporter(ctx context.Context, exporterType string) (sdktrace.SpanExporter, error) {
	if exporterType == exporterTypeConsole {
		return stdouttrace.New(stdouttrace.WithPrettyPrint())
	}

	return otlptracegrpc.New(ctx)
}
