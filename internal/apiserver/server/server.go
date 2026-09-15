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

// The file provides the HTTP server implementation for the batch gateway API.
package server

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/pprof"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/batch"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/file"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/health"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/middleware"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/readiness"
	tlspkg "github.com/llm-d/llm-d-batch-gateway/internal/tls"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

type Server struct {
	logger      logr.Logger
	config      *common.ServerConfig
	serverReady *atomic.Bool
	apiHandler  http.Handler
	obsHandler  http.Handler
	clients     *clientset.Clientset
}

func buildClients(ctx context.Context, config *common.ServerConfig) (*clientset.Clientset, error) {
	logger := logr.FromContextOrDiscard(ctx)

	config.DBClientCfg.RedisCfg.ServiceName = "batch-apiserver"
	config.DBClientCfg.RedisCfg.EnableTracing = config.OTelCfg.RedisTracing
	config.DBClientCfg.PostgreSQLCfg.EnableTracing = config.OTelCfg.PostgresqlTracing

	clients, err := clientset.NewClientset(ctx, ucom.ComponentApiserver,
		clientset.WithDB(config.DBClientCfg),
		clientset.WithFile(config.FileClientCfg),
		clientset.WithExchange(config.DBClientCfg.RedisCfg),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create clients: %w", err)
	}
	logger.Info("clients initialized")

	return clients, nil
}

func New(ctx context.Context, config *common.ServerConfig) (*Server, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	logger := logr.FromContextOrDiscard(ctx).WithName("api_server")
	serverReady := &atomic.Bool{}
	serverReady.Store(false)

	// build clients
	clients, err := buildClients(ctx, config)
	if err != nil {
		return nil, err
	}

	// API mux: business endpoints with per-route middleware.
	// Request flow:
	//   Matched:   Client → ServeMux → Recovery → RequestMiddleware (+ OTel) → SecurityHeaders → Handler
	//   Unmatched: Client → ServeMux → Recovery → RequestMiddleware → SecurityHeaders → NotFoundHandler
	apiMux := http.NewServeMux()
	fileHandler := file.NewFileAPIHandler(config, clients)
	batchHandler, err := batch.NewBatchAPIHandler(config, clients)
	if err != nil {
		return nil, fmt.Errorf("failed to create batch API handler: %w", err)
	}
	apiMiddlewares := []common.RouteMiddleware{
		middleware.Recovery,                     // outermost: catches panics from all inner layers
		middleware.NewRequestMiddleware(config), // request ID, tenant, logging, metrics, OTel tracing
		middleware.SecurityHeaders,              // innermost: security response headers
	}
	for _, h := range []common.ApiHandler{fileHandler, batchHandler} {
		common.RegisterHandler(apiMux, h, apiMiddlewares...)
	}
	common.RegisterNotFoundHandler(apiMux, apiMiddlewares...)

	// Observability mux: health, readiness, metrics (always plain HTTP)
	obsMux := http.NewServeMux()
	healthHandler := health.NewHealthApiHandler()
	readinessHandler := readiness.NewReadinessApiHandler(serverReady)
	metricsHandler := metrics.NewMetricsApiHandler()
	for _, h := range []common.ApiHandler{healthHandler, readinessHandler, metricsHandler} {
		common.RegisterHandler(obsMux, h)
	}

	if config.EnablePprof {
		obsMux.HandleFunc("/debug/pprof/", pprof.Index)
		obsMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		obsMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		obsMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		obsMux.HandleFunc("/debug/pprof/trace", pprof.Trace)
		logger.Info("pprof profiling enabled on observability server")
	}

	return &Server{
		config:      config,
		logger:      logger,
		serverReady: serverReady,
		apiHandler:  apiMux,
		obsHandler:  obsMux,
		clients:     clients,
	}, nil
}

// Start the API server and the observability server.
func (s *Server) Start(ctx context.Context) error {
	logger := s.logger

	// --- Observability server (always plain HTTP) ---
	obsAddr := s.config.Host + ":" + s.config.ObservabilityPort
	obsServer := &http.Server{
		Addr:              obsAddr,
		Handler:           s.obsHandler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		logger.Info("starting observability server", "addr", obsAddr)
		if err := obsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error(err, "observability server failed")
		}
	}()

	// --- API server ---
	ln, err := net.Listen("tcp", s.config.Host+":"+s.config.Port)
	if err != nil {
		logger.Error(err, "failed to start")
		return err
	}
	defer func() { _ = ln.Close() }()

	httpserver := &http.Server{
		Handler: s.apiHandler,
		BaseContext: func(_ net.Listener) context.Context {
			return logr.NewContext(context.Background(), logr.FromContextOrDiscard(ctx))
		},
		ReadHeaderTimeout: time.Duration(s.config.GetReadHeaderTimeoutSeconds()) * time.Second,
		ReadTimeout:       time.Duration(s.config.GetReadTimeoutSeconds()) * time.Second,
		WriteTimeout:      time.Duration(s.config.GetWriteTimeoutSeconds()) * time.Second,
		IdleTimeout:       time.Duration(s.config.GetIdleTimeoutSeconds()) * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
	}

	// Enable TLS if cert and key are provided
	if s.config.SSLEnabled() {
		cert, err := tls.LoadX509KeyPair(s.config.SSLCertFile, s.config.SSLKeyFile)
		if err != nil {
			return err
		}

		tlsOpts, err := tlspkg.Resolve(s.logger, s.config.TLSMinVersion, s.config.TLSCipherSuites, s.config.TLSNextProtos)
		if err != nil {
			return fmt.Errorf("failed to resolve TLS configuration: %w", err)
		}

		httpserver.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
		}
		for _, opt := range tlsOpts {
			opt(httpserver.TLSConfig)
		}
	} else if s.config.SSLCertFile != "" || s.config.SSLKeyFile != "" {
		err := fmt.Errorf("both tls-cert-file and tls-private-key-file must be provided to enable TLS")
		return err
	}

	logger.Info("starting API server", "addr", ln.Addr().String())

	// Start serving in a goroutine
	serveDone := make(chan error, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("server panicked: %v", r)
				logger.Error(err, "server goroutine panicked", "panic", r)
				serveDone <- err
			}
		}()
		var err error
		if s.config.SSLEnabled() {
			err = httpserver.ServeTLS(ln, "", "")
		} else {
			err = httpserver.Serve(ln)
		}
		serveDone <- err
	}()

	// Wait for immediate startup failure or mark ready after 100ms
	select {
	case <-time.After(100 * time.Millisecond):
		logger.Info("server is ready")
		s.serverReady.Store(true)
	case err := <-serveDone:
		logger.Error(err, "server failed to start")
		return err
	case <-ctx.Done():
		logger.Info("shutdown requested before server ready", "reason", ctx.Err())
		return ctx.Err()
	}

	// Continue waiting for shutdown or failure after marking ready
	select {
	case <-ctx.Done():
		// Normal shutdown path
		s.serverReady.Store(false)
		logger.Info("shutting down", "reason", ctx.Err())

		// Gracefully shutdown both servers
		apiSd := time.Duration(s.config.GetAPIShutdownTimeoutSeconds()) * time.Second
		sdApiCtx, cancelApi := context.WithTimeout(context.Background(), apiSd)
		defer cancelApi()

		if err := httpserver.Shutdown(sdApiCtx); err != nil {
			logger.Error(err, "failed to gracefully shutdown API server")
		}

		obsSd := time.Duration(s.config.GetObservabilityShutdownTimeoutSeconds()) * time.Second
		sdObsCtx, cancelObs := context.WithTimeout(context.Background(), obsSd)
		defer cancelObs()

		if err := obsServer.Shutdown(sdObsCtx); err != nil {
			logger.Error(err, "failed to gracefully shutdown observability server")
		}

		// Wait for server goroutine to finish with timeout
		select {
		case err = <-serveDone:
			if err != nil && err != http.ErrServerClosed {
				logger.Error(err, "server exited with error after shutdown")
				return err
			}
		case <-time.After(5 * time.Second):
			err := fmt.Errorf("server goroutine did not exit after shutdown")
			logger.Error(err, "timeout waiting for server goroutine to exit")
			return err
		}

		if err := s.clients.Close(); err != nil {
			logger.Error(err, "failed to close clients")
		}
		logger.Info("shutdown complete")

	case err := <-serveDone:
		// Server failed after becoming ready
		s.serverReady.Store(false)
		if err != nil && err != http.ErrServerClosed {
			logger.Error(err, "server exited unexpectedly")
			return err
		}
	}

	return nil
}
