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

// The entry point for the batch processor.

package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/klog/v2"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/worker"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/interrupt"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

func main() {
	defer klog.Flush()

	if err := run(); err != nil {
		klog.Fatalf("Processor failed: %v", err)
	}
}

func run() error {
	// load configuration & logging setup
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Errorf("get Processor hostname: %w", err)
	}
	logger := klog.NewKlogr().WithValues("hostname", hostname, "service", "batch-processor")
	ctx := logr.NewContext(context.Background(), logger)

	cfg := config.NewConfig()
	fs := flag.NewFlagSet("batch-gateway-processor", flag.ExitOnError)

	cfgFilePath := fs.String("config", "cmd/batch-processor/config.yaml", "Path to configuration file")
	klog.InitFlags(fs)
	_ = fs.Parse(os.Args[1:]) // ExitOnError mode will exit on error

	if err := cfg.LoadFromYAML(*cfgFilePath); err != nil {
		logger.Error(err, "Failed to load config file. Processor cannot start", "path", *cfgFilePath, "err", err)
		return err
	}

	if err := cfg.Validate(); err != nil {
		logger.Error(err, "Invalid config. Processor cannot start", "err", err)
		return err
	}

	// initialize OpenTelemetry tracing
	shutdownTracer, err := uotel.InitTracer(ctx)
	if err != nil {
		logger.Error(err, "Failed to initialize tracer")
		return err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := shutdownTracer(shutdownCtx); err != nil {
			logger.Error(err, "Failed to shutdown tracer")
		}
	}()

	// metrics setup
	if err := metrics.InitMetrics(*cfg); err != nil {
		logger.Error(err, "Failed to initialize metrics")
		return err
	}
	logger.V(logging.INFO).Info("Metrics initialized", "numWorkers", cfg.NumWorkers)

	// setup context with graceful shutdown
	ctx, cancel := interrupt.ContextWithSignal(ctx)
	defer cancel()

	// readiness starts as false and flips right before entering polling loop execution.
	var ready atomic.Bool
	// read only channel for observability server's fatal error
	obsFatalCh := startObservabilityServer(
		ctx,
		cfg,
		&ready,
		cancel,
		cfg.TerminateOnObservabilityFailure,
	)

	procClients, err := buildProcessorClients(ctx, cfg, hostname)
	if err != nil {
		logger.Error(err, "Failed to build processor clients")
		return err
	}
	defer func() { _ = procClients.Close() }()

	// init processor
	logger.V(logging.INFO).Info("Initializing worker processor", "maxWorkers", cfg.NumWorkers)
	proc, err := worker.NewProcessor(cfg, procClients, hostname, logger)
	if err != nil {
		logger.Error(err, "Failed to create processor")
		return err
	}
	defer func() {
		// stop with a fresh timeout ctx (avoid already-cancelled ctx)
		// timeout should be less than k8s terminationGracePeriodSeconds
		stopCtx, stopCtxCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer stopCtxCancel()
		logger.V(logging.INFO).Info("Processor exited, shutting down")
		proc.Stop(stopCtx) // wait for all workers to finish
		logger.V(logging.INFO).Info("Processor exited gracefully")
	}()

	// ready flips to true only after processor pre-flight checks succeed and
	// right before the polling loop begins accepting work.
	go func() {
		<-ctx.Done()
		ready.Store(false)
	}()
	err = proc.Run(ctx, func() {
		ready.Store(true)
		logger.V(logging.INFO).Info("Processor polling loop started", "pollInterval", cfg.PollInterval.String())
	})
	// Run may return before ctx is cancelled (e.g. semaphore guard shutdown).
	// Mark not-ready immediately so the readiness probe reflects the actual state.
	ready.Store(false)
	if cfg.TerminateOnObservabilityFailure {
		// Give the observability goroutine a brief chance to publish the fatal cause,
		// so we can prefer it over a derived context-cancel error from the polling loop.
		if obsErr := waitObservabilityFatalError(ctx, obsFatalCh, 100*time.Millisecond); obsErr != nil {
			logger.Error(obsErr, "Processor stopped due to observability server failure")
			return obsErr
		}
	}
	if err != nil {
		logger.Error(err, "Processor polling loop exited with error")
		return err
	}
	return nil
}

func startObservabilityServer(
	ctx context.Context,
	cfg *config.ProcessorConfig,
	ready *atomic.Bool,
	cancel context.CancelFunc,
	terminateOnObservabilityFailure bool,
) <-chan error {
	logger := logr.FromContextOrDiscard(ctx)
	errCh := make(chan error, 1)

	go func() {
		// event channel - no need to close (1 buffer, max 1 event sent)
		reportFatal := func(err error) {
			if err == nil {
				return
			}
			if !terminateOnObservabilityFailure {
				logger.Error(err, "Observability server failed in best-effort mode; processor will continue")
				return
			}

			// Keep observability failure as primary shutdown cause.
			select {
			case errCh <- err:
			default:
			}
			cancel()
		}

		m := http.NewServeMux()
		m.Handle("/metrics", metrics.NewMetricsHandler())
		m.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		})
		if cfg.EnablePprof {
			m.HandleFunc("/debug/pprof/", pprof.Index)
			m.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			m.HandleFunc("/debug/pprof/profile", pprof.Profile)
			m.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			m.HandleFunc("/debug/pprof/trace", pprof.Trace)
			logger.V(logging.INFO).Info("pprof profiling enabled on observability server")
		}
		// ready endpoint - indicates the processor is ready to process requests
		m.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
			if !ready.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("not ready"))
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("OK"))
		})

		server := &http.Server{
			Addr:              cfg.Addr,
			Handler:           m,
			ReadHeaderTimeout: 10 * time.Second,
		}

		go func() {
			<-ctx.Done()
			logger.V(logging.INFO).Info("Shutting down observability server")
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := server.Shutdown(shutdownCtx); err != nil {
				logger.Error(err, "Observability server shutdown failed")
			}
		}()

		logger.V(logging.INFO).Info("Start observability server", "addr", cfg.Addr)

		err := server.ListenAndServe()

		if err != nil && err != http.ErrServerClosed {
			logger.Error(err, "Observability server failed")
			reportFatal(err)
		}
	}()

	return errCh
}

func waitObservabilityFatalError(ctx context.Context, obsFatalCh <-chan error, wait time.Duration) error {
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case err, ok := <-obsFatalCh:
		if ok {
			return err
		}
		return nil
	case <-ctx.Done():
		// graceful shutdown can race with publishing observability fatal causes.
		// wait a short additional window and prefer the explicit fatal error when present.
		fallback := time.NewTimer(wait)
		defer fallback.Stop()
		select {
		case err, ok := <-obsFatalCh:
			if ok {
				return err
			}
			return nil
		case <-fallback.C:
			return nil
		}
	case <-timer.C:
		return nil
	}
}

// buildProcessorClients constructs all processor clients using the same backend as the apiserver
func buildProcessorClients(ctx context.Context, cfg *config.ProcessorConfig, processorID string) (*clientset.Clientset, error) {
	logger := logr.FromContextOrDiscard(ctx)

	cfg.DBClientCfg.PostgreSQLCfg.EnableTracing = cfg.OTelCfg.PostgresqlTracing

	resolved, err := config.ResolveModelGateways(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve model gateways: %w", err)
	}

	opts := []clientset.Option{
		clientset.WithDB(cfg.DBClientCfg),
		clientset.WithFile(cfg.FileClientCfg),
	}
	if resolved.Global != nil {
		opts = append(opts, clientset.WithGlobalInference(*resolved.Global))
	}
	if len(resolved.PerModel) > 0 {
		opts = append(opts, clientset.WithPerModelInference(resolved.PerModel))
	}
	if resolved.Async != nil {
		resolved.Async.ConsumerID = processorID
		opts = append(opts, clientset.WithAsyncInference(*resolved.Async))
	}
	clients, err := clientset.NewClientset(ctx, ucom.ComponentProcessor, opts...)
	if err != nil {
		logger.Error(err, "Failed to create clients")
		return nil, err
	}

	// ResolveModelGateways populates exactly one of Async, Global, or PerModel
	// based on the dispatch mode validated by Validate().
	switch {
	case resolved.Async != nil:
		logger.V(logging.INFO).Info("Processor clients initialized",
			"mode", "async",
			"numModels", len(resolved.Async.Models),
			"fileClientType", cfg.FileClientCfg.Type)
	case resolved.Global != nil:
		logger.V(logging.INFO).Info("Processor clients initialized",
			"mode", "global",
			"gatewayURL", resolved.Global.URL,
			"fileClientType", cfg.FileClientCfg.Type)
	default:
		logger.V(logging.INFO).Info("Processor clients initialized",
			"mode", "per-model",
			"numModelGateways", len(resolved.PerModel),
			"fileClientType", cfg.FileClientCfg.Type)
	}

	return clients, nil
}
