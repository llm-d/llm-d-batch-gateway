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

// The entry point for the batch garbage collector.
// This command runs as a long-lived process that periodically scans for
// expired batch jobs and files and removes them from the database.

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/klog/v2"

	"golang.org/x/sync/errgroup"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql/migrate"
	"github.com/llm-d/llm-d-batch-gateway/internal/gc/collector"
	gcconfig "github.com/llm-d/llm-d-batch-gateway/internal/gc/config"
	gcmetrics "github.com/llm-d/llm-d-batch-gateway/internal/gc/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/gc/reconciler"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/interrupt"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

func main() {
	defer klog.Flush()

	if err := run(); err != nil {
		klog.Fatalf("Garbage collector failed: %v", err)
	}
}

func run() error {
	hostname, _ := os.Hostname()
	logger := klog.NewKlogr().WithValues("hostname", hostname, "service", "batch-gc")
	ctx := logr.NewContext(context.Background(), logger)

	if len(os.Args) > 1 && os.Args[1] == migrate.Subcommand {
		return migrate.RunCommand(ctx, os.Args[2:])
	}

	flagSet := flag.NewFlagSet("batch-gc", flag.ExitOnError)
	configFile := flagSet.String("config", "./config.yaml", "path to YAML config file")
	klog.InitFlags(flagSet)
	_ = flagSet.Parse(os.Args[1:]) // ExitOnError mode handles errors

	cfg, err := gcconfig.Load(*configFile)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	logger.Info("Starting batch garbage collector", "dryRun", cfg.DryRun, "interval", cfg.Collector.Interval)

	shutdownTracer, err := uotel.InitTracer(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize tracer: %w", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := shutdownTracer(shutdownCtx); err != nil {
			logger.Error(err, "Failed to shutdown tracer")
		}
	}()

	if err := gcmetrics.InitMetrics(); err != nil {
		return fmt.Errorf("failed to initialize metrics: %w", err)
	}

	ctx, cancel := interrupt.ContextWithSignal(ctx)
	defer cancel()

	cfg.DBClientCfg.RedisCfg.ServiceName = "batch-gc"
	cfg.DBClientCfg.RedisCfg.EnableTracing = cfg.OTelCfg.RedisTracing
	cfg.DBClientCfg.PostgreSQLCfg.EnableTracing = cfg.OTelCfg.PostgresqlTracing

	clientOpts := []clientset.Option{
		clientset.WithDB(cfg.DBClientCfg),
		clientset.WithFile(cfg.FileClientCfg),
	}
	if cfg.Reconciler.Enabled {
		clientOpts = append(clientOpts, clientset.WithExchange(cfg.DBClientCfg.RedisCfg))
	}

	clients, err := clientset.NewClientset(ctx, ucom.ComponentGC, clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to create clients: %w", err)
	}
	defer func() { _ = clients.Close() }()

	var ready atomic.Bool
	metricsErrCh, err := startMetricsServer(ctx, cfg.MetricsAddr, logger, &ready)
	if err != nil {
		return err
	}

	gc := collector.NewGarbageCollector(clients.BatchDB, clients.FileDB, clients.File, cfg.DryRun, cfg.Collector.Interval, cfg.Collector.MaxConcurrency, nil)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		select {
		case err := <-metricsErrCh:
			return err
		case <-gCtx.Done():
			return gCtx.Err()
		}
	})
	g.Go(func() error { return gc.RunLoop(gCtx) })

	if cfg.Reconciler.Enabled {
		onCycle := func(r *reconciler.Result) {
			gcmetrics.RecordCycleDuration(r.Duration)
			if !cfg.DryRun {
				gcmetrics.RecordOrphansRecovered(gcmetrics.ActionCancelled, r.Cancelled)
				gcmetrics.RecordOrphansRecovered(gcmetrics.ActionExpired, r.Expired)
				gcmetrics.RecordOrphansRecovered(gcmetrics.ActionReEnqueued, r.ReEnqueued)
				gcmetrics.RecordOrphansRecovered(gcmetrics.ActionFailed, r.Failed)
				gcmetrics.RecordStaleCleanup(r.StaleCleanup)
			}
			gcmetrics.RecordCASConflicts(r.Conflicts)
			gcmetrics.RecordErrors(r.Errors)
		}
		rec, err := reconciler.NewReconciler(clients.BatchDB, clients.Queue, clients.InFlight, cfg.Reconciler.Interval, cfg.DryRun, onCycle)
		if err != nil {
			return fmt.Errorf("failed to create reconciler: %w", err)
		}
		g.Go(func() error { return rec.RunLoop(gCtx) })
	}

	ready.Store(true)
	logger.Info("GC workers started, marking ready")

	if err := g.Wait(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("gc/reconciler failed: %w", err)
	}

	logger.Info("Garbage collector shut down gracefully")
	return nil
}

func startMetricsServer(ctx context.Context, addr string, logger logr.Logger, ready *atomic.Bool) (<-chan error, error) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("metrics server listen on %s: %w", addr, err)
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error(err, "Metrics server shutdown failed")
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		logger.Info("Metrics server listening", "addr", ln.Addr())
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Error(err, "Metrics server failed")
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
	}()

	return errCh, nil
}
