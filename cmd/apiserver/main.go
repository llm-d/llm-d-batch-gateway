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

// The entry point for the batch gateway API server.
// It handles server initialization, configuration, and graceful shutdown.
package main

import (
	"context"
	"os"
	"time"

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/server"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql/migrate"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/interrupt"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
	"k8s.io/klog/v2"
)

func main() {
	defer klog.Flush()

	if err := run(); err != nil {
		klog.Fatalf("apiserver failed: %v", err)
	}
}

func run() error {
	logger := klog.NewKlogr()
	ctx := logr.NewContext(context.Background(), logger)

	if len(os.Args) > 1 && os.Args[1] == migrate.Subcommand {
		return migrate.RunCommand(ctx, os.Args[2:])
	}

	config := common.NewConfig()
	if err := config.Load(); err != nil {
		logger.Error(err, "Failed to load config")
		return err
	}

	// graceful shutdown
	ctx, cancel := interrupt.ContextWithSignal(ctx)
	defer cancel()

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

	metrics.InitMetrics()

	logger.Info("starting api server")

	server, err := server.New(ctx, config)
	if err != nil {
		return err
	}
	if err = server.Start(ctx); err != nil {
		return err
	}
	logger.Info("api server is terminated")
	return nil
}
