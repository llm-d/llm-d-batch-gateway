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

// Package eventpurge schedules the periodic removal of expired batch events.
package eventpurge

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/gc/metrics"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

// Purger periodically removes expired events via the purge client.
type Purger struct {
	client   dbapi.BatchEventPurgeClient
	interval time.Duration
}

// New creates a new event purger.
func New(client dbapi.BatchEventPurgeClient, interval time.Duration) (*Purger, error) {
	if client == nil {
		return nil, fmt.Errorf("purge client is required")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("interval must be positive, got %v", interval)
	}
	return &Purger{client: client, interval: interval}, nil
}

// RunLoop runs the purger in a continuous loop at the configured interval.
// It blocks until the context is cancelled, purging immediately on start
// and then on every tick of the interval.
func (p *Purger) RunLoop(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("Starting event purge loop", "interval", p.interval)

	// Run immediately on startup before waiting for the first tick.
	p.runOnce(ctx)

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Event purge loop stopped")
			return ctx.Err()
		case <-ticker.C:
			p.runOnce(ctx)
		}
	}
}

// runOnce executes a single purge and records metrics.
func (p *Purger) runOnce(ctx context.Context) {
	logger := logr.FromContextOrDiscard(ctx)

	purged, err := p.client.PurgeExpiredEvents(ctx)
	if err != nil {
		if ctx.Err() == nil {
			logger.Error(err, "Event purge failed")
			metrics.RecordEventPurgeFailures(1)
		}
		return
	}
	metrics.RecordEventsPurged(purged)
	if purged > 0 {
		logger.V(logging.INFO).Info("Purged expired events", "purged", purged)
	}
}
