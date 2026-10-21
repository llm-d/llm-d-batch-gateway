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
)

// Purger periodically removes expired events via the purge client.
type Purger struct {
	client   dbapi.BatchEventPurgeClient
	interval time.Duration
	onPurge  func(purged int64, err error)
}

// New creates a new event purger. The optional onPurge callback is invoked
// after every purge attempt with its result; a nil callback is allowed.
func New(client dbapi.BatchEventPurgeClient, interval time.Duration, onPurge func(purged int64, err error)) (*Purger, error) {
	if client == nil {
		return nil, fmt.Errorf("purge client is required")
	}
	if interval <= 0 {
		return nil, fmt.Errorf("interval must be positive, got %v", interval)
	}
	return &Purger{client: client, interval: interval, onPurge: onPurge}, nil
}

// RunLoop runs the purger in a continuous loop at the configured interval.
// It blocks until the context is cancelled, purging immediately on start
// and then on every tick of the interval.
func (p *Purger) RunLoop(ctx context.Context) error {
	logger := logr.FromContextOrDiscard(ctx)
	logger.Info("Starting event purge loop", "interval", p.interval)

	// Run immediately on startup before waiting for the first tick.
	if ctx.Err() == nil {
		p.runOnce(ctx)
	}

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

// runOnce executes a single purge and reports its result via onPurge.
// Outcomes racing context cancellation are neither logged nor reported:
// they are shutdown noise, not purge results.
func (p *Purger) runOnce(ctx context.Context) {
	logger := logr.FromContextOrDiscard(ctx)

	purged, err := p.client.PurgeExpiredEvents(ctx)
	if ctx.Err() != nil {
		return
	}
	if p.onPurge != nil {
		p.onPurge(purged, err)
	}
	if err != nil {
		logger.Error(err, "Event purge failed")
		return
	}
	if purged > 0 {
		logger.Info("Purged expired events", "purged", purged)
	}
}
