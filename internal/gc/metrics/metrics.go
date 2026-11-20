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

// Package metrics provides Prometheus instrumentation for the batch garbage collector.
package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	initOnce              sync.Once
	initErr               error
	orphansRecoveredTotal *prometheus.CounterVec
	cycleDuration         prometheus.Histogram
	casConflictsTotal     prometheus.Counter
	staleCleanupTotal     prometheus.Counter
	errorsTotal           prometheus.Counter
	eventsPurgedTotal     prometheus.Counter
	eventPurgeFailures    prometheus.Counter
)

// InitMetrics creates and registers all batch-gc Prometheus metrics.
// It is safe to call multiple times; only the first call has effect.
func InitMetrics() error {
	initOnce.Do(func() {
		initErr = initMetrics()
	})
	return initErr
}

func initMetrics() error {
	orphansRecoveredTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "batch_reconciler_orphans_recovered_total",
			Help: "Orphans recovered by action type",
		},
		[]string{"action"},
	)

	cycleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "batch_reconciler_cycle_duration_seconds",
			Help:    "Time taken per reconciliation cycle",
			Buckets: prometheus.ExponentialBuckets(0.1, 3, 10),
		},
	)

	casConflictsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "batch_reconciler_cas_conflicts_total",
			Help: "CAS conflicts (another actor won the race)",
		},
	)

	staleCleanupTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "batch_reconciler_stale_cleanup_total",
			Help: "Stale in-flight entries cleaned up",
		},
	)

	errorsTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "batch_reconciler_errors_total",
			Help: "Errors encountered during a reconciliation cycle",
		},
	)

	eventPurgeFailures = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "batch_gc_event_purge_failures_total",
			Help: "Failed event purge attempts",
		},
	)

	eventsPurgedTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "batch_gc_events_purged_total",
			Help: "Expired events removed from the batch events table",
		},
	)

	for _, c := range []prometheus.Collector{
		orphansRecoveredTotal,
		cycleDuration,
		casConflictsTotal,
		staleCleanupTotal,
		errorsTotal,
		eventsPurgedTotal,
		eventPurgeFailures,
	} {
		if err := prometheus.Register(c); err != nil {
			return err
		}
	}

	return nil
}

// Action labels for orphans_recovered_total.
const (
	ActionCancelled  = "cancelled"
	ActionExpired    = "expired"
	ActionReEnqueued = "re_enqueued"
	ActionFailed     = "failed"
)

// RecordOrphansRecovered increments the orphan recovery counter for the given action.
func RecordOrphansRecovered(action string, count int) {
	if count > 0 {
		orphansRecoveredTotal.WithLabelValues(action).Add(float64(count))
	}
}

// RecordCycleDuration observes the duration of a reconciliation cycle.
func RecordCycleDuration(d time.Duration) {
	cycleDuration.Observe(d.Seconds())
}

// RecordCASConflicts adds the given count to the CAS conflicts counter.
func RecordCASConflicts(count int) {
	if count > 0 {
		casConflictsTotal.Add(float64(count))
	}
}

// RecordStaleCleanup adds the given count to the stale cleanup counter.
func RecordStaleCleanup(count int) {
	if count > 0 {
		staleCleanupTotal.Add(float64(count))
	}
}

// RecordErrors adds the given count to the errors counter.
func RecordErrors(count int) {
	if count > 0 {
		errorsTotal.Add(float64(count))
	}
}

// RecordEventsPurged adds the given count to the events purged counter.
func RecordEventsPurged(count int64) {
	if count > 0 {
		eventsPurgedTotal.Add(float64(count))
	}
}

// RecordEventPurgeFailures adds the given count to the event purge failures counter.
func RecordEventPurgeFailures(count int) {
	if count > 0 {
		eventPurgeFailures.Add(float64(count))
	}
}
