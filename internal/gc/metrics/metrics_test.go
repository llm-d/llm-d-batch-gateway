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

package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func withIsolatedPromRegistry(t *testing.T, fn func(reg *prometheus.Registry)) {
	t.Helper()
	oldReg, oldGather := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
	reg := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = reg
	prometheus.DefaultGatherer = reg
	initOnce = sync.Once{}
	initErr = nil
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = oldReg
		prometheus.DefaultGatherer = oldGather
		initOnce = sync.Once{}
		initErr = nil
	})
	fn(reg)
}

func collectFamilies(t *testing.T, reg *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		out[mf.GetName()] = mf
	}
	return out
}

func counterValue(mf *dto.MetricFamily) float64 {
	if mf == nil || len(mf.Metric) == 0 {
		return -1
	}
	return mf.Metric[0].GetCounter().GetValue()
}

func counterWithLabel(mf *dto.MetricFamily, label, value string) float64 {
	if mf == nil {
		return -1
	}
	for _, m := range mf.Metric {
		for _, lp := range m.Label {
			if lp.GetName() == label && lp.GetValue() == value {
				return m.GetCounter().GetValue()
			}
		}
	}
	return -1
}

func TestInitMetrics(t *testing.T) {
	t.Run("registers all metrics", func(t *testing.T) {
		withIsolatedPromRegistry(t, func(reg *prometheus.Registry) {
			if err := InitMetrics(); err != nil {
				t.Fatalf("InitMetrics: %v", err)
			}

			RecordOrphansRecovered(ActionCancelled, 3)
			RecordOrphansRecovered(ActionExpired, 1)
			RecordOrphansRecovered(ActionReEnqueued, 2)
			RecordOrphansRecovered(ActionFailed, 1)
			RecordCycleDuration(5 * time.Second)
			RecordCASConflicts(4)
			RecordStaleCleanup(2)
			RecordErrors(1)
			RecordEventsPurged(7)
			RecordEventPurgeFailures(2)

			f := collectFamilies(t, reg)

			if v := counterWithLabel(f["batch_reconciler_orphans_recovered_total"], "action", ActionCancelled); v != 3 {
				t.Fatalf("orphans_recovered{cancelled}=%v, want 3", v)
			}
			if v := counterWithLabel(f["batch_reconciler_orphans_recovered_total"], "action", ActionExpired); v != 1 {
				t.Fatalf("orphans_recovered{expired}=%v, want 1", v)
			}
			if v := counterWithLabel(f["batch_reconciler_orphans_recovered_total"], "action", ActionReEnqueued); v != 2 {
				t.Fatalf("orphans_recovered{re_enqueued}=%v, want 2", v)
			}
			if v := counterWithLabel(f["batch_reconciler_orphans_recovered_total"], "action", ActionFailed); v != 1 {
				t.Fatalf("orphans_recovered{failed}=%v, want 1", v)
			}

			mf := f["batch_reconciler_cycle_duration_seconds"]
			if mf == nil || len(mf.Metric) == 0 {
				t.Fatal("cycle_duration_seconds: missing")
			}
			if mf.Metric[0].GetHistogram().GetSampleCount() != 1 {
				t.Fatalf("cycle_duration_seconds sample_count=%d, want 1", mf.Metric[0].GetHistogram().GetSampleCount())
			}

			if v := counterValue(f["batch_reconciler_cas_conflicts_total"]); v != 4 {
				t.Fatalf("cas_conflicts=%v, want 4", v)
			}
			if v := counterValue(f["batch_reconciler_stale_cleanup_total"]); v != 2 {
				t.Fatalf("stale_cleanup=%v, want 2", v)
			}
			if v := counterValue(f["batch_reconciler_errors_total"]); v != 1 {
				t.Fatalf("errors=%v, want 1", v)
			}
			if v := counterValue(f["batch_gc_events_purged_total"]); v != 7 {
				t.Fatalf("events_purged=%v, want 7", v)
			}
			if v := counterValue(f["batch_gc_event_purge_failures_total"]); v != 2 {
				t.Fatalf("event_purge_failures=%v, want 2", v)
			}
		})
	})

	t.Run("zero counts are not recorded", func(t *testing.T) {
		withIsolatedPromRegistry(t, func(reg *prometheus.Registry) {
			if err := InitMetrics(); err != nil {
				t.Fatalf("InitMetrics: %v", err)
			}

			RecordOrphansRecovered(ActionCancelled, 0)
			RecordCASConflicts(0)
			RecordStaleCleanup(0)
			RecordErrors(0)
			RecordEventsPurged(0)
			RecordEventPurgeFailures(0)

			f := collectFamilies(t, reg)

			if _, ok := f["batch_reconciler_orphans_recovered_total"]; ok {
				t.Fatal("orphans_recovered should not appear for zero-count recording")
			}
			if v := counterValue(f["batch_reconciler_cas_conflicts_total"]); v != 0 {
				t.Fatalf("cas_conflicts=%v, want 0", v)
			}
		})
	})

	t.Run("double init does not error", func(t *testing.T) {
		withIsolatedPromRegistry(t, func(_ *prometheus.Registry) {
			if err := InitMetrics(); err != nil {
				t.Fatalf("first InitMetrics: %v", err)
			}
			if err := InitMetrics(); err != nil {
				t.Fatalf("second InitMetrics: %v", err)
			}
		})
	})
}
