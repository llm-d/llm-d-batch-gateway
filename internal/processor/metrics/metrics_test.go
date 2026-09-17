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
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func withIsolatedPromRegistry(t *testing.T, fn func(reg *prometheus.Registry)) {
	t.Helper()
	oldReg, oldGather := prometheus.DefaultRegisterer, prometheus.DefaultGatherer
	reg := prometheus.NewRegistry()
	prometheus.DefaultRegisterer = reg
	prometheus.DefaultGatherer = reg
	t.Cleanup(func() {
		prometheus.DefaultRegisterer = oldReg
		prometheus.DefaultGatherer = oldGather
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

func labelNamesOf(m *dto.Metric) []string {
	if m == nil || len(m.Label) == 0 {
		return nil
	}
	names := make([]string, len(m.Label))
	for i, lp := range m.Label {
		names[i] = lp.GetName()
	}
	return names
}

func assertLabelNames(t *testing.T, mf *dto.MetricFamily, want []string) {
	t.Helper()
	if mf == nil || len(mf.Metric) == 0 {
		t.Fatal("empty metric family")
	}
	got := labelNamesOf(mf.Metric[0])
	if len(got) != len(want) {
		t.Fatalf("labels=%v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("labels=%v, want %v", got, want)
		}
	}
}

func gaugeValue(mf *dto.MetricFamily) float64 {
	if mf == nil || len(mf.Metric) != 1 {
		return -1
	}
	return mf.Metric[0].GetGauge().GetValue()
}

func counterWithLabels(mf *dto.MetricFamily, want map[string]string) float64 {
	if mf == nil {
		return -1
	}
outer:
	for _, m := range mf.Metric {
		for k, v := range want {
			var got string
			for _, lp := range m.Label {
				if lp.GetName() == k {
					got = lp.GetValue()
					break
				}
			}
			if got != v {
				continue outer
			}
		}
		return m.GetCounter().GetValue()
	}
	return -1
}

func gaugeWithLabel(mf *dto.MetricFamily, label, value string) float64 {
	if mf == nil {
		return -1
	}
	for _, m := range mf.Metric {
		for _, lp := range m.Label {
			if lp.GetName() == label && lp.GetValue() == value {
				return m.GetGauge().GetValue()
			}
		}
	}
	return -1
}

func TestGetSizeBucket(t *testing.T) {
	cases := []struct {
		lines int
		want  string
	}{
		{0, Bucket100},
		{99, Bucket100},
		{100, Bucket1000},
		{999, Bucket1000},
		{1000, Bucket10000},
		{9999, Bucket10000},
		{10000, Bucket30000},
		{29999, Bucket30000},
		{30000, BucketLarge},
		{999999, BucketLarge},
	}
	for _, tc := range cases {
		if got := GetSizeBucket(tc.lines); got != tc.want {
			t.Fatalf("GetSizeBucket(%d)=%q, want %q", tc.lines, got, tc.want)
		}
	}
}

func TestInitMetrics_AndRecorders(t *testing.T) {
	withIsolatedPromRegistry(t, func(reg *prometheus.Registry) {
		cfg := *config.NewConfig()
		cfg.NumWorkers = 20

		if err := InitMetrics(cfg); err != nil {
			t.Fatalf("InitMetrics: %v", err)
		}

		RecordJobProcessed(ResultSuccess, ReasonNone)
		RecordJobProcessed(ResultFailed, ReasonSystemError)
		RecordQueueWaitDuration(250 * time.Millisecond)
		RecordJobProcessingDuration(1500*time.Millisecond, Bucket1000)
		IncActiveWorkers()
		IncActiveWorkers()
		DecActiveWorkers()
		RecordRequestError("test")
		RecordRequestError("test")
		IncProcessorInflightRequests()
		IncProcessorInflightRequests()
		DecProcessorInflightRequests()
		RecordPlanBuildDuration(2*time.Second, Bucket1000)
		IncModelInflightRequests("modelA")
		IncModelInflightRequests("modelA")
		DecModelInflightRequests("modelA")
		RecordModelRequestExecutionDuration(300*time.Millisecond, "modelA")

		f := collectFamilies(t, reg)

		if v := gaugeValue(f["total_workers"]); v != 5 {
			t.Fatalf("total_workers=%v, want effective worker count 5", v)
		}
		if v := gaugeValue(f["active_workers"]); v != 1 {
			t.Fatalf("active_workers=%v, want 1", v)
		}
		if v := gaugeValue(f["processor_inflight_requests"]); v != 1 {
			t.Fatalf("processor_inflight_requests=%v, want 1", v)
		}
		if v := gaugeValue(f["processor_max_inflight_concurrency"]); v != float64(cfg.Concurrency.Global) {
			t.Fatalf("processor_max_inflight_concurrency=%v, want %v", v, cfg.Concurrency.Global)
		}

		if v := counterWithLabels(f["jobs_processed_total"], map[string]string{"result": ResultSuccess, "reason": ReasonNone}); v != 1 {
			t.Fatalf("jobs_processed success/none=%v, want 1", v)
		}
		if v := counterWithLabels(f["jobs_processed_total"], map[string]string{"result": ResultFailed, "reason": ReasonSystemError}); v != 1 {
			t.Fatalf("jobs_processed failed/system_error=%v, want 1", v)
		}
		if v := counterWithLabels(f["request_errors_by_model_total"], map[string]string{"model": "test"}); v != 2 {
			t.Fatalf("request_errors_by_model_total=%v, want 2", v)
		}
		if v := gaugeWithLabel(f["model_inflight_requests"], "model", "modelA"); v != 1 {
			t.Fatalf("model_inflight_requests{modelA}=%v, want 1", v)
		}

		// No high-cardinality tenant label on these histograms (regression guard).
		assertLabelNames(t, f["job_queue_wait_duration_seconds"], nil)
		assertLabelNames(t, f["job_processing_duration_seconds"], []string{"size_bucket"})
		assertLabelNames(t, f["plan_build_duration_seconds"], []string{"size_bucket"})

		for _, name := range []string{
			"job_queue_wait_duration_seconds",
			"job_processing_duration_seconds",
			"plan_build_duration_seconds",
			"model_request_execution_duration_seconds",
		} {
			mf := f[name]
			if mf == nil || len(mf.Metric) == 0 {
				t.Fatalf("%s: missing or empty", name)
			}
			if mf.Metric[0].GetHistogram().GetSampleCount() < 1 {
				t.Fatalf("%s: expected ≥1 observation", name)
			}
		}
	})
}

func histogramSampleCount(mf *dto.MetricFamily, labels map[string]string) uint64 {
	if mf == nil {
		return 0
	}
	for _, m := range mf.Metric {
		match := true
		for k, v := range labels {
			found := false
			for _, lp := range m.Label {
				if lp.GetName() == k && lp.GetValue() == v {
					found = true
					break
				}
			}
			if !found {
				match = false
				break
			}
		}
		if match {
			return m.GetHistogram().GetSampleCount()
		}
	}
	return 0
}

func TestTokenUsageMetrics(t *testing.T) {
	withIsolatedPromRegistry(t, func(reg *prometheus.Registry) {
		cfg := *config.NewConfig()
		if err := InitMetrics(cfg); err != nil {
			t.Fatalf("InitMetrics: %v", err)
		}

		RecordTokenUsage(100, 50, "gpt-4")
		RecordTokenUsage(200, 80, "gpt-4")
		RecordTokenUsage(50, 30, "llama-3")

		f := collectFamilies(t, reg)

		if v := counterWithLabels(f["batch_request_prompt_tokens_total"], map[string]string{"model": "gpt-4"}); v != 300 {
			t.Fatalf("prompt_tokens{gpt-4}=%v, want 300", v)
		}
		if v := counterWithLabels(f["batch_request_generation_tokens_total"], map[string]string{"model": "gpt-4"}); v != 130 {
			t.Fatalf("generation_tokens{gpt-4}=%v, want 130", v)
		}
		if v := counterWithLabels(f["batch_request_prompt_tokens_total"], map[string]string{"model": "llama-3"}); v != 50 {
			t.Fatalf("prompt_tokens{llama-3}=%v, want 50", v)
		}

		assertLabelNames(t, f["batch_request_prompt_tokens_total"], []string{"model"})
		assertLabelNames(t, f["batch_request_generation_tokens_total"], []string{"model"})
	})
}

func TestJobE2ELatencyMetric(t *testing.T) {
	withIsolatedPromRegistry(t, func(reg *prometheus.Registry) {
		cfg := *config.NewConfig()
		if err := InitMetrics(cfg); err != nil {
			t.Fatalf("InitMetrics: %v", err)
		}

		RecordJobE2ELatency(30*time.Second, "completed")
		RecordJobE2ELatency(10*time.Second, "cancelled")

		f := collectFamilies(t, reg)
		if c := histogramSampleCount(f["batch_job_e2e_latency_seconds"], map[string]string{"status": "completed"}); c != 1 {
			t.Fatalf("e2e_latency{completed} sample_count=%d, want 1", c)
		}
		if c := histogramSampleCount(f["batch_job_e2e_latency_seconds"], map[string]string{"status": "cancelled"}); c != 1 {
			t.Fatalf("e2e_latency{cancelled} sample_count=%d, want 1", c)
		}
		assertLabelNames(t, f["batch_job_e2e_latency_seconds"], []string{"status"})
	})
}

func TestCancellationMetric(t *testing.T) {
	withIsolatedPromRegistry(t, func(reg *prometheus.Registry) {
		cfg := *config.NewConfig()
		if err := InitMetrics(cfg); err != nil {
			t.Fatalf("InitMetrics: %v", err)
		}

		RecordCancellation(CancelPhaseQueued)
		RecordCancellation(CancelPhaseInProgress)
		RecordCancellation(CancelPhaseInProgress)
		RecordCancellation(CancelPhaseFinalizing)

		f := collectFamilies(t, reg)
		if v := counterWithLabels(f["batch_cancellation_total"], map[string]string{"phase": CancelPhaseQueued}); v != 1 {
			t.Fatalf("cancellation{queued}=%v, want 1", v)
		}
		if v := counterWithLabels(f["batch_cancellation_total"], map[string]string{"phase": CancelPhaseInProgress}); v != 2 {
			t.Fatalf("cancellation{in_progress}=%v, want 2", v)
		}
		if v := counterWithLabels(f["batch_cancellation_total"], map[string]string{"phase": CancelPhaseFinalizing}); v != 1 {
			t.Fatalf("cancellation{finalizing}=%v, want 1", v)
		}
		assertLabelNames(t, f["batch_cancellation_total"], []string{"phase"})
	})
}

func TestAIMDMetrics(t *testing.T) {
	withIsolatedPromRegistry(t, func(reg *prometheus.Registry) {
		cfg := *config.NewConfig()
		if err := InitMetrics(cfg); err != nil {
			t.Fatalf("InitMetrics: %v", err)
		}

		SetAIMDConcurrencyLimit("ep-a", 20)
		SetAIMDConcurrencyLimit("ep-b", 10)

		RecordAIMDDecrease("ep-a", AIMDSignal429)
		RecordAIMDDecrease("ep-a", AIMDSignal429)
		RecordAIMDDecrease("ep-a", AIMDSignal5xx)
		RecordAIMDDecrease("ep-b", AIMDSignalCapacityRetry)

		RecordAIMDIncrease("ep-a")
		RecordAIMDIncrease("ep-b")
		RecordAIMDIncrease("ep-b")

		SetAIMDConcurrencyLimit("ep-a", 15)

		f := collectFamilies(t, reg)

		if v := gaugeWithLabel(f["batch_processor_aimd_concurrency_limit"], "endpoint", "ep-a"); v != 15 {
			t.Fatalf("aimd_limit{ep-a}=%v, want 15", v)
		}
		if v := gaugeWithLabel(f["batch_processor_aimd_concurrency_limit"], "endpoint", "ep-b"); v != 10 {
			t.Fatalf("aimd_limit{ep-b}=%v, want 10", v)
		}

		if v := counterWithLabels(f["batch_processor_aimd_decreases_total"], map[string]string{"endpoint": "ep-a", "signal": AIMDSignal429}); v != 2 {
			t.Fatalf("aimd_decreases{ep-a,429}=%v, want 2", v)
		}
		if v := counterWithLabels(f["batch_processor_aimd_decreases_total"], map[string]string{"endpoint": "ep-a", "signal": AIMDSignal5xx}); v != 1 {
			t.Fatalf("aimd_decreases{ep-a,5xx}=%v, want 1", v)
		}
		if v := counterWithLabels(f["batch_processor_aimd_decreases_total"], map[string]string{"endpoint": "ep-b", "signal": AIMDSignalCapacityRetry}); v != 1 {
			t.Fatalf("aimd_decreases{ep-b,capacity_retry}=%v, want 1", v)
		}

		if v := counterWithLabels(f["batch_processor_aimd_increases_total"], map[string]string{"endpoint": "ep-a"}); v != 1 {
			t.Fatalf("aimd_increases{ep-a}=%v, want 1", v)
		}
		if v := counterWithLabels(f["batch_processor_aimd_increases_total"], map[string]string{"endpoint": "ep-b"}); v != 2 {
			t.Fatalf("aimd_increases{ep-b}=%v, want 2", v)
		}

		assertLabelNames(t, f["batch_processor_aimd_concurrency_limit"], []string{"endpoint"})
		assertLabelNames(t, f["batch_processor_aimd_decreases_total"], []string{"endpoint", "signal"})
		assertLabelNames(t, f["batch_processor_aimd_increases_total"], []string{"endpoint"})
	})
}

func TestInitMetrics_Twice_DoesNotError(t *testing.T) {
	withIsolatedPromRegistry(t, func(*prometheus.Registry) {
		cfg := *config.NewConfig()
		if err := InitMetrics(cfg); err != nil {
			t.Fatalf("first InitMetrics: %v", err)
		}
		if err := InitMetrics(cfg); err != nil {
			t.Fatalf("second InitMetrics: %v", err)
		}
	})
}
