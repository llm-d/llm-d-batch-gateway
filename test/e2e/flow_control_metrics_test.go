// Copyright 2026 The llm-d Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e_test

import "testing"

func TestEPPMetricFamilySelection(t *testing.T) {
	tests := []struct {
		name              string
		metrics           string
		wantPrefix        string
		wantDispatchMatch bool
		wantSaturation    bool
	}{
		{
			name: "router metrics",
			metrics: `llm_d_epp_flow_control_request_queue_duration_seconds_count{priority="-1",outcome="Dispatched"} 1
llm_d_epp_flow_control_pool_saturation{inference_pool="pool"} 1.2`,
			wantPrefix:        "llm_d_epp",
			wantDispatchMatch: true,
			wantSaturation:    true,
		},
		{
			name: "legacy GIE metrics",
			metrics: `inference_extension_flow_control_request_queue_duration_seconds_count{priority="-1",outcome="Dispatched"} 1
inference_extension_flow_control_pool_saturation{inference_pool="pool"} 1.2`,
			wantPrefix:        "inference_extension",
			wantDispatchMatch: true,
			wantSaturation:    true,
		},
		{
			name: "Router metrics take precedence",
			metrics: `llm_d_epp_flow_control_request_queue_duration_seconds_count{priority="-1",outcome="Dispatched"} 1
inference_extension_flow_control_request_queue_duration_seconds_count{priority="-1",outcome="Dispatched"} 2`,
			wantPrefix:        "llm_d_epp",
			wantDispatchMatch: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			family, matches := findEPPDispatchedCountMatches(tc.metrics)
			if tc.wantDispatchMatch != (len(matches) > 0) {
				t.Fatalf("dispatch metric match = %t, want %t", len(matches) > 0, tc.wantDispatchMatch)
			}
			if family.prefix != tc.wantPrefix {
				t.Errorf("metric family = %q, want %q", family.prefix, tc.wantPrefix)
			}

			_, saturationMatch := findEPPPoolSaturationMatch(tc.metrics)
			if saturationMatch != tc.wantSaturation {
				t.Errorf("saturation metric match = %t, want %t", saturationMatch, tc.wantSaturation)
			}
		})
	}
}
