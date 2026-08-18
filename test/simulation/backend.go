//go:build simulation

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

package simulation

import (
	"os"
	"testing"
	"time"
)

// stackBackend runs the gateway topology for one scenario. Two
// implementations: compose (default, fast, docker compose + host-vcr
// fallback) and kind (a dev-deploy'd Kind cluster with real pod semantics
// and, with ENABLE_GIE=true, the EPP on the inference path).
//
// Scenario-facing env keys are the abstract knobs; each backend translates
// them: *_FAILPOINTS become the FAILPOINTS env of that component, and
// PROCESSOR_CONFIG selects the stale-heartbeat variant (a config file in
// compose, a helm value on kind).
type stackBackend interface {
	// ensureUp brings the stack to a fresh state with env applied.
	ensureUp(env map[string]string)
	// applyEnv records a changed knob; effective at the next restart of the
	// component it affects.
	applyEnv(key, value string)
	restart(service string)
	stop(service string)
	kill(service string)
	apiURL() string
	// harvest saves the scenario window's traces under dir.
	harvest(dir string, start time.Time)
	dumpLogs() string
}

// simParams are backend timing characteristics scenarios must scale to.
type simParams struct {
	// ReconcilerInterval is the orphan reconciler cycle (and staleness
	// threshold): 5s in compose configs, 30s in dev-deploy.
	ReconcilerInterval time.Duration
}

func backendName() string {
	if os.Getenv("SIM_BACKEND") == "kind" {
		return "kind"
	}
	return "compose"
}

func params() simParams {
	if backendName() == "kind" {
		return simParams{ReconcilerInterval: 30 * time.Second}
	}
	return simParams{ReconcilerInterval: 5 * time.Second}
}

func selectBackend(t *testing.T) stackBackend {
	t.Helper()
	if backendName() == "kind" {
		return newKindBackend(t)
	}
	return newComposeBackend(t)
}
