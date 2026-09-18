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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"gopkg.in/yaml.v3"
)

// Expectation is a scenario's entry in the ratchet manifest.
type Expectation string

const (
	// ExpectBroken means the bug exists: the scenario must reproduce its
	// invariant violation, proving the reproduction has not rotted.
	ExpectBroken Expectation = "broken"
	// ExpectFixed means the bug is fixed: the scenario must run with no
	// violations. A violation is a regression and fails CI.
	ExpectFixed Expectation = "fixed"
)

type ratchetManifest struct {
	Scenarios map[string]Expectation `yaml:"scenarios"`
}

// expectation loads the scenario's entry from ratchet.yaml. A scenario absent
// from the manifest is an error: every scenario must declare its state.
func expectation(t *testing.T, scenario string) Expectation {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	data, err := os.ReadFile(filepath.Join(filepath.Dir(thisFile), "ratchet.yaml"))
	if err != nil {
		t.Fatalf("read ratchet manifest: %v", err)
	}
	var manifest ratchetManifest
	if err := yaml.Unmarshal(data, &manifest); err != nil {
		t.Fatalf("parse ratchet manifest: %v", err)
	}
	exp, ok := manifest.Scenarios[scenario]
	if !ok {
		t.Fatalf("scenario %q missing from ratchet.yaml; add it as broken or fixed", scenario)
	}
	if exp != ExpectBroken && exp != ExpectFixed {
		t.Fatalf("scenario %q has invalid expectation %q", scenario, exp)
	}
	return exp
}

// judge applies the ratchet semantics to a scenario outcome. reproduced is
// whether the scenario observed the invariant violation it exists to detect;
// detail describes what was (or was not) observed.
func judge(t *testing.T, scenario string, reproduced bool, detail string) {
	t.Helper()
	switch exp := expectation(t, scenario); exp {
	case ExpectBroken:
		if !reproduced {
			t.Fatalf("scenario %s is marked broken but the violation did not reproduce (%s); "+
				"if the bug is fixed, promote it to fixed in ratchet.yaml", scenario, detail)
		}
		t.Logf("scenario %s: violation reproduced as expected (%s)", scenario, detail)
	case ExpectFixed:
		if reproduced {
			t.Fatalf("REGRESSION: scenario %s is marked fixed but the violation reproduced: %s", scenario, detail)
		}
		t.Logf("scenario %s: no violation, invariants hold (%s)", scenario, detail)
	default:
		t.Fatalf("unhandled expectation %q", fmt.Sprint(exp))
	}
}
