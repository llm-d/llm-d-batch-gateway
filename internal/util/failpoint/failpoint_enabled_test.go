//go:build failpoints

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

package failpoint

import (
	"testing"
	"time"
)

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		spec string
		want map[string]action
	}{
		{
			name: "empty spec",
			spec: "",
			want: map[string]action{},
		},
		{
			name: "bare exit",
			spec: "a/b=exit",
			want: map[string]action{"a/b": {kind: "exit", value: 137}},
		},
		{
			name: "exit with code",
			spec: "a/b=exit(1)",
			want: map[string]action{"a/b": {kind: "exit", value: 1}},
		},
		{
			name: "sleep",
			spec: "a/b=sleep(250)",
			want: map[string]action{"a/b": {kind: "sleep", value: 250}},
		},
		{
			name: "multiple entries with whitespace",
			spec: " a/b=exit ; c/d=sleep(10) ",
			want: map[string]action{
				"a/b": {kind: "exit", value: 137},
				"c/d": {kind: "sleep", value: 10},
			},
		},
		{
			name: "malformed action preserved as invalid",
			spec: "a/b=explode;c/d=sleep(x);e/f",
			want: map[string]action{
				"a/b": {invalid: "explode"},
				"c/d": {invalid: "sleep(x)"},
				"e/f": {invalid: "e/f"},
			},
		},
		{
			name: "negative sleep is invalid",
			spec: "a/b=sleep(-5)",
			want: map[string]action{"a/b": {invalid: "sleep(-5)"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parse(tt.spec)
			if len(got) != len(tt.want) {
				t.Fatalf("parse(%q) = %v, want %v", tt.spec, got, tt.want)
			}
			for name, want := range tt.want {
				if got[name] != want {
					t.Errorf("parse(%q)[%q] = %v, want %v", tt.spec, name, got[name], want)
				}
			}
		})
	}
}

func TestInject(t *testing.T) {
	tests := []struct {
		name        string
		point       string
		minDuration time.Duration
	}{
		{name: "unarmed point is a no-op", point: "not/armed", minDuration: 0},
		{name: "sleep blocks the caller", point: "test/sleep", minDuration: 100 * time.Millisecond},
		{name: "malformed action is ignored", point: "test/bad", minDuration: 0},
	}
	t.Setenv("FAILPOINTS", "test/sleep=sleep(100);test/bad=explode")
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Now()
			Inject(tt.point)
			if elapsed := time.Since(start); elapsed < tt.minDuration {
				t.Errorf("Inject(%q) returned after %v, want at least %v", tt.point, elapsed, tt.minDuration)
			}
		})
	}
}
