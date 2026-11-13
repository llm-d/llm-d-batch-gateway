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

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGroupFirstUserMessageConfig(t *testing.T) {
	if NewConfig().GroupFirstUserMessage {
		t.Fatal("grouping enabled by default")
	}
	for _, tc := range []struct {
		name, yaml string
		want       bool
	}{
		{"omitted", "num_workers: 2\n", false},
		{"false", "group_first_user_message: false\n", false},
		{"true", "group_first_user_message: true\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			cfg := NewConfig()
			if err := cfg.LoadFromYAML(path); err != nil {
				t.Fatal(err)
			}
			if cfg.GroupFirstUserMessage != tc.want {
				t.Fatalf("setting = %v, want %v", cfg.GroupFirstUserMessage, tc.want)
			}
		})
	}
}
