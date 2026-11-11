//go:build integration

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

// Package integration hosts feature- and client-level integration tests gated
// behind the `integration` build tag. This file holds small shared helpers used
// by tests that need to provision a containerized dependency on the host.
package integration

import (
	"os/exec"
)

// findContainerTool returns the first available container runtime ("docker" or
// "podman"), or "" if neither is on PATH. Mirrors the Makefile's
// CONTAINER_TOOL detection so the standalone integration tests pick the same
// tool a contributor's `make` invocation would. Hoisted here from
// inference_client_test.go so the GCS emulator provisioning can reuse it
// without duplicating the lookup.
func findContainerTool() string {
	for _, tool := range []string{"docker", "podman"} {
		if _, err := exec.LookPath(tool); err == nil {
			return tool
		}
	}
	return ""
}
