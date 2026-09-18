//go:build !failpoints

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

// Package failpoint provides named fault-injection points for the consistency
// simulation harness (docs/design/consistency-harness.md). Without the
// "failpoints" build tag every injection is an empty function that the
// compiler inlines away, so production builds carry no cost and no behavior.
package failpoint

// Inject evaluates the named failpoint. No-op unless built with -tags failpoints.
func Inject(name string) {}
