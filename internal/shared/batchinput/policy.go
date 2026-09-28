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

package batchinput

import (
	"fmt"
	"sort"
	"sync"
)

// PolicyID is the stable, versioned identifier of an ordering policy. It is
// recorded alongside every stored batch input object and is the only thing
// that tells the processor how the bytes on disk are arranged.
//
// A policy ID is an immutable contract: changing how an existing ID orders
// lines would silently reinterpret objects already in storage. Ship a new ID
// (bump the version suffix) instead.
type PolicyID string

const (
	// PolicyModelPrefixV1 groups requests by model, then by system-prompt
	// prefix hash, so that consecutive dispatches are likely to hit the same
	// prefix cache on the inference backend.
	PolicyModelPrefixV1 PolicyID = "model-prefix-v1"

	// PolicyOriginalV1 preserves the order in which the client uploaded the
	// requests. It performs no grouping and exists both as an escape hatch and
	// as proof that the ordering contract is policy-agnostic.
	PolicyOriginalV1 PolicyID = "original-v1"
)

// DispatchOrder describes how a stored object must be consumed at dispatch
// time. It is derived from the policy that wrote the object, never from the
// consumer's own configuration.
type DispatchOrder string

const (
	// DispatchSequential means dispatch order equals stored byte order, so the
	// object can be streamed front-to-back in contiguous chunks.
	DispatchSequential DispatchOrder = "sequential"
)

// Entry describes one JSONL line located during an input scan. Offset and
// Length address the line in the source (uploaded) byte stream; Index is its
// zero-based position in that stream. Ordering policies rearrange entries,
// they never rewrite line content.
type Entry struct {
	Offset     int64
	Length     int32
	Index      int32
	ModelID    string
	PrefixHash uint32
}

// OrderPolicy decides the order in which scanned request lines are written to
// object storage.
//
// Implementations must be deterministic: the same entries must always produce
// the same order, so that a re-upload or a retried upload yields an identical
// object.
type OrderPolicy interface {
	// ID returns the identifier recorded with every object this policy writes.
	ID() PolicyID

	// DispatchOrder reports how a consumer must read objects written by this
	// policy. A consumer that does not recognise the returned value must fall
	// back to a self-describing read rather than guessing.
	DispatchOrder() DispatchOrder

	// Order sorts entries in place into dispatch order.
	Order(entries []Entry)
}

// modelPrefixV1 sorts by model, then system-prompt prefix hash, then original
// position. The final key makes ties deterministic, which keeps repeated
// uploads of the same content byte-identical.
type modelPrefixV1 struct{}

func (modelPrefixV1) ID() PolicyID                 { return PolicyModelPrefixV1 }
func (modelPrefixV1) DispatchOrder() DispatchOrder { return DispatchSequential }

func (modelPrefixV1) Order(entries []Entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		a, b := entries[i], entries[j]
		if a.ModelID != b.ModelID {
			return a.ModelID < b.ModelID
		}
		if a.PrefixHash != b.PrefixHash {
			return a.PrefixHash < b.PrefixHash
		}
		return a.Index < b.Index
	})
}

// originalV1 keeps the client's upload order.
type originalV1 struct{}

func (originalV1) ID() PolicyID                 { return PolicyOriginalV1 }
func (originalV1) DispatchOrder() DispatchOrder { return DispatchSequential }

func (originalV1) Order(entries []Entry) {
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Index < entries[j].Index
	})
}

var (
	registryMu sync.RWMutex
	registry   = map[PolicyID]OrderPolicy{}
)

func init() {
	MustRegister(modelPrefixV1{})
	MustRegister(originalV1{})
}

// Register adds a policy to the shared registry. Both the API server (to order
// uploads) and the processor (to decide whether it understands a stored
// object) resolve policies through this registry, so a policy that only one
// binary knows about is exactly the mismatch the fallback path handles.
func Register(p OrderPolicy) error {
	if p == nil {
		return fmt.Errorf("ordering policy is nil")
	}
	id := p.ID()
	if id == "" {
		return fmt.Errorf("ordering policy has an empty ID")
	}
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[id]; exists {
		return fmt.Errorf("ordering policy %q is already registered", id)
	}
	registry[id] = p
	return nil
}

// MustRegister is Register for package initialisation, where a duplicate ID is
// a programming error rather than a runtime condition.
func MustRegister(p OrderPolicy) {
	if err := Register(p); err != nil {
		panic(err)
	}
}

// Lookup returns the policy registered under id. A false result means this
// binary cannot reason about the layout of an object written under that ID.
func Lookup(id PolicyID) (OrderPolicy, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	p, ok := registry[id]
	return p, ok
}

// RegisteredPolicies returns the sorted IDs of all known policies. Intended
// for config validation and diagnostics.
func RegisteredPolicies() []PolicyID {
	registryMu.RLock()
	ids := make([]PolicyID, 0, len(registry))
	for id := range registry {
		ids = append(ids, id)
	}
	registryMu.RUnlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}
