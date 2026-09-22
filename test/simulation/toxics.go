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
	toxiproxy "github.com/Shopify/toxiproxy/v2/client"
)

// Proxy names from config/toxiproxy.json: one per component-store pair, so a
// toxic partitions exactly one edge of the topology.
const (
	proxyAPIServerPostgres = "apiserver-postgres"
	proxyAPIServerRedis    = "apiserver-redis"
	proxyAPIServerS3       = "apiserver-s3"
	proxyProcessorPostgres = "processor-postgres"
	proxyProcessorRedis    = "processor-redis"
	proxyProcessorS3       = "processor-s3"
	proxyGCPostgres        = "gc-postgres"
	proxyGCRedis           = "gc-redis"
	proxyGCS3              = "gc-s3"
)

// toxics returns a toxiproxy client for the active backend, skipping the
// scenario when it has no network fault injection. Cleanup removes every
// toxic so a failed scenario cannot poison the next.
func (h *harness) toxics() *toxiproxy.Client {
	h.t.Helper()
	addr, ok := h.b.toxiproxyAddr()
	if !ok {
		h.t.Skipf("backend %s has no toxiproxy; network-fault scenarios are compose-only", backendName())
	}
	client := toxiproxy.NewClient(addr)
	h.t.Cleanup(func() { h.healToxics(client) })
	return client
}

// blackholeResponses makes the store swallow responses on one proxy: requests
// are delivered and execute server-side, but the caller never hears back and
// times out. The false-failure primitive.
func (h *harness) blackholeResponses(client *toxiproxy.Client, proxyName string) {
	h.t.Helper()
	h.addToxic(client, proxyName, "timeout", "downstream", toxiproxy.Attributes{"timeout": 0})
}

// resetConnections RSTs both directions of one proxy immediately: every
// in-flight and new operation fails fast, a clean partition without waiting
// out client timeouts.
func (h *harness) resetConnections(client *toxiproxy.Client, proxyName string) {
	h.t.Helper()
	h.addToxic(client, proxyName, "reset_peer", "upstream", toxiproxy.Attributes{"timeout": 0})
	h.addToxic(client, proxyName, "reset_peer", "downstream", toxiproxy.Attributes{"timeout": 0})
}

func (h *harness) addToxic(client *toxiproxy.Client, proxyName, toxicType, stream string, attrs toxiproxy.Attributes) {
	h.t.Helper()
	proxy, err := client.Proxy(proxyName)
	if err != nil {
		h.t.Fatalf("look up proxy %s: %v", proxyName, err)
	}
	name := toxicType + "-" + stream
	if _, err := proxy.AddToxic(name, toxicType, stream, 1.0, attrs); err != nil {
		h.t.Fatalf("add toxic %s to %s: %v", name, proxyName, err)
	}
	h.rec.event("toxic-added", map[string]any{"proxy": proxyName, "toxic": name})
}

// healToxics removes every toxic from every proxy.
func (h *harness) healToxics(client *toxiproxy.Client) {
	h.t.Helper()
	proxies, err := client.Proxies()
	if err != nil {
		h.t.Fatalf("list proxies: %v", err)
	}
	for _, proxy := range proxies {
		toxics, err := proxy.Toxics()
		if err != nil {
			h.t.Fatalf("list toxics on %s: %v", proxy.Name, err)
		}
		for _, tox := range toxics {
			if err := proxy.RemoveToxic(tox.Name); err != nil {
				h.t.Fatalf("remove toxic %s from %s: %v", tox.Name, proxy.Name, err)
			}
			h.rec.event("toxic-removed", map[string]any{"proxy": proxy.Name, "toxic": tox.Name})
		}
	}
}

// inferenceWitness returns the number of requests the simulated engine has
// served, skipping the scenario when the backend has no request log.
func (h *harness) inferenceWitness() int {
	h.t.Helper()
	n, ok := h.b.inferenceRequests()
	if !ok {
		h.t.Skipf("backend %s has no inference request log; witness scenarios are compose-only", backendName())
	}
	return n
}
