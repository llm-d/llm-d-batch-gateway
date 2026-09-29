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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"
)

const tempoBaseURL = "http://127.0.0.1:13200"

var traceServices = []string{"batch-apiserver", "batch-processor", "batch-gc"}

// harvest pulls every trace the gateway components exported during the
// scenario window out of Tempo and writes them under dir, so the run's
// component-side timing survives the stack teardown. Best-effort: a
// SIGKILLed process loses its unexported span batch, and that gap is itself
// part of the record.
func (b *composeBackend) harvest(dir string, start time.Time) {
	// Two lags to outwait: the SDK batcher's export schedule (compose sets
	// OTEL_BSP_SCHEDULE_DELAY=200) and Tempo's ingester making just-received
	// traces searchable, which takes several seconds.
	time.Sleep(8 * time.Second)

	if err := os.MkdirAll(dir, 0o755); err != nil {
		b.t.Logf("trace harvest: create dir: %v", err)
		return
	}

	seen := map[string]bool{}
	client := &http.Client{Timeout: 10 * time.Second}
	for _, svc := range traceServices {
		ids, err := searchTraceIDs(client, svc, start)
		if err != nil {
			b.t.Logf("trace harvest: search %s: %v", svc, err)
			continue
		}
		for _, id := range ids {
			if seen[id] {
				continue
			}
			seen[id] = true
			if err := fetchTrace(client, id, dir); err != nil {
				b.t.Logf("trace harvest: fetch %s: %v", id, err)
			}
		}
	}
	b.t.Logf("harvested %d traces into %s", len(seen), dir)
}

func searchTraceIDs(client *http.Client, service string, start time.Time) ([]string, error) {
	q := url.Values{
		"tags":  {"service.name=" + service},
		"start": {fmt.Sprint(start.Unix())},
		"end":   {fmt.Sprint(time.Now().Unix() + 60)},
		"limit": {"100"},
	}
	resp, err := client.Get(tempoBaseURL + "/api/search?" + q.Encode())
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, body)
	}
	var result struct {
		Traces []struct {
			TraceID string `json:"traceID"`
		} `json:"traces"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(result.Traces))
	for _, tr := range result.Traces {
		ids = append(ids, tr.TraceID)
	}
	return ids, nil
}

func fetchTrace(client *http.Client, id, dir string) error {
	resp, err := client.Get(tempoBaseURL + "/api/traces/" + id)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, id+".json"), data, 0o644)
}
