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

// The file provides in-memory mock implementations for BatchDBClient.
package mock

import (
	"context"
	"sync"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

// Compile-time check: MockBatchPriorityQueueClient implements api.BatchPriorityQueueClient.
var _ api.BatchPriorityQueueClient = (*MockBatchPriorityQueueClient)(nil)

type MockBatchPriorityQueueClient struct {
	mu    sync.Mutex
	queue []*api.BatchJobPriority

	// OnDelete is called by PQDelete to perform the job's DB transition. It
	// mirrors the Postgres PQDelete behavior of atomically transitioning the
	// job to cancelled in the DB. For jobs that are not in the in-memory
	// slice it is also the queue check, since Postgres models the queue as
	// DB rows rather than a separate queue structure.
	OnDelete func(ctx context.Context, id string) error

	// OnClaimOwned backs PQClaimOwned; nil means no owned jobs.
	OnClaimOwned func(ctx context.Context) ([]*api.BatchJobPriority, error)
}

func NewMockBatchPriorityQueueClient() *MockBatchPriorityQueueClient {
	return &MockBatchPriorityQueueClient{
		queue: make([]*api.BatchJobPriority, 0),
	}
}

func (m *MockBatchPriorityQueueClient) PQClaimOwned(ctx context.Context) ([]*api.BatchJobPriority, error) {
	if m.OnClaimOwned == nil {
		return nil, nil
	}
	return m.OnClaimOwned(ctx)
}

func (m *MockBatchPriorityQueueClient) PQEnqueue(ctx context.Context, jobPriority *api.BatchJobPriority) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Insert in sorted order by SLO (earlier SLO = higher priority)
	insertIdx := len(m.queue)
	for i, jp := range m.queue {
		if jobPriority.SLO.Before(jp.SLO) {
			insertIdx = i
			break
		}
	}

	// Insert at the correct position
	m.queue = append(m.queue, nil)
	copy(m.queue[insertIdx+1:], m.queue[insertIdx:])
	m.queue[insertIdx] = jobPriority

	return nil
}

// PQDequeue returns up to maxObjs tasks without blocking, mirroring the
// Postgres implementation's non-blocking claim. The lease parameter is
// irrelevant in memory: tests seed requeued resumable tasks directly.
func (m *MockBatchPriorityQueueClient) PQDequeue(ctx context.Context, _ time.Duration, maxObjs int) ([]*api.BatchJobPriority, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	count := min(maxObjs, len(m.queue))
	if count == 0 {
		return []*api.BatchJobPriority{}, nil
	}
	result := make([]*api.BatchJobPriority, count)
	copy(result, m.queue[:count])
	m.queue = m.queue[count:]
	return result, nil
}

func (m *MockBatchPriorityQueueClient) PQDelete(ctx context.Context, jobPriority *api.BatchJobPriority) (nDeleted int, err error) {
	m.mu.Lock()
	inQueue := false
	for i, jp := range m.queue {
		if jp.ID == jobPriority.ID {
			// Remove the item
			m.queue = append(m.queue[:i], m.queue[i+1:]...)
			inQueue = true
			break
		}
	}
	hasHook := m.OnDelete != nil
	m.mu.Unlock()

	if !inQueue && !hasHook {
		return 0, nil
	}

	// Postgres models the queue as DB rows, so a job that is not in the
	// in-memory slice may still be queued: let the DB transition hook decide.
	if hasHook {
		if err := m.OnDelete(ctx, jobPriority.ID); err != nil {
			return 0, err
		}
		return 1, nil
	}
	return 1, nil
}

func (m *MockBatchPriorityQueueClient) PQGetIDs(ctx context.Context) (map[string]bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ids := make(map[string]bool, len(m.queue))
	for _, jp := range m.queue {
		ids[jp.ID] = true
	}
	return ids, nil
}

func (m *MockBatchPriorityQueueClient) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}

func (m *MockBatchPriorityQueueClient) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.queue = nil
	return nil
}
