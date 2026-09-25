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

package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
)

// --- Priority Queue Spy ---

type pqSpy struct {
	inner *mockdb.MockBatchPriorityQueueClient

	enqueueCalled int
	deleteCalled  int
	dequeueErr    error
}

func (p *pqSpy) PQEnqueue(ctx context.Context, jobPriority *db.BatchJobPriority) error {
	p.enqueueCalled++
	return p.inner.PQEnqueue(ctx, jobPriority)
}

func (p *pqSpy) PQDequeue(ctx context.Context, timeout time.Duration, maxObjs int) ([]*db.BatchJobPriority, error) {
	if p.dequeueErr != nil {
		return nil, p.dequeueErr
	}
	return p.inner.PQDequeue(ctx, timeout, maxObjs)
}

func (p *pqSpy) PQDelete(ctx context.Context, jobPriority *db.BatchJobPriority) (int, error) {
	p.deleteCalled++
	return p.inner.PQDelete(ctx, jobPriority)
}

func (p *pqSpy) PQClaimOwned(context.Context) ([]*db.BatchJobPriority, error) { return nil, nil }

func (p *pqSpy) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}

func (p *pqSpy) PQGetIDs(ctx context.Context) (map[string]bool, error) {
	return p.inner.PQGetIDs(ctx)
}

func (p *pqSpy) Close() error {
	return p.inner.Close()
}

// --- DB wrapper that forces DBGet to return an error (for transient DB error path) ---

type dbGetErrWrapper struct {
	inner *mockdb.MockDBClient[db.BatchItem, db.BatchQuery]
	err   error
}

func (d *dbGetErrWrapper) DBStore(ctx context.Context, job *db.BatchItem) error {
	return d.inner.DBStore(ctx, job)
}

func (d *dbGetErrWrapper) DBGet(ctx context.Context, query *db.BatchQuery, includeStatic bool, start, limit int) (
	[]*db.BatchItem, int, bool, error,
) {
	return nil, 0, false, d.err
}

func (d *dbGetErrWrapper) DBUpdate(ctx context.Context, job *db.BatchItem, expectedStatus []byte) error {
	return d.inner.DBUpdate(ctx, job, expectedStatus)
}

func (d *dbGetErrWrapper) DBDelete(ctx context.Context, IDs []string) ([]string, error) {
	return d.inner.DBDelete(ctx, IDs)
}

func (d *dbGetErrWrapper) DBUpdateProgress(ctx context.Context, id string, epoch int64, counts db.BatchRequestCounts) error {
	return d.inner.DBUpdateProgress(ctx, id, epoch, counts)
}

func (d *dbGetErrWrapper) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}

func (d *dbGetErrWrapper) Close() error {
	return d.inner.Close()
}

// tests
func TestPoller_DequeueOne_EmptyQueue_ReturnsNil(t *testing.T) {
	ctx := context.Background()

	pq := &pqSpy{inner: mockdb.NewMockBatchPriorityQueueClient()}
	dbClient := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)

	p := NewPoller(pq, dbClient)

	task, err := p.dequeueOne(ctx)
	if err != nil {
		t.Fatalf("DequeueOne() err=%v, want nil", err)
	}
	if task != nil {
		t.Fatalf("DequeueOne() task=%v, want nil", task)
	}
}

func TestPoller_DequeueOne_PQError_ReturnsError(t *testing.T) {
	ctx := context.Background()

	pq := &pqSpy{
		inner:      mockdb.NewMockBatchPriorityQueueClient(),
		dequeueErr: errors.New("pq down"),
	}
	dbClient := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)

	p := NewPoller(pq, dbClient)

	task, err := p.dequeueOne(ctx)
	if err == nil {
		t.Fatalf("DequeueOne() err=nil, want error")
	}
	if task != nil {
		t.Fatalf("DequeueOne() task=%v, want nil", task)
	}
}

func TestPoller_DequeueOne_ReturnsFirstTask(t *testing.T) {
	ctx := context.Background()

	pq := &pqSpy{inner: mockdb.NewMockBatchPriorityQueueClient()}
	dbClient := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)

	// put one item into PQ
	wantID := "job-1"
	if err := pq.PQEnqueue(ctx, &db.BatchJobPriority{
		ID:  wantID,
		SLO: time.Now().Add(1 * time.Hour),
	}); err != nil {
		t.Fatalf("PQEnqueue() err=%v", err)
	}

	p := NewPoller(pq, dbClient)

	task, err := p.dequeueOne(ctx)
	if err != nil {
		t.Fatalf("DequeueOne() err=%v, want nil", err)
	}
	if task == nil {
		t.Fatalf("DequeueOne() task=nil, want non-nil")
		return
	}
	if task.ID != wantID {
		t.Fatalf("DequeueOne() id=%q, want %q", task.ID, wantID)
	}
}

func TestPoller_EnqueueOne_NilTask_ReturnsErrorWithoutCallingPQ(t *testing.T) {
	ctx := context.Background()
	pq := &pqSpy{inner: mockdb.NewMockBatchPriorityQueueClient()}
	dbClient := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)
	p := NewPoller(pq, dbClient)

	err := p.enqueueOne(ctx, nil)
	if err == nil {
		t.Fatal("enqueueOne(nil) err=nil, want error")
	}
	if pq.enqueueCalled != 0 {
		t.Fatalf("PQEnqueue called=%d, want 0", pq.enqueueCalled)
	}
}

func TestPoller_FetchJobItem_DBError_ReturnsErrorWithoutEnqueue(t *testing.T) {
	ctx := context.Background()

	pq := &pqSpy{inner: mockdb.NewMockBatchPriorityQueueClient()}
	innerDB := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)

	dbErr := errors.New("db temporary error")
	dbClient := &dbGetErrWrapper{
		inner: innerDB,
		err:   dbErr,
	}

	p := NewPoller(pq, dbClient)

	task := &db.BatchJobPriority{
		ID:  "job-err",
		SLO: time.Now().Add(1 * time.Hour),
	}

	jobItem, err := p.fetchJobItemByID(ctx, task.ID)
	if err == nil {
		t.Fatalf("FetchJobItemByID() err=nil, want error")
	}
	if !errors.Is(err, dbErr) {
		t.Fatalf("FetchJobItemByID() err=%v, want %v", err, dbErr)
	}
	if jobItem != nil {
		t.Fatalf("FetchJobItemByID() jobItem=%v, want nil", jobItem)
	}
	if pq.enqueueCalled != 0 {
		t.Fatalf("PQEnqueue called=%d, want 0", pq.enqueueCalled)
	}
}

func TestPoller_FetchJobItem_DBInconsistency_NoReenqueueNoDelete_ReturnsNilNil(t *testing.T) {
	ctx := context.Background()

	pq := &pqSpy{inner: mockdb.NewMockBatchPriorityQueueClient()}
	dbClient := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)

	p := NewPoller(pq, dbClient)

	// task exists but DB has no item => data inconsistency
	task := &db.BatchJobPriority{
		ID:  "job-missing",
		SLO: time.Now().Add(1 * time.Hour),
	}

	jobItem, err := p.fetchJobItemByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("FetchJobItemByID() err=%v, want nil", err)
	}
	if jobItem != nil {
		t.Fatalf("FetchJobItemByID() jobItem=%v, want nil", jobItem)
	}
}

func TestPoller_FetchJobItem_Found_ReturnsJobItem(t *testing.T) {
	ctx := context.Background()

	pq := &pqSpy{inner: mockdb.NewMockBatchPriorityQueueClient()}
	dbClient := mockdb.NewMockDBClient(
		func(b *db.BatchItem) string { return b.ID },
		func(q *db.BatchQuery) *db.BaseQuery { return &q.BaseQuery },
	)

	p := NewPoller(pq, dbClient)

	task := &db.BatchJobPriority{
		ID:  "job-ok",
		SLO: time.Now().Add(1 * time.Hour),
	}

	// Seed DB with the job item
	seed := &db.BatchItem{
		BaseIndexes: db.BaseIndexes{
			ID:       task.ID,
			TenantID: "tenantA",
			Tags:     map[string]string{},
		},
	}
	if err := dbClient.DBStore(ctx, seed); err != nil {
		t.Fatalf("DBStore() err=%v", err)
	}

	jobItem, err := p.fetchJobItemByID(ctx, task.ID)
	if err != nil {
		t.Fatalf("FetchJobItemByID() err=%v, want nil", err)
	}
	if jobItem == nil {
		t.Fatalf("FetchJobItemByID() jobItem=nil, want non-nil")
		return
	}
	if jobItem.ID != task.ID {
		t.Fatalf("FetchJobItemByID() id=%q, want %q", jobItem.ID, task.ID)
	}
	// no re-enqueue / delete expected
	if pq.enqueueCalled != 0 {
		t.Fatalf("PQEnqueue called=%d, want 0", pq.enqueueCalled)
	}
	if pq.deleteCalled != 0 {
		t.Fatalf("PQDelete called=%d, want 0", pq.deleteCalled)
	}
}
