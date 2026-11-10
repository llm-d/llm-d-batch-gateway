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

package postgresql

import (
	"sync"
	"testing"
	"time"
)

// requeuedResumableBatch activates a batch and requeues it the way the GC
// reconciler does, returning the requeued row's epoch.
func (env *resumablePgEnv) requeuedResumableBatch(t *testing.T, id string) int64 {
	t.Helper()
	epoch := env.activate(t, id)
	if err := env.store.RequeueResumableOrphan(env.ctx, id, testProcessorID, epoch, false, true); err != nil {
		t.Fatalf("RequeueResumableOrphan: %v", err)
	}
	return epoch + 1
}

func TestPQDequeueUnifiedPG(t *testing.T) {
	env := newResumablePgEnv(t)

	t.Run("claims a requeued resumable row with a fresh lease", func(t *testing.T) {
		const id = "batch-dequeue-requeued"
		epoch := env.requeuedResumableBatch(t, id)

		jobs, err := env.queue.PQDequeue(env.ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("PQDequeue: %v", err)
		}
		if len(jobs) != 1 || jobs[0].ID != id {
			t.Fatalf("expected to claim %s, got %+v", id, jobs)
		}
		job := jobs[0]
		if !job.Resumable || job.ExpectedStatus != "in_progress" {
			t.Fatalf("requeued task not flagged: %+v", job)
		}
		if job.Epoch != epoch+1 || job.ProcessorID != testProcessorID {
			t.Fatalf("unexpected claim: %+v", job)
		}

		var processor *string
		var rowEpoch, attempts int64
		var lease *time.Time
		if err := env.pool.QueryRow(env.ctx,
			`SELECT processor_id, epoch, recovery_attempts, owner_lease_expires_at
			 FROM batch_items WHERE id = $1`, id,
		).Scan(&processor, &rowEpoch, &attempts, &lease); err != nil {
			t.Fatalf("read row: %v", err)
		}
		if processor == nil || *processor != testProcessorID {
			t.Fatalf("ownership not taken: processor=%v", processor)
		}
		if rowEpoch != epoch+1 {
			t.Fatalf("epoch not bumped: got %d want %d", rowEpoch, epoch+1)
		}
		if attempts != 1 {
			t.Fatalf("requeued claim must not reset recovery_attempts, got %d", attempts)
		}
		if lease == nil || !lease.After(time.Now()) {
			t.Fatalf("lease not set in the future: %v", lease)
		}
	})

	t.Run("claims a fresh row without a resumable lease", func(t *testing.T) {
		const id = "batch-dequeue-fresh"
		env.seedValidatingBatch(t, id)
		if _, err := env.pool.Exec(env.ctx,
			`UPDATE batch_items SET recovery_attempts = 2 WHERE id = $1`, id,
		); err != nil {
			t.Fatalf("seed attempts: %v", err)
		}

		jobs, err := env.queue.PQDequeue(env.ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("PQDequeue: %v", err)
		}
		if len(jobs) != 1 || jobs[0].ID != id {
			t.Fatalf("expected to claim %s, got %+v", id, jobs)
		}
		if jobs[0].Resumable || jobs[0].ExpectedStatus != "validating" {
			t.Fatalf("fresh task misclassified: %+v", jobs[0])
		}

		var lease *time.Time
		var attempts int64
		if err := env.pool.QueryRow(env.ctx,
			`SELECT owner_lease_expires_at, recovery_attempts FROM batch_items WHERE id = $1`, id,
		).Scan(&lease, &attempts); err != nil {
			t.Fatalf("read row: %v", err)
		}
		if lease != nil {
			t.Fatalf("fresh claim must not set a resumable lease: lease=%v", lease)
		}
		if attempts != 0 {
			t.Fatalf("fresh claim must reset recovery_attempts, got %d", attempts)
		}
	})

	t.Run("does not claim requeued rows without a lease offer", func(t *testing.T) {
		const id = "batch-dequeue-no-lease"
		epoch := env.requeuedResumableBatch(t, id)

		jobs, err := env.queue.PQDequeue(env.ctx, 0, 10)
		if err != nil {
			t.Fatalf("PQDequeue: %v", err)
		}
		if len(jobs) != 0 {
			t.Fatalf("zero lease must not claim requeued rows, got %+v", jobs)
		}

		var processor *string
		var rowEpoch int64
		if err := env.pool.QueryRow(env.ctx,
			`SELECT processor_id, epoch FROM batch_items WHERE id = $1`, id,
		).Scan(&processor, &rowEpoch); err != nil {
			t.Fatalf("read row: %v", err)
		}
		if processor != nil || rowEpoch != epoch {
			t.Fatalf("row must stay requeued: processor=%v epoch=%d", processor, rowEpoch)
		}
	})

	t.Run("does not claim terminal resumable rows", func(t *testing.T) {
		const id = "batch-dequeue-terminal"
		env.requeuedResumableBatch(t, id)
		if _, err := env.pool.Exec(env.ctx,
			`UPDATE batch_items SET status = jsonb_set(status, '{status}', '"failed"') WHERE id = $1`, id,
		); err != nil {
			t.Fatalf("seed terminal status: %v", err)
		}

		// Earlier subtests may leave claimable rows behind; only this
		// subtest's terminal row must not be claimed.
		jobs, err := env.queue.PQDequeue(env.ctx, 5*time.Minute, 10)
		if err != nil {
			t.Fatalf("PQDequeue: %v", err)
		}
		for _, job := range jobs {
			if job.ID == id {
				t.Fatalf("terminal row %s was claimed", id)
			}
		}
	})

	t.Run("concurrent dequeues never double-claim", func(t *testing.T) {
		queueB, err := NewPostgresBatchQueueClient(env.ctx, &PostgreSQLConfig{Url: env.url}, "processor-b")
		if err != nil {
			t.Fatalf("NewPostgresBatchQueueClient: %v", err)
		}
		t.Cleanup(func() { _ = queueB.Close() })

		const perClass = 6
		var wantIDs []string
		// Create the requeued rows first: the activation helper claims its
		// row through a fresh-only dequeue that would swallow any unclaimed
		// fresh rows seeded alongside it.
		for i := range perClass {
			requeuedID := "batch-race-requeued-" + string(rune('a'+i))
			env.requeuedResumableBatch(t, requeuedID)
			wantIDs = append(wantIDs, requeuedID)
		}
		for i := range perClass {
			freshID := "batch-race-fresh-" + string(rune('a'+i))
			env.seedValidatingBatch(t, freshID)
			wantIDs = append(wantIDs, freshID)
		}

		claims := make(chan string, 2*perClass)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for _, q := range []*PostgresBatchQueueClient{env.queue, queueB} {
			wg.Add(1)
			go func(q *PostgresBatchQueueClient) {
				defer wg.Done()
				<-start
				for {
					jobs, err := q.PQDequeue(env.ctx, time.Minute, 3)
					if err != nil {
						t.Errorf("PQDequeue: %v", err)
						return
					}
					if len(jobs) == 0 {
						return
					}
					for _, job := range jobs {
						claims <- job.ID
					}
				}
			}(q)
		}
		close(start)
		wg.Wait()
		close(claims)

		seen := make(map[string]int, 2*perClass)
		for id := range claims {
			seen[id]++
		}
		if len(seen) != 2*perClass {
			t.Fatalf("expected %d distinct claims, got %d: %v", 2*perClass, len(seen), seen)
		}
		for _, id := range wantIDs {
			if seen[id] != 1 {
				t.Fatalf("job %s claimed %d times, want exactly 1", id, seen[id])
			}
		}
	})
}
