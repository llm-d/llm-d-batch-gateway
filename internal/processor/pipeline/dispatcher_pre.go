package pipeline

import (
	"context"
	"errors"
	"sync"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/metrics"
)

// PreDispatcher performs shared pre-dispatch logic before forwarding
// requests to the next dispatcher in the chain. It handles:
//   - context cancellation and draining remaining requests
//   - filtering requests with ParseError
//   - the ownership fence (beforeSubmit), when installed
//   - stamping SubmittedAt
//   - incrementing inflight metrics
type PreDispatcher struct {
	next RequestDispatcher
	// beforeSubmit, when set, runs immediately before each request is
	// forwarded. It returns false when the request must be dropped without
	// dispatching (a duplicate delivery whose result is already terminal).
	// A db.ErrConflict return means the submitter no longer owns the batch:
	// the dispatcher stops forwarding (but keeps draining) so a zombie does
	// not stream the remaining requests into inference. Any other error is
	// routed to the per-request error path like a submit failure.
	beforeSubmit func(msg *RequestItem) (bool, error)
}

var _ RequestDispatcher = (*PreDispatcher)(nil)

func NewPreDispatcher(next RequestDispatcher) *PreDispatcher {
	return &PreDispatcher{next: next}
}

// WithBeforeSubmit installs the pre-dispatch ownership fence (see
// PreDispatcher.beforeSubmit).
func (d *PreDispatcher) WithBeforeSubmit(fn func(msg *RequestItem) (bool, error)) *PreDispatcher {
	d.beforeSubmit = fn
	return d
}

func (d *PreDispatcher) Run(ctx context.Context, requestCh <-chan RequestItem, resultCh chan<- ResultItem) error {
	delegateRequestCh := make(chan RequestItem)
	delegateResultCh := make(chan ResultItem)

	var innerWg sync.WaitGroup
	var innerErr error
	innerWg.Add(1)
	go func() {
		defer innerWg.Done()
		innerErr = d.next.Run(ctx, delegateRequestCh, delegateResultCh)
	}()

	var resultWg sync.WaitGroup
	resultWg.Add(1)
	go func() {
		defer resultWg.Done()
		for result := range delegateResultCh {
			resultCh <- result
		}
	}()

	var lostOwnership bool
	for msg := range requestCh {
		if ctx.Err() != nil {
			resultCh <- *msg.Error(cancelCode(ctx))
			break
		}

		if msg.ParseError != nil {
			resultCh <- *msg.Error(msg.ParseError.Code, msg.ParseError.Message)
			continue
		}

		if d.beforeSubmit != nil {
			submit, err := d.beforeSubmit(&msg)
			if err != nil {
				if errors.Is(err, db.ErrConflict) {
					lostOwnership = true
					break
				}
				resultCh <- *msg.Error("server_error", err.Error())
				continue
			}
			if !submit {
				// Duplicate delivery: the entry is already terminal, so its
				// result is checkpointed; drop without dispatching.
				continue
			}
		}

		msg.SubmittedAt = time.Now()
		metrics.IncProcessorInflightRequests()
		metrics.IncModelInflightRequests(msg.ModelID)

		delegateRequestCh <- msg
	}

	if lostOwnership {
		// Drain the remainder without emitting results: the new owner
		// re-runs these requests, so checkpointing cancel errors here would
		// commit failures the batch never incurred.
		for range requestCh {
		}
	} else {
		for msg := range requestCh {
			resultCh <- *msg.Error(cancelCode(ctx))
		}
	}

	close(delegateRequestCh)

	innerWg.Wait()
	resultWg.Wait()
	close(resultCh)
	return innerErr
}
