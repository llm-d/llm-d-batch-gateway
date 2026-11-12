package pipeline

import (
	"context"
	"sync"
)

// PendingRequests tracks in-flight requests by RequestID.
// The dispatcher stores entries before dispatching; the collector
// resolves them when results arrive.
//
// Membership and the pending count are one and the same state: the count is
// the size of the entry map, and every mutation (Store, Resolve,
// DrainUnresolved) holds mu across both, so no interleaving can leave an
// entry the count does not account for. Wait therefore returns only when
// every stored request has been resolved or drained.
type PendingRequests struct {
	numRequests int64

	mu   sync.Mutex
	m    map[string]RequestItem
	done chan struct{}
}

func NewPendingRequests(numRequests int64) *PendingRequests {
	return &PendingRequests{
		numRequests: numRequests,
		m:           make(map[string]RequestItem),
		done:        make(chan struct{}, 1),
	}
}

// NumRequests returns the total number of requests in the job.
func (p *PendingRequests) NumRequests() int64 {
	return p.numRequests
}

// Store registers a request as pending an async result. Storing the same
// request ID twice keeps a single pending entry — recovery pre-stores an
// entry before the dispatcher stores it again at (re-)submission — and
// refreshes the stored item so a later store's metadata (e.g. SubmittedAt)
// wins.
//
// The insert/refresh and the pending count move under one lock, so a Store
// racing a Resolve for the same ID has exactly two outcomes: the Resolve
// removes the entry before this Store re-adds it (a fresh, counted entry) or
// after (no entry, no count). Unlike the previous split map+atomic design,
// the map can never hold an entry the count does not account for, so a
// concurrent Resolve can no longer drive Wait to return while other requests
// are still in flight.
func (p *PendingRequests) Store(msg RequestItem) {
	p.mu.Lock()
	p.m[msg.RequestID] = msg
	p.mu.Unlock()
}

// Has reports whether the request is still awaiting a result. It is the
// broadcaster's per-request routing predicate and is non-destructive: the
// entry is removed only when the result's checkpoint succeeded (Resolve),
// so a redelivery arriving between the enrichment and the checkpoint still
// routes to its owner instead of being consumed as residue.
func (p *PendingRequests) Has(requestID string) bool {
	p.mu.Lock()
	_, ok := p.m[requestID]
	p.mu.Unlock()
	return ok
}

// signalDone wakes Wait after the last entry left the map. Callers hold mu;
// the send is non-blocking and the buffered channel coalesces repeated
// signals, so a stale wakeup only causes Wait to re-check the count.
func (p *PendingRequests) signalDone() {
	select {
	case p.done <- struct{}{}:
	default:
	}
}

// Enrich attaches request metadata to a result while it stays pending.
// Returns the same acceptance verdict as Resolve: either the result already
// has metadata (cancels, inline errors) or it was found in the pending map.
// The durable pipeline calls this on arrival and Resolve only after the
// result's checkpoint succeeded, so a failed checkpoint leaves the request
// pending for a later resume.
func (p *PendingRequests) Enrich(result *ResultItem) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if result.Error != nil {
		if _, ok := p.m[result.RequestID]; ok {
			return true
		}
		return result.CustomID != ""
	}
	if result.CustomID != "" {
		return true
	}
	if msg, ok := p.m[result.RequestID]; ok {
		result.CustomID = msg.CustomID
		result.ModelID = msg.ModelID
		result.SubmittedAt = msg.SubmittedAt
		return true
	}
	return false
}

// Resolve enriches a result with request metadata. Returns true if the
// result is accepted: either it already has metadata (cancels, inline errors)
// or it was found in the pending map (async inference results).
// Returns false only for broadcast results that belong to another job.
//
// Removing the entry and updating the count are one transition under mu: a
// Resolve that drains the last entry signals Wait exactly when the map
// empties.
func (p *PendingRequests) Resolve(result *ResultItem) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	accepted := false
	switch {
	case result.Error != nil:
		if _, ok := p.m[result.RequestID]; ok {
			delete(p.m, result.RequestID)
			accepted = true
		} else {
			return result.CustomID != ""
		}
	case result.CustomID != "":
		// Inline errors and cancels carry their own metadata; the entry is
		// removed if present (a no-op otherwise).
		delete(p.m, result.RequestID)
		accepted = true
	default:
		if msg, ok := p.m[result.RequestID]; ok {
			delete(p.m, result.RequestID)
			result.CustomID = msg.CustomID
			result.ModelID = msg.ModelID
			result.SubmittedAt = msg.SubmittedAt
			accepted = true
		}
	}

	if accepted && len(p.m) == 0 {
		p.signalDone()
	}
	return accepted
}

// Wait blocks until all pending entries are resolved or ctx is cancelled.
func (p *PendingRequests) Wait(ctx context.Context) {
	for {
		p.mu.Lock()
		empty := len(p.m) == 0
		p.mu.Unlock()
		if empty {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-p.done:
		}
	}
}

// IDs returns the request IDs of all still-pending entries.
func (p *PendingRequests) IDs() []string {
	p.mu.Lock()
	ids := make([]string, 0, len(p.m))
	for id := range p.m {
		ids = append(ids, id)
	}
	p.mu.Unlock()
	return ids
}

// DrainUnresolved removes all remaining entries from the pending map and
// calls fn for each. Used after cancellation to emit error results for
// submitted-but-uncollected requests.
func (p *PendingRequests) DrainUnresolved(fn func(RequestItem)) {
	p.mu.Lock()
	remaining := make([]RequestItem, 0, len(p.m))
	for _, msg := range p.m {
		remaining = append(remaining, msg)
	}
	clear(p.m)
	drained := len(remaining) > 0
	p.mu.Unlock()

	if drained {
		p.signalDone()
	}
	// fn sends on the result channel; run it outside the lock so a full
	// channel cannot stall the broadcaster's per-request Has calls.
	for _, msg := range remaining {
		fn(msg)
	}
}

// pendingCount reports the number of unresolved entries. Exposed for tests
// that need to observe the count without draining it.
func (p *PendingRequests) pendingCount() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(len(p.m))
}
