package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/shared/syncutil"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

const defaultResultBuffer = 64

// BroadcasterGroup is a set of broadcasters for subscribe/unsubscribe.
// Lifecycle (Run/Wait) is owned by the registry that created them.
type BroadcasterGroup struct {
	broadcasters []*ResultBroadcaster
}

func NewBroadcasterGroup(broadcasters []*ResultBroadcaster) *BroadcasterGroup {
	return &BroadcasterGroup{broadcasters: broadcasters}
}

func (bs *BroadcasterGroup) Subscribe(ch chan<- ResultItem, owns func(string) bool) {
	for _, b := range bs.broadcasters {
		b.Subscribe(ch, owns)
	}
}

func (bs *BroadcasterGroup) Unsubscribe(ch chan<- ResultItem) {
	for _, b := range bs.broadcasters {
		b.Unsubscribe(ch)
	}
}

// ResultBroadcaster reads results from a shared async client and routes
// each one to the subscriber that owns its request.
type ResultBroadcaster struct {
	client      inference.AsyncInferenceClient
	subscribers *syncutil.MutexMap[chan<- ResultItem, func(string) bool]
	// residueSafe durably verifies an unrouted delivery before the
	// broadcaster acknowledges it (nil acknowledges unconditionally —
	// the plain broadcast shape).
	residueSafe func(ctx context.Context, requestID string) (bool, error)
	logger      logr.Logger
}

func NewResultBroadcaster(client inference.AsyncInferenceClient, logger logr.Logger) *ResultBroadcaster {
	return &ResultBroadcaster{
		client:      client,
		subscribers: syncutil.NewMutexMap[chan<- ResultItem, func(string) bool](),
		logger:      logger,
	}
}

// Subscribe registers dest to receive the results whose request it owns.
// owns reports whether a request ID belongs to the subscriber (its pending
// set); a nil owns accepts every result, the plain broadcast shape. dest is
// handed each matching result from Run's delivery loop: a dest that is not
// ready to receive delays result delivery for the whole route, so buffer it
// enough to ride out consumer bursts.
func (b *ResultBroadcaster) Subscribe(dest chan<- ResultItem, owns func(string) bool) {
	b.subscribers.Store(dest, owns)
}

// WithResidueSafetyCheck arms the durable verification for unrouted
// deliveries. safe reports whether a delivery may be acknowledged — its
// entry is terminal or absent, so no processor can still account for it.
// A delivery whose entry is still open is left unacknowledged: its lease
// redelivers it to whichever processor recovers its batch, so a completed
// result is never discarded just because no local pending set owns it.
// Without a check the broadcaster acknowledges residue unconditionally.
func (b *ResultBroadcaster) WithResidueSafetyCheck(safe func(ctx context.Context, requestID string) (bool, error)) *ResultBroadcaster {
	b.residueSafe = safe
	return b
}

// Unsubscribe removes dest from the subscriber list.
func (b *ResultBroadcaster) Unsubscribe(dest chan<- ResultItem) {
	b.subscribers.Delete(dest)
}

// receivedResult pairs one pulled result with the acknowledgement of its
// durable delivery (nil on destructive transports). The ack travels with the
// result so the consumer that accounts for it can acknowledge afterwards.
type receivedResult struct {
	resp *inference.GenerateResponse
	ack  func(ctx context.Context) error
}

// Run reads results and routes each one to the subscriber that owns its
// request, acknowledging unrouted residue itself.
func (b *ResultBroadcaster) Run(ctx context.Context) {
	incomingCh := make(chan receivedResult)

	go func() {
		defer close(incomingCh)
		backoff := 100 * time.Millisecond
		const maxBackoff = 10 * time.Second
		for ctx.Err() == nil {
			received, err := b.receive(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				b.logger.Error(err, "GetResult failed, retrying", "backoff", backoff)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return
				}
				backoff = min(backoff*2, maxBackoff)
				continue
			}
			backoff = 100 * time.Millisecond
			select {
			case incomingCh <- received:
			case <-ctx.Done():
				// Unrouted delivery: leave it unacked so lease expiry
				// redelivers it to whoever runs next.
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case received, ok := <-incomingCh:
			if !ok {
				return
			}
			result := asyncResult(received.resp, b.logger)
			result.Ack = received.ack

			// Per-request routing: only the subscriber that owns the request
			// receives the delivery, so the ack is released by exactly one
			// consumer — the owner, after its durable checkpoint. A request
			// can only be in flight after its job subscribed and stored it,
			// so an unrouted request belongs to no live job. The scan keeps
			// going past a match: during a takeover, a fenced-out zombie and
			// the new owner may both hold the request, and both must receive
			// it — only the owner whose fence holds can commit and ack.
			var targets []chan<- ResultItem
			b.subscribers.Range(func(ch chan<- ResultItem, owns func(string) bool) bool {
				if owns == nil || owns(result.RequestID) {
					targets = append(targets, ch)
				}
				return true
			})
			// Send after Range returns: it holds the subscribers lock for
			// the whole iteration, and a send blocked on a full channel must
			// not stall Subscribe and Unsubscribe for the other jobs sharing
			// the route. A send recovered from a closed-channel panic drops
			// the result while its target still matched, so the delivery
			// stays unacked and its lease redelivers it.
			for _, ch := range targets {
				b.safeChannelSend(result, ch)
			}
			if len(targets) > 0 {
				continue
			}
			// Residue: no live job owns this request — e.g. a late result for
			// a batch that terminalized while its request was in flight, or
			// residue from an owner that crashed. Acknowledge it only when a
			// durable check proves no entry can still account for it; the
			// destructive transport consumed it the same way.
			if received.ack != nil {
				safe := true
				if b.residueSafe != nil {
					var err error
					safe, err = b.residueSafe(ctx, result.RequestID)
					if err != nil {
						// Unverifiable is not safe: leave the delivery unacked so
						// its lease redelivers it once the check can run again.
						b.logger.Info("Residue safety check failed; leaving delivery for redelivery",
							"requestID", result.RequestID, "err", err)
						safe = false
					}
				}
				if safe {
					if err := received.ack(ctx); err != nil {
						b.logger.Info("Acknowledging unowned result failed; its lease will redeliver it",
							"requestID", result.RequestID, "err", err)
					}
				} else {
					b.logger.Info("Unrouted delivery held unacked; its entry is still open",
						"requestID", result.RequestID)
				}
			}
		}
	}
}

// receive pulls one result, preferring the durable leased delivery: the
// acknowledgement travels with the result so its consumer acknowledges only
// after durably accounting for it. Transports without the capability keep
// the destructive GetResult contract.
func (b *ResultBroadcaster) receive(ctx context.Context) (receivedResult, error) {
	if dc, ok := b.client.(inference.DurableResultClient); ok {
		resp, ack, err := dc.ReceiveResult(ctx)
		if err == nil || !errors.Is(err, inference.ErrDurableResultsUnavailable) {
			return receivedResult{resp: resp, ack: ack}, err
		}
	}
	resp, err := b.client.GetResult(ctx)
	return receivedResult{resp: resp}, err
}

// safeChannelSend sends to ch and recovers from the panic if the channel
// was closed concurrently (subscriber unsubscribed). The only operation in
// this function is a channel send, so the only possible panic is
// "send on closed channel".
func (b *ResultBroadcaster) safeChannelSend(result ResultItem, ch chan<- ResultItem) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && err.Error() == "send on closed channel" {
				b.logger.Info("Broadcast send recovered (subscriber unsubscribed)",
					"requestID", result.RequestID)
			} else {
				panic(r)
			}
		}
	}()
	ch <- result
}

func asyncResult(resp *inference.GenerateResponse, logger logr.Logger) ResultItem {
	result := ResultItem{
		RequestID: resp.RequestID,
	}

	if resp.IsNonHTTPFailure() {
		code := resp.ErrorCode
		if code == "" {
			code = "server_error"
		}
		msg := resp.ErrorMessage
		if msg == "" {
			msg = "async request failed"
		}
		result.Error = &OutputError{Code: code, Message: msg}
		return result
	}

	statusCode := resp.StatusCode
	if statusCode == 0 {
		// Legacy ResultMessage with only Payload — treat as HTTP 200.
		statusCode = 200
	}

	if len(resp.Response) == 0 {
		if statusCode >= 200 && statusCode < 300 {
			result.Error = &OutputError{Code: "server_error", Message: "async response has no body"}
			return result
		}
		// Non-2xx with empty body (e.g. 403 from auth): preserve status code.
		result.Response = &batch_types.ResponseData{
			StatusCode: statusCode,
			RequestID:  resp.RequestID,
			Body:       map[string]any{},
		}
		return result
	}

	var body map[string]any
	if err := json.Unmarshal(resp.Response, &body); err != nil {
		if statusCode >= 200 && statusCode < 300 {
			logger.Error(err, "Failed to unmarshal async response", "requestID", resp.RequestID)
			result.Error = &OutputError{
				Code:    "parse_error",
				Message: fmt.Sprintf("response body could not be parsed: %v", err),
			}
			return result
		}
		// Non-2xx with unparsable body: still preserve the HTTP status.
		body = map[string]any{
			"error": map[string]any{
				"message": string(resp.Response),
			},
		}
	}
	result.Response = &batch_types.ResponseData{
		StatusCode: statusCode,
		RequestID:  resp.RequestID,
		Body:       body,
	}
	return result
}
