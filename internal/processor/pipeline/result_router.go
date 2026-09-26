package pipeline

import (
	"context"
	"encoding/json"
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

func (bs *BroadcasterGroup) Subscribe(ch chan<- ResultItem) {
	for _, b := range bs.broadcasters {
		b.Subscribe(ch)
	}
}

func (bs *BroadcasterGroup) Unsubscribe(ch chan<- ResultItem) {
	for _, b := range bs.broadcasters {
		b.Unsubscribe(ch)
	}
}

// ResultBroadcaster reads results from a shared async client and
// broadcasts to all subscribed channels.
type ResultBroadcaster struct {
	client      inference.AsyncInferenceClient
	subscribers *syncutil.MutexMap[chan<- ResultItem, struct{}]
	logger      logr.Logger
}

func NewResultBroadcaster(client inference.AsyncInferenceClient, logger logr.Logger) *ResultBroadcaster {
	return &ResultBroadcaster{
		client:      client,
		subscribers: syncutil.NewMutexMap[chan<- ResultItem, struct{}](),
		logger:      logger,
	}
}

// Subscribe registers dest to receive all results.
// dest should be buffered to avoid blocking the broadcaster.
func (b *ResultBroadcaster) Subscribe(dest chan<- ResultItem) {
	b.subscribers.Store(dest, struct{}{})
}

// Unsubscribe removes dest from the broadcast list.
func (b *ResultBroadcaster) Unsubscribe(dest chan<- ResultItem) {
	b.subscribers.Delete(dest)
}

// Run reads results and broadcasts to all subscribers.
func (b *ResultBroadcaster) Run(ctx context.Context) {
	type incomingResult struct {
		response *inference.GenerateResponse
		ack      func(context.Context) error
	}
	incomingCh := make(chan incomingResult)

	go func() {
		defer close(incomingCh)
		backoff := 100 * time.Millisecond
		const maxBackoff = 10 * time.Second
		for ctx.Err() == nil {
			var (
				resp *inference.GenerateResponse
				ack  func(context.Context) error
				err  error
			)
			if durable, ok := b.client.(inference.DurableAsyncInferenceClient); ok && durable.SupportsDurableResults() {
				var delivery *inference.DurableGenerateResult
				delivery, err = durable.ReceiveResult(ctx)
				if err == nil {
					resp = delivery.Response
					ack = delivery.Ack
				}
			} else {
				resp, err = b.client.GetResult(ctx)
			}
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
			case incomingCh <- incomingResult{response: resp, ack: ack}:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return

		case incoming, ok := <-incomingCh:
			if !ok {
				return
			}
			result := asyncResult(incoming.response, b.logger)
			result.Ack = incoming.ack

			for _, ch := range b.subscribers.Keys() {
				b.safeChannelSend(result, ch)
			}
		}
	}
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
