package pipeline

import (
	"context"
	"errors"
	"time"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// AsyncDispatcher submits requests to async queues (fire-and-forget).
// Results arrive via per-model ResultBroadcasters (backed by shared
// clients) that send directly to resultCh.
type AsyncDispatcher struct {
	resolver     *inference.AsyncGatewayResolver
	broadcasters *BroadcasterGroup
	pending      *PendingRequests
	logger       logr.Logger
}

var _ RequestDispatcher = (*AsyncDispatcher)(nil)

func NewAsyncDispatcher(
	resolver *inference.AsyncGatewayResolver,
	broadcasters *BroadcasterGroup,
	pending *PendingRequests,
	logger logr.Logger,
) *AsyncDispatcher {
	return &AsyncDispatcher{
		resolver:     resolver,
		broadcasters: broadcasters,
		pending:      pending,
		logger:       logger,
	}
}

func (d *AsyncDispatcher) Run(ctx context.Context, requestCh <-chan RequestItem, resultCh chan<- ResultItem) error {
	d.broadcasters.Subscribe(resultCh)

	// Submit phase — fast queue writes.
	for msg := range requestCh {
		client := d.resolver.SharedClientFor(msg.ModelID)
		if client == nil {
			resultCh <- *msg.ModelNotFound()
			continue
		}

		d.pending.Store(msg)

		req := &inference.GenerateRequest{
			RequestID: msg.RequestID,
			Endpoint:  msg.Endpoint,
			Params:    msg.Body,
			Headers:   msg.Headers,
		}

		if submitErr := client.Submit(ctx, req); submitErr != nil {
			resultCh <- *msg.Error(
				string(submitErr.Category),
				submitErr.Message,
			)
			continue
		}
	}

	// Wait for all pending results to be resolved by the collector.
	d.pending.Wait(ctx)

	// Best-effort: tell the queue to drop still-pending requests before
	// dispatch. Use a detached context — ctx is often already cancelled.
	if ids := d.pending.IDs(); len(ids) > 0 {
		cancelCtx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
		d.cancelPending(cancelCtx, ids)
		cancelFn()
	}

	// Drain submitted-but-uncollected requests as errors so that
	// output_lines + error_lines == total_requests.
	code, message := cancelCode(ctx)
	d.pending.DrainUnresolved(func(msg RequestItem) {
		resultCh <- *msg.Error(code, message)
	})

	// Unsubscribe removes resultCh from the broadcast list. A concurrent
	// send from the broadcaster may still race with close(resultCh);
	// safeChannelSend recovers from the resulting panic.
	d.broadcasters.Unsubscribe(resultCh)
	close(resultCh)

	return nil
}

func cancelCode(ctx context.Context) (string, string) {
	cause := context.Cause(ctx)
	if errors.Is(cause, context.DeadlineExceeded) || errors.Is(cause, batchctx.ErrExpired) {
		return string(batch_types.ErrCodeBatchExpired), batch_types.ErrCodeBatchExpired.Message()
	}
	return string(batch_types.ErrCodeBatchCancelled), batch_types.ErrCodeBatchCancelled.Message()
}

// cancelPending calls Cancel on every shared client with all pending IDs.
// Different models may map to different producers, but CancelRequests
// silently ignores unknown IDs, so broadcasting to all is safe.
func (d *AsyncDispatcher) cancelPending(ctx context.Context, ids []string) {
	for _, modelID := range d.resolver.Models() {
		client := d.resolver.SharedClientFor(modelID)
		if client == nil {
			continue
		}
		if err := client.Cancel(ctx, ids); err != nil {
			d.logger.Error(err, "Failed to cancel pending async requests", "model", modelID)
		}
	}
}
