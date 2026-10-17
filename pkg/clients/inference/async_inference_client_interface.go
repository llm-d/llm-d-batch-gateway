package inference

import (
	"context"
	"errors"
)

// ErrDurableResultsUnavailable is returned by DurableResultClient.ReceiveResult
// when the underlying producer lacks the durable result capability. Callers
// fall back to the destructive GetResult contract.
var ErrDurableResultsUnavailable = errors.New("durable result delivery unavailable")

// ResultAck acknowledges one durable result delivery. It must be called only
// after the result is durably accounted for (checkpointed, or dropped as a
// duplicate whose earlier delivery was checkpointed): until the ack, lease
// expiry keeps the delivery eligible for redelivery.
type ResultAck func(ctx context.Context) error

// DurableResultClient is an additive capability of AsyncInferenceClient.
// Results arrive as leased, non-destructive deliveries that stay recoverable
// in the queue until acknowledged, so a crash between the delivery and the
// consumer's durable checkpoint redelivers the result instead of losing it —
// the completed inference is not repeated. The consumer must checkpoint the
// result and only then call the returned ack. Transports without the
// capability keep the destructive GetResult contract, reported by
// ReceiveResult as ErrDurableResultsUnavailable.
type DurableResultClient interface {
	// ReceiveResult claims one result delivery under a lease and returns it
	// together with its acknowledgement.
	ReceiveResult(ctx context.Context) (*GenerateResponse, ResultAck, error)
}

// AsyncInferenceClient defines the interface for non-blocking async dispatch.
type AsyncInferenceClient interface {
	Submit(ctx context.Context, req *GenerateRequest) *ClientError
	GetResult(ctx context.Context) (*GenerateResponse, error)
	// Cancel marks all still-pending submitted requests as cancelled in the
	// dispatcher (best-effort pre-dispatch). It does not unregister waiters;
	// callers should still Close after local drain.
	Cancel(ctx context.Context, ids []string) error
	Close() error
}
