package inference

import "context"

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

// DurableGenerateResult keeps the Async lease opaque while allowing the
// consumer to checkpoint the translated response before acknowledgement.
type DurableGenerateResult struct {
	Response *GenerateResponse
	ack      func(context.Context) error
}

func (r *DurableGenerateResult) Ack(ctx context.Context) error {
	return r.ack(ctx)
}

// DurableAsyncInferenceClient is an additive capability backed by
// producer.DurableResultProducer in llm-d-async v0.10.0 and newer.
type DurableAsyncInferenceClient interface {
	AsyncInferenceClient
	SupportsDurableResults() bool
	ReceiveResult(ctx context.Context) (*DurableGenerateResult, error)
}
