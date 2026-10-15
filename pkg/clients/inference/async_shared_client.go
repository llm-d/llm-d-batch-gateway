package inference

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	asyncapi "github.com/llm-d/llm-d-async/api"

	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"

	"github.com/llm-d/llm-d-async/producer"
)

// asyncSharedClient decouples submit from collect.
// GetResult reads directly from the producer — the external ResultBroadcaster
// handles routing to the correct job.
type asyncSharedClient struct {
	producer    producer.Producer
	pollTimeout time.Duration
	logger      logr.Logger
}

var _ AsyncInferenceClient = (*asyncSharedClient)(nil)

func newAsyncSharedClient(p producer.Producer, pollTimeout time.Duration, logger logr.Logger) *asyncSharedClient {
	return &asyncSharedClient{
		producer:    p,
		pollTimeout: pollTimeout,
		logger:      logger,
	}
}

func (c *asyncSharedClient) Submit(ctx context.Context, req *GenerateRequest) *ClientError {
	now := time.Now()
	deadline := now.Add(defaultDeadline)
	if dl, ok := ctx.Deadline(); ok {
		deadline = dl
	}

	metadata := make(map[string]string)
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(metadata))
	payload, err := json.Marshal(req.Params)
	if err != nil {
		return &ClientError{
			Category: httpclient.ErrCategoryInvalidReq,
			Message:  fmt.Sprintf("marshal async request parameters: %v", err),
			RawError: err,
		}
	}

	reqMsg := &asyncapi.RequestMessage{
		ID:       req.RequestID,
		Created:  now.Unix(),
		Deadline: deadline.Unix(),
		Payload:  payload,
		Headers:  req.Headers,
		Endpoint: req.Endpoint,
		Metadata: metadata,
	}

	if err := c.producer.SubmitRequest(ctx, reqMsg); err != nil {
		return &ClientError{
			Category: httpclient.ErrCategoryServer,
			Message:  fmt.Sprintf("submit async request: %v", err),
			RawError: err,
		}
	}

	c.logger.V(logging.TRACE).Info("Submitted async request", "requestID", req.RequestID)
	return nil
}

func (c *asyncSharedClient) GetResult(ctx context.Context) (*GenerateResponse, error) {
	pollCtx, pollCancel := context.WithTimeout(ctx, c.pollTimeout)
	defer pollCancel()

	result, err := c.producer.GetResult(pollCtx)
	if err != nil {
		return nil, err
	}

	return &GenerateResponse{
		RequestID:    result.ID,
		Response:     []byte(result.Payload),
		StatusCode:   result.StatusCode,
		ErrorCode:    result.ErrorCode,
		ErrorMessage: result.ErrorMessage,
	}, nil
}

func (c *asyncSharedClient) Cancel(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := c.producer.CancelRequests(ctx, ids); err != nil {
		return fmt.Errorf("cancel async requests: %w", err)
	}
	c.logger.V(logging.INFO).Info("Cancelled pending async requests", "count", len(ids))
	return nil
}

func (c *asyncSharedClient) Close() error {
	return nil
}
