package pipeline

import (
	"context"
	"fmt"
	"time"

	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// RequestItem is a fully-parsed inference request ready for dispatch.
// If ParseError is set, the request could not be parsed and the dispatcher
// should convert it to an error result instead of dispatching.
// NOTE: parse-error handling could also be implemented as a filter dispatcher
// in the chain, keeping the core dispatchers unaware of parse errors.
type RequestItem struct {
	RequestID   string
	CustomID    string
	ModelID     string
	Endpoint    string
	Body        map[string]any
	Headers     map[string]string
	ParseError  *OutputError
	SubmittedAt time.Time
}

// ResultItem is the outcome of one inference request.
type ResultItem struct {
	RequestID        string
	CustomID         string
	ModelID          string
	Response         *batch_types.ResponseData
	Error            *OutputError
	HadCapacityRetry bool
	SubmittedAt      time.Time
	// Ack is non-nil for a leased Async result. The collector invokes it only
	// after the result is durably accepted.
	Ack func(context.Context) error
}

func (r *RequestItem) Canceled() *ResultItem {
	return r.Error("batch_cancelled", "request cancelled")
}

func (r *RequestItem) ModelNotFound() *ResultItem {
	return r.Error(inference.ErrCodeModelNotFound, fmt.Sprintf("model %q not configured", r.ModelID))
}

func (r *RequestItem) Error(code, message string) *ResultItem {
	return &ResultItem{
		RequestID:   r.RequestID,
		CustomID:    r.CustomID,
		ModelID:     r.ModelID,
		SubmittedAt: r.SubmittedAt,
		Error:       &OutputError{Code: code, Message: message},
	}
}

// OutputError is the error structure for JSONL output.
type OutputError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
