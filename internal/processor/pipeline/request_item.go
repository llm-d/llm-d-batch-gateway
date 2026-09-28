package pipeline

import (
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
	RequestID string
	CustomID  string

	// ModelID is the routing key used to resolve a client. Under tenant-scoped
	// routing it is "<tenantID>/<model>", so it is an internal identifier and
	// must never reach customer-visible output.
	ModelID string

	// ModelName is the model exactly as the client wrote it, used for error
	// messages. Falls back to ModelID when unset.
	ModelName string

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
}

func (r *RequestItem) Canceled() *ResultItem {
	return r.Error("batch_cancelled", "request cancelled")
}

// ModelNotFound reports a request whose model resolves to no inference client.
// The message names the client's own model rather than the routing key, and
// matches the wording ingestion uses when it rejects the same condition, so
// the error file reads the same whichever path produced it.
func (r *RequestItem) ModelNotFound() *ResultItem {
	name := r.ModelName
	if name == "" {
		name = r.ModelID
	}
	return r.Error(inference.ErrCodeModelNotFound,
		fmt.Sprintf("model %q is not configured in any gateway", name))
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
