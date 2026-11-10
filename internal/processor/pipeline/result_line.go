package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
)

// outputLine is the JSONL format for one result, matching the OpenAI batch output schema.
type outputLine struct {
	ID       string                    `json:"id"`
	CustomID string                    `json:"custom_id"`
	Response *batch_types.ResponseData `json:"response"`
	Error    *OutputError              `json:"error"`
}

// Succeeded reports whether a result is a success line: no error object and
// an HTTP 2xx response. It matches the outputLine classification: the
// resumable artifact contract treats non-2xx responses as error responses,
// so every successful status — not only 200 — counts as completed.
func (r *ResultItem) Succeeded() bool {
	return r.Error == nil && r.Response != nil &&
		r.Response.StatusCode >= 200 && r.Response.StatusCode <= 299
}

// ErrorCode classifies a non-success result for error reporting: the output
// error's code when present, otherwise the HTTP status.
func (r *ResultItem) ErrorCode() string {
	if r.Error != nil {
		return r.Error.Code
	}
	if r.Response != nil {
		return fmt.Sprintf("http_%d", r.Response.StatusCode)
	}
	return "unknown"
}

// MarshalResultLine renders one result as an output JSONL line (with its
// trailing newline) and returns the line bytes, its SHA-256 hex digest, and
// its size in bytes — the metadata the durable pipeline checkpoints per entry.
func MarshalResultLine(msg *ResultItem) ([]byte, string, int64, error) {
	line := &outputLine{
		ID:       msg.RequestID,
		CustomID: msg.CustomID,
		Response: msg.Response,
		Error:    msg.Error,
	}
	lineBytes, err := json.Marshal(line)
	if err != nil {
		return nil, "", 0, fmt.Errorf("marshal output for %s: %w", msg.RequestID, err)
	}
	lineBytes = append(lineBytes, '\n')
	sum := sha256.Sum256(lineBytes)
	return lineBytes, hex.EncodeToString(sum[:]), int64(len(lineBytes)), nil
}
