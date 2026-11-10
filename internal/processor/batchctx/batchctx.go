/*
Copyright 2026 The llm-d Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package batchctx defines the cancellation causes carried by a batch job's
// abort context and maps them back to routing sentinels.
//
// runJob drives every phase from a single context created with
// context.WithCancelCause. Each source records a distinct cause by calling the
// cancel func with a sentinel — first call wins, because cancelling a context
// propagates the cause down to its not-yet-cancelled descendants but never
// overwrites an already-cancelled one:
//
//	base := context.WithoutCancel(parent)
//	if !deadline.IsZero() {
//		base, stopDeadline = context.WithDeadline(base, deadline) // -> DeadlineExceeded
//	}
//	ctx, cause := context.WithCancelCause(base)
//	// user cancel:  cause(batchctx.ErrCancelled)
//	// SIGTERM:       cause(batchctx.ErrShutdown)
//	// reconciler:    cause(context.Canceled)   // neutral
//
// Read the outcome with batchctx.Cause(ctx).
package batchctx

import (
	"context"
	"errors"
)

// Cause sentinels carried by an abort context. Expiry is reported as the stdlib
// context.DeadlineExceeded (so it composes with context.WithDeadline and any
// existing deadline checks); Cause maps that to ErrExpired for routing.
var (
	// ErrCancelled signals a user-initiated batch job cancellation.
	ErrCancelled = errors.New("batch job cancelled")
	// ErrShutdown signals that the processor is shutting down (SIGTERM).
	ErrShutdown = errors.New("processor shutting down")
	// ErrExpired signals that the batch SLO deadline was reached.
	ErrExpired = errors.New("batch SLO expired")
)

// Cause maps ctx's cancellation cause to a routing sentinel, or nil if ctx was
// not cancelled for a known reason. A neutral context.Canceled (a reconciler
// abort or cleanup) maps to nil so it routes as a system error, not a terminal
// user/shutdown/expiry state.
func Cause(ctx context.Context) error {
	switch cause := context.Cause(ctx); {
	case errors.Is(cause, context.DeadlineExceeded), errors.Is(cause, ErrExpired):
		return ErrExpired
	case errors.Is(cause, ErrCancelled):
		return ErrCancelled
	case errors.Is(cause, ErrShutdown):
		return ErrShutdown
	default:
		return nil
	}
}

// IsTerminal reports whether err is one of the expected terminal routing
// sentinels (ErrExpired, ErrCancelled, ErrShutdown) rather than a system error.
// These represent normal end states of a batch job, so callers use it to skip
// error-level logging and span error recording.
func IsTerminal(err error) bool {
	return errors.Is(err, ErrExpired) ||
		errors.Is(err, ErrCancelled) ||
		errors.Is(err, ErrShutdown)
}
