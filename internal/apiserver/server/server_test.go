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

package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	tlspkg "github.com/llm-d/llm-d-batch-gateway/internal/tls"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
)

// freePort returns an available TCP port on localhost.
func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	return fmt.Sprintf("%d", port)
}

func TestServeTLSWithIncompatibleCipherDisablesHTTP2(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	tlsOpts, err := tlspkg.Resolve(
		testr.New(t),
		"VersionTLS12",
		"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
		"h2",
	)
	if err != nil {
		t.Fatal(err)
	}

	httpserver := &http.Server{
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{{}},
		},
	}
	defer httpserver.Close()
	for _, opt := range tlsOpts {
		opt(httpserver.TLSConfig)
	}
	if httpserver.TLSConfig != nil && !slices.Contains(httpserver.TLSConfig.NextProtos, "h2") {
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		httpserver.Protocols = protocols
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- httpserver.ServeTLS(ln, "", "")
	}()

	select {
	case err := <-serveDone:
		t.Fatalf("ServeTLS returned before shutdown: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := httpserver.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("ServeTLS() error = %v, want %v", err, http.ErrServerClosed)
	}
}

// TestGracefulShutdown_InFlightRequestCompletes verifies that cancelling the
// parent context (simulating SIGTERM) does NOT cancel in-flight request
// contexts, so handlers can finish gracefully before the server drains.
func TestGracefulShutdown_InFlightRequestCompletes(t *testing.T) {
	logger := testr.New(t)
	apiPort := freePort(t)
	obsPort := freePort(t)

	// A handler that blocks until we signal it, letting us cancel the
	// parent context while the request is in flight.
	reqStarted := make(chan struct{})
	var reqCtxErr error

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(reqStarted)

		// Wait for the test to cancel the parent context.
		// If BaseContext leaked the cancellable ctx, r.Context() would
		// be cancelled here.
		time.Sleep(100 * time.Millisecond)

		reqCtxErr = r.Context().Err()
		w.WriteHeader(http.StatusOK)
	})

	srv := &Server{
		logger: logger,
		config: &common.ServerConfig{
			Host:              "127.0.0.1",
			Port:              apiPort,
			ObservabilityPort: obsPort,
		},
		serverReady: &atomic.Bool{},
		apiHandler:  handler,
		obsHandler:  http.NewServeMux(),
		clients:     &clientset.Clientset{}, // all-nil is safe for Close()
	}

	parentCtx, cancelParent := context.WithCancel(
		logr.NewContext(context.Background(), logger),
	)

	startErr := make(chan error, 1)
	go func() {
		startErr <- srv.Start(parentCtx)
	}()

	// Wait for the server to be ready.
	for i := 0; i < 50; i++ {
		if srv.serverReady.Load() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.serverReady.Load() {
		t.Fatal("server did not become ready")
	}

	// Send a request in the background.
	respCh := make(chan *http.Response, 1)
	errCh := make(chan error, 1)
	go func() {
		resp, err := http.Get(fmt.Sprintf("http://127.0.0.1:%s/test", apiPort))
		if err != nil {
			errCh <- err
		} else {
			respCh <- resp
		}
	}()

	// Wait for handler to start, then simulate SIGTERM.
	<-reqStarted
	cancelParent()

	// Wait for the response.
	select {
	case resp := <-respCh:
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200, got %d", resp.StatusCode)
		}
	case err := <-errCh:
		t.Fatalf("request failed: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for response")
	}

	if reqCtxErr != nil {
		t.Fatalf("in-flight request context was cancelled during graceful shutdown: %v", reqCtxErr)
	}

	// Wait for Start() to return.
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Start() to return")
	}
}
