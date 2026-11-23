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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-logr/logr/testr"
	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
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

// generateSelfSignedCert creates a temporary self-signed certificate and key
// suitable for a test TLS server on 127.0.0.1.
func generateSelfSignedCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	certDir := t.TempDir()
	certFile = filepath.Join(certDir, "tls.crt")
	keyFile = filepath.Join(certDir, "tls.key")

	certOut, err := os.Create(certFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		t.Fatal(err)
	}
	certOut.Close()

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	keyOut, err := os.Create(keyFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}); err != nil {
		t.Fatal(err)
	}
	keyOut.Close()

	return certFile, keyFile
}

// TestStartTLSWithIncompatibleCipherServesHTTP1Only exercises the full
// Server.Start() wiring: config through tlspkg.Resolve to the TLS handshake.
// An AES-256-GCM-only TLS 1.2 cipher list cannot serve HTTP/2 (Go's net/http
// requires an ECDHE AES-128-GCM suite), so the connection must negotiate
// HTTP/1.1.
func TestStartTLSWithIncompatibleCipherServesHTTP1Only(t *testing.T) {
	logger := testr.New(t)
	apiPort := freePort(t)
	obsPort := freePort(t)
	certFile, keyFile := generateSelfSignedCert(t)

	srv := &Server{
		logger: logger,
		config: &common.ServerConfig{
			Host:              "127.0.0.1",
			Port:              apiPort,
			ObservabilityPort: obsPort,
			SSLCertFile:       certFile,
			SSLKeyFile:        keyFile,
			TLSMinVersion:     "VersionTLS12",
			TLSCipherSuites:   "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
			TLSNextProtos:     "h2",
		},
		serverReady: &atomic.Bool{},
		apiHandler:  http.NewServeMux(),
		obsHandler:  http.NewServeMux(),
		clients:     &clientset.Clientset{},
	}

	parentCtx, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()

	startErr := make(chan error, 1)
	go func() {
		startErr <- srv.Start(parentCtx)
	}()

	for i := 0; i < 50; i++ {
		if srv.serverReady.Load() {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !srv.serverReady.Load() {
		t.Fatal("server did not become ready")
	}

	// Dial with a TLS client that offers both h2 and http/1.1 via ALPN.
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
			ForceAttemptHTTP2: true,
		},
	}
	resp, err := client.Get(fmt.Sprintf("https://127.0.0.1:%s/test", apiPort))
	if err != nil {
		t.Fatalf("TLS request failed: %v", err)
	}
	resp.Body.Close()

	if resp.Proto != "HTTP/1.1" {
		t.Fatalf("negotiated protocol = %s, want HTTP/1.1 (HTTP/2 requires an AES-128-GCM suite)", resp.Proto)
	}

	cancelParent()
	select {
	case err := <-startErr:
		if err != nil {
			t.Fatalf("Start() returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for Start() to return")
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
