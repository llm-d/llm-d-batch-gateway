// Copyright 2026 The llm-d Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package e2e_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestIsUnreachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	closedAddr := ln.Addr().String()
	ln.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	unready := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer unready.Close()

	tests := []struct {
		name string
		err  func() error
		want bool
	}{
		{
			name: "redis ping to closed port",
			err: func() error {
				client := redis.NewClient(&redis.Options{Addr: closedAddr, MaxRetries: -1})
				defer client.Close()
				return client.Ping(ctx).Err()
			},
			want: true,
		},
		{
			name: "http get to closed port",
			err: func() error {
				resp, err := http.Get("http://" + closedAddr + "/ready")
				if err == nil {
					resp.Body.Close()
				}
				return err
			},
			want: true,
		},
		{
			name: "dns failure",
			err: func() error {
				return &net.OpError{Op: "dial", Net: "tcp", Err: &net.DNSError{Err: "no such host", Name: "redis.invalid", IsNotFound: true}}
			},
			want: true,
		},
		{
			name: "reachable but unready",
			err: func() error {
				resp, err := http.Get(unready.URL + "/ready")
				if err == nil {
					resp.Body.Close()
				}
				return err
			},
			want: false,
		},
		{
			name: "connection reset after dial",
			err: func() error {
				return &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
			},
			want: false,
		},
		{
			name: "server error reply",
			err:  func() error { return errors.New("WRONGPASS invalid username-password pair") },
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUnreachable(tt.err()); got != tt.want {
				t.Fatalf("isUnreachable() = %v, want %v", got, tt.want)
			}
		})
	}
}
