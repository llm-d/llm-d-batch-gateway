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

package tls

import (
	"crypto/tls"
	"reflect"
	"testing"

	"github.com/go-logr/logr"
)

func TestResolveNextProtos(t *testing.T) {
	tests := []struct {
		name       string
		minVersion string
		cipher     string
		nextProtos string
		want       []string
	}{
		{
			name:       "TLS 1.2 AES-256 cipher excludes h2",
			minVersion: "VersionTLS12",
			cipher:     "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
			nextProtos: "h2",
			want:       []string{"http/1.1"},
		},
		{
			name:       "TLS 1.2 AES-128-GCM cipher keeps h2",
			minVersion: "VersionTLS12",
			cipher:     "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
			nextProtos: "h2,http/1.1",
			want:       []string{"h2", "http/1.1"},
		},
		{
			name:       "empty cipher list keeps h2",
			minVersion: "VersionTLS12",
			nextProtos: "h2,http/1.1",
			want:       []string{"h2", "http/1.1"},
		},
		{
			name:       "TLS 1.3 keeps h2",
			minVersion: "VersionTLS13",
			nextProtos: "h2,http/1.1",
			want:       []string{"h2", "http/1.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := Resolve(logr.Discard(), tt.minVersion, tt.cipher, tt.nextProtos)
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}

			config := new(tls.Config)
			for _, opt := range opts {
				opt(config)
			}
			if !reflect.DeepEqual(config.NextProtos, tt.want) {
				t.Errorf("NextProtos = %v, want %v", config.NextProtos, tt.want)
			}
		})
	}
}
