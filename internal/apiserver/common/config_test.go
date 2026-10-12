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

// The file contains unit tests for the configuration.
package common

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestAPIServerConfig(t *testing.T) {
	t.Run("LoadFromYAML", func(t *testing.T) {
		tests := []struct {
			name       string
			yamlConfig string
			fileName   string
			want       ServerConfig
			wantErr    bool
		}{
			{
				name: "valid yaml config",
				yamlConfig: `
host: 0.0.0.0
port: "8080"
ssl_cert_file: testdata/cert.pem
ssl_key_file: testdata/key.pem
db_client:
  type: "mock"
file_client:
  type: "fs"
  fs_base_path: "/tmp/batch-gateway"
`,
				fileName: "config.yaml",
				want: ServerConfig{
					Host:        "0.0.0.0",
					Port:        "8080",
					SSLCertFile: "testdata/cert.pem",
					SSLKeyFile:  "testdata/key.pem",
				},
				wantErr: false,
			},
			{
				name: "valid yml extension",
				yamlConfig: `
host: 127.0.0.1
port: "9000"
db_client:
  type: "mock"
file_client:
  type: "fs"
  fs_base_path: "/tmp/batch-gateway"
`,
				fileName: "config.yml",
				want: ServerConfig{
					Host: "127.0.0.1",
					Port: "9000",
				},
				wantErr: false,
			},
			{
				name: "yaml config without ssl",
				yamlConfig: `
host: localhost
port: "3000"
db_client:
  type: "mock"
file_client:
  type: "fs"
  fs_base_path: "/tmp/batch-gateway"
`,
				fileName: "config.yaml",
				want: ServerConfig{
					Host: "localhost",
					Port: "3000",
				},
				wantErr: false,
			},
			{
				name:       "invalid yaml",
				yamlConfig: `invalid: yaml: syntax: error`,
				fileName:   "config.yaml",
				wantErr:    true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				// Create temporary YAML config file
				tmpDir := t.TempDir()
				configFile := filepath.Join(tmpDir, tt.fileName)
				yamlConfig := tt.yamlConfig
				want := tt.want

				// Keep TLS fixtures isolated to this subtest. Relative shared files
				// can be removed by another test invocation while this test runs.
				if tt.want.SSLCertFile != "" {
					certFile, keyFile := setupTestSSLFiles(t, tmpDir)
					yamlConfig = strings.ReplaceAll(yamlConfig, "testdata/cert.pem", certFile)
					yamlConfig = strings.ReplaceAll(yamlConfig, "testdata/key.pem", keyFile)
					want.SSLCertFile = certFile
					want.SSLKeyFile = keyFile
				}

				if err := os.WriteFile(configFile, []byte(yamlConfig), 0644); err != nil {
					t.Fatalf("Failed to create test config file: %v", err)
				}

				// Save original os.Args and restore after test
				oldArgs := os.Args
				defer func() { os.Args = oldArgs }()

				// Set args with config file
				os.Args = []string{"test", "--config=" + configFile}

				config := NewConfig()
				err := config.Load()

				if (err != nil) != tt.wantErr {
					cwd, _ := os.Getwd()
					t.Errorf("Load() error = %v, wantErr %v (cwd: %s, configFile: %s)", err, tt.wantErr, cwd, configFile)
					return
				}

				if !tt.wantErr {
					if config.Host != tt.want.Host {
						t.Errorf("Host = %v, want %v", config.Host, tt.want.Host)
					}
					if config.Port != tt.want.Port {
						t.Errorf("Port = %v, want %v", config.Port, tt.want.Port)
					}
					if config.SSLCertFile != want.SSLCertFile {
						t.Errorf("SSLCertFile = %v, want %v", config.SSLCertFile, want.SSLCertFile)
					}
					if config.SSLKeyFile != want.SSLKeyFile {
						t.Errorf("SSLKeyFile = %v, want %v", config.SSLKeyFile, want.SSLKeyFile)
					}
				}
			})
		}
	})

	t.Run("LoadNegative", func(t *testing.T) {
		t.Run("MissingConfigFile", func(t *testing.T) {
			// Save original os.Args and restore after test
			oldArgs := os.Args
			defer func() { os.Args = oldArgs }()

			// Set args without config file
			os.Args = []string{"test"}

			config := NewConfig()
			err := config.Load()

			if err == nil {
				t.Error("Load() expected error for missing config file, got nil")
			}
		})

		t.Run("UnsupportedFormat", func(t *testing.T) {
			// Create temporary config file with unsupported extension
			tmpDir := t.TempDir()
			configFile := filepath.Join(tmpDir, "config.toml")
			if err := os.WriteFile(configFile, []byte("host = \"localhost\""), 0644); err != nil {
				t.Fatalf("Failed to create test config file: %v", err)
			}

			// Save original os.Args and restore after test
			oldArgs := os.Args
			defer func() { os.Args = oldArgs }()

			// Set args with config file
			os.Args = []string{"test", "--config=" + configFile}

			config := NewConfig()
			err := config.Load()

			if err == nil {
				t.Error("Load() expected error for unsupported format, got nil")
			}
		})

		t.Run("FileNotFound", func(t *testing.T) {
			// Save original os.Args and restore after test
			oldArgs := os.Args
			defer func() { os.Args = oldArgs }()

			// Set args with non-existent config file
			os.Args = []string{"test", "--config=/nonexistent/config.yaml"}

			config := NewConfig()
			err := config.Load()

			if err == nil {
				t.Error("Load() expected error for non-existent file, got nil")
			}
		})

	})
}

func TestBatchAPIExtraEndpoints(t *testing.T) {
	t.Run("valid", func(t *testing.T) {
		cfg := ServerConfig{Port: "8080", BatchAPI: BatchAPIConfig{ExtraEndpoints: []string{"/v1/classify", "/v1/pooling"}}}
		if err := cfg.Validate(); err != nil {
			t.Fatalf("Validate() unexpected error: %v", err)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		cfg := ServerConfig{Port: "8080", BatchAPI: BatchAPIConfig{ExtraEndpoints: []string{"/v1/classify?mode=test"}}}
		err := cfg.Validate()
		if err == nil {
			t.Fatal("Validate() expected error")
		}
		if !strings.Contains(err.Error(), "batch_api.extra_endpoints") {
			t.Errorf("Validate() error = %q, want configuration field context", err)
		}
	})

	t.Run("yaml", func(t *testing.T) {
		var cfg ServerConfig
		err := yaml.Unmarshal([]byte("batch_api:\n  extra_endpoints:\n    - /v1/classify\n    - /v1/pooling\n"), &cfg)
		if err != nil {
			t.Fatalf("yaml.Unmarshal() unexpected error: %v", err)
		}
		want := []string{"/v1/classify", "/v1/pooling"}
		if !reflect.DeepEqual(cfg.BatchAPI.ExtraEndpoints, want) {
			t.Errorf("ExtraEndpoints = %v, want %v", cfg.BatchAPI.ExtraEndpoints, want)
		}
	})
}

func TestGetInputHeader(t *testing.T) {
	tests := []struct {
		name     string
		headers  map[string]string
		key      string
		fallback string
		want     string
	}{
		{
			name:     "present key returns value",
			headers:  map[string]string{"tenant": "X-Custom-Tenant"},
			key:      "tenant",
			fallback: "X-Default",
			want:     "X-Custom-Tenant",
		},
		{
			name:     "missing key returns fallback",
			headers:  map[string]string{"other": "X-Other"},
			key:      "tenant",
			fallback: "X-Default",
			want:     "X-Default",
		},
		{
			name:     "empty string value returns fallback",
			headers:  map[string]string{"tenant": ""},
			key:      "tenant",
			fallback: "X-Default",
			want:     "X-Default",
		},
		{
			name:     "nil map returns fallback",
			headers:  nil,
			key:      "tenant",
			fallback: "X-Default",
			want:     "X-Default",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &ServerConfig{InputHeaders: tt.headers}
			got := cfg.GetInputHeader(tt.key, tt.fallback)
			if got != tt.want {
				t.Errorf("GetInputHeader(%q, %q) = %q, want %q", tt.key, tt.fallback, got, tt.want)
			}
		})
	}
}

func TestGetTenantHeader(t *testing.T) {
	tests := []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{
			name:    "custom tenant header",
			headers: map[string]string{"tenant": "X-My-Tenant"},
			want:    "X-My-Tenant",
		},
		{
			name:    "nil map falls back to default",
			headers: nil,
			want:    DefaultTenantHeader,
		},
		{
			name:    "empty tenant falls back to default",
			headers: map[string]string{"tenant": ""},
			want:    DefaultTenantHeader,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := &ServerConfig{InputHeaders: tt.headers}
			got := cfg.GetTenantHeader()
			if got != tt.want {
				t.Errorf("GetTenantHeader() = %q, want %q", got, tt.want)
			}
		})
	}
}

// Helper functions
func setupTestSSLFiles(t *testing.T, dir string) (string, string) {
	certFile := filepath.Join(dir, "cert.pem")
	keyFile := filepath.Join(dir, "key.pem")

	// Create dummy cert file
	certContent := []byte("-----BEGIN CERTIFICATE-----\nDUMMY CERT\n-----END CERTIFICATE-----")
	if err := os.WriteFile(certFile, certContent, 0644); err != nil {
		t.Fatalf("Failed to create cert file: %v", err)
	}

	// Create dummy key file
	keyContent := []byte("-----BEGIN PRIVATE KEY-----\nDUMMY KEY\n-----END PRIVATE KEY-----")
	if err := os.WriteFile(keyFile, keyContent, 0644); err != nil {
		t.Fatalf("Failed to create key file: %v", err)
	}

	return certFile, keyFile
}
