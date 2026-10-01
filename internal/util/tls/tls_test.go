package tls

import (
	"crypto/tls"
	"testing"
)

func TestIsEmpty(t *testing.T) {
	tests := []struct {
		name  string
		certs Certificates
		want  bool
	}{
		{
			name:  "zero value is empty",
			certs: Certificates{},
			want:  true,
		},
		{
			name:  "cert dir set is not empty",
			certs: Certificates{Dir: "/etc/certs"},
			want:  false,
		},
		{
			name:  "all fields set is not empty",
			certs: Certificates{Dir: "/etc/certs", CertFile: "tls.crt", KeyFile: "tls.key", CaCertFile: "ca.crt"},
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.certs.IsEmpty(); got != tt.want {
				t.Fatalf("IsEmpty() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJoinCertPath(t *testing.T) {
	tests := []struct {
		name string
		dir  string
		file string
		want string
	}{
		{
			name: "empty file returns empty string",
			dir:  "/etc/certs",
			file: "",
			want: "",
		},
		{
			name: "non empty file is joined with dir",
			dir:  "/etc/certs",
			file: "tls.crt",
			want: "/etc/certs/tls.crt",
		},
		{
			name: "empty dir still joins file",
			dir:  "",
			file: "tls.crt",
			want: "tls.crt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := JoinCertPath(tt.dir, tt.file); got != tt.want {
				t.Fatalf("JoinCertPath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestGetTlsConfig(t *testing.T) {
	tests := []struct {
		name     string
		loadType LoadType
		insecure bool
	}{
		{
			name:     "client defaults to TLS 1.2 minimum",
			loadType: LOAD_TYPE_CLIENT,
		},
		{
			name:     "server defaults to TLS 1.2 minimum",
			loadType: LOAD_TYPE_SERVER,
		},
		{
			name:     "insecure client still enforces TLS 1.2 minimum",
			loadType: LOAD_TYPE_CLIENT,
			insecure: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := GetTlsConfig(tt.loadType, tt.insecure, "", "", "")
			if err != nil {
				t.Fatalf("GetTlsConfig() error = %v", err)
			}
			if cfg == nil {
				t.Fatal("GetTlsConfig() returned nil config")
				return
			}
			if cfg.MinVersion != tls.VersionTLS12 {
				t.Fatalf("GetTlsConfig() MinVersion = %v, want %v", cfg.MinVersion, tls.VersionTLS12)
			}
		})
	}
}
