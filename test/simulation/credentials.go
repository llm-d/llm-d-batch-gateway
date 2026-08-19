//go:build simulation

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

package simulation

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const simS3AccessKeyID = "sim-access-key"

// simCredentials are the store credentials for one compose run, generated
// fresh so no credential-shaped file is ever committed.
type simCredentials struct {
	pgPassword  string
	s3SecretKey string
}

// simCreds is set by the compose backend before the stack starts; the kind
// backend leaves it nil and uses the dev-deploy chart's credentials.
var simCreds *simCredentials

func generateCredentials(t *testing.T) *simCredentials {
	t.Helper()
	return &simCredentials{pgPassword: randomToken(t), s3SecretKey: randomToken(t)}
}

func randomToken(t *testing.T) string {
	t.Helper()
	var b [18]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generate credential: %v", err)
	}
	return hex.EncodeToString(b[:])
}

// writeSecretFiles renders each component's /etc/.secrets mount under dir,
// with connection URLs routed through that component's toxiproxy listeners.
func (c *simCredentials) writeSecretFiles(t *testing.T, dir string) {
	t.Helper()
	ports := map[string]struct{ pg, redis int }{
		"apiserver": {25432, 26379},
		"processor": {35432, 36379},
		"gc":        {45432, 46379},
	}
	for component, p := range ports {
		root := filepath.Join(dir, component)
		if err := os.MkdirAll(root, 0o755); err != nil {
			t.Fatalf("create secrets dir: %v", err)
		}
		files := map[string]string{
			"postgresql-url":       fmt.Sprintf("postgres://sim:%s@toxiproxy:%d/batchgw?sslmode=disable", c.pgPassword, p.pg),
			"redis-url":            fmt.Sprintf("redis://toxiproxy:%d/0", p.redis),
			"s3-secret-access-key": c.s3SecretKey,
		}
		for name, content := range files {
			if err := os.WriteFile(filepath.Join(root, name), []byte(content+"\n"), 0o600); err != nil {
				t.Fatalf("write secret %s/%s: %v", component, name, err)
			}
		}
	}
}
