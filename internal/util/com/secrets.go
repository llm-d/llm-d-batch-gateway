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

// This file provides utilities for reading Kubernetes-mounted secrets.

package com

import (
	"io"
	"os"
	"strings"
)

const secretsMountPath = "/etc/.secrets"

// Secret key names within the mounted app secret (/etc/.secrets/).
// These are hardcoded conventions; the secret must use these exact key names.
const (
	SecretKeyRedisURL          = "redis-url"
	SecretKeyPostgreSQLURL     = "postgresql-url"
	SecretKeyInferenceAPI      = "inference-api-key"
	SecretKeyS3SecretAccessKey = "s3-secret-access-key"
	SecretKeyProxyToken        = "apiserver-proxy-token"
)

// ReadSecretFile reads a secret value from the mounted secrets directory.
// filename is the key name only (not a full path); the secrets mount path is prepended internally.
// Returns an empty string without error if the file does not exist.
func ReadSecretFile(filename string) (string, error) {
	f, err := os.OpenInRoot(secretsMountPath, filename)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(data)), nil
}
