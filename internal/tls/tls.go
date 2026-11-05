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
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/go-logr/logr"
)

var tlsVersionMap = map[string]uint16{
	"VersionTLS12": tls.VersionTLS12,
	"VersionTLS13": tls.VersionTLS13,
}

func parseMinVersion(version string) (uint16, error) {
	version = strings.TrimSpace(version)
	if version == "" {
		return tls.VersionTLS12, nil
	}
	v, ok := tlsVersionMap[version]
	if !ok {
		return 0, fmt.Errorf("unrecognized TLS version %q, valid values: %s",
			version, strings.Join(validVersionNames(), ", "))
	}
	return v, nil
}

func parseCipherSuites(commaSeparated string) ([]uint16, error) {
	commaSeparated = strings.TrimSpace(commaSeparated)
	if commaSeparated == "" {
		return nil, nil
	}

	allCiphers := make(map[string]uint16)
	for _, cs := range tls.CipherSuites() {
		allCiphers[cs.Name] = cs.ID
	}

	var ids []uint16
	var unknown []string
	for _, name := range strings.Split(commaSeparated, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if id, ok := allCiphers[name]; ok {
			ids = append(ids, id)
		} else {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		return nil, fmt.Errorf("unknown TLS cipher suite(s): %s", strings.Join(unknown, ", "))
	}
	if len(ids) == 0 {
		return nil, errors.New("cipher suites flag was set but no valid suites were specified")
	}
	return ids, nil
}

func parseNextProtos(commaSeparated string) []string {
	commaSeparated = strings.TrimSpace(commaSeparated)
	if commaSeparated == "" {
		return []string{"h2", "http/1.1"}
	}

	var protos []string
	for _, proto := range strings.Split(commaSeparated, ",") {
		proto = strings.TrimSpace(proto)
		if proto != "" {
			protos = append(protos, proto)
		}
	}
	if len(protos) == 0 {
		return []string{"h2", "http/1.1"}
	}
	return protos
}

func validVersionNames() []string {
	names := make([]string, 0, len(tlsVersionMap))
	for name := range tlsVersionMap {
		names = append(names, name)
	}
	return names
}

// Resolve builds TLS option functions from the provided min version, cipher
// suites, and next protos strings. When all are empty, it returns hardened
// Intermediate defaults (TLS 1.2, ALPN h2/http1.1).
func Resolve(logger logr.Logger, tlsMinVersion, tlsCipherSuites, tlsNextProtos string) ([]func(*tls.Config), error) {
	minVersion, err := parseMinVersion(tlsMinVersion)
	if err != nil {
		return nil, err
	}

	ciphers, err := parseCipherSuites(tlsCipherSuites)
	if err != nil {
		return nil, err
	}

	if minVersion >= tls.VersionTLS13 && len(ciphers) > 0 {
		return nil, errors.New("cipher suites cannot be configured with TLS 1.3 (Go manages TLS 1.3 ciphers internally)")
	}

	nextProtos := parseNextProtos(tlsNextProtos)
	if minVersion < tls.VersionTLS13 && len(ciphers) > 0 &&
		!slices.Contains(ciphers, tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256) &&
		!slices.Contains(ciphers, tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256) {
		nextProtos = slices.DeleteFunc(nextProtos, func(proto string) bool {
			return proto == "h2"
		})
		if len(nextProtos) == 0 {
			nextProtos = []string{"http/1.1"}
		}
	}

	logger.Info("TLS configuration resolved",
		"minVersion", versionName(minVersion),
		"cipherSuites", len(ciphers),
		"nextProtos", nextProtos)

	return []func(*tls.Config){
		func(c *tls.Config) {
			c.MinVersion = minVersion
			if len(ciphers) > 0 {
				c.CipherSuites = ciphers
			}
			c.NextProtos = nextProtos
		},
	}, nil
}

func versionName(v uint16) string {
	for name, val := range tlsVersionMap {
		if val == v {
			return name
		}
	}
	return fmt.Sprintf("0x%04x", v)
}
