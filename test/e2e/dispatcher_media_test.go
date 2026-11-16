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
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/v3"
	"gopkg.in/yaml.v3"
)

const (
	mediaModel         = "media-model"
	mediaTranscription = "/v1/audio/transcriptions"
	mediaImageEdits    = "/v1/images/edits"
)

// testMediaBucket is the dev files bucket; the dev S3 file client uses no key prefix.
var testMediaBucket = getEnvOrDefault("TEST_MEDIA_BUCKET", "llm-d-batch-gateway")

// detectMediaDispatcherDeployed reports whether the processor dispatches async
// and allows the media endpoints (dev-deploy ENABLE_DISPATCHER_MEDIA=true).
func detectMediaDispatcherDeployed(t *testing.T) bool {
	t.Helper()

	out, ok := processorConfigYAML(t)
	if !ok {
		return false
	}
	var cfg struct {
		DispatchMode   string   `yaml:"dispatch_mode"`
		ExtraEndpoints []string `yaml:"extra_endpoints"`
	}
	if err := yaml.Unmarshal(out, &cfg); err != nil {
		t.Logf("parse processor config failed: %v", err)
		return false
	}
	return cfg.DispatchMode == "async" && slices.Contains(cfg.ExtraEndpoints, mediaTranscription)
}

// TestDispatcherMedia sends JSON batch lines for the multipart-only media
// endpoints through llm-d-async's s3_multipart transform to the mediaecho
// backend, which answers with the SHA-256 of the media it received.
func TestDispatcherMedia(t *testing.T) {
	if !detectMediaDispatcherDeployed(t) {
		t.Skip("skipping: media dispatcher not deployed (ENABLE_DISPATCHER_MEDIA=true)")
	}
	waitForServerUp(t, testApiserverURL, 30*time.Second)

	t.Run("Transcription", doTestMediaTranscription)
	t.Run("ImageEdit", doTestMediaImageEdit)
	t.Run("OtherTenantFileRejected", doTestMediaOtherTenantFileRejected)
}

// doTestMediaTranscription transcribes one audio file uploaded through the
// Files API (s3:// reference) and one inline data: URL.
func doTestMediaTranscription(t *testing.T) {
	audio := randomMedia(t)
	ref := mustUploadMedia(t, testTenantID, "clip.wav", audio)
	inline := randomMedia(t)

	results := runMediaBatch(t, mediaTranscription, map[string]string{
		"s3":     fmt.Sprintf(`{"model":%q,"file":%q,"language":"en","response_format":"json"}`, mediaModel, ref),
		"inline": fmt.Sprintf(`{"model":%q,"file":"data:audio/wav;base64,%s"}`, mediaModel, base64.StdEncoding.EncodeToString(inline)),
	})
	for customID, media := range map[string][]byte{"s3": audio, "inline": inline} {
		want := fmt.Sprintf("sha256:%s model:%s", sha256Hex(media), mediaModel)
		if got := results[customID]["text"]; got != want {
			t.Errorf("%s: text = %v, want %q", customID, got, want)
		}
	}
}

// doTestMediaImageEdit edits with two inputs: an uploaded image, which the
// backend downloads through a presigned URL, and an inline data:image/ URL.
func doTestMediaImageEdit(t *testing.T) {
	image := randomMedia(t)
	ref := mustUploadMedia(t, testTenantID, "in.png", image)
	inline := randomMedia(t)

	results := runMediaBatch(t, mediaImageEdits, map[string]string{
		"edit": fmt.Sprintf(`{"model":%q,"prompt":"make it blue","images":[{"image_url":%q},{"image_url":"data:image/png;base64,%s"}],"response_format":"b64_json"}`,
			mediaModel, ref, base64.StdEncoding.EncodeToString(inline)),
	})
	data, _ := results["edit"]["data"].([]any)
	if len(data) != 2 {
		t.Fatalf("data = %v, want two images", results["edit"]["data"])
	}
	for i, media := range [][]byte{image, inline} {
		entry, _ := data[i].(map[string]any)
		b64, _ := entry["b64_json"].(string)
		got, err := base64.StdEncoding.DecodeString(b64)
		if err != nil {
			t.Fatalf("data[%d].b64_json: %v", i, err)
		}
		if want := "sha256:" + sha256Hex(media); string(got) != want {
			t.Errorf("data[%d] = %q, want %q", i, got, want)
		}
	}
}

// doTestMediaOtherTenantFileRejected references another tenant's upload in
// the shared files bucket. The processor must fail the batch at ingestion,
// before the transform reads the object with its storage credentials.
func doTestMediaOtherTenantFileRejected(t *testing.T) {
	ref := mustUploadMedia(t, "media-other-"+testRunID, "clip.wav", randomMedia(t))

	line := fmt.Sprintf(`{"custom_id":"x","method":"POST","url":%q,"body":{"model":%q,"file":%q}}`, mediaTranscription, mediaModel, ref)
	batchID := mustCreateMediaBatch(t, mediaTranscription, line)
	waitForIngestionFailure(t, batchID, 2*time.Minute)
}

// runMediaBatch submits one line per custom_id, waits for completion, and
// returns each line's 200 response body.
func runMediaBatch(t *testing.T, endpoint string, bodies map[string]string) map[string]map[string]any {
	t.Helper()

	var lines []string
	for customID, body := range bodies {
		lines = append(lines, fmt.Sprintf(`{"custom_id":%q,"method":"POST","url":%q,"body":%s}`, customID, endpoint, body))
	}
	batchID := mustCreateMediaBatch(t, endpoint, strings.Join(lines, "\n"))
	// waitForBatchStatus would check the output for chat-completion fields.
	batch := waitForTerminalBatch(t, batchID, 3*time.Minute)
	if batch.Status != openai.BatchStatusCompleted {
		t.Fatalf("batch %s status = %s, want completed", batchID, batch.Status)
	}
	results := fetchBatchResults(t, batch)
	if results.ErrorLines != 0 {
		t.Fatalf("batch %s error file:\n%s", batchID, results.ErrorBody)
	}

	got := make(map[string]map[string]any, len(bodies))
	for _, raw := range strings.Split(results.OutputBody, "\n") {
		var line batchResultLine
		if err := json.Unmarshal([]byte(raw), &line); err != nil {
			t.Fatalf("parse output line %q: %v", raw, err)
		}
		if line.Response == nil || line.Response.StatusCode != 200 {
			t.Fatalf("%s: response = %s, want status 200", line.CustomID, raw)
		}
		got[line.CustomID] = line.Response.Body
	}
	if len(got) != len(bodies) {
		t.Fatalf("got %d output lines, want %d:\n%s", len(got), len(bodies), results.OutputBody)
	}
	return got
}

func mustCreateMediaBatch(t *testing.T, endpoint, jsonl string) string {
	t.Helper()

	fileID := mustCreateFile(t, fmt.Sprintf("test-media-%s.jsonl", testRunID), jsonl)
	batch, err := createBatchRaw(newClient(), openai.BatchNewParams{
		InputFileID:      fileID,
		Endpoint:         openai.BatchNewParamsEndpoint(endpoint),
		CompletionWindow: openai.BatchNewParamsCompletionWindow24h,
	})
	if err != nil {
		t.Fatalf("create %s batch failed: %v", endpoint, err)
	}
	return batch.ID
}

// mustUploadMedia uploads media for a tenant through the Files API and
// returns its s3:// reference: the files bucket, the tenant's folder
// (t-<first 61 hex chars of sha256(tenant)>), and <file ID><extension>.
func mustUploadMedia(t *testing.T, tenant, filename string, data []byte) string {
	t.Helper()

	file, err := newClientForTenant(tenant).Files.New(context.Background(), openai.FileNewParams{
		File:    openai.File(bytes.NewReader(data), filename, "application/octet-stream"),
		Purpose: openai.FilePurposeUserData,
	})
	if err != nil {
		t.Fatalf("upload %s for tenant %q failed: %v", filename, tenant, err)
	}
	folder := "t-" + sha256Hex([]byte(tenant))[:61]
	ext := filename[strings.LastIndex(filename, "."):]
	return fmt.Sprintf("s3://%s/%s/%s%s", testMediaBucket, folder, file.ID, ext)
}

// randomMedia returns bytes that stand in for audio or image content; the
// backends under test only hash them.
func randomMedia(t *testing.T) []byte {
	t.Helper()

	b := make([]byte, 4096)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
