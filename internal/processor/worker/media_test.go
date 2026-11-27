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

package worker

import (
	"fmt"
	"strings"
	"testing"

	s3client "github.com/llm-d/llm-d-batch-gateway/internal/files_store/s3"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	sharedcfg "github.com/llm-d/llm-d-batch-gateway/internal/shared/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

func TestExtractAndValidateLine_MediaEndpoints(t *testing.T) {
	allowlist, err := openai.NewEndpointAllowlist([]string{
		openai.EndpointAudioTranscriptions.String(),
		openai.EndpointAudioTranslations.String(),
		openai.EndpointImagesEdits.String(),
		// The same endpoints under a path prefix, which the transform also claims.
		"/api/v1/audio/transcriptions",
		"/api/v1/images/edits",
	})
	if err != nil {
		t.Fatal(err)
	}
	// The files bucket holds every tenant; this job's tenant owns pfx/t-mine/.
	rules := mediaRules{bucket: "files", tenantPrefix: "pfx/t-mine/"}
	const (
		transcribe = "/v1/audio/transcriptions"
		translate  = "/v1/audio/translations"
		edit       = "/v1/images/edits"
		// Prefixed paths: the transform matches by suffix, so the checks must too.
		prefixedTranscribe = "/api/v1/audio/transcriptions"
		prefixedEdit       = "/api/v1/images/edits"
	)

	tests := []struct {
		name     string
		endpoint string
		body     string // the request body after "model"
		extra    string // raw line fields after "body"
		noFiles  bool   // the files store isn't S3, so there is no bucket to guard
		wantErr  string // substring; empty means the line is accepted
	}{
		// Transcriptions and translations.
		{name: "audio in the tenant's folder", endpoint: transcribe, body: `"file":"s3://files/pfx/t-mine/a.wav"`},
		{name: "audio as a data URL", endpoint: transcribe, body: `"file":"data:audio/wav;base64,UklGRg=="`},
		{name: "audio in another bucket", endpoint: transcribe, body: `"file":"s3://media/a.wav"`},
		{name: "translations use the same rules", endpoint: translate, body: `"file":"s3://files/pfx/t-other/a.wav"`, wantErr: "outside this tenant's files"},
		{name: "audio in another tenant's folder", endpoint: transcribe, body: `"file":"s3://files/pfx/t-other/a.wav"`, wantErr: "outside this tenant's files"},
		{name: "audio at the bucket root", endpoint: transcribe, body: `"file":"s3://files/a.wav"`, wantErr: "outside this tenant's files"},
		{name: "prefix lookalike", endpoint: transcribe, body: `"file":"s3://files/pfx/t-mine-not/a.wav"`, wantErr: "outside this tenant's files"},
		{name: "dot-dot segment out of the folder", endpoint: transcribe, body: `"file":"s3://files/pfx/t-mine/../t-other/a.wav"`, wantErr: "relative path segment"},
		{name: "no tenant rule without an s3 files store", endpoint: transcribe, body: `"file":"s3://files/pfx/t-other/a.wav"`, noFiles: true},
		{name: "https URL", endpoint: transcribe, body: `"file":"https://example.com/a.wav"`, wantErr: "must be an s3:// reference"},
		{name: "s3 reference without a key", endpoint: transcribe, body: `"file":"s3://files"`, wantErr: "not an s3://bucket/key reference"},
		{name: "s3 reference with an empty key", endpoint: transcribe, body: `"file":"s3://media/"`, wantErr: "not an s3://bucket/key reference"},
		{name: "missing file", endpoint: transcribe, body: `"language":"en"`, wantErr: "file must be"},
		{name: "file not a string", endpoint: transcribe, body: `"file":42`, wantErr: "file must be"},
		{name: "verbose_json", endpoint: transcribe, body: `"file":"data:audio/wav;base64,UklGRg==","response_format":"verbose_json"`},
		{name: "null response_format", endpoint: transcribe, body: `"file":"data:audio/wav;base64,UklGRg==","response_format":null`},
		{name: "text response_format", endpoint: transcribe, body: `"file":"data:audio/wav;base64,UklGRg==","response_format":"text"`, wantErr: "response_format must be one of json, verbose_json"},
		{name: "File key cannot shadow file", endpoint: transcribe, body: `"file":"s3://files/pfx/t-other/a.wav","File":"data:audio/wav;base64,UklGRg=="`, wantErr: "outside this tenant's files"},
		{name: "second Body object merges into body", endpoint: transcribe, body: `"file":"data:audio/wav;base64,UklGRg=="`, extra: `,"Body":{"file":"s3://files/pfx/t-other/a.wav"}`, wantErr: "outside this tenant's files"},
		{name: "data URL without a media type", endpoint: transcribe, body: `"file":"data:;base64,UklGRg=="`},
		{name: "malformed data URL", endpoint: transcribe, body: `"file":"data:garbage"`, wantErr: "malformed data URL"},
		{name: "data URL not base64", endpoint: transcribe, body: `"file":"data:audio/wav,QUJD"`, wantErr: "must be base64-encoded"},
		{name: "data URL with invalid base64", endpoint: transcribe, body: `"file":"data:audio/wav;base64,!!!"`, wantErr: "invalid base64"},
		{name: "data URL with invalid media type", endpoint: transcribe, body: `"file":"data:a b;base64,QUJD"`, wantErr: "invalid media type"},

		// Image edits.
		{name: "images in the tenant's folder and inline", endpoint: edit, body: `"images":[{"image_url":"s3://files/pfx/t-mine/in.png"},{"image_url":"data:image/png;base64,iVBORw=="}]`},
		{name: "image in another tenant's folder", endpoint: edit, body: `"images":[{"image_url":"s3://files/pfx/t-mine/a.png"},{"image_url":"s3://files/pfx/t-other/b.png"}]`, wantErr: "images[1].image_url"},
		{name: "image file_id", endpoint: edit, body: `"images":[{"file_id":"file-abc"}]`, wantErr: "file_id is not supported"},
		{name: "non-image data URL", endpoint: edit, body: `"images":[{"image_url":"data:text/plain;base64,QUJD"}]`, wantErr: "data:image/"},
		{name: "image data URL with invalid base64", endpoint: edit, body: `"images":[{"image_url":"data:image/png;base64,!!!"}]`, wantErr: "images[0].image_url: data URL has invalid base64"},
		{name: "image data URL not base64", endpoint: edit, body: `"images":[{"image_url":"data:image/png,QUJD"}]`, wantErr: "images[0].image_url: data URL must be base64-encoded"},
		{name: "no images", endpoint: edit, body: `"prompt":"x"`, wantErr: "non-empty list"},
		{name: "empty images", endpoint: edit, body: `"images":[]`, wantErr: "non-empty list"},
		{name: "b64_json", endpoint: edit, body: `"images":[{"image_url":"data:image/png;base64,iVBORw=="}],"response_format":"b64_json"`},
		{name: "url response_format", endpoint: edit, body: `"images":[{"image_url":"data:image/png;base64,iVBORw=="}],"response_format":"url"`, wantErr: "response_format must be one of b64_json"},
		{name: "IMAGE_URL key cannot shadow image_url", endpoint: edit, body: `"images":[{"image_url":"s3://files/pfx/t-mine/a.png","IMAGE_URL":"s3://files/pfx/t-other/b.png"}]`, wantErr: `"IMAGE_URL" must be spelled image_url`},
		{name: "image_url not a string", endpoint: edit, body: `"images":[{"image_url":42}]`, wantErr: "images[0].image_url must be a string"},
		{name: "null mask", endpoint: edit, body: `"images":[{"image_url":"data:image/png;base64,iVBORw=="}],"mask":null`},
		{name: "mask", endpoint: edit, body: `"images":[{"image_url":"data:image/png;base64,iVBORw=="}],"mask":{"image_url":"data:image/png;base64,iVBORw=="}`, wantErr: "mask is not supported"},
		{name: "caller url field", endpoint: edit, body: `"images":[{"image_url":"data:image/png;base64,iVBORw=="}],"url":"http://example.com/a.png"`, wantErr: "url is not accepted"},
		{name: "reference_image field", endpoint: edit, body: `"images":[{"image_url":"data:image/png;base64,iVBORw=="}],"reference_image":"x"`, wantErr: "reference_image is not accepted"},

		// Media endpoints under a path prefix get the same checks.
		{name: "prefixed transcriptions, another tenant's folder", endpoint: prefixedTranscribe, body: `"file":"s3://files/pfx/t-other/a.wav"`, wantErr: "outside this tenant's files"},
		{name: "prefixed transcriptions, own folder", endpoint: prefixedTranscribe, body: `"file":"s3://files/pfx/t-mine/a.wav"`},
		{name: "prefixed image edits, another tenant's folder", endpoint: prefixedEdit, body: `"images":[{"image_url":"s3://files/pfx/t-other/b.png"}]`, wantErr: "images[0].image_url"},
		// The attack: a prefixed image edit with a valid audio-style file, so
		// audio rules would pass it, and another tenant's image, which the
		// transform (matching by suffix) would fetch.
		{name: "prefixed image edits, valid file but another tenant's image", endpoint: prefixedEdit, body: `"file":"s3://files/pfx/t-mine/a.wav","images":[{"image_url":"s3://files/pfx/t-other/b.png"}]`, wantErr: "images[0].image_url"},

		// Other endpoints are not media endpoints.
		{name: "chat completions ignore media fields", endpoint: "/v1/chat/completions", body: `"messages":[{"role":"user","content":"hi"}],"file":"https://x"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := rules
			if tc.noFiles {
				r = mediaRules{}
			}
			line := fmt.Appendf(nil, `{"custom_id":"r1","method":"POST","url":%q,"body":{"model":"m",%s}%s}`+"\n", tc.endpoint, tc.body, tc.extra)
			_, err := extractAndValidateLine(line, allowlist, tc.endpoint, r)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("error = %v, want one containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestMediaRulesFor(t *testing.T) {
	folder, err := ucom.GetFolderNameByTenantID("tenant-a")
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		files sharedcfg.FileClientConfig
		want  mediaRules
	}{
		{
			name:  "s3 files store with a key prefix",
			files: sharedcfg.FileClientConfig{Type: sharedcfg.FileTypeS3, S3Config: s3client.Config{Bucket: "files", Prefix: "pfx"}},
			want:  mediaRules{bucket: "files", tenantPrefix: "pfx/" + folder + "/"},
		},
		{
			name:  "s3 files store without a key prefix",
			files: sharedcfg.FileClientConfig{Type: sharedcfg.FileTypeS3, S3Config: s3client.Config{Bucket: "files"}},
			want:  mediaRules{bucket: "files", tenantPrefix: folder + "/"},
		},
		{
			name:  "filesystem store has no bucket to guard",
			files: sharedcfg.FileClientConfig{Type: sharedcfg.FileTypeFS},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := &Processor{cfg: &config.ProcessorConfig{FileClientCfg: tc.files}}
			got, err := p.mediaRulesFor("tenant-a")
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("mediaRulesFor = %+v, want %+v", got, tc.want)
			}
		})
	}
}
