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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"strings"

	s3client "github.com/llm-d/llm-d-batch-gateway/internal/files_store/s3"
	sharedcfg "github.com/llm-d/llm-d-batch-gateway/internal/shared/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

const s3Scheme = "s3://"

// mediaRules checks request bodies for the multipart media endpoints, which
// llm-d-async's s3_multipart transform rewrites. The transform enforces the same
// shapes, but checking here fails the batch before any request is dispatched.
//
// The transform resolves s3:// references with the processor's storage
// credentials and only knows which buckets are allowed, not which tenant asked.
// When a reference names this gateway's own files bucket, which holds every
// tenant under its own key prefix, the key must therefore be under the job
// tenant's prefix.
type mediaRules struct {
	// bucket is the files store's S3 bucket; empty unless file_client is s3.
	bucket string
	// tenantPrefix is the key prefix of the job tenant's folder in bucket.
	tenantPrefix string
}

// mediaRulesFor returns the media rules for a job owned by tenantID.
func (p *Processor) mediaRulesFor(tenantID string) (mediaRules, error) {
	files := p.cfg.FileClientCfg
	if files.Type != sharedcfg.FileTypeS3 {
		return mediaRules{}, nil
	}
	folder, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		return mediaRules{}, fmt.Errorf("resolve tenant folder: %w", err)
	}
	return mediaRules{
		bucket:       files.S3Config.Bucket,
		tenantPrefix: s3client.FolderKeyPrefix(files.S3Config.Prefix, folder),
	}, nil
}

// check validates the media fields of a request line for endpoint. Lines for
// other endpoints pass unchecked.
//
// It decodes the body the way dispatch does (types.Request: a "body" struct
// field holding a case-sensitive map), so it checks the exact fields the
// transform receives; a "File" key cannot stand in for the "file" sent on.
func (m mediaRules) check(line []byte, endpoint string) error {
	if !openai.IsMultipartMediaEndpoint(openai.Endpoint(endpoint)) {
		return nil
	}
	var req struct {
		Body map[string]json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(line, &req); err != nil {
		return err
	}
	if openai.Endpoint(endpoint) == openai.EndpointImagesEdits {
		return m.checkImageEdit(req.Body)
	}
	return m.checkAudio(req.Body)
}

func (m mediaRules) checkAudio(body map[string]json.RawMessage) error {
	var ref string
	if err := json.Unmarshal(body["file"], &ref); err != nil {
		return errors.New("file must be an s3:// reference or a data: URL")
	}
	err := m.checkRef(ref, "data:")
	if err == nil && strings.HasPrefix(ref, "data:") {
		err = checkDataURL(ref)
	}
	if err != nil {
		return fmt.Errorf("file: %w", err)
	}
	// text, srt and vtt come back as plain text, which batch output can't parse.
	return checkResponseFormat(body["response_format"], "json", "verbose_json")
}

func (m mediaRules) checkImageEdit(body map[string]json.RawMessage) error {
	var images []map[string]json.RawMessage
	if err := json.Unmarshal(body["images"], &images); err != nil || len(images) == 0 {
		return errors.New(`images must be a non-empty list of {"image_url": ...}`)
	}
	for i, img := range images {
		// The transform reads image_url case-insensitively, so a key such as
		// "IMAGE_URL" could replace the value checked here.
		for k := range img {
			if k != "image_url" && strings.EqualFold(k, "image_url") {
				return fmt.Errorf("images[%d]: %q must be spelled image_url", i, k)
			}
		}
		raw, ok := img["image_url"]
		if !ok {
			return fmt.Errorf("images[%d]: image_url is required (file_id is not supported)", i)
		}
		var ref string
		if err := json.Unmarshal(raw, &ref); err != nil {
			return fmt.Errorf("images[%d].image_url must be a string", i)
		}
		if err := m.checkRef(ref, "data:image/"); err != nil {
			return fmt.Errorf("images[%d].image_url: %w", i, err)
		}
	}
	// The transform rejects these: vllm-omni ignores a mask and fetches any
	// other image source it is given.
	if raw, ok := body["mask"]; ok && string(raw) != "null" {
		return errors.New("mask is not supported")
	}
	for _, name := range []string{"url", "url[]", "mask_image", "reference_image"} {
		if _, ok := body[name]; ok {
			return fmt.Errorf("%s is not accepted; list input images in images", name)
		}
	}
	// vllm-omni returns edited images only as b64_json.
	return checkResponseFormat(body["response_format"], "b64_json")
}

// checkRef accepts an inline data URL with dataPrefix or an s3:// reference,
// which must stay inside the job tenant's prefix when it names the files bucket.
func (m mediaRules) checkRef(ref, dataPrefix string) error {
	if strings.HasPrefix(ref, dataPrefix) {
		return nil
	}
	rest, ok := strings.CutPrefix(ref, s3Scheme)
	if !ok {
		return fmt.Errorf("must be an s3:// reference or a %s URL", dataPrefix)
	}
	bucket, key, ok := strings.Cut(rest, "/")
	if !ok || bucket == "" || key == "" {
		return fmt.Errorf("%q is not an s3://bucket/key reference", ref)
	}
	if bucket != m.bucket {
		return nil
	}
	// A "." or ".." segment could step out of the prefix if any hop between
	// here and the store normalizes the path.
	for _, seg := range strings.Split(key, "/") {
		if seg == "." || seg == ".." {
			return fmt.Errorf("%q has a relative path segment", ref)
		}
	}
	if !strings.HasPrefix(key, m.tenantPrefix) {
		return fmt.Errorf("%q is outside this tenant's files", ref)
	}
	return nil
}

// checkDataURL accepts the data URLs the transform can decode:
// data:[<mediatype>];base64,<data>.
func checkDataURL(ref string) error {
	meta, data, ok := strings.Cut(strings.TrimPrefix(ref, "data:"), ",")
	if !ok {
		return errors.New("malformed data URL")
	}
	mediaType, ok := strings.CutSuffix(meta, ";base64")
	if !ok {
		return errors.New("data URL must be base64-encoded")
	}
	if mediaType != "" {
		if _, _, err := mime.ParseMediaType(mediaType); err != nil {
			return fmt.Errorf("data URL has an invalid media type: %w", err)
		}
	}
	// Decode in chunks: this checks the encoding without holding the file.
	if _, err := io.Copy(io.Discard, base64.NewDecoder(base64.StdEncoding, strings.NewReader(data))); err != nil {
		return fmt.Errorf("data URL has invalid base64: %w", err)
	}
	return nil
}

// checkResponseFormat accepts an absent or null response_format, or one of allowed.
func checkResponseFormat(raw json.RawMessage, allowed ...string) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var format string
	if err := json.Unmarshal(raw, &format); err == nil {
		for _, a := range allowed {
			if format == a {
				return nil
			}
		}
	}
	return fmt.Errorf("response_format must be one of %s", strings.Join(allowed, ", "))
}
