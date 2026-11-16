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

// Command mediaecho stands in for the multipart-only media backends in e2e
// tests: vLLM's /v1/audio/transcriptions and /v1/audio/translations, which take
// an uploaded `file` part, and vllm-omni's /v1/images/edits, which fetches each
// `url` form field itself. It rejects non-multipart bodies like the real
// servers, and answers with the SHA-256 of the media it received so a test can
// check the bytes made it through.
package main

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

const maxMedia = 32 << 20

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /v1/audio/transcriptions", transcribe)
	mux.HandleFunc("POST /v1/audio/translations", transcribe)
	mux.HandleFunc("POST /v1/images/edits", editImages)
	addr := ":" + envOr("PORT", "8000")
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	fmt.Println("mediaecho listening on", addr)
	if err := srv.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// transcribe answers like a transcription with "sha256:<hex> model:<model>"
// for the uploaded file part.
func transcribe(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxMedia); err != nil {
		http.Error(w, "multipart/form-data required: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}
	f, _, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file part required: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]any{"text": fmt.Sprintf("sha256:%s model:%s", digest(data), r.FormValue("model"))})
}

// editImages fetches every `url` field like vllm-omni and returns one image per
// input whose b64_json decodes to "sha256:<hex>" of the fetched bytes.
func editImages(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(maxMedia); err != nil {
		http.Error(w, "multipart/form-data required: "+err.Error(), http.StatusUnprocessableEntity)
		return
	}
	urls := r.MultipartForm.Value["url"]
	if len(urls) == 0 {
		http.Error(w, "url field required", http.StatusUnprocessableEntity)
		return
	}
	var data []map[string]any
	for _, u := range urls {
		img, err := fetch(r, u)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		data = append(data, map[string]any{"b64_json": base64.StdEncoding.EncodeToString([]byte("sha256:" + digest(img)))})
	}
	writeJSON(w, map[string]any{"created": time.Now().Unix(), "data": data})
}

// fetch resolves an image like vllm-omni: base64 data URLs inline, anything
// else with a plain GET and no credentials.
func fetch(r *http.Request, u string) ([]byte, error) {
	if rest, ok := strings.CutPrefix(u, "data:"); ok {
		_, b64, ok := strings.Cut(rest, ",")
		if !ok {
			return nil, errors.New("malformed data URL")
		}
		return base64.StdEncoding.DecodeString(b64)
	}
	req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch image: %s", resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxMedia))
}

func digest(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
