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

package batchinput

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func line(customID, model, systemPrompt string) string {
	if systemPrompt == "" {
		return fmt.Sprintf(
			`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"hi"}]}}`,
			customID, model)
	}
	return fmt.Sprintf(
		`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"system","content":%q},{"role":"user","content":"hi"}]}}`,
		customID, model, systemPrompt)
}

func jsonl(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

func mustScan(t *testing.T, content string) *ScanResult {
	t.Helper()
	src := strings.NewReader(content)
	res, err := Scan(src, int64(len(content)), ScanOptions{MaxLines: 1000})
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	return res
}

// readPlan renders the ordered object a plan describes.
func readPlan(t *testing.T, plan *Plan, source string) string {
	t.Helper()
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, plan.Reader(strings.NewReader(source))); err != nil {
		t.Fatalf("read plan: %v", err)
	}
	if int64(buf.Len()) != plan.Size {
		t.Fatalf("rendered %d bytes, plan declared Size=%d", buf.Len(), plan.Size)
	}
	return buf.String()
}

func customIDs(t *testing.T, content string) []string {
	t.Helper()
	var ids []string
	for _, l := range strings.Split(strings.TrimSuffix(content, "\n"), "\n") {
		meta, err := ParseLine([]byte(l))
		if err != nil {
			t.Fatalf("ParseLine(%q): %v", l, err)
		}
		ids = append(ids, meta.CustomID)
	}
	return ids
}

func TestParseLine(t *testing.T) {
	t.Run("derives model and custom id", func(t *testing.T) {
		meta, err := ParseLine([]byte(line("a", "m1", "") + "\n"))
		if err != nil {
			t.Fatalf("ParseLine: %v", err)
		}
		if meta.CustomID != "a" || meta.ModelID != "m1" {
			t.Fatalf("got %+v", meta)
		}
		if meta.PrefixHash != NoPrefixHash {
			t.Fatalf("expected NoPrefixHash without a system prompt, got %d", meta.PrefixHash)
		}
	})

	t.Run("identical system prompts hash identically", func(t *testing.T) {
		a, err := ParseLine([]byte(line("a", "m1", "you are helpful")))
		if err != nil {
			t.Fatal(err)
		}
		b, err := ParseLine([]byte(line("b", "m2", "you are helpful")))
		if err != nil {
			t.Fatal(err)
		}
		c, err := ParseLine([]byte(line("c", "m1", "you are different")))
		if err != nil {
			t.Fatal(err)
		}
		if a.PrefixHash != b.PrefixHash {
			t.Fatal("same system prompt must hash the same regardless of model")
		}
		if a.PrefixHash == c.PrefixHash {
			t.Fatal("different system prompts must not collide here")
		}
		if a.PrefixHash == NoPrefixHash {
			t.Fatal("a system prompt must produce a real hash")
		}
	})

	t.Run("rejects invalid lines", func(t *testing.T) {
		cases := map[string]string{
			"malformed json":   `{"custom_id":`,
			"no custom_id":     `{"method":"POST","url":"/v1/chat/completions","body":{"model":"m"}}`,
			"bad method":       `{"custom_id":"a","method":"GET","url":"/v1/chat/completions","body":{"model":"m"}}`,
			"absolute url":     `{"custom_id":"a","method":"POST","url":"http://x/v1/chat/completions","body":{"model":"m"}}`,
			"unknown endpoint": `{"custom_id":"a","method":"POST","url":"/v1/nope","body":{"model":"m"}}`,
			"no model":         `{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{}}`,
			"streaming":        `{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":"m","stream":true}}`,
		}
		for name, content := range cases {
			t.Run(name, func(t *testing.T) {
				if _, err := ParseLine([]byte(content)); err == nil {
					t.Fatal("expected a validation error")
				}
			})
		}
	})
}

func TestScan(t *testing.T) {
	t.Run("records offsets that address the original bytes", func(t *testing.T) {
		content := jsonl(line("a", "m1", ""), line("b", "m2", ""), line("c", "m1", ""))
		res := mustScan(t, content)

		if !res.Valid() {
			t.Fatalf("unexpected validation error: %v", res.Invalid)
		}
		if res.LineCount != 3 {
			t.Fatalf("LineCount = %d, want 3", res.LineCount)
		}
		for _, e := range res.Entries {
			got := content[e.Offset : e.Offset+int64(e.Length)]
			if !strings.HasSuffix(got, "\n") {
				t.Fatalf("entry %d does not cover a whole line: %q", e.Index, got)
			}
			if _, err := ParseLine([]byte(got)); err != nil {
				t.Fatalf("entry %d is not a valid line: %v", e.Index, err)
			}
		}
	})

	t.Run("reports duplicate custom_id with its line number", func(t *testing.T) {
		content := jsonl(line("a", "m1", ""), line("a", "m1", ""))
		res := mustScan(t, content)
		if res.Valid() {
			t.Fatal("expected duplicate custom_id to invalidate the input")
		}
		if res.Invalid.Line != 2 {
			t.Fatalf("Invalid.Line = %d, want 2", res.Invalid.Line)
		}
		if !strings.Contains(res.Invalid.Message, "duplicate custom_id") {
			t.Fatalf("unexpected message %q", res.Invalid.Message)
		}
	})

	t.Run("reports a malformed line with its line number", func(t *testing.T) {
		content := jsonl(line("a", "m1", ""), `{"nope":`)
		res := mustScan(t, content)
		if res.Valid() {
			t.Fatal("expected malformed line to invalidate the input")
		}
		if res.Invalid.Line != 2 {
			t.Fatalf("Invalid.Line = %d, want 2", res.Invalid.Line)
		}
	})

	t.Run("flags an unterminated final line", func(t *testing.T) {
		content := line("a", "m1", "") + "\n" + line("b", "m1", "")
		res := mustScan(t, content)
		if res.FinalLineIndex != 1 {
			t.Fatalf("FinalLineIndex = %d, want 1", res.FinalLineIndex)
		}
	})

	t.Run("enforces the line budget", func(t *testing.T) {
		content := jsonl(line("a", "m1", ""), line("b", "m1", ""), line("c", "m1", ""))
		_, err := Scan(strings.NewReader(content), int64(len(content)), ScanOptions{MaxLines: 2})
		if !errors.Is(err, ErrTooManyLines) {
			t.Fatalf("err = %v, want ErrTooManyLines", err)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		res := mustScan(t, "")
		if !res.Valid() || res.LineCount != 0 || len(res.Entries) != 0 {
			t.Fatalf("got %+v", res)
		}
	})
}

func TestPlanModelPrefixV1(t *testing.T) {
	policy, ok := Lookup(PolicyModelPrefixV1)
	if !ok {
		t.Fatal("model-prefix-v1 must be registered")
	}

	t.Run("groups by model then prefix, ties by original position", func(t *testing.T) {
		content := jsonl(
			line("m2-p1-first", "m2", "shared"),
			line("m1-none", "m1", ""),
			line("m2-p1-second", "m2", "shared"),
			line("m1-p2", "m1", "other"),
		)
		plan, err := NewPlan(mustScan(t, content), policy)
		if err != nil {
			t.Fatalf("NewPlan: %v", err)
		}
		got := customIDs(t, readPlan(t, plan, content))

		// m1 sorts before m2. Within m1 the real prefix hash sorts before
		// NoPrefixHash (MaxUint32), so the no-prompt request goes last.
		want := []string{"m1-p2", "m1-none", "m2-p1-first", "m2-p1-second"}
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Fatalf("order = %v, want %v", got, want)
		}
	})

	t.Run("is deterministic across repeated planning", func(t *testing.T) {
		content := jsonl(
			line("a", "m1", "x"), line("b", "m1", "x"), line("c", "m1", "x"),
			line("d", "m2", "y"), line("e", "m2", "y"),
		)
		first := readPlan(t, mustPlan(t, content, policy), content)
		for range 5 {
			if got := readPlan(t, mustPlan(t, content, policy), content); got != first {
				t.Fatal("ordering is not deterministic")
			}
		}
	})

	t.Run("preserves every line exactly once", func(t *testing.T) {
		content := jsonl(
			line("a", "m2", "x"), line("b", "m1", ""), line("c", "m2", ""), line("d", "m1", "z"),
		)
		got := readPlan(t, mustPlan(t, content, policy), content)

		gotIDs := customIDs(t, got)
		if len(gotIDs) != 4 {
			t.Fatalf("expected 4 lines, got %d", len(gotIDs))
		}
		seen := map[string]int{}
		for _, id := range gotIDs {
			seen[id]++
		}
		for _, id := range []string{"a", "b", "c", "d"} {
			if seen[id] != 1 {
				t.Fatalf("custom_id %q appears %d times", id, seen[id])
			}
		}
	})

	t.Run("terminates a reordered final line", func(t *testing.T) {
		// "zzz" sorts last by model, so the unterminated first line moves to
		// the middle and must gain a newline.
		content := line("a", "zzz", "") + "\n" + line("b", "m1", "")
		res := mustScan(t, content)
		plan, err := NewPlan(res, policy)
		if err != nil {
			t.Fatalf("NewPlan: %v", err)
		}
		out := readPlan(t, plan, content)

		if plan.Size != int64(len(content))+1 {
			t.Fatalf("Size = %d, want %d (one added newline)", plan.Size, len(content)+1)
		}
		ids := customIDs(t, out)
		if strings.Join(ids, ",") != "b,a" {
			t.Fatalf("order = %v, want [b a]", ids)
		}
		// Every line except possibly the last must be newline-terminated; here
		// the moved line is in the middle so the whole object is terminated.
		if strings.Count(out, "\n") != 2 {
			t.Fatalf("expected 2 newlines, got %d in %q", strings.Count(out, "\n"), out)
		}
	})

	t.Run("leaves an unterminated final line alone when it stays last", func(t *testing.T) {
		// "m1" sorts before "zzz", so the unterminated last line stays last
		// and the object must come out byte-identical to the upload.
		content := line("a", "m1", "") + "\n" + line("b", "zzz", "")
		plan := mustPlan(t, content, policy)

		if plan.Size != int64(len(content)) {
			t.Fatalf("Size = %d, want %d: no newline should have been added",
				plan.Size, len(content))
		}
		if got := readPlan(t, plan, content); got != content {
			t.Fatalf("object differs from the upload\ngot:  %q\nwant: %q", got, content)
		}
	})

	t.Run("rejects an invalid scan", func(t *testing.T) {
		res := mustScan(t, jsonl(line("a", "m1", ""), `{"bad":`))
		if _, err := NewPlan(res, policy); err == nil {
			t.Fatal("expected NewPlan to refuse an invalid scan")
		}
	})

	t.Run("reports models", func(t *testing.T) {
		content := jsonl(line("a", "m2", ""), line("b", "m1", ""), line("c", "m2", ""))
		plan := mustPlan(t, content, policy)
		got := plan.Models()
		if strings.Join(got, ",") != "m1,m2" {
			t.Fatalf("Models() = %v", got)
		}
	})
}

func TestPlanOriginalV1(t *testing.T) {
	policy, ok := Lookup(PolicyOriginalV1)
	if !ok {
		t.Fatal("original-v1 must be registered")
	}
	content := jsonl(line("c", "m2", "x"), line("a", "m1", ""), line("b", "m2", ""))
	got := readPlan(t, mustPlan(t, content, policy), content)
	if got != content {
		t.Fatalf("original-v1 must be a byte-for-byte passthrough\ngot:  %q\nwant: %q", got, content)
	}

	// Including when the upload has no trailing newline.
	unterminated := strings.TrimSuffix(content, "\n")
	if got := readPlan(t, mustPlan(t, unterminated, policy), unterminated); got != unterminated {
		t.Fatalf("original-v1 must not add a trailing newline\ngot:  %q\nwant: %q", got, unterminated)
	}
}

func mustPlan(t *testing.T, content string, policy OrderPolicy) *Plan {
	t.Helper()
	plan, err := NewPlan(mustScan(t, content), policy)
	if err != nil {
		t.Fatalf("NewPlan: %v", err)
	}
	return plan
}

func TestOrderedReader(t *testing.T) {
	policy, _ := Lookup(PolicyModelPrefixV1)
	content := jsonl(
		line("a", "m3", "x"), line("b", "m1", ""), line("c", "m2", "y"), line("d", "m1", "z"),
	)
	plan := mustPlan(t, content, policy)
	want := readPlan(t, plan, content)

	t.Run("survives tiny reads", func(t *testing.T) {
		r := plan.Reader(strings.NewReader(content))
		var buf bytes.Buffer
		one := make([]byte, 1)
		for {
			n, err := r.Read(one)
			buf.Write(one[:n])
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
		}
		if buf.String() != want {
			t.Fatal("byte-at-a-time read differs from bulk read")
		}
	})

	t.Run("rewinds for upload retry", func(t *testing.T) {
		r := plan.Reader(strings.NewReader(content))
		if _, err := io.CopyN(io.Discard, r, 10); err != nil {
			t.Fatalf("partial read: %v", err)
		}
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			t.Fatalf("Seek: %v", err)
		}
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, r); err != nil {
			t.Fatalf("Copy after rewind: %v", err)
		}
		if buf.String() != want {
			t.Fatal("content after rewind differs from the original read")
		}
	})

	t.Run("reports its size", func(t *testing.T) {
		r := plan.Reader(strings.NewReader(content))
		if r.Size() != plan.Size {
			t.Fatalf("Size() = %d, plan.Size = %d", r.Size(), plan.Size)
		}
	})
}

func TestMetadata(t *testing.T) {
	t.Run("round-trips through tags", func(t *testing.T) {
		m := &Metadata{
			Version:   MetadataVersion,
			Policy:    PolicyModelPrefixV1,
			LineCount: 7,
			Bytes:     1234,
			Models:    []string{"m1", "m2"},
		}
		encoded, err := m.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		got, err := DecodeMetadata(map[string]string{TagKey: encoded})
		if err != nil {
			t.Fatalf("DecodeMetadata: %v", err)
		}
		if got.Policy != m.Policy || got.LineCount != m.LineCount || got.Bytes != m.Bytes {
			t.Fatalf("round-trip mismatch: %+v", got)
		}
		if !got.Readable() {
			t.Fatal("a current-version known-policy object must be readable")
		}
	})

	t.Run("absent metadata is not an error", func(t *testing.T) {
		got, err := DecodeMetadata(map[string]string{})
		if err != nil || got != nil {
			t.Fatalf("got (%+v, %v), want (nil, nil)", got, err)
		}
		if got.Readable() {
			t.Fatal("missing metadata must not be readable")
		}
	})

	t.Run("unknown policy and future version are not readable", func(t *testing.T) {
		unknown := &Metadata{Version: MetadataVersion, Policy: "invented-by-a-newer-apiserver-v9"}
		if unknown.Readable() {
			t.Fatal("an unregistered policy must force the fallback path")
		}
		future := &Metadata{Version: MetadataVersion + 1, Policy: PolicyModelPrefixV1}
		if future.Readable() {
			t.Fatal("a newer metadata version must force the fallback path")
		}
	})

	t.Run("carries a validation failure", func(t *testing.T) {
		m := &Metadata{
			Version: MetadataVersion,
			Policy:  PolicyModelPrefixV1,
			Invalid: &ValidationError{Line: 3, Message: "duplicate custom_id \"a\""},
		}
		encoded, err := m.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		got, err := DecodeMetadata(map[string]string{TagKey: encoded})
		if err != nil {
			t.Fatalf("DecodeMetadata: %v", err)
		}
		if got.Invalid == nil || got.Invalid.Line != 3 {
			t.Fatalf("validation error lost: %+v", got.Invalid)
		}
		if !strings.Contains(got.Invalid.Error(), "line 3") {
			t.Fatalf("unexpected error text %q", got.Invalid.Error())
		}
	})

	t.Run("corrupt metadata surfaces an error", func(t *testing.T) {
		if _, err := DecodeMetadata(map[string]string{TagKey: "{not json"}); err == nil {
			t.Fatal("expected a decode error")
		}
	})
}

func TestRegistry(t *testing.T) {
	t.Run("built-in policies are registered", func(t *testing.T) {
		ids := RegisteredPolicies()
		if len(ids) < 2 {
			t.Fatalf("expected the built-in policies, got %v", ids)
		}
		for _, want := range []PolicyID{PolicyModelPrefixV1, PolicyOriginalV1} {
			if _, ok := Lookup(want); !ok {
				t.Fatalf("policy %q is not registered", want)
			}
		}
	})

	t.Run("rejects duplicates and empty ids", func(t *testing.T) {
		if err := Register(modelPrefixV1{}); err == nil {
			t.Fatal("expected duplicate registration to fail")
		}
		if err := Register(nil); err == nil {
			t.Fatal("expected nil policy to be rejected")
		}
	})
}

func TestScanRejectsAnOverlongLine(t *testing.T) {
	// A single line larger than Entry.Length can represent must be refused
	// rather than silently truncated by the int32 conversion.
	huge := `{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":"m1","messages":[{"role":"user","content":"` +
		strings.Repeat("x", MaxLineBytes) + `"}]}}`

	_, err := Scan(strings.NewReader(huge), int64(len(huge)), ScanOptions{MaxLines: 10})
	if !errors.Is(err, ErrLineTooLong) {
		t.Fatalf("err = %v, want ErrLineTooLong", err)
	}
}

func TestOrderedReaderSeekRejectsOutOfRange(t *testing.T) {
	policy, _ := Lookup(PolicyModelPrefixV1)
	content := jsonl(line("a", "m1", ""), line("b", "m1", ""))
	r := mustPlan(t, content, policy).Reader(strings.NewReader(content))

	if _, err := r.Seek(r.Size()+1, io.SeekStart); err == nil {
		t.Fatal("seeking past the end must be an error, not a silently empty read")
	}
	if _, err := r.Seek(-1, io.SeekStart); err == nil {
		t.Fatal("seeking before the start must be an error")
	}
	// The valid boundaries still work.
	for _, pos := range []int64{0, r.Size()} {
		if _, err := r.Seek(pos, io.SeekStart); err != nil {
			t.Fatalf("Seek(%d): %v", pos, err)
		}
	}
}
