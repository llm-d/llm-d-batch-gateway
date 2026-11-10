package worker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

func TestStableBatchRequestID(t *testing.T) {
	id := stableBatchRequestID("job-1", "req-1")
	if len(id) <= len("batch_req_") || id[:len("batch_req_")] != "batch_req_" {
		t.Fatalf("ID %q lacks the batch_req_ prefix", id)
	}
	if got := stableBatchRequestID("job-1", "req-1"); got != id {
		t.Fatalf("ID not deterministic: %q vs %q", got, id)
	}
	if got := stableBatchRequestID("job-2", "req-1"); got == id {
		t.Fatal("IDs collide across batches")
	}
	if got := stableBatchRequestID("job-1", "req-2"); got == id {
		t.Fatal("IDs collide across custom IDs")
	}
	// Within one batch the derivation is injective: the batchID prefix is
	// fixed, so equal derived IDs imply equal custom IDs.
	if stableBatchRequestID("job-1", "a") == stableBatchRequestID("job-1", "b") {
		t.Fatal("IDs collide within a batch")
	}
}

// runPreProcessOnce preprocesses a fixed mixed input (one accepted line, one
// rejected line for an unregistered model, one accepted line) and returns the
// manifest and the error-file bytes.
func runPreProcessOnce(t *testing.T, jobID string) *requestManifest {
	t.Helper()
	ctx := testLoggerCtx(t)

	cfg := config.NewConfig()
	cfg.WorkDir = t.TempDir()

	dbClient := newMockBatchDBClient()
	fileDBClient := newMockFileDBClient()
	filesClient := mockfiles.NewMockBatchFilesClient(t.TempDir())

	tenantID := "tenant__tenantA"
	folder, _ := ucom.GetFolderNameByTenantID(tenantID)
	filename := "input.jsonl"

	lines := [][]byte{
		[]byte(`{"custom_id":"req-1","method":"POST","url":"/v1/chat/completions","body":{"model":"model-a","messages":[{"role":"user","content":"hi"}]}}` + "\n"),
		[]byte(`{"custom_id":"req-2","method":"POST","url":"/v1/chat/completions","body":{"model":"model-b","messages":[{"role":"user","content":"hi"}]}}` + "\n"),
		[]byte(`{"custom_id":"req-3","method":"POST","url":"/v1/chat/completions","body":{"model":"model-a","messages":[{"role":"user","content":"bye"}]}}` + "\n"),
	}
	var remoteBuf bytes.Buffer
	for _, ln := range lines {
		remoteBuf.Write(ln)
	}

	inputFileID := "file-manifest"
	if _, err := filesClient.Store(ctx, ucom.FileStorageName(inputFileID, filename), folder, 0, 0, bytes.NewReader(remoteBuf.Bytes())); err != nil {
		t.Fatalf("files.Store: %v", err)
	}
	fileItem := &db.FileItem{
		BaseIndexes:  db.BaseIndexes{ID: inputFileID, TenantID: tenantID},
		BaseContents: db.BaseContents{Spec: mustJSON(t, &openai.FileObject{Filename: filename})},
	}
	if err := fileDBClient.DBStore(ctx, fileItem); err != nil {
		t.Fatalf("DBStore file item: %v", err)
	}

	resolver, err := inference.NewPerModelResolver(
		map[string]inference.GatewayClientConfig{
			"model-a": {URL: "http://fake:8000"},
		},
		testLogger(t),
	)
	if err != nil {
		t.Fatalf("NewPerModelResolver: %v", err)
	}

	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		BatchDB:   dbClient,
		FileDB:    fileDBClient,
		File:      filesClient,
		Inference: resolver,
	})
	jobInfo := &batch_types.JobInfo{
		JobID: jobID,
		BatchJob: &openai.Batch{
			ID: jobID,
			BatchSpec: openai.BatchSpec{
				InputFileID: inputFileID,
				Endpoint:    openai.EndpointChatCompletions,
			},
			BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusInProgress},
		},
		TenantID: tenantID,
	}

	manifest, err := p.preProcessJob(ctx, jobInfo)
	if err != nil {
		t.Fatalf("preProcessJob: %v", err)
	}

	return manifest
}

func TestPreProcess_ManifestAccumulation(t *testing.T) {
	manifest := runPreProcessOnce(t, "job-manifest")

	if len(manifest.Entries) != 3 {
		t.Fatalf("manifest entries = %d, want 3", len(manifest.Entries))
	}

	for i, e := range manifest.Entries {
		if e.Ordinal != int64(i) {
			t.Fatalf("entry %d has ordinal %d", i, e.Ordinal)
		}
		if want := stableBatchRequestID("job-manifest", e.CustomID); e.RequestID != want {
			t.Fatalf("entry %d request ID %q, want stable ID %q", i, e.RequestID, want)
		}
	}

	accepted, rejected := manifest.Entries[0], manifest.Entries[1]
	if accepted.Rejected || accepted.ErrorCode != "" || accepted.ResultKey != "" {
		t.Fatalf("accepted entry carries rejection metadata: %+v", accepted)
	}
	if !rejected.Rejected || rejected.ErrorCode != "model_not_found" {
		t.Fatalf("rejected entry not marked: %+v", rejected)
	}
	if rejected.ResultKey != resumableResultKey("job-manifest", rejected.RequestID) {
		t.Fatalf("rejected result key %q", rejected.ResultKey)
	}

	// The rejected entry's metadata must describe its error-object line
	// (including its trailing newline).
	errLine := rejected.ErrLine
	if int64(len(errLine)) != rejected.ResultBytes {
		t.Fatalf("ResultBytes = %d, want %d", rejected.ResultBytes, len(errLine))
	}
	sum := sha256.Sum256(errLine)
	if hex.EncodeToString(sum[:]) != rejected.ResultSHA256 {
		t.Fatalf("ResultSHA256 mismatch for error line %s", errLine)
	}
	if !bytes.Contains(errLine, []byte(`"`+rejected.RequestID+`"`)) {
		t.Fatalf("error line does not carry the stable request ID: %s", errLine)
	}
}

func TestPreProcess_ManifestDeterministic(t *testing.T) {
	first := runPreProcessOnce(t, "job-manifest")
	second := runPreProcessOnce(t, "job-manifest")
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("manifests differ across runs:\n%+v\n%+v", first.Entries, second.Entries)
	}
}

func TestManifest_ActivationEntries(t *testing.T) {
	manifest := runPreProcessOnce(t, "job-manifest")

	entries := manifest.activationEntries()
	if len(entries) != len(manifest.Entries) {
		t.Fatalf("activation entries = %d, want %d", len(entries), len(manifest.Entries))
	}

	for i, e := range entries {
		if e.RequestID != manifest.Entries[i].RequestID ||
			e.Ordinal != manifest.Entries[i].Ordinal ||
			e.CustomID != manifest.Entries[i].CustomID {
			t.Fatalf("entry %d does not mirror the manifest: %+v", i, e)
		}
	}

	accepted, rejected := entries[0], entries[1]
	if accepted.Status != string(db.ResumableEntryPending) {
		t.Fatalf("accepted entry activates as %q, want pending", accepted.Status)
	}
	if accepted.ErrorCode != "" || accepted.ResultKey != "" || accepted.ResultBytes != 0 || accepted.ResultSHA256 != "" {
		t.Fatalf("pending entry carries result metadata: %+v", accepted)
	}

	src := manifest.Entries[1]
	if rejected.Status != string(db.ResumableEntryFailed) {
		t.Fatalf("rejected entry activates as %q, want failed", rejected.Status)
	}
	if rejected.ErrorCode != src.ErrorCode || rejected.ResultKey != src.ResultKey ||
		rejected.ResultBytes != src.ResultBytes || rejected.ResultSHA256 != src.ResultSHA256 {
		t.Fatalf("rejected entry = %+v, want the manifest metadata %+v", rejected, src)
	}
}
