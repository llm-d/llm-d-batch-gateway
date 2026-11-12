package worker

import (
	"errors"
	"testing"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
)

// --- resolveOutputExpiration ---

func TestResolveOutputExpiration_UserTagOverridesConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 7776000 // 90 days
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{batch_types.TagOutputExpiresAfterSeconds: "3600"}

	got := p.resolveOutputExpiration(now, tags)
	want := now + 3600
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (user tag should override config)", got, want)
	}
}

func TestResolveOutputExpiration_FallsBackToConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{}

	got := p.resolveOutputExpiration(now, tags)
	want := now + 86400
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (should fall back to config)", got, want)
	}
}

func TestResolveOutputExpiration_ZeroWhenNeitherSet(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 0
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{}

	got := p.resolveOutputExpiration(now, tags)
	if got != 0 {
		t.Fatalf("resolveOutputExpiration = %d, want 0 (no expiration)", got)
	}
}

func TestResolveOutputExpiration_InvalidTagFallsBackToConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{batch_types.TagOutputExpiresAfterSeconds: "not-a-number"}

	got := p.resolveOutputExpiration(now, tags)
	want := now + 86400
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (invalid tag should fall back to config)", got, want)
	}
}

func TestResolveOutputExpiration_ZeroTagFallsBackToConfig(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)
	tags := db.Tags{batch_types.TagOutputExpiresAfterSeconds: "0"}

	got := p.resolveOutputExpiration(now, tags)
	want := now + 86400
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (zero tag should fall back to config)", got, want)
	}
}

func TestResolveOutputExpiration_NilTags(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	p := mustNewProcessor(t, cfg, validProcessorClients(t))

	now := int64(1000000)

	got := p.resolveOutputExpiration(now, nil)
	want := now + 86400
	if got != want {
		t.Fatalf("resolveOutputExpiration = %d, want %d (nil tags should fall back to config)", got, want)
	}
}

// --- uploadFileAndStoreFileRecord ---

// --- storeFileRecord ---

func TestStoreOutputFileRecord_Success(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 86400
	fileDB := newMockFileDBClient()
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		FileDB: fileDB,
	})

	ctx := testLoggerCtx(t)
	tags := db.Tags{batch_types.TagOutputExpiresAfterSeconds: "3600"}

	err := p.storeFileRecord(ctx, "file_abc", "output.jsonl", "tenant-1", 1024, tags)
	if err != nil {
		t.Fatalf("storeFileRecord returned error: %v", err)
	}

	items, _, _, err := fileDB.DBGet(ctx, &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{"file_abc"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}

	if items[0].ID != "file_abc" {
		t.Fatalf("stored file ID = %q, want %q", items[0].ID, "file_abc")
	}
	if items[0].Purpose != string(openai.FileObjectPurposeBatchOutput) {
		t.Fatalf("stored purpose = %q, want %q", items[0].Purpose, openai.FileObjectPurposeBatchOutput)
	}
}

func TestStoreFileRecord_NoExpiration_NilExpiresAt(t *testing.T) {
	cfg := config.NewConfig()
	cfg.DefaultOutputExpirationSeconds = 0
	fileDB := newMockFileDBClient()
	p := mustNewProcessor(t, cfg, &clientset.Clientset{
		FileDB: fileDB,
	})

	ctx := testLoggerCtx(t)
	err := p.storeFileRecord(ctx, "file_no_exp", "output.jsonl", "tenant-1", 1024, nil)
	if err != nil {
		t.Fatalf("storeFileRecord: %v", err)
	}

	items, _, _, err := fileDB.DBGet(ctx, &db.FileQuery{BaseQuery: db.BaseQuery{IDs: []string{"file_no_exp"}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: err=%v len=%d", err, len(items))
	}
	fileObj, err := converter.DBItemToFile(items[0])
	if err != nil {
		t.Fatalf("DBItemToFile: %v", err)
	}
	if fileObj.ExpiresAt != nil {
		t.Fatalf("ExpiresAt = %v, want nil when resolveOutputExpiration returns 0", fileObj.ExpiresAt)
	}
}

// TestEnsureFileRecord covers the re-finalization contract of the file-record
// write: a duplicate insert is tolerated only when the existing record is the
// same artifact (same tenant, ID, purpose, filename, and size); anything else
// surfaces the store error.
func TestEnsureFileRecord(t *testing.T) {
	const (
		fileID   = "file_reuse"
		fileName = "output.jsonl"
		tenantID = "tenant-1"
		size     = int64(1024)
	)

	seedRecord := func(bytes int64) db.FileDBClient {
		item, err := converter.FileToDBItem(&openai.FileObject{
			ID:       fileID,
			Filename: fileName,
			Bytes:    bytes,
			Purpose:  openai.FileObjectPurposeBatchOutput,
			Object:   "file",
		}, tenantID, db.Tags{})
		if err != nil {
			t.Fatalf("FileToDBItem: %v", err)
		}
		inner := newMockFileDBClient()
		if err := inner.DBStore(testLoggerCtx(t), item); err != nil {
			t.Fatalf("DBStore file item: %v", err)
		}
		return inner
	}

	storeErr := errors.New("db write failed")
	tests := []struct {
		name    string
		files   db.FileDBClient
		wantErr bool
	}{
		{name: "matching record tolerates the duplicate insert", files: &dbStoreErrFileClient{FileDBClient: seedRecord(size), err: storeErr}},
		{name: "size mismatch surfaces the store error", files: &dbStoreErrFileClient{FileDBClient: seedRecord(2048), err: storeErr}, wantErr: true},
		{name: "missing record surfaces the store error", files: &dbStoreErrFileClient{FileDBClient: newMockFileDBClient(), err: storeErr}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := mustNewProcessor(t, config.NewConfig(), &clientset.Clientset{FileDB: tt.files})

			err := p.ensureFileRecord(testLoggerCtx(t), fileID, fileName, tenantID, size, db.Tags{})
			if tt.wantErr != (err != nil) {
				t.Fatalf("ensureFileRecord error = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
}

// --- failover file-ID preservation tests ---

// --- parallel upload tests ---
