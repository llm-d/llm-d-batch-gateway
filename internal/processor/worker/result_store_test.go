package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	mockdb "github.com/llm-d/llm-d-batch-gateway/internal/database/mock"
	filesapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	mockfiles "github.com/llm-d/llm-d-batch-gateway/internal/files_store/mock"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
)

const (
	rsBatchID  = "batch-rs"
	rsTenantID = "tenant__tenantA"
)

// hookFilesClient wraps a files client and invokes onStore after each
// successful Store, letting tests interleave with the sink's object writes.
type hookFilesClient struct {
	filesapi.BatchFilesClient
	onStore func(fileName string)
}

func (c *hookFilesClient) Store(ctx context.Context, fileName, folderName string, fileSizeLimit, lineNumLimit int64, reader io.Reader) (*filesapi.BatchFileMetadata, error) {
	md, err := c.BatchFilesClient.Store(ctx, fileName, folderName, fileSizeLimit, lineNumLimit, reader)
	if err == nil && c.onStore != nil {
		c.onStore(fileName)
	}
	return md, err
}

type resultStoreEnv struct {
	store  *mockdb.MockResumableStore
	files  *mockfiles.MockBatchFilesClient
	sink   *batchResultStore
	root   string
	folder string
}

func newResultStoreEnv(t *testing.T, entries ...db.ResumableActivationEntry) *resultStoreEnv {
	t.Helper()

	root := t.TempDir()
	env := &resultStoreEnv{store: mockdb.NewMockResumableStore(), files: mockfiles.NewMockBatchFilesClient(root), root: root}
	env.store.SeedBatch(rsBatchID, "proc-1", 1)
	if len(entries) > 0 {
		if err := env.store.ActivateResumableBatch(testLoggerCtx(t), &db.ResumableActivation{
			BatchID:     rsBatchID,
			ProcessorID: "proc-1",
			Epoch:       1,
			LeaseTTL:    time.Minute,
			Entries:     entries,
		}); err != nil {
			t.Fatalf("ActivateResumableBatch: %v", err)
		}
	}

	folder, err := ucom.GetFolderNameByTenantID(rsTenantID)
	if err != nil {
		t.Fatalf("GetFolderNameByTenantID: %v", err)
	}
	env.folder = folder

	env.sink, err = newBatchResultStore(env.files, env.store, rsBatchID, rsTenantID, "proc-1", 1)
	if err != nil {
		t.Fatalf("newBatchResultStore: %v", err)
	}
	return env
}

func pendingActivationEntry(requestID string, ordinal int64) db.ResumableActivationEntry {
	return db.ResumableActivationEntry{
		RequestID: requestID,
		Ordinal:   ordinal,
		CustomID:  "c-" + requestID,
		Status:    string(db.ResumableEntryPending),
	}
}

func rsSuccessItem(id string) pipeline.ResultItem {
	return pipeline.ResultItem{
		RequestID: id,
		CustomID:  "c-" + id,
		Response:  &batch_types.ResponseData{StatusCode: 200, RequestID: id, Body: map[string]any{"id": id}},
	}
}

// rsCheckpoint marshals msg and checkpoints it through the sink.
func rsCheckpoint(ctx context.Context, sink *batchResultStore, msg pipeline.ResultItem) (bool, error) {
	line, digest, size, err := pipeline.MarshalResultLine(&msg)
	if err != nil {
		return false, fmt.Errorf("MarshalResultLine: %w", err)
	}
	return sink.Checkpoint(ctx, &msg, line, digest, size)
}

// rsStoredObjects returns every object stored under the env's tenant folder.
func (env *resultStoreEnv) rsStoredObjects(t *testing.T) []string {
	t.Helper()
	var objects []string
	root := filepath.Join(env.root, env.folder)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			rel, _ := filepath.Rel(env.root, path)
			objects = append(objects, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk tenant folder: %v", err)
	}
	return objects
}

func TestBatchResultStore_Checkpoint(t *testing.T) {
	ctx := testLoggerCtx(t)

	t.Run("commits a completed entry with the canonical key", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))

		committed, err := rsCheckpoint(ctx, env.sink, rsSuccessItem("req-1"))
		if err != nil || !committed {
			t.Fatalf("Checkpoint = (%v, %v), want (true, nil)", committed, err)
		}

		entry, err := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if err != nil {
			t.Fatalf("FindEntryByRequestID: %v", err)
		}
		key := resumableResultKey(rsBatchID, "req-1")
		if entry.Status != db.ResumableEntryCompleted || entry.ResultKey != key || entry.ErrorCode != "" {
			t.Fatalf("entry after commit: %+v", entry)
		}

		msg := rsSuccessItem("req-1")
		line, digest, size, _ := pipeline.MarshalResultLine(&msg)
		if entry.ResultBytes != size || entry.ResultSHA256 != digest {
			t.Fatalf("entry metadata = (%d, %s), want (%d, %s)", entry.ResultBytes, entry.ResultSHA256, size, digest)
		}

		reader, _, err := env.files.Retrieve(ctx, key, env.folder)
		if err != nil {
			t.Fatalf("Retrieve canonical object: %v", err)
		}
		defer reader.Close()
		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, line) {
			t.Fatalf("stored object = %s, want the marshaled line", data)
		}
	})

	t.Run("commits a failed entry with the error code", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))

		msg := rsSuccessItem("req-1")
		msg.Response = nil
		msg.Error = &pipeline.OutputError{Code: "server_error", Message: "boom"}

		committed, err := rsCheckpoint(ctx, env.sink, msg)
		if err != nil || !committed {
			t.Fatalf("Checkpoint = (%v, %v), want (true, nil)", committed, err)
		}

		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if entry.Status != db.ResumableEntryFailed || entry.ErrorCode != "server_error" {
			t.Fatalf("entry after commit: %+v", entry)
		}
	})

	t.Run("commits a non-200 response without an error code", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))

		msg := rsSuccessItem("req-1")
		msg.Response.StatusCode = 500

		if _, err := rsCheckpoint(ctx, env.sink, msg); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}

		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if entry.Status != db.ResumableEntryFailed || entry.ErrorCode != "" {
			t.Fatalf("entry after commit: %+v", entry)
		}
	})

	t.Run("commits a 2xx response other than 200 as completed", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))

		// A 201 is a successful response: it must be checkpointed with
		// completed accounting, not inflate request_counts.failed.
		msg := rsSuccessItem("req-1")
		msg.Response.StatusCode = 201

		if _, err := rsCheckpoint(ctx, env.sink, msg); err != nil {
			t.Fatalf("Checkpoint: %v", err)
		}

		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if entry.Status != db.ResumableEntryCompleted || entry.ErrorCode != "" {
			t.Fatalf("entry after commit: %+v, want completed without an error code", entry)
		}
	})

	t.Run("duplicate delivery of a terminal entry drops without a PUT", func(t *testing.T) {
		rejected := db.ResumableActivationEntry{
			RequestID:    "req-r",
			Ordinal:      0,
			CustomID:     "c-req-r",
			Status:       string(db.ResumableEntryFailed),
			ErrorCode:    "model_not_found",
			ResultKey:    resumableResultKey(rsBatchID, "req-r"),
			ResultBytes:  42,
			ResultSHA256: "abc",
		}
		env := newResultStoreEnv(t, rejected)

		committed, err := rsCheckpoint(ctx, env.sink, rsSuccessItem("req-r"))
		if err != nil || committed {
			t.Fatalf("Checkpoint = (%v, %v), want (false, nil)", committed, err)
		}

		// No object was written and the activation-time metadata survived.
		if _, _, err := env.files.Retrieve(ctx, rejected.ResultKey, env.folder); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Retrieve after duplicate drop = %v, want not-exist", err)
		}
		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-r")
		if entry.ErrorCode != "model_not_found" || entry.ResultBytes != 42 || entry.ResultSHA256 != "abc" {
			t.Fatalf("terminal entry was modified: %+v", entry)
		}
	})

	t.Run("own retry reuses the canonical object", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))
		msg := rsSuccessItem("req-1")

		// The object left behind by an attempt that crashed before commit.
		line, digest, size, _ := pipeline.MarshalResultLine(&msg)
		key := resumableResultKey(rsBatchID, "req-1")
		if _, err := env.files.Store(ctx, key, env.folder, 0, 0, bytes.NewReader(line)); err != nil {
			t.Fatalf("seed canonical object: %v", err)
		}

		committed, err := rsCheckpoint(ctx, env.sink, msg)
		if err != nil || !committed {
			t.Fatalf("Checkpoint = (%v, %v), want (true, nil)", committed, err)
		}

		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if entry.ResultKey != key || entry.ResultBytes != size || entry.ResultSHA256 != digest {
			t.Fatalf("entry after commit: %+v", entry)
		}
		if objects := env.rsStoredObjects(t); len(objects) != 1 || objects[0] != filepath.Join(env.folder, key) {
			t.Fatalf("stored objects = %v, want only the canonical key", objects)
		}
	})

	t.Run("foreign object at the canonical key falls back to a unique key", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))
		msg := rsSuccessItem("req-1")
		line, digest, size, _ := pipeline.MarshalResultLine(&msg)

		// A retried request produced a different result: the object at the
		// canonical key no longer matches the line being checkpointed.
		key := resumableResultKey(rsBatchID, "req-1")
		if _, err := env.files.Store(ctx, key, env.folder, 0, 0, bytes.NewReader([]byte("earlier result\n"))); err != nil {
			t.Fatalf("seed canonical object: %v", err)
		}

		committed, err := rsCheckpoint(ctx, env.sink, msg)
		if err != nil || !committed {
			t.Fatalf("Checkpoint = (%v, %v), want (true, nil)", committed, err)
		}

		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if entry.ResultKey == key {
			t.Fatal("entry kept the canonical key despite content mismatch")
		}
		if entry.ResultBytes != size || entry.ResultSHA256 != digest {
			t.Fatalf("entry metadata = (%d, %s), want (%d, %s)", entry.ResultBytes, entry.ResultSHA256, size, digest)
		}

		reader, _, err := env.files.Retrieve(ctx, entry.ResultKey, env.folder)
		if err != nil {
			t.Fatalf("Retrieve fallback object: %v", err)
		}
		defer reader.Close()
		data, _ := io.ReadAll(reader)
		if !bytes.Equal(data, line) {
			t.Fatalf("fallback object = %s, want the marshaled line", data)
		}
	})

	t.Run("lost race deletes the fallback object", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))
		msg := rsSuccessItem("req-1")

		key := resumableResultKey(rsBatchID, "req-1")
		if _, err := env.files.Store(ctx, key, env.folder, 0, 0, bytes.NewReader([]byte("earlier result\n"))); err != nil {
			t.Fatalf("seed canonical object: %v", err)
		}

		// While this delivery writes its fallback object, a concurrent
		// delivery of the same request wins the commit.
		hook := &hookFilesClient{BatchFilesClient: env.files}
		// The canonical key is taken, so the hook fires exactly once — when
		// the fallback object lands — while the entry is still updatable.
		hook.onStore = func(string) {
			_, err := env.store.CheckpointEntryResult(ctx, rsBatchID, "req-1", "proc-1", 1, &db.ResumableResult{
				Status:       db.ResumableEntryCompleted,
				ResultKey:    "winner-key",
				ResultBytes:  1,
				ResultSHA256: "winner",
			})
			if err != nil {
				t.Errorf("simulated winner commit: %v", err)
			}
		}
		sink, err := newBatchResultStore(hook, env.store, rsBatchID, rsTenantID, "proc-1", 1)
		if err != nil {
			t.Fatalf("newBatchResultStore: %v", err)
		}

		committed, err := rsCheckpoint(ctx, sink, msg)
		if err != nil || committed {
			t.Fatalf("Checkpoint = (%v, %v), want (false, nil)", committed, err)
		}

		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if entry.ResultKey != "winner-key" {
			t.Fatalf("entry after lost race: %+v", entry)
		}

		// The fallback object was cleaned up; only the seeded canonical
		// object remains.
		objects := env.rsStoredObjects(t)
		if len(objects) != 1 || objects[0] != filepath.Join(env.folder, key) {
			t.Fatalf("stored objects = %v, want only the pre-existing canonical object", objects)
		}
	})

	t.Run("concurrent duplicates: one commit wins, the loser drops", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))
		msg := rsSuccessItem("req-1")

		const deliveries = 2
		results := make([]bool, deliveries)
		errs := make([]error, deliveries)
		var wg sync.WaitGroup
		for i := range deliveries {
			wg.Add(1)
			go func() {
				defer wg.Done()
				results[i], errs[i] = rsCheckpoint(ctx, env.sink, msg)
			}()
		}
		wg.Wait()

		won := 0
		for i := range deliveries {
			if errs[i] != nil {
				t.Fatalf("delivery %d: %v", i, errs[i])
			}
			if results[i] {
				won++
			}
		}
		if won != 1 {
			t.Fatalf("winning deliveries = %d, want 1", won)
		}

		key := resumableResultKey(rsBatchID, "req-1")
		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if entry.Status != db.ResumableEntryCompleted || entry.ResultKey != key {
			t.Fatalf("entry after concurrent duplicates: %+v", entry)
		}
		if objects := env.rsStoredObjects(t); len(objects) != 1 {
			t.Fatalf("stored objects = %v, want exactly one", objects)
		}
	})

	t.Run("stale epoch surfaces ErrConflict without committing", func(t *testing.T) {
		env := newResultStoreEnv(t, pendingActivationEntry("req-1", 0))
		sink, err := newBatchResultStore(env.files, env.store, rsBatchID, rsTenantID, "proc-1", 2)
		if err != nil {
			t.Fatalf("newBatchResultStore: %v", err)
		}

		_, err = rsCheckpoint(ctx, sink, rsSuccessItem("req-1"))
		if !errors.Is(err, db.ErrConflict) {
			t.Fatalf("Checkpoint error = %v, want db.ErrConflict", err)
		}

		entry, _ := env.store.FindEntryByRequestID(ctx, rsBatchID, "req-1")
		if entry.Status != db.ResumableEntryPending || entry.ResultKey != "" {
			t.Fatalf("entry after conflict: %+v", entry)
		}
	})
}
