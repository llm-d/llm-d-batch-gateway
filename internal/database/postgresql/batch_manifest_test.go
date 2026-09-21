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

package postgresql

import (
	"context"
	"errors"
	"testing"

	"github.com/pashagolub/pgxmock/v5"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func testManifest() *api.BatchManifest {
	return &api.BatchManifest{
		BatchID: "batch-1",
		Version: api.BatchManifestVersion,
		Entries: []api.BatchManifestEntry{{
			Ordinal:   0,
			RequestID: "batch_req_stable",
			CustomID:  "request-1",
			ModelID:   "model-a",
			Payload:   []byte(`{"custom_id":"request-1","body":{"model":"model-a"}}`),
		}},
	}
}

func TestActivateResumableBatch(t *testing.T) {
	ctx := context.Background()
	oldStatus := []byte(`{"status":"validating"}`)
	newStatus := []byte(`{"status":"in_progress"}`)

	t.Run("atomically stores manifest and activates batch", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		item.ProcessorID = "processor-0"
		item.Epoch = 7
		item.Status = newStatus

		mock.ExpectQuery("(?s)WITH activated AS.*INSERT INTO batch_manifests").
			WithArgs(newStatus, item.ID, item.ProcessorID, item.Epoch, oldStatus, api.BatchManifestVersion, pgxmock.AnyArg()).
			WillReturnRows(pgxmock.NewRows([]string{"batch_id"}).AddRow(item.ID))

		if err := client.ActivateResumableBatch(ctx, item, oldStatus, testManifest()); err != nil {
			t.Fatalf("ActivateResumableBatch: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("unmet expectations: %v", err)
		}
	})

	t.Run("returns conflict when ownership changed", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()

		item := newTestBatchItem("batch-1", testTenantID)
		item.ProcessorID = "processor-0"
		item.Epoch = 7
		item.Status = newStatus

		mock.ExpectQuery("(?s)WITH activated AS.*INSERT INTO batch_manifests").
			WithArgs(newStatus, item.ID, item.ProcessorID, item.Epoch, oldStatus, api.BatchManifestVersion, pgxmock.AnyArg()).
			WillReturnRows(pgxmock.NewRows([]string{"batch_id"}))

		err := client.ActivateResumableBatch(ctx, item, oldStatus, testManifest())
		if !errors.Is(err, api.ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})

	t.Run("rejects mismatched manifest", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()
		item := newTestBatchItem("batch-2", testTenantID)
		item.ProcessorID = "processor-0"
		if err := client.ActivateResumableBatch(ctx, item, oldStatus, testManifest()); err == nil {
			t.Fatal("expected validation error")
		}
	})
}

func TestGetBatchManifest(t *testing.T) {
	client, mock := newTestBatchClient(t)
	defer mock.Close()

	entries := `[{"ordinal":0,"request_id":"batch_req_stable","custom_id":"request-1","model_id":"model-a","payload":{"custom_id":"request-1"}}]`
	mock.ExpectQuery("SELECT version, entries FROM batch_manifests").
		WithArgs("batch-1").
		WillReturnRows(pgxmock.NewRows([]string{"version", "entries"}).AddRow(api.BatchManifestVersion, []byte(entries)))

	manifest, err := client.GetBatchManifest(context.Background(), "batch-1")
	if err != nil {
		t.Fatalf("GetBatchManifest: %v", err)
	}
	if manifest == nil || len(manifest.Entries) != 1 || manifest.Entries[0].RequestID != "batch_req_stable" {
		t.Fatalf("unexpected manifest: %#v", manifest)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
