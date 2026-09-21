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

func TestFinalizeResumableBatch(t *testing.T) {
	oldStatus := []byte(`{"status":"finalizing"}`)
	newStatus := []byte(`{"status":"completed","output_file_id":"file_stable"}`)
	batch := &api.BatchItem{
		BaseIndexes:  api.BaseIndexes{ID: "batch-1"},
		BaseContents: api.BaseContents{Status: newStatus},
		Epoch:        7,
	}

	t.Run("publishes terminal state and clears ownership", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()
		mock.ExpectExec("UPDATE batch_items").
			WithArgs(batch.ID, batch.Epoch, oldStatus, newStatus).
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		if err := client.FinalizeResumableBatch(context.Background(), batch, oldStatus); err != nil {
			t.Fatalf("FinalizeResumableBatch: %v", err)
		}
	})

	t.Run("accepts an identical already committed state", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()
		mock.ExpectExec("UPDATE batch_items").
			WithArgs(batch.ID, batch.Epoch, oldStatus, newStatus).
			WillReturnResult(pgxmock.NewResult("UPDATE", 0))
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(batch.ID, batch.Epoch, newStatus).
			WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
		if err := client.FinalizeResumableBatch(context.Background(), batch, oldStatus); err != nil {
			t.Fatalf("FinalizeResumableBatch retry: %v", err)
		}
	})

	t.Run("rejects a different owner or terminal value", func(t *testing.T) {
		client, mock := newTestBatchClient(t)
		defer mock.Close()
		mock.ExpectExec("UPDATE batch_items").
			WithArgs(batch.ID, batch.Epoch, oldStatus, newStatus).
			WillReturnResult(pgxmock.NewResult("UPDATE", 0))
		mock.ExpectQuery("SELECT EXISTS").
			WithArgs(batch.ID, batch.Epoch, newStatus).
			WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
		if err := client.FinalizeResumableBatch(context.Background(), batch, oldStatus); !errors.Is(err, api.ErrConflict) {
			t.Fatalf("expected ErrConflict, got %v", err)
		}
	})
}
