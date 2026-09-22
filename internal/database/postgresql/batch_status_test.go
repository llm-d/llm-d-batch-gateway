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
	"testing"

	"github.com/go-logr/logr"
	"github.com/pashagolub/pgxmock/v5"
)

func newTestStatusClient(t *testing.T) (*PostgresBatchStatusClient, pgxmock.PgxPoolIface) {
	t.Helper()
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock: %v", err)
	}
	return &PostgresBatchStatusClient{pool: mock, logger: logr.Discard()}, mock
}

func TestPostgresBatchStatusClientSetGetDelete(t *testing.T) {
	client, mock := newTestStatusClient(t)
	defer mock.Close()
	payload := []byte(`{"completed":3}`)

	mock.ExpectExec("INSERT INTO batch_status").
		WithArgs("job-1", payload, pgxmock.AnyArg()).
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	if err := client.StatusSet(context.Background(), "job-1", 60, payload); err != nil {
		t.Fatalf("StatusSet: %v", err)
	}

	mock.ExpectQuery("SELECT data FROM batch_status").
		WithArgs("job-1").
		WillReturnRows(pgxmock.NewRows([]string{"data"}).AddRow(payload))
	got, err := client.StatusGet(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("StatusGet: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("StatusGet = %q, want %q", got, payload)
	}

	mock.ExpectExec("DELETE FROM batch_status WHERE job_id").
		WithArgs("job-1").
		WillReturnResult(pgxmock.NewResult("DELETE", 1))
	deleted, err := client.StatusDelete(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("StatusDelete: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestPostgresBatchStatusClientGetExpiredOrMissing(t *testing.T) {
	client, mock := newTestStatusClient(t)
	defer mock.Close()
	mock.ExpectQuery("SELECT data FROM batch_status").
		WithArgs("job-1").
		WillReturnRows(pgxmock.NewRows([]string{"data"}))
	got, err := client.StatusGet(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("StatusGet: %v", err)
	}
	if got != nil {
		t.Fatalf("StatusGet = %q, want nil", got)
	}
}

func TestPostgresBatchStatusClientValidation(t *testing.T) {
	client, mock := newTestStatusClient(t)
	defer mock.Close()
	if err := client.StatusSet(context.Background(), "", 1, []byte("value")); err == nil {
		t.Fatal("StatusSet accepted an empty ID")
	}
	if err := client.StatusSet(context.Background(), "job-1", 1, nil); err == nil {
		t.Fatal("StatusSet accepted empty data")
	}
	if _, err := client.StatusGet(context.Background(), ""); err == nil {
		t.Fatal("StatusGet accepted an empty ID")
	}
	if _, err := client.StatusDelete(context.Background(), ""); err == nil {
		t.Fatal("StatusDelete accepted an empty ID")
	}
}

func TestPostgresBatchStatusClientPurgeExpired(t *testing.T) {
	client, mock := newTestStatusClient(t)
	defer mock.Close()
	mock.ExpectExec("DELETE FROM batch_status").
		WillReturnResult(pgxmock.NewResult("DELETE", 3))
	purged, err := client.purgeExpiredStatuses(context.Background())
	if err != nil {
		t.Fatalf("purgeExpiredStatuses: %v", err)
	}
	if purged != 3 {
		t.Fatalf("purged = %d, want 3", purged)
	}
}
