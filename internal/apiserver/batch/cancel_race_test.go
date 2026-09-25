package batch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

// completingQueue models the owner finishing the job between the handler's
// read and its write: PQDelete reports the job as already claimed and, before
// returning, the owner persists a terminal status.
type completingQueue struct {
	dbapi.BatchPriorityQueueClient
	db dbapi.BatchDBClient
	t  *testing.T
}

func (q *completingQueue) PQDelete(ctx context.Context, jp *dbapi.BatchJobPriority) (int, error) {
	items, _, _, err := q.db.DBGet(ctx, &dbapi.BatchQuery{BaseQuery: dbapi.BaseQuery{IDs: []string{jp.ID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		q.t.Fatalf("completingQueue: read %s: %v (%d items)", jp.ID, err, len(items))
	}
	// Copy: the mock hands out its stored pointer, a real DB row is independent.
	item := *items[0]
	var info openai.BatchStatusInfo
	if err := json.Unmarshal(item.Status, &info); err != nil {
		q.t.Fatalf("completingQueue: unmarshal: %v", err)
	}
	now := time.Now().UTC().Unix()
	info.Status = openai.BatchStatusCompleted
	info.CompletedAt = &now
	item.Status, _ = json.Marshal(info)
	if err := q.db.DBUpdate(ctx, &item, nil); err != nil {
		q.t.Fatalf("completingQueue: owner write: %v", err)
	}
	return 0, nil
}

func TestCancelBatchDoesNotRegressTerminalStatus(t *testing.T) {
	handler := setupTestHandler()
	handler.clients.Queue = &completingQueue{
		BatchPriorityQueueClient: handler.clients.Queue,
		db:                       handler.clients.BatchDB,
		t:                        t,
	}

	batchID := "batch-cancel-vs-complete"
	batch := openai.Batch{
		ID: batchID,
		BatchSpec: openai.BatchSpec{
			Object:           "batch",
			InputFileID:      "file-abc123",
			Endpoint:         openai.EndpointChatCompletions,
			CompletionWindow: "24h",
			CreatedAt:        time.Now().UTC().Unix(),
		},
		BatchStatusInfo: openai.BatchStatusInfo{
			Status:        openai.BatchStatusInProgress,
			RequestCounts: openai.BatchRequestCounts{Total: 10, Completed: 5},
		},
	}
	item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, nil)
	if err != nil {
		t.Fatalf("BatchToDBItem: %v", err)
	}
	item.ProcessorID = "processor-0"
	item.Epoch = 3
	if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
	req.SetPathValue("batch_id", batchID)
	rr := httptest.NewRecorder()
	handler.CancelBatch(rr, req)

	items, _, _, err := handler.clients.BatchDB.DBGet(context.Background(),
		&dbapi.BatchQuery{BaseQuery: dbapi.BaseQuery{IDs: []string{batchID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: %v (%d items)", err, len(items))
	}
	var stored openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &stored); err != nil {
		t.Fatalf("unmarshal stored status: %v", err)
	}
	if stored.Status != openai.BatchStatusCompleted {
		t.Errorf("stored status regressed from completed to %q (http %d, body %s)", stored.Status, rr.Code, rr.Body.String())
	}

	var resp openai.Batch
	if rr.Code == http.StatusOK {
		if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if resp.Status == openai.BatchStatusCancelling {
			t.Errorf("handler reported cancelling for a batch that had already completed")
		}
	}
}

func TestCancelBatchDoesNotMutateQueuedResumableBatch(t *testing.T) {
	handler := setupTestHandler()
	batchID := "batch-cancel-resumable"
	batch := openai.Batch{
		ID: batchID,
		BatchSpec: openai.BatchSpec{
			Object:           "batch",
			InputFileID:      "file-abc123",
			Endpoint:         openai.EndpointChatCompletions,
			CompletionWindow: "24h",
			CreatedAt:        time.Now().UTC().Unix(),
		},
		BatchStatusInfo: openai.BatchStatusInfo{Status: openai.BatchStatusValidating},
	}
	item, err := converter.BatchToDBItem(&batch, common.DefaultTenantID, nil)
	if err != nil {
		t.Fatalf("BatchToDBItem: %v", err)
	}
	item.Resumable = true
	if err := handler.clients.BatchDB.DBStore(context.Background(), item); err != nil {
		t.Fatalf("DBStore: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
	req.SetPathValue("batch_id", batchID)
	rr := httptest.NewRecorder()
	handler.CancelBatch(rr, req)
	if rr.Code != http.StatusConflict {
		t.Fatalf("expected status %d, got %d: %s", http.StatusConflict, rr.Code, rr.Body.String())
	}

	items, _, _, err := handler.clients.BatchDB.DBGet(context.Background(),
		&dbapi.BatchQuery{BaseQuery: dbapi.BaseQuery{IDs: []string{batchID}}}, true, 0, 1)
	if err != nil || len(items) != 1 {
		t.Fatalf("DBGet: %v (%d items)", err, len(items))
	}
	var stored openai.BatchStatusInfo
	if err := json.Unmarshal(items[0].Status, &stored); err != nil {
		t.Fatalf("unmarshal stored status: %v", err)
	}
	if stored.Status != openai.BatchStatusValidating {
		t.Errorf("stored status changed to %q", stored.Status)
	}
}
