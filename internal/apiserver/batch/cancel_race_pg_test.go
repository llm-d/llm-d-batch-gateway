package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/postgresql"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
)

// interleavingQueue runs a concurrent writer between the handler's read and
// its conditional write: PQDelete reports the job as claimed after the writer
// has committed.
type interleavingQueue struct {
	dbapi.BatchPriorityQueueClient
	write func(ctx context.Context, id string) error
	t     *testing.T
}

func (q *interleavingQueue) PQDelete(ctx context.Context, jp *dbapi.BatchJobPriority) (int, error) {
	if err := q.write(ctx, jp.ID); err != nil {
		q.t.Fatalf("interleaved write: %v", err)
	}
	return 0, nil
}

func TestCancelBatchConcurrentWritesPostgres(t *testing.T) {
	url := os.Getenv("TEST_POSTGRES_URL")
	if url == "" {
		t.Skip("TEST_POSTGRES_URL not set")
	}
	ctx := context.Background()
	cfg := &postgresql.PostgreSQLConfig{Url: url}

	batchDB, err := postgresql.NewPostgresBatchDBClient(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPostgresBatchDBClient: %v", err)
	}
	t.Cleanup(func() { _ = batchDB.Close() })
	events, err := postgresql.NewPostgresBatchEventProducer(ctx, cfg)
	if err != nil {
		t.Fatalf("NewPostgresBatchEventProducer: %v", err)
	}
	t.Cleanup(func() { _ = events.Close() })
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool: %v", err)
	}
	t.Cleanup(pool.Close)

	const epoch = int64(3)
	progressCounts := openai.BatchRequestCounts{Total: 10, Completed: 7, Failed: 1}

	tests := []struct {
		name       string
		write      func(ctx context.Context, id string) error
		wantCode   int
		wantStatus openai.BatchStatus
		wantCounts openai.BatchRequestCounts
		wantEvents int
	}{
		{
			name:       "no concurrent write",
			write:      func(context.Context, string) error { return nil },
			wantCode:   http.StatusOK,
			wantStatus: openai.BatchStatusCancelling,
			wantCounts: openai.BatchRequestCounts{Total: 10, Completed: 5},
			wantEvents: 1,
		},
		{
			name: "progress write keeps cancel and counts",
			write: func(ctx context.Context, id string) error {
				counts, err := json.Marshal(progressCounts)
				if err != nil {
					return err
				}
				return batchDB.DBUpdateProgress(ctx, id, epoch, counts)
			},
			wantCode:   http.StatusOK,
			wantStatus: openai.BatchStatusCancelling,
			wantCounts: progressCounts,
			wantEvents: 1,
		},
		{
			name: "epoch bump conflicts",
			write: func(ctx context.Context, id string) error {
				_, err := pool.Exec(ctx, "UPDATE batch_items SET epoch = epoch + 1 WHERE id = $1", id)
				return err
			},
			wantCode:   http.StatusConflict,
			wantStatus: openai.BatchStatusInProgress,
			wantCounts: openai.BatchRequestCounts{Total: 10, Completed: 5},
		},
		{
			name: "completion conflicts without regressing",
			write: func(ctx context.Context, id string) error {
				items, _, _, err := batchDB.DBGet(ctx, &dbapi.BatchQuery{BaseQuery: dbapi.BaseQuery{IDs: []string{id}}}, true, 0, 1)
				if err != nil || len(items) != 1 {
					return fmt.Errorf("read %s: %v (%d items)", id, err, len(items))
				}
				var info openai.BatchStatusInfo
				if err := json.Unmarshal(items[0].Status, &info); err != nil {
					return err
				}
				now := time.Now().UTC().Unix()
				info.Status = openai.BatchStatusCompleted
				info.CompletedAt = &now
				if items[0].Status, err = json.Marshal(info); err != nil {
					return err
				}
				return batchDB.DBUpdate(ctx, items[0], nil)
			},
			wantCode:   http.StatusConflict,
			wantStatus: openai.BatchStatusCompleted,
			wantCounts: openai.BatchRequestCounts{Total: 10, Completed: 5},
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchID := fmt.Sprintf("batch-cancel-pg-%d-%d", time.Now().UnixNano(), i)
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
			item.Epoch = epoch
			if err := batchDB.DBStore(ctx, item); err != nil {
				t.Fatalf("DBStore: %v", err)
			}
			t.Cleanup(func() {
				_, _ = batchDB.DBDelete(ctx, []string{batchID})
				_, _ = pool.Exec(ctx, "DELETE FROM batch_events WHERE job_id = $1", batchID)
			})

			handler, err := NewBatchAPIHandler(&common.ServerConfig{BatchAPI: common.BatchAPIConfig{BatchEventTTLSeconds: common.DefaultBatchEventTTLSeconds}}, &clientset.Clientset{
				BatchDB: batchDB,
				Queue:   &interleavingQueue{write: tt.write, t: t},
				Event:   events,
			})
			if err != nil {
				t.Fatalf("NewBatchAPIHandler: %v", err)
			}

			req := httptest.NewRequest(http.MethodPost, "/v1/batches/"+batchID+"/cancel", nil)
			req.SetPathValue("batch_id", batchID)
			rr := httptest.NewRecorder()
			handler.CancelBatch(rr, req)

			if rr.Code != tt.wantCode {
				t.Fatalf("http %d, want %d (body %s)", rr.Code, tt.wantCode, rr.Body.String())
			}

			items, _, _, err := batchDB.DBGet(ctx, &dbapi.BatchQuery{BaseQuery: dbapi.BaseQuery{IDs: []string{batchID}}}, true, 0, 1)
			if err != nil || len(items) != 1 {
				t.Fatalf("DBGet: %v (%d items)", err, len(items))
			}
			var stored openai.BatchStatusInfo
			if err := json.Unmarshal(items[0].Status, &stored); err != nil {
				t.Fatalf("unmarshal stored status: %v", err)
			}
			if stored.Status != tt.wantStatus {
				t.Errorf("stored status %q, want %q", stored.Status, tt.wantStatus)
			}
			if stored.RequestCounts != tt.wantCounts {
				t.Errorf("stored counts %+v, want %+v", stored.RequestCounts, tt.wantCounts)
			}

			var nEvents int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM batch_events WHERE job_id = $1", batchID).Scan(&nEvents); err != nil {
				t.Fatalf("count events: %v", err)
			}
			if nEvents != tt.wantEvents {
				t.Errorf("cancel events %d, want %d", nEvents, tt.wantEvents)
			}

			if rr.Code != http.StatusOK {
				return
			}
			var resp openai.Batch
			if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if resp.Status != openai.BatchStatusCancelling || resp.RequestCounts != tt.wantCounts {
				t.Errorf("response status %q counts %+v, want cancelling %+v", resp.Status, resp.RequestCounts, tt.wantCounts)
			}
		})
	}
}
