package worker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/batchctx"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/semaphore"
	httpclient "github.com/llm-d/llm-d-batch-gateway/pkg/clients/http"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// errEpochUpdater is a stub epochProgressUpdater returning a fixed error.
type errEpochUpdater struct{ err error }

func (e errEpochUpdater) UpdateProgressCounts(context.Context, string, int64, *openai.BatchRequestCounts) error {
	return e.err
}

func (e errEpochUpdater) CheckJobStatus(context.Context, string, int64) (openai.BatchStatus, error) {
	return "", e.err
}

// errGetBatchDB is a BatchProgressDBClient whose DBGet always returns err.
type errGetBatchDB struct {
	db.BatchProgressDBClient
	err error
}

func (d *errGetBatchDB) DBGet(context.Context, *db.BatchQuery, bool, int, int) ([]*db.BatchItem, int, bool, error) {
	return nil, 0, false, d.err
}

// progressSpyDB counts the fenced progress writes that reach the database.
type progressSpyDB struct {
	db.BatchProgressDBClient
	writes atomic.Int32
}

func (d *progressSpyDB) DBUpdateProgress(ctx context.Context, id string, epoch int64, countsJSON []byte) error {
	defer d.writes.Add(1)
	return d.BatchProgressDBClient.DBUpdateProgress(ctx, id, epoch, countsJSON)
}

func TestJobProgressUpdater_FencedOutSignalsOwnershipLost(t *testing.T) {
	// A fenced-out progress write (ErrConflict) signals ownership loss via
	// onFencedOut and is not propagated to the progress tracker.
	var fencedOut bool
	u := jobProgressUpdater{
		inner:       errEpochUpdater{err: db.ErrConflict},
		jobID:       "job-1",
		epoch:       5,
		onFencedOut: func() { fencedOut = true },
	}
	if err := u.UpdateProgressCounts(context.Background(), "job-1", &openai.BatchRequestCounts{Total: 1}); err != nil {
		t.Fatalf("fenced-out write should not propagate to the tracker, got %v", err)
	}
	if !fencedOut {
		t.Fatal("expected onFencedOut to be called on ErrConflict")
	}

	// A non-conflict error propagates unchanged and does not trip onFencedOut.
	boom := errors.New("boom")
	tripped := false
	u2 := jobProgressUpdater{
		inner:       errEpochUpdater{err: boom},
		jobID:       "job-1",
		epoch:       5,
		onFencedOut: func() { tripped = true },
	}
	if err := u2.UpdateProgressCounts(context.Background(), "job-1", &openai.BatchRequestCounts{Total: 1}); !errors.Is(err, boom) {
		t.Fatalf("expected non-conflict error to propagate, got %v", err)
	}
	if tripped {
		t.Fatal("onFencedOut must not be called for a non-conflict error")
	}

	// A nil onFencedOut is safe to skip.
	u3 := jobProgressUpdater{inner: errEpochUpdater{err: db.ErrConflict}, jobID: "job-1", epoch: 5}
	if err := u3.UpdateProgressCounts(context.Background(), "job-1", &openai.BatchRequestCounts{Total: 1}); err != nil {
		t.Fatalf("nil onFencedOut with ErrConflict should not error, got %v", err)
	}
}

func TestJobProgressUpdater_CheckJobStatus(t *testing.T) {
	const (
		jobID = "job-1"
		epoch = int64(3)
	)
	row := func(t *testing.T, epoch int64, status openai.BatchStatus) *db.BatchItem {
		return &db.BatchItem{
			BaseIndexes:  db.BaseIndexes{ID: jobID, TenantID: "tenant-1"},
			BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: status})},
			Epoch:        epoch,
		}
	}

	tests := []struct {
		name           string
		stored         func(t *testing.T) *db.BatchItem // nil: no row
		readErr        error
		wantErr        bool
		wantFencedOut  bool
		wantCancelling bool
	}{
		{
			name:   "owned and in progress keeps running",
			stored: func(t *testing.T) *db.BatchItem { return row(t, epoch, openai.BatchStatusInProgress) },
		},
		{
			name:          "epoch bumped by a reclaimer signals ownership lost",
			stored:        func(t *testing.T) *db.BatchItem { return row(t, epoch+1, openai.BatchStatusInProgress) },
			wantFencedOut: true,
		},
		{
			name:          "missing row signals ownership lost",
			wantFencedOut: true,
		},
		{
			name:          "terminal status signals ownership lost",
			stored:        func(t *testing.T) *db.BatchItem { return row(t, epoch, openai.BatchStatusFailed) },
			wantFencedOut: true,
		},
		{
			name:           "cancelling status signals a user cancel",
			stored:         func(t *testing.T) *db.BatchItem { return row(t, epoch, openai.BatchStatusCancelling) },
			wantCancelling: true,
		},
		{
			name:          "cancelling under a newer epoch is left to the new owner",
			stored:        func(t *testing.T) *db.BatchItem { return row(t, epoch+1, openai.BatchStatusCancelling) },
			wantFencedOut: true,
		},
		{
			name:    "transient read error is returned and keeps the job running",
			stored:  func(t *testing.T) *db.BatchItem { return row(t, epoch, openai.BatchStatusInProgress) },
			readErr: errors.New("connection reset"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			batchDB := newMockBatchDBClient()
			if tt.stored != nil {
				if err := batchDB.DBStore(context.Background(), tt.stored(t)); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			if tt.readErr != nil {
				batchDB = &errGetBatchDB{BatchProgressDBClient: batchDB, err: tt.readErr}
			}

			var fencedOut, cancelling bool
			u := jobProgressUpdater{
				inner:        NewStatusUpdater(batchDB),
				jobID:        jobID,
				epoch:        epoch,
				onFencedOut:  func() { fencedOut = true },
				onCancelling: func() { cancelling = true },
			}
			err := u.CheckJobStatus(context.Background(), jobID)
			if tt.wantErr != (err != nil) {
				t.Fatalf("CheckJobStatus() error = %v, wantErr %v", err, tt.wantErr)
			}
			if fencedOut != tt.wantFencedOut {
				t.Errorf("onFencedOut called = %v, want %v", fencedOut, tt.wantFencedOut)
			}
			if cancelling != tt.wantCancelling {
				t.Errorf("onCancelling called = %v, want %v", cancelling, tt.wantCancelling)
			}
		})
	}
}

// TestExecuteJob_QuietIntervalStatusChange verifies that a job with a long
// running request and no completions still stops when its row changes: the
// progress tracker checks the row on every interval, not only after a write.
func TestExecuteJob_QuietIntervalStatusChange(t *testing.T) {
	const epoch = int64(1)

	tests := []struct {
		name string
		// change edits the stored row while no counts are pending.
		change    func(t *testing.T, item *db.BatchItem)
		wantCause error // nil: ownership lost (neutral cause)
	}{
		{
			name:   "epoch bump aborts the job as lost ownership",
			change: func(_ *testing.T, item *db.BatchItem) { item.Epoch++ },
		},
		{
			name: "cancelling status aborts the job as a user cancel",
			change: func(t *testing.T, item *db.BatchItem) {
				item.Status = mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusCancelling})
			},
			wantCause: batchctx.ErrCancelled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := config.NewConfig()
			cfg.WorkDir = t.TempDir()
			cfg.ProgressUpdateInterval = 20 * time.Millisecond

			inferStarted := make(chan struct{})
			mock := &mockInferenceClient{
				generateFn: func(ctx context.Context, _ *inference.GenerateRequest) (*inference.GenerateResponse, *inference.ClientError) {
					close(inferStarted)
					<-ctx.Done()
					return nil, &inference.ClientError{
						Category: httpclient.ErrCategoryServer,
						Message:  "context cancelled",
						RawError: ctx.Err(),
					}
				},
			}
			requests := []batch_types.Request{
				{CustomID: "a", Method: "POST", URL: "/v1/chat/completions", Body: map[string]interface{}{"model": "m1"}},
			}
			env, jobInfo := setupExecutionJob(t, cfg, mock, requests, map[string]string{"m1": "m1"})

			newRow := func() *db.BatchItem {
				return &db.BatchItem{
					BaseIndexes:  db.BaseIndexes{ID: jobInfo.JobID, TenantID: jobInfo.TenantID},
					BaseContents: db.BaseContents{Status: mustJSON(t, openai.BatchStatusInfo{Status: openai.BatchStatusInProgress})},
					Epoch:        epoch,
				}
			}
			if err := env.dbClient.DBStore(context.Background(), newRow()); err != nil {
				t.Fatalf("seed: %v", err)
			}
			spy := &progressSpyDB{BatchProgressDBClient: env.dbClient}

			// Mirror runJob's abort wiring.
			ctx, abort := context.WithCancelCause(testLoggerCtx(t))
			params := &jobExecutionParams{
				updater:         NewStatusUpdater(spy),
				jobItem:         newRow(),
				jobInfo:         jobInfo,
				cancelUser:      func() { abort(batchctx.ErrCancelled) },
				onOwnershipLost: func() { abort(context.Canceled) },
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = env.p.executeJob(ctx, params)
			}()
			// Never leave the job running (and logging) past the test.
			defer func() {
				abort(context.Canceled)
				<-done
			}()

			<-inferStarted
			// The first tick flushes the initial counts. Change the row after
			// it, so no further result or write would reveal the change.
			deadline := time.Now().Add(5 * time.Second)
			for spy.writes.Load() == 0 {
				if time.Now().After(deadline) {
					t.Fatal("initial progress write did not happen")
				}
				time.Sleep(5 * time.Millisecond)
			}
			changed := newRow()
			tt.change(t, changed)
			if err := env.dbClient.DBStore(context.Background(), changed); err != nil {
				t.Fatalf("change row: %v", err)
			}

			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("job kept running after its row changed in a quiet interval")
			}
			if ctx.Err() == nil {
				t.Fatal("job returned without being aborted")
			}
			if got := batchctx.Cause(ctx); !errors.Is(got, tt.wantCause) {
				t.Fatalf("abort cause = %v, want %v", got, tt.wantCause)
			}
		})
	}
}

func TestBuildAIMDModels(t *testing.T) {
	// Tenant-scoped gateway config, as produced when route_key_method is
	// "tenant": only "<tenantID>/<modelID>" entries exist in the resolver.
	scopedResolver, err := inference.NewPerModelResolver(
		map[string]inference.GatewayClientConfig{
			"tenant-a/m1": {URL: "http://fake-a:8000"},
			"tenant-b/m1": {URL: "http://fake-b:8000"},
		},
		testLogger(t),
	)
	if err != nil {
		t.Fatalf("NewPerModelResolver: %v", err)
	}
	defer func() { _ = scopedResolver.Close() }()

	bareResolver, err := inference.NewPerModelResolver(
		map[string]inference.GatewayClientConfig{
			"m1": {URL: "http://fake:8000"},
		},
		testLogger(t),
	)
	if err != nil {
		t.Fatalf("NewPerModelResolver: %v", err)
	}
	defer func() { _ = bareResolver.Close() }()

	limitsFor := func(t *testing.T, resolver *inference.GatewayResolver) map[inference.InferenceClient]*endpointLimit {
		t.Helper()
		limits := make(map[inference.InferenceClient]*endpointLimit)
		for _, client := range resolver.Clients() {
			sem, err := semaphore.NewAdaptive(2, nil)
			if err != nil {
				t.Fatalf("endpoint semaphore: %v", err)
			}
			limits[client] = &endpointLimit{sem: sem, label: resolver.ClientLabel(client)}
		}
		return limits
	}

	modelMap := &modelMapFile{SafeToModel: map[string]string{"m1": "m1"}, LineCount: 1}

	tests := []struct {
		name      string
		resolver  *inference.GatewayResolver
		method    config.RouteKeyMethod
		tenantID  string
		wantKeys  []string
		wantEmpty bool
	}{
		{
			name:     "tenant method registers the scoped key so dispatch finds the endpoint",
			resolver: scopedResolver,
			method:   config.RouteKeyMethodTenant,
			tenantID: "tenant-a",
			wantKeys: []string{"tenant-a/m1"},
		},
		{
			name:      "bare method against scoped config registers nothing (guards the reported regression)",
			resolver:  scopedResolver,
			method:    config.RouteKeyMethodBare,
			tenantID:  "tenant-a",
			wantEmpty: true,
		},
		{
			name:     "bare method against bare config keeps the default behavior",
			resolver: bareResolver,
			method:   config.RouteKeyMethodBare,
			tenantID: "tenant-a",
			wantKeys: []string{"m1"},
		},
		{
			name:     "tenant method ignores endpoints of other tenants",
			resolver: scopedResolver,
			method:   config.RouteKeyMethodTenant,
			tenantID: "tenant-b",
			wantKeys: []string{"tenant-b/m1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			models := buildAIMDModels(modelMap, tt.resolver, limitsFor(t, tt.resolver), tt.method, tt.tenantID)
			if tt.wantEmpty {
				if len(models) != 0 {
					t.Fatalf("buildAIMDModels() = %v keys, want empty", mapKeys(models))
				}
				return
			}
			if len(models) != len(tt.wantKeys) {
				t.Fatalf("buildAIMDModels() = %v keys, want %v", mapKeys(models), tt.wantKeys)
			}
			for _, key := range tt.wantKeys {
				ep := models[key]
				if ep == nil {
					t.Fatalf("buildAIMDModels() missing key %q (per-endpoint limiting would be silently disabled)", key)
					return
				}
				if ep.Sem == nil {
					t.Fatalf("buildAIMDModels()[%q].Sem is nil", key)
					return
				}
			}
		})
	}
}

func mapKeys(m map[string]*pipeline.EndpointAIMD) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
