package worker

import (
	"context"
	"errors"
	"testing"

	db "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/semaphore"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// errEpochUpdater is a stub epochProgressUpdater returning a fixed error.
type errEpochUpdater struct{ err error }

func (e errEpochUpdater) UpdateProgressCounts(context.Context, string, int64, *openai.BatchRequestCounts) error {
	return e.err
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
