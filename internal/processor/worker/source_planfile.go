package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/uuid"

	"github.com/llm-d/llm-d-batch-gateway/internal/processor/config"
	"github.com/llm-d/llm-d-batch-gateway/internal/processor/pipeline"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/pkg/clients/inference"
)

// PlanFileSource reads plan files and input JSONL to produce RequestItems.
type PlanFileSource struct {
	inputFile          *os.File
	plansDir           string
	modelMap           *modelMapFile
	resolver           *inference.GatewayResolver
	cfg                *config.ProcessorConfig
	passThroughHeaders map[string]string
	sloDeadline        time.Time
	tenantID           string
	batchID            string
	skip               func(requestID string) bool
	// recover holds request IDs whose results may still be recoverable from
	// a previous owner's async result route: they are pre-stored into the
	// job's pending set immediately and their submission is deferred by
	// recoveryWindow so the stranded delivery can be checkpointed instead
	// of re-running inference.
	recover        map[string]struct{}
	recoveryWindow time.Duration
	preStore       func(pipeline.RequestItem)
	// stillPending reports whether a held request is still awaiting its
	// result; the recovery window ends early once it returns false for
	// every held entry (nil waits the full window).
	stillPending func(requestID string) bool
	logger       logr.Logger
}

var _ pipeline.RequestSource = (*PlanFileSource)(nil)

type PlanFileSourceConfig struct {
	InputFile          *os.File
	PlansDir           string
	ModelMap           *modelMapFile
	Resolver           *inference.GatewayResolver
	Cfg                *config.ProcessorConfig
	PassThroughHeaders map[string]string
	SLODeadline        time.Time
	TenantID           string
	// BatchID, when set, derives stable request IDs so the durable
	// pipeline's dispatched IDs match the activation entry set across
	// retries. The legacy pipeline leaves it empty and keeps per-run
	// random IDs.
	BatchID string
	// Skip, when set, drops requests whose request ID is terminal in the
	// entry store — a resumed batch must not re-dispatch them.
	Skip func(requestID string) bool
	// Recover, when set, holds the request IDs of in-flight entries a
	// previous owner submitted. Their results arrive on that owner's
	// per-processor result route (recovered by the job's broadcaster group),
	// so they are pre-stored and submitted only after RecoveryWindow
	// elapses without a delivery — the fallback for a lost result.
	Recover        map[string]struct{}
	RecoveryWindow time.Duration
	// PreStore registers a recovering request in the job's pending set
	// before any delivery can arrive, so the per-request routing finds its
	// owner from the start.
	PreStore func(pipeline.RequestItem)
	// StillPending reports whether a recovering request is still awaiting
	// its result (the job's pending set). The recovery window exits early
	// once no held entry is still pending — every stranded delivery was
	// received and checkpointed, and the fence drops the held items as
	// terminal duplicates — so a fully recovered batch does not wait out
	// the window. Nil always waits the full window.
	StillPending func(requestID string) bool
	Logger       logr.Logger
}

func NewPlanFileSource(cfg PlanFileSourceConfig) *PlanFileSource {
	return &PlanFileSource{
		inputFile:          cfg.InputFile,
		plansDir:           cfg.PlansDir,
		modelMap:           cfg.ModelMap,
		resolver:           cfg.Resolver,
		cfg:                cfg.Cfg,
		passThroughHeaders: cfg.PassThroughHeaders,
		sloDeadline:        cfg.SLODeadline,
		tenantID:           cfg.TenantID,
		batchID:            cfg.BatchID,
		skip:               cfg.Skip,
		recover:            cfg.Recover,
		recoveryWindow:     cfg.RecoveryWindow,
		preStore:           cfg.PreStore,
		stillPending:       cfg.StillPending,
		logger:             cfg.Logger,
	}
}

// PreStoreRecovering stores every recovering entry in the job's pending
// set. It must run before the job's recovery broadcasters start claiming
// the previous submitter's route — i.e. before the dispatcher is built —
// because a delivery claimed before its entry is pre-stored routes to no
// owner and bounces on its claim lease instead of being checkpointed.
func (s *PlanFileSource) PreStoreRecovering() error {
	if len(s.recover) == 0 {
		return nil
	}
	if s.preStore == nil {
		return fmt.Errorf("recovering entries require a pre-store hook")
	}
	for safeModelID, modelID := range s.modelMap.SafeToModel {
		planPath := filepath.Join(s.plansDir, safeModelID+".plan")
		entries, err := readPlanEntries(planPath)
		if err != nil {
			return fmt.Errorf("read plan for model %s: %w", modelID, err)
		}
		for _, entry := range entries {
			item, err := s.readEntry(entry, modelID)
			if err != nil {
				return err
			}
			if _, ok := s.recover[item.RequestID]; ok {
				s.preStore(*item)
			}
		}
	}
	return nil
}

// Produce sends one item per plan entry to the channel. It always reads the
// input line so each item retains the original custom_id: cancel / expire
// drain still needs that identity in the error file even when inference is
// skipped. Entries whose request ID is terminal in the entry store are dropped
// entirely — a resumed batch only dispatches unfinished work. Recovering
// entries (in-flight under a previous owner) are pre-stored into the pending
// set and held back until the recovery window elapses: their result delivery
// is recovered through the previous submitter's route meanwhile, and only an
// entry with no delivery by then falls back to submission. The window ends
// early when every held entry's delivery has been checkpointed. Context
// cancellation is handled by the dispatcher drain path.
func (s *PlanFileSource) Produce(ctx context.Context, outgoingRequestCh chan<- pipeline.RequestItem) error {
	defer close(outgoingRequestCh)

	var held []pipeline.RequestItem
	for safeModelID, modelID := range s.modelMap.SafeToModel {
		planPath := filepath.Join(s.plansDir, safeModelID+".plan")
		entries, err := readPlanEntries(planPath)
		if err != nil {
			return fmt.Errorf("read plan for model %s: %w", modelID, err)
		}

		for _, entry := range entries {
			item, err := s.readEntry(entry, modelID)
			if err != nil {
				return err
			}
			if s.skip != nil && s.skip(item.RequestID) {
				continue
			}
			if _, ok := s.recover[item.RequestID]; ok {
				// Already pre-stored by PreStoreRecovering before the recovery
				// broadcasters started claiming; hold for the window.
				held = append(held, *item)
				continue
			}
			outgoingRequestCh <- *item
		}
	}

	if len(held) > 0 {
		s.logger.Info("Deferring re-submission of recoverable in-flight entries",
			"count", len(held), "window", s.recoveryWindow)
		s.waitForRecoveryWindow(ctx, held)
		// Entries whose delivery arrived during the window are already
		// checkpointed; their fence (MarkEntryInFlight) drops them as
		// terminal duplicates, so only genuinely unrecovered work submits.
		for _, item := range held {
			outgoingRequestCh <- item
		}
	}

	return nil
}

// recoveryResolvedPoll is how often the recovery window checks whether every
// held entry's delivery has already been checkpointed (left the pending
// set), so a fully recovered batch can skip the rest of the window.
const recoveryResolvedPoll = time.Second

// waitForRecoveryWindow pauses before submitting recoverable entries —
// bounded by the remaining SLO budget so a short-deadline batch still gets
// its fallback submissions. The wait also ends early once no held entry is
// still pending: every stranded delivery was received and checkpointed, so
// the fence will drop the held items as terminal duplicates and waiting the
// rest of the window buys nothing. Cancellation ends the wait early too; the
// dispatcher's drain path accounts the entries from there.
func (s *PlanFileSource) waitForRecoveryWindow(ctx context.Context, held []pipeline.RequestItem) {
	wait := s.recoveryWindow
	if !s.sloDeadline.IsZero() {
		if until := time.Until(s.sloDeadline); until < wait {
			wait = until
		}
	}
	if wait <= 0 {
		return
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	ticker := time.NewTicker(recoveryResolvedPoll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			return
		case <-ticker.C:
			if s.allResolved(held) {
				return
			}
		}
	}
}

// allResolved reports whether every held entry has left the pending set —
// its stranded delivery was received and its checkpoint succeeded — so the
// recovery window has nothing left to wait for.
func (s *PlanFileSource) allResolved(held []pipeline.RequestItem) bool {
	if s.stillPending == nil {
		return false
	}
	for _, item := range held {
		if s.stillPending(item.RequestID) {
			return false
		}
	}
	return true
}

func (s *PlanFileSource) readEntry(entry planEntry, modelID string) (*pipeline.RequestItem, error) {
	buf := make([]byte, entry.Length)
	if _, err := s.inputFile.ReadAt(buf, entry.Offset); err != nil {
		return nil, fmt.Errorf("%w at offset %d: %w", errRequestInputRead, entry.Offset, err)
	}

	trimmed := bytes.TrimSuffix(buf, []byte{'\n'})
	var req batch_types.Request
	if err := json.Unmarshal(trimmed, &req); err != nil {
		s.logger.Error(err, "Failed to parse request line, recording as error")
		reqID := fmt.Sprintf("batch_req_%s", uuid.NewString())
		return &pipeline.RequestItem{
			RequestID: reqID,
			CustomID:  reqID,
			ParseError: &pipeline.OutputError{
				Code:    "parse_error",
				Message: fmt.Sprintf("failed to parse request line: %v", err),
			},
		}, nil
	}

	requestID := newBatchRequestID(uuid.NewString())
	if s.batchID != "" {
		requestID = stableBatchRequestID(s.batchID, req.CustomID)
	}

	// When route_key_method is "tenant", scope the gateway lookup key by
	// tenant so identically-named models of different tenants route to their
	// own backends (model_gateways entries keyed "<tenantID>/<modelID>").
	// The request body itself is forwarded verbatim to the inference backend.
	lookupID := routeKey(s.cfg.RouteKeyMethod, s.tenantID, modelID)

	headers := maps.Clone(s.passThroughHeaders)
	headers = s.mergeHeaders(headers, lookupID)

	return &pipeline.RequestItem{
		RequestID: requestID,
		CustomID:  req.CustomID,
		ModelID:   lookupID,
		Endpoint:  req.URL,
		Body:      req.Body,
		Headers:   headers,
	}, nil
}

func (s *PlanFileSource) mergeHeaders(headers map[string]string, modelID string) map[string]string {
	if headers == nil {
		headers = make(map[string]string)
	}

	if !s.sloDeadline.IsZero() {
		ms := time.Until(s.sloDeadline).Milliseconds()
		if ms >= 0 {
			headers[sloTTFTMSHeader] = strconv.FormatInt(ms, 10)
		}
	}

	if obj := s.cfg.InferenceObjectiveFor(modelID); obj != "" {
		headers[inferenceObjectiveHeader] = obj
	}

	if s.cfg.SendFairnessHeader && s.tenantID != "" {
		if _, exists := headers[fairnessIDHeader]; !exists {
			headers[fairnessIDHeader] = s.tenantID
		}
	}

	return headers
}
