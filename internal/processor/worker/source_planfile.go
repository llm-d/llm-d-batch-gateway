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
	batchID            string
	stableRequestIDs   bool
	inputFile          *os.File
	plansDir           string
	modelMap           *modelMapFile
	resolver           *inference.GatewayResolver
	cfg                *config.ProcessorConfig
	passThroughHeaders map[string]string
	sloDeadline        time.Time
	tenantID           string
	logger             logr.Logger
	completedIDs       map[string]bool
}

var _ pipeline.RequestSource = (*PlanFileSource)(nil)

type PlanFileSourceConfig struct {
	BatchID            string
	StableRequestIDs   bool
	InputFile          *os.File
	PlansDir           string
	ModelMap           *modelMapFile
	Resolver           *inference.GatewayResolver
	Cfg                *config.ProcessorConfig
	PassThroughHeaders map[string]string
	SLODeadline        time.Time
	TenantID           string
	Logger             logr.Logger
	CompletedIDs       map[string]bool
}

func NewPlanFileSource(cfg PlanFileSourceConfig) *PlanFileSource {
	return &PlanFileSource{
		batchID:            cfg.BatchID,
		stableRequestIDs:   cfg.StableRequestIDs,
		inputFile:          cfg.InputFile,
		plansDir:           cfg.PlansDir,
		modelMap:           cfg.ModelMap,
		resolver:           cfg.Resolver,
		cfg:                cfg.Cfg,
		passThroughHeaders: cfg.PassThroughHeaders,
		sloDeadline:        cfg.SLODeadline,
		tenantID:           cfg.TenantID,
		logger:             cfg.Logger,
		completedIDs:       cfg.CompletedIDs,
	}
}

// Produce sends one item per plan entry to the channel. It always reads the
// input line so each item retains the original custom_id: cancel / expire
// drain still needs that identity in the error file even when inference is
// skipped. Context cancellation is handled by the dispatcher drain path.
func (s *PlanFileSource) Produce(_ context.Context, outgoingRequestCh chan<- pipeline.RequestItem) error {
	defer close(outgoingRequestCh)

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
			if item != nil {
				outgoingRequestCh <- *item
			}
		}
	}

	return nil
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

	// When route_key_method is "tenant", scope the gateway lookup key by
	// tenant so identically-named models of different tenants route to their
	// own backends (model_gateways entries keyed "<tenantID>/<modelID>").
	// The request body itself is forwarded verbatim to the inference backend.
	lookupID := routeKey(s.cfg.RouteKeyMethod, s.tenantID, modelID)

	headers := maps.Clone(s.passThroughHeaders)
	headers = s.mergeHeaders(headers, lookupID)

	requestID := fmt.Sprintf("batch_req_%s", uuid.NewString())
	if s.stableRequestIDs {
		requestID = stableBatchRequestID(s.batchID, req.CustomID)
	}
	if s.completedIDs[requestID] {
		return nil, nil
	}
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
