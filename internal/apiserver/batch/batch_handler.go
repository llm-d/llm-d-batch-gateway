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

// The file provides HTTP handlers for batch-related API endpoints.
// It implements the OpenAI compatible Batch API endpoints for creating, listing, retrieving, and canceling batches.
package batch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	batch_types "github.com/llm-d/llm-d-batch-gateway/internal/shared/types"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/failpoint"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

// Compile-time check: BatchAPIHandler implements common.ApiHandler.
var _ common.ApiHandler = (*BatchAPIHandler)(nil)

type BatchAPIHandler struct {
	config  *common.ServerConfig
	clients *clientset.Clientset
}

func NewBatchAPIHandler(config *common.ServerConfig, clients *clientset.Clientset) *BatchAPIHandler {
	return &BatchAPIHandler{
		config:  config,
		clients: clients,
	}
}

func (c *BatchAPIHandler) GetRoutes() []common.Route {
	return []common.Route{
		{
			Method:      http.MethodPost,
			Pattern:     "/v1/batches",
			HandlerFunc: c.CreateBatch,
			SpanName:    "api-create-batch",
		},
		{
			Method:      http.MethodGet,
			Pattern:     "/v1/batches",
			HandlerFunc: c.ListBatches,
			SpanName:    "api-list-batch",
		},
		{
			Method:      http.MethodGet,
			Pattern:     "/v1/batches/{batch_id}",
			HandlerFunc: c.RetrieveBatch,
			SpanName:    "api-get-batch",
		},
		{
			Method:      http.MethodPost,
			Pattern:     "/v1/batches/{batch_id}/cancel",
			HandlerFunc: c.CancelBatch,
			SpanName:    "api-cancel-batch",
		},
	}
}

func (c *BatchAPIHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	createdAt := time.Now().UTC().Unix()

	// parse request
	batchReq := &openai.CreateBatchRequest{}
	if err := common.DecodeJSON(r.Body, batchReq); err != nil {
		logger.Error(err, "failed to decode request")
		apiErr := openai.NewAPIError(http.StatusBadRequest, "", err.Error(), nil)
		common.WriteAPIError(w, r, apiErr)
		return
	}

	// validate request
	if err := batchReq.Validate(); err != nil {
		logger.Error(err, "failed to validate request")
		apiErr := openai.NewAPIError(http.StatusBadRequest, "", err.Error(), nil)
		common.WriteAPIError(w, r, apiErr)
		return
	}

	// Get tenant ID from context
	tenantID := common.GetTenantIDFromContext(ctx)

	// Verify input file exists
	fileQuery := &api.FileQuery{
		BaseQuery: api.BaseQuery{
			IDs:      []string{batchReq.InputFileID},
			TenantID: tenantID,
		},
	}
	fileItems, _, _, err := c.clients.FileDB.DBGet(ctx, fileQuery, true, 0, 1)
	if err != nil {
		logger.Error(err, "failed to query input file", "file_id", batchReq.InputFileID)
		common.WriteInternalServerError(w, r)
		return
	}
	if len(fileItems) == 0 {
		logger.Info("input file not found", "file_id", batchReq.InputFileID)
		apiErr := openai.NewAPIError(
			http.StatusBadRequest,
			"invalid_request_error",
			fmt.Sprintf("Input file with ID '%s' not found", batchReq.InputFileID),
			nil,
		)
		common.WriteAPIError(w, r, apiErr)
		return
	}

	if fileItems[0].Purpose != string(openai.FileObjectPurposeBatch) {
		logger.Info("input file has wrong purpose", "file_id", batchReq.InputFileID, "purpose", fileItems[0].Purpose)
		apiErr := openai.NewAPIError(
			http.StatusBadRequest,
			"invalid_request_error",
			fmt.Sprintf("Input file '%s' has purpose '%s', but must have purpose 'batch'", batchReq.InputFileID, fileItems[0].Purpose),
			nil,
		)
		common.WriteAPIError(w, r, apiErr)
		return
	}

	batchID := ucom.NewBatchID()

	// add attributes to span
	trace.SpanFromContext(ctx).SetAttributes(
		attribute.String(uotel.AttrInputFileID, batchReq.InputFileID),
		attribute.String(uotel.AttrBatchID, batchID),
	)

	// Create openai.Batch object
	batch := &openai.Batch{
		ID: batchID,
		BatchSpec: openai.BatchSpec{
			Object:           "batch",
			Endpoint:         batchReq.Endpoint,
			InputFileID:      batchReq.InputFileID,
			CompletionWindow: batchReq.CompletionWindow,
			Metadata:         batchReq.Metadata,
			CreatedAt:        createdAt,
		},
		BatchStatusInfo: openai.BatchStatusInfo{
			Status: openai.BatchStatusValidating,
		},
	}

	// TODO: output_expires_after_anchor and output_expires_after_seconds are saved to database as tag. The cleanup service should delete the output file by this value
	// Note that the output_expires_after_anchor is the file creation time, not the time the batch is created.
	tags := api.Tags{}
	if batchReq.OutputExpiresAfter != nil {
		tags[batch_types.TagOutputExpiresAfterAnchor] = batchReq.OutputExpiresAfter.Anchor
		tags[batch_types.TagOutputExpiresAfterSeconds] = fmt.Sprintf("%d", batchReq.OutputExpiresAfter.Seconds)
		logger.V(logging.DEBUG).Info("output expiration configured",
			"anchor", batchReq.OutputExpiresAfter.Anchor,
			"seconds", batchReq.OutputExpiresAfter.Seconds,
		)
	}

	// Capture configured pass-through headers into tags with "pth:" prefix
	for _, headerName := range c.config.BatchAPI.PassThroughHeaders {
		if v := common.LastHeaderValue(r, headerName, ""); v != "" {
			tags[batch_types.TagPrefixPassThroughHeader+headerName] = v
		}
	}

	// Inject OTel trace context into tags with "otel:" prefix
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	for k, v := range carrier {
		tags[batch_types.TagPrefixOTel+k] = v
	}

	// Convert to database item
	dbItem, err := converter.BatchToDBItem(batch, tenantID, tags)
	if err != nil {
		logger.Error(err, "failed to convert batch to database item")
		common.WriteInternalServerError(w, r)
		return
	}

	if err := c.clients.BatchDB.DBStore(ctx, dbItem); err != nil {
		logger.Error(err, "failed to store batch job")
		common.WriteInternalServerError(w, r)
		return
	}

	failpoint.Inject("apiserver/after-batch-dbstore")

	common.WriteJSONResponse(w, r, http.StatusOK, batch)
}

func (c *BatchAPIHandler) ListBatches(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	// Parse query parameters
	query := r.URL.Query()
	limit := 20
	if limitStr := query.Get(common.QueryParamLimit); limitStr != "" {
		var parsedLimit int
		if _, err := fmt.Sscanf(limitStr, "%d", &parsedLimit); err != nil {
			apiErr := openai.NewAPIError(http.StatusBadRequest, "", "invalid limit parameter: must be an integer", nil)
			common.WriteAPIError(w, r, apiErr)
			return
		}

		if parsedLimit < 1 || parsedLimit > 100 {
			apiErr := openai.NewAPIError(http.StatusBadRequest, "", "invalid limit parameter: must be between 1 and 100", nil)
			common.WriteAPIError(w, r, apiErr)
			return
		}
		limit = parsedLimit
	}

	after := 0
	if afterStr := query.Get(common.QueryParamAfter); afterStr != "" {
		var parsedAfter int
		if _, err := fmt.Sscanf(afterStr, "%d", &parsedAfter); err != nil {
			apiErr := openai.NewAPIError(http.StatusBadRequest, "", "invalid after parameter: must be an integer", nil)
			common.WriteAPIError(w, r, apiErr)
			return
		}

		if parsedAfter < 0 {
			apiErr := openai.NewAPIError(http.StatusBadRequest, "", "invalid after parameter: must be equal to or greater than 0", nil)
			common.WriteAPIError(w, r, apiErr)
			return
		}
		after = parsedAfter
	}

	// Get tenant ID from context
	tenantID := common.GetTenantIDFromContext(ctx)

	// Request items
	items, _, expectMore, err := c.clients.BatchDB.DBGet(ctx,
		&api.BatchQuery{
			BaseQuery: api.BaseQuery{TenantID: tenantID},
		},
		true, after, limit)
	if err != nil {
		logger.Error(err, "failed to list batches from database")
		common.WriteInternalServerError(w, r)
		return
	}

	// Convert to batch responses
	batches := make([]openai.Batch, 0, len(items))
	for _, item := range items {
		batch, err := converter.DBItemToBatch(item)
		if err != nil {
			logger.Error(err, "failed to convert database item to batch")
			common.WriteInternalServerError(w, r)
			return
		}
		batches = append(batches, *batch)
	}

	resp := openai.ListBatchResponse{
		Object:  "list",
		Data:    batches,
		HasMore: expectMore,
	}
	if len(batches) > 0 {
		first := batches[0].ID
		last := batches[len(batches)-1].ID
		resp.FirstID = &first
		resp.LastID = &last
	}

	common.WriteJSONResponse(w, r, http.StatusOK, resp)
}

// mergeProgressCounts retrieves real-time progress counts from Redis and merges them
// into the batch object. This is only done for batches in the "in_progress" state.
func (c *BatchAPIHandler) mergeProgressCounts(ctx context.Context, batch *openai.Batch) error {
	// Only merge progress for in-progress batches
	if batch.Status != openai.BatchStatusInProgress {
		return nil
	}

	// Try to get progress counts from Redis
	data, err := c.clients.Status.StatusGet(ctx, batch.ID)
	if err != nil {
		return fmt.Errorf("failed to get status from Redis: %w", err)
	}

	// If no data in Redis, keep the DB values
	if data == nil {
		return nil
	}

	// Parse the progress counts from Redis
	var progressCounts openai.BatchRequestCounts
	if err := json.Unmarshal(data, &progressCounts); err != nil {
		return fmt.Errorf("failed to unmarshal progress counts: %w", err)
	}

	// Merge the counts - use Redis values as they are more up-to-date
	batch.RequestCounts = progressCounts

	return nil
}

func (c *BatchAPIHandler) getBatchItemFromDB(r *http.Request, operation string) (*api.BatchItem, *openai.APIError) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	batchID := r.PathValue(common.PathParamBatchID)
	if batchID == "" {
		apiErr := openai.NewAPIError(
			http.StatusBadRequest,
			"",
			common.PathParamBatchID+" is required",
			nil,
		)
		return nil, &apiErr
	}

	logger.V(logging.DEBUG).Info(operation + " batch request")

	tenantID := common.GetTenantIDFromContext(ctx)

	items, _, _, err := c.clients.BatchDB.DBGet(ctx,
		&api.BatchQuery{
			BaseQuery: api.BaseQuery{
				IDs:      []string{batchID},
				TenantID: tenantID,
			},
		},
		true, 0, 1)
	if err != nil {
		logger.Error(err, "failed to get batch from database")
		apiErr := openai.NewAPIError(http.StatusInternalServerError, "", "Internal Server Error", nil)
		return nil, &apiErr
	}

	if len(items) == 0 {
		logger.Info("batch not found")
		apiErr := openai.NewAPIError(
			http.StatusNotFound,
			"",
			fmt.Sprintf("Batch with ID %s not found", batchID),
			nil,
		)
		return nil, &apiErr
	}

	item := items[0]

	if item.TenantID != tenantID {
		logger.Info("batch not found - tenant mismatch", "request_tenant", tenantID, "batch_tenant", item.TenantID)
		apiErr := openai.NewAPIError(
			http.StatusNotFound,
			"",
			fmt.Sprintf("Batch with ID %s not found", batchID),
			nil,
		)
		return nil, &apiErr
	}

	return item, nil
}

func (c *BatchAPIHandler) RetrieveBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	item, apiErr := c.getBatchItemFromDB(r, "retrieve")
	if apiErr != nil {
		common.WriteAPIError(w, r, *apiErr)
		return
	}

	batch, err := converter.DBItemToBatch(item)
	if err != nil {
		logger.Error(err, "failed to convert database item to batch")
		common.WriteInternalServerError(w, r)
		return
	}

	// Merge real-time progress counts from Redis for in-progress batches
	if err := c.mergeProgressCounts(ctx, batch); err != nil {
		logger.Error(err, "failed to merge progress counts", "batch_id", batch.ID, "status", batch.Status)
		// Log error but don't fail the request - return what we have from DB
	}

	spanAttrs := []attribute.KeyValue{attribute.String(uotel.AttrInputFileID, batch.InputFileID)}
	if batch.OutputFileID != nil {
		spanAttrs = append(spanAttrs, attribute.String(uotel.AttrOutputFileID, *batch.OutputFileID))
	}
	if batch.ErrorFileID != nil {
		spanAttrs = append(spanAttrs, attribute.String(uotel.AttrErrorFileID, *batch.ErrorFileID))
	}
	trace.SpanFromContext(ctx).SetAttributes(spanAttrs...)

	common.WriteJSONResponse(w, r, http.StatusOK, batch)
}

func (c *BatchAPIHandler) CancelBatch(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	item, apiErr := c.getBatchItemFromDB(r, "cancel")
	if apiErr != nil {
		common.WriteAPIError(w, r, *apiErr)
		return
	}

	batch, err := converter.DBItemToBatch(item)
	if err != nil {
		logger.Error(err, "failed to convert database item to batch")
		common.WriteInternalServerError(w, r)
		return
	}

	spanAttrs := []attribute.KeyValue{attribute.String(uotel.AttrInputFileID, batch.InputFileID)}
	if batch.OutputFileID != nil {
		spanAttrs = append(spanAttrs, attribute.String(uotel.AttrOutputFileID, *batch.OutputFileID))
	}
	if batch.ErrorFileID != nil {
		spanAttrs = append(spanAttrs, attribute.String(uotel.AttrErrorFileID, *batch.ErrorFileID))
	}
	trace.SpanFromContext(ctx).SetAttributes(spanAttrs...)

	if batch.Status == openai.BatchStatusCancelled {
		common.WriteJSONResponse(w, r, http.StatusOK, batch)
		return
	}

	if !batch.Status.IsCancellable() {
		apiErr := openai.NewAPIError(http.StatusBadRequest, "", fmt.Sprintf("Batch with status %s cannot be cancelled", batch.Status), nil)
		common.WriteAPIError(w, r, apiErr)
		return
	}

	// Idempotent cancel retry: DB already shows cancelling (e.g. first attempt updated DB
	// but ECProducerSendEvents failed). Re-send the event only — skip queue removal and DB write.
	if batch.Status == openai.BatchStatusCancelling {
		event := []api.BatchEvent{
			{
				ID:   batch.ID,
				Type: api.BatchEventCancel,
				TTL:  c.config.BatchAPI.GetBatchEventTTLSeconds(),
			},
		}
		if _, err := c.clients.Event.ECProducerSendEvents(ctx, event); err != nil {
			logger.Error(err, "failed to send cancel event")
			common.WriteInternalServerError(w, r)
			return
		}
		common.WriteJSONResponse(w, r, http.StatusOK, batch)
		return
	}

	// Try to remove from the priority queue first.
	// PQDelete atomically transitions the job to cancelled (with cancelled_at)
	// if it is still unclaimed. Returns 1 if cancelled, 0 if already claimed.
	nDeleted, err := c.clients.Queue.PQDelete(ctx, &api.BatchJobPriority{ID: batch.ID})
	if err != nil {
		logger.Error(err, "failed to remove batch from queue")
		common.WriteInternalServerError(w, r)
		return
	}

	if nDeleted > 0 {
		// Job was in queue — PQDelete already transitioned it to cancelled.
		// Re-read the updated state and return it.
		freshItem, apiErr := c.getBatchItemFromDB(r, "cancel")
		if apiErr != nil {
			common.WriteAPIError(w, r, *apiErr)
			return
		}
		freshBatch, convErr := converter.DBItemToBatch(freshItem)
		if convErr != nil {
			logger.Error(convErr, "failed to convert database item to batch")
			common.WriteInternalServerError(w, r)
			return
		}
		failpoint.Inject("apiserver/after-cancel-pqdelete")
		common.WriteJSONResponse(w, r, http.StatusOK, freshBatch)
		return
	}

	// Job is being processed — mark as cancelling and send cancel event.
	batch.Status = openai.BatchStatusCancelling
	cancellingAt := time.Now().UTC().Unix()
	batch.CancellingAt = &cancellingAt

	// Persist the status change *before* sending the cancel event to prevent a
	// write-write race between the API server and the worker.
	//
	// Without this ordering:
	//   1. API server sends cancel event to worker
	//   2. Worker receives event, cancels job, writes "cancelled" to DB
	//   3. API server writes "cancelling" to DB (still processing the cancel request)
	//   Result: status regresses from "cancelled" back to "cancelling"
	//
	// By writing first, the API server's "cancelling" is already in the DB before the
	// worker can act, so any subsequent worker write is the final state.

	tenantID := common.GetTenantIDFromContext(ctx)

	dbItem, err := converter.BatchToDBItem(batch, tenantID, item.Tags)
	if err != nil {
		logger.Error(err, "failed to convert batch to database item")
		common.WriteInternalServerError(w, r)
		return
	}

	failpoint.Inject("apiserver/before-cancel-dbupdate")

	dbItem.Epoch = item.Epoch
	if err := c.clients.BatchDB.DBUpdate(ctx, dbItem, item.Status); err != nil {
		if errors.Is(err, api.ErrConflict) {
			apiErr := openai.NewAPIError(http.StatusConflict, "", "batch changed state during cancel, retry", nil)
			common.WriteAPIError(w, r, apiErr)
			return
		}
		logger.Error(err, "failed to update batch in database")
		common.WriteInternalServerError(w, r)
		return
	}

	failpoint.Inject("apiserver/after-cancel-dbupdate")

	// Send the cancel event *after* DB update succeeds.
	event := []api.BatchEvent{
		{
			ID:   batch.ID,
			Type: api.BatchEventCancel,
			TTL:  c.config.BatchAPI.GetBatchEventTTLSeconds(),
		},
	}
	_, err = c.clients.Event.ECProducerSendEvents(ctx, event)
	if err != nil {
		logger.Error(err, "failed to send cancel event")
		common.WriteInternalServerError(w, r)
		return
	}

	common.WriteJSONResponse(w, r, http.StatusOK, batch)
}
