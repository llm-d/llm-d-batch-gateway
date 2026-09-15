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

// The file provides HTTP handlers for file-related API endpoints.
// It implements the OpenAI compatible Files API endpoints for uploading, downloading, listing, and deleting files.
package file

import (
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/llm-d/llm-d-batch-gateway/internal/apiserver/common"
	dbapi "github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	fsapi "github.com/llm-d/llm-d-batch-gateway/internal/files_store/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/converter"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/clientset"
	ucom "github.com/llm-d/llm-d-batch-gateway/internal/util/com"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
	uotel "github.com/llm-d/llm-d-batch-gateway/internal/util/otel"
)

const (
	defaultListFilesLimit = 20
	maxListFilesLimit     = 10000
	maxUploadFormOverhead = 1 << 20
	maxUploadFileSize     = math.MaxInt64 - maxUploadFormOverhead
)

// Compile-time check: FileAPIHandler implements common.ApiHandler.
var _ common.ApiHandler = (*FileAPIHandler)(nil)

type FileAPIHandler struct {
	config  *common.ServerConfig
	clients *clientset.Clientset
}

func NewFileAPIHandler(config *common.ServerConfig, clients *clientset.Clientset) *FileAPIHandler {
	return &FileAPIHandler{
		config:  config,
		clients: clients,
	}
}

func (c *FileAPIHandler) GetRoutes() []common.Route {
	return []common.Route{
		{
			Method:      http.MethodPost,
			Pattern:     "/v1/files",
			HandlerFunc: c.CreateFile,
			SpanName:    "api-create-file",
		},
		{
			Method:      http.MethodGet,
			Pattern:     "/v1/files",
			HandlerFunc: c.ListFiles,
			SpanName:    "api-list-file",
		},
		{
			Method:      http.MethodGet,
			Pattern:     "/v1/files/{file_id}",
			HandlerFunc: c.RetrieveFile,
			SpanName:    "api-get-file",
		},
		{
			Method:      http.MethodGet,
			Pattern:     "/v1/files/{file_id}/content",
			HandlerFunc: c.DownloadFile,
			SpanName:    "api-download-file",
		},
		{
			Method:      http.MethodDelete,
			Pattern:     "/v1/files/{file_id}",
			HandlerFunc: c.DeleteFile,
			SpanName:    "api-delete-file",
		},
	}
}

// getFileItemFromDB retrieves a file item by ID.
// Returns the file item if found, or an API error.
func (c *FileAPIHandler) getFileItemFromDB(r *http.Request, operation string) (*dbapi.FileItem, *openai.APIError) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	fileID := r.PathValue(common.PathParamFileID)
	if fileID == "" {
		apiErr := openai.NewAPIError(
			http.StatusBadRequest,
			"",
			"file_id is required",
			nil,
		)
		return nil, &apiErr
	}

	logger.V(logging.DEBUG).Info(operation + " file request")

	// Get tenant ID from context
	tenantID := common.GetTenantIDFromContext(ctx)

	// Retrieve file metadata from database
	query := &dbapi.FileQuery{
		BaseQuery: dbapi.BaseQuery{
			IDs:      []string{fileID},
			TenantID: tenantID,
		},
	}
	items, _, _, err := c.clients.FileDB.DBGet(ctx, query, true, 0, 1)
	if err != nil {
		logger.Error(err, "failed to retrieve file metadata")
		apiErr := openai.NewAPIError(http.StatusInternalServerError, "", "Internal Server Error", nil)
		return nil, &apiErr
	}

	if len(items) == 0 {
		logger.Info("file not found")
		apiErr := openai.NewAPIError(
			http.StatusNotFound,
			"",
			fmt.Sprintf("File with ID %s not found", fileID),
			nil,
		)
		return nil, &apiErr
	}

	item := items[0]

	// Validate tenant isolation if tenant ID is present in request
	if item.TenantID != tenantID {
		logger.Info("file not found - tenant mismatch", "request_tenant", tenantID, "file_tenant", item.TenantID)
		apiErr := openai.NewAPIError(
			http.StatusNotFound,
			"",
			fmt.Sprintf("File with ID %s not found", fileID),
			nil,
		)
		return nil, &apiErr
	}

	return item, nil
}

func (c *FileAPIHandler) CreateFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	// Input file must be formatted as a JSONL file, and must be uploaded with the purpose batch.
	// The file can contain up to 50,000 requests, and can be up to 200 MB in size.

	// Bound the whole request while allowing multipart framing and metadata.
	maxFileSize := c.config.FileAPI.GetMaxSizeBytes()
	maxRequestSize := int64(math.MaxInt64)
	if maxFileSize <= maxUploadFileSize {
		maxRequestSize = maxFileSize + maxUploadFormOverhead
	}
	writeSizeError := func() {
		logger.V(logging.DEBUG).Info("file upload request exceeds size limit",
			"contentLength", r.ContentLength, "limit", maxRequestSize)
		common.WriteAPIError(w, r, openai.NewAPIError(
			http.StatusBadRequest, "",
			fmt.Sprintf("File size exceeds the maximum allowed size of %d bytes or multipart overhead exceeds %d bytes", maxFileSize, maxUploadFormOverhead), nil,
		))
	}
	if r.ContentLength > maxRequestSize {
		writeSizeError()
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)
	defer func() {
		if r.MultipartForm != nil {
			if err := r.MultipartForm.RemoveAll(); err != nil {
				logger.Error(err, "failed to remove temporary multipart files")
			}
		}
	}()

	// Read form file from request
	fileReader, fileHeader, err := r.FormFile("file")
	if err != nil {
		var sizeErr *http.MaxBytesError
		if errors.As(err, &sizeErr) {
			writeSizeError()
			return
		}
		logger.Error(err, "failed to read form file from request")
		common.WriteInternalServerError(w, r)
		return
	}
	defer fileReader.Close()

	// Limit file content, excluding multipart fields and framing.
	if fileHeader.Size > maxFileSize {
		logger.V(logging.DEBUG).Info("file size exceeds limit",
			"fileSize", fileHeader.Size, "limit", maxFileSize)
		apiErr := openai.NewAPIError(
			http.StatusBadRequest,
			"",
			fmt.Sprintf("File size exceeds the maximum allowed size of %d bytes", maxFileSize),
			nil,
		)
		common.WriteAPIError(w, r, apiErr)
		return
	}

	// Parse purpose parameter
	purposeStr := r.FormValue("purpose")
	if purposeStr == "" {
		logger.V(logging.DEBUG).Info("missing required parameter: purpose")
		apiErr := openai.NewAPIError(
			http.StatusBadRequest,
			"",
			"Missing required parameter: 'purpose'",
			nil,
		)
		common.WriteAPIError(w, r, apiErr)
		return
	}
	purpose := openai.FileObjectPurpose(purposeStr)
	if !purpose.IsValid() {
		logger.Info("invalid file purpose", "purpose", purpose)
		apiErr := openai.NewAPIError(
			http.StatusBadRequest,
			"",
			fmt.Sprintf("Invalid file purpose '%s'. Must be one of: assistants, assistants_output, batch, batch_output, fine-tune, fine-tune-results, vision, user_data", purpose),
			nil,
		)
		common.WriteAPIError(w, r, apiErr)
		return
	}

	// Parse expires_after parameters
	// By default, files with purpose=batch expire after 30 days.
	createdAt := time.Now().UTC().Unix()
	var expiresAt int64
	expiresAfterAnchor := r.FormValue("expires_after[anchor]")
	expiresAfterSecondsStr := r.FormValue("expires_after[seconds]")
	if expiresAfterAnchor != "" || expiresAfterSecondsStr != "" {
		// validate that both anchor and seconds are provided
		if expiresAfterAnchor == "" || expiresAfterSecondsStr == "" {
			logger.V(logging.DEBUG).Info("incomplete expires_after parameters", "anchor", expiresAfterAnchor, "seconds", expiresAfterSecondsStr)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				"both expires_after[anchor] and expires_after[seconds] must be provided together",
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		}

		// validate anchor value - only "created_at" is supported
		if expiresAfterAnchor != "created_at" {
			logger.V(logging.DEBUG).Info("invalid expires_after anchor", "anchor", expiresAfterAnchor)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				fmt.Sprintf("invalid expires_after[anchor] '%s'. Only 'created_at' is supported", expiresAfterAnchor),
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		}

		expiresAfterSeconds, err := strconv.ParseInt(expiresAfterSecondsStr, 10, 64)
		if err != nil {
			logger.V(logging.DEBUG).Info("invalid expires_after seconds", "seconds", expiresAfterSecondsStr, "error", err)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				"invalid expires_after[seconds]: must be a valid integer",
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		}

		if expiresAfterSeconds < openai.MinExpirationSeconds || expiresAfterSeconds > openai.MaxExpirationSeconds {
			logger.V(logging.DEBUG).Info("expires_after seconds out of range", "seconds", expiresAfterSeconds)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				fmt.Sprintf("expires_after[seconds] must be between %d (1 hour) and %d (30 days)", openai.MinExpirationSeconds, openai.MaxExpirationSeconds),
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		}

		expiresAt = createdAt + expiresAfterSeconds

		logger.V(logging.DEBUG).Info("file expiration set from request", "anchor", expiresAfterAnchor, "seconds", expiresAfterSeconds, "expiresAt", expiresAt)
	} else {
		// Use default expire seconds from config if expires_after not provided
		expiresAfterSeconds := c.config.FileAPI.GetDefaultExpirationSeconds()
		expiresAt = createdAt + expiresAfterSeconds
		logger.V(logging.DEBUG).Info("file expiration set from config default", "seconds", expiresAfterSeconds, "expiresAt", expiresAt)
	}

	fileID := ucom.NewFileID()

	trace.SpanFromContext(ctx).SetAttributes(attribute.String(uotel.AttrInputFileID, fileID))

	// Sanitize filename
	origName := filepath.Base(fileHeader.Filename)
	if origName == "" || origName == "." || origName == ".." || origName == "/" {
		origName = fileID + ".jsonl"
	}

	// Use fileID-based name for storage to guarantee uniqueness
	storageName := ucom.FileStorageName(fileID, origName)

	// Get tenant ID from context to use as folder name
	tenantID := common.GetTenantIDFromContext(ctx)
	folderName, err := ucom.GetFolderNameByTenantID(tenantID)
	if err != nil {
		logger.Error(err, "failed to get folder name from tenant ID", "tenantID", tenantID)
		common.WriteInternalServerError(w, r)
		return
	}
	fileMeta, err := c.clients.File.Store(ctx, storageName, folderName, c.config.FileAPI.GetMaxSizeBytes(), c.config.FileAPI.GetMaxLineCount(), fileReader)
	if err != nil {
		switch {
		case errors.Is(err, fsapi.ErrFileTooLarge):
			logger.V(logging.DEBUG).Info("file size exceeds limit during upload", "limit", maxFileSize)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				fmt.Sprintf("File size exceeds the maximum allowed size of %d bytes", maxFileSize),
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		case errors.Is(err, fsapi.ErrTooManyLines):
			maxLines := c.config.FileAPI.GetMaxLineCount()
			logger.V(logging.DEBUG).Info("file line count exceeds limit during upload", "limit", maxLines)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				fmt.Sprintf("File exceeds the maximum allowed line count of %d", maxLines),
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		case errors.Is(err, fsapi.ErrFileExists):
			logger.V(logging.DEBUG).Info("file already exists in storage", "storageName", storageName)
			apiErr := openai.NewAPIError(
				http.StatusConflict,
				"",
				"A file with this name already exists",
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		default:
			logger.Error(err, "failed to store file content")
			common.WriteInternalServerError(w, r)
			return
		}
	}
	logger.V(logging.DEBUG).Info("file content stored successfully", "file_id", fileID, "size", fileMeta.Size)

	// Set up cleanup - will delete the file if we don't successfully complete
	var success bool
	defer func() {
		if !success {
			if err := c.clients.File.Delete(ctx, storageName, folderName); err != nil {
				logger.Error(err, "failed to cleanup file", "storageName", storageName, "folderName", folderName)
			}
		}
	}()

	// Construct the FileObject domain object
	var expiresAtPtr *int64
	if expiresAt > 0 {
		expiresAtPtr = &expiresAt
	}
	fileObj := openai.FileObject{
		ID:        fileID,
		Bytes:     fileMeta.Size,
		CreatedAt: createdAt,
		ExpiresAt: expiresAtPtr,
		Filename:  origName,
		Object:    "file",
		Purpose:   purpose,
		Status:    openai.FileObjectStatusUploaded,
	}

	// Save file metadata to database
	dbItem, err := converter.FileToDBItem(&fileObj, tenantID, dbapi.Tags{})
	if err != nil {
		logger.Error(err, "failed to convert file to database item", "file_id", fileID)
		common.WriteInternalServerError(w, r)
		return
	}
	if err := c.clients.FileDB.DBStore(ctx, dbItem); err != nil {
		logger.Error(err, "failed to store file metadata", "file_id", fileID)
		common.WriteInternalServerError(w, r)
		return
	}
	logger.V(logging.DEBUG).Info("file metadata stored successfully", "file_id", fileID)

	// Mark as successful to prevent cleanup
	success = true

	common.WriteJSONResponse(w, r, http.StatusOK, &fileObj)
}

func (c *FileAPIHandler) ListFiles(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	// Parse query parameters
	after := r.URL.Query().Get(common.QueryParamAfter)
	limitStr := r.URL.Query().Get(common.QueryParamLimit)
	order := r.URL.Query().Get(common.QueryParamOrder)
	purposeStr := r.URL.Query().Get(common.QueryParamPurpose)

	// Validate and parse start
	// TODO : OpenAI's after is a cursor for use in pagination. after is an object ID that defines your place in the list. For instance, if you make a list request and receive 100 objects, ending with obj_foo, your subsequent call can include after=obj_foo in order to fetch the next page of the list.
	// We have to use `after` as integer
	start := 0
	if after != "" {
		parsedStart, err := strconv.Atoi(after)
		if err != nil {
			logger.V(logging.DEBUG).Info("invalid after parameter", "after", after)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				"Invalid after parameter: must be a valid integer cursor",
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		}
		start = parsedStart
	}

	// Validate and parse limit (default: 20, range: 1-10000)
	limit := defaultListFilesLimit
	if limitStr != "" {
		parsedLimit, err := strconv.Atoi(limitStr)
		if err != nil {
			logger.V(logging.DEBUG).Info("invalid limit parameter", "limit", limitStr)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				"Invalid limit parameter: must be an integer",
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		}
		if parsedLimit < 1 || parsedLimit > maxListFilesLimit {
			logger.V(logging.DEBUG).Info("limit out of range", "limit", parsedLimit)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				fmt.Sprintf("Invalid limit parameter: must be between 1 and %d", maxListFilesLimit),
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		}
		limit = parsedLimit
	}

	// Validate order (default: desc)
	if order == "" {
		order = "desc"
	} else if order != "asc" && order != "desc" {
		logger.V(logging.DEBUG).Info("invalid order parameter", "order", order)
		apiErr := openai.NewAPIError(
			http.StatusBadRequest,
			"",
			"Invalid order parameter: must be 'asc' or 'desc'",
			nil,
		)
		common.WriteAPIError(w, r, apiErr)
		return
	}

	// Validate purpose if provided
	if purposeStr != "" {
		purpose := openai.FileObjectPurpose(purposeStr)
		if !purpose.IsValid() {
			logger.V(logging.DEBUG).Info("invalid purpose parameter", "purpose", purposeStr)
			apiErr := openai.NewAPIError(
				http.StatusBadRequest,
				"",
				fmt.Sprintf("Invalid purpose '%s'.", purposeStr),
				nil,
			)
			common.WriteAPIError(w, r, apiErr)
			return
		}
	}

	logger.V(logging.DEBUG).Info("list files request", "after", after, "limit", limit, "order", order, "purpose", purposeStr)

	// Query files from database with pagination
	tenantID := common.GetTenantIDFromContext(ctx)
	query := &dbapi.FileQuery{
		BaseQuery: dbapi.BaseQuery{
			TenantID:     tenantID,
			TagSelectors: nil,
		},
	}
	if purposeStr != "" {
		query.Purpose = purposeStr
	}
	items, _, expectMore, err := c.clients.FileDB.DBGet(ctx, query, true, start, limit)
	if err != nil {
		logger.Error(err, "failed to list files")
		common.WriteInternalServerError(w, r)
		return
	}

	// Extract FileObjects from items
	fileObjects := make([]openai.FileObject, 0, len(items))
	for _, item := range items {
		fileObj, err := converter.DBItemToFile(item)
		if err != nil {
			logger.Error(err, "failed to convert database item to file")
			common.WriteInternalServerError(w, r)
			return
		}
		fileObjects = append(fileObjects, *fileObj)
	}

	// TODO: can Get support sorting directly?
	// Handle order parameter (reverse for desc since DB returns asc by default)
	if order == "desc" {
		// Reverse the slice
		for i, j := 0, len(fileObjects)-1; i < j; i, j = i+1, j-1 {
			fileObjects[i], fileObjects[j] = fileObjects[j], fileObjects[i]
		}
	}

	// Populate FirstID and LastID
	firstID := ""
	lastID := ""
	if len(fileObjects) > 0 {
		firstID = fileObjects[0].ID
		lastID = fileObjects[len(fileObjects)-1].ID
	}

	// Construct response
	response := openai.ListFilesResponse{
		Object:  "list",
		Data:    fileObjects,
		FirstID: firstID,
		LastID:  lastID,
		HasMore: expectMore,
	}

	common.WriteJSONResponse(w, r, http.StatusOK, response)
}

func (c *FileAPIHandler) RetrieveFile(w http.ResponseWriter, r *http.Request) {
	logger := logging.FromRequest(r)

	item, apiErr := c.getFileItemFromDB(r, "retrieve")
	if apiErr != nil {
		common.WriteAPIError(w, r, *apiErr)
		return
	}

	fileObj, err := converter.DBItemToFile(item)
	if err != nil {
		logger.Error(err, "failed to convert database item to file")
		common.WriteInternalServerError(w, r)
		return
	}

	common.WriteJSONResponse(w, r, http.StatusOK, fileObj)
}

func (c *FileAPIHandler) DownloadFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	item, apiErr := c.getFileItemFromDB(r, "download")
	if apiErr != nil {
		common.WriteAPIError(w, r, *apiErr)
		return
	}

	fileObj, err := converter.DBItemToFile(item)
	if err != nil {
		logger.Error(err, "failed to convert database item to file")
		common.WriteInternalServerError(w, r)
		return
	}
	folderName, err := ucom.GetFolderNameByTenantID(item.TenantID)
	if err != nil {
		logger.Error(err, "failed to get folder name from tenant ID", "tenantID", item.TenantID)
		common.WriteInternalServerError(w, r)
		return
	}

	// Retrieve file content from storage
	storageName := ucom.FileStorageName(fileObj.ID, fileObj.Filename)
	fileReader, fileMeta, err := c.clients.File.Retrieve(ctx, storageName, folderName)
	if err != nil {
		logger.Error(err, "failed to retrieve file content", "storageName", storageName, "folderName", folderName)
		common.WriteInternalServerError(w, r)
		return
	}
	defer fileReader.Close()

	// Set response headers for file download
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", fileObj.Filename))
	if fileMeta.Size > 0 {
		w.Header().Set("Content-Length", strconv.FormatInt(fileMeta.Size, 10))
	}

	// Stream file content to response
	written, err := io.Copy(w, fileReader)
	if err != nil {
		logger.Error(err, "failed to stream file content", "bytes_written", written)
		return
	}

	logger.Info("file download completed", "bytes_written", written)
}

func (c *FileAPIHandler) DeleteFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.FromRequest(r)

	item, apiErr := c.getFileItemFromDB(r, "delete")
	if apiErr != nil {
		common.WriteAPIError(w, r, *apiErr)
		return
	}

	fileObj, err := converter.DBItemToFile(item)
	if err != nil {
		logger.Error(err, "failed to convert database item to file")
		common.WriteInternalServerError(w, r)
		return
	}
	folderName, err := ucom.GetFolderNameByTenantID(item.TenantID)
	if err != nil {
		logger.Error(err, "failed to get folder name from tenant ID", "tenantID", item.TenantID)
		common.WriteInternalServerError(w, r)
		return
	}

	// Delete physical file from storage.
	// ErrNotExist is tolerated so that a retry of a previously half-completed
	// delete (blob gone, metadata still present) can finish successfully.
	storageName := ucom.FileStorageName(fileObj.ID, fileObj.Filename)
	err = c.clients.File.Delete(ctx, storageName, folderName)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Error(err, "failed to delete physical file", "storageName", storageName, "folderName", folderName)
		common.WriteInternalServerError(w, r)
		return
	}

	// Delete file metadata from database.
	deletedIDs, err := c.clients.FileDB.DBDelete(ctx, []string{fileObj.ID})
	if err != nil {
		logger.Error(err, "failed to delete file metadata")
		common.WriteInternalServerError(w, r)
		return
	}

	logger.V(logging.DEBUG).Info("file deleted successfully", "deleted_ids", deletedIDs)

	// Construct delete response
	deleteResp := openai.FileDeleteResponse{
		ID:      fileObj.ID,
		Object:  "file",
		Deleted: true,
	}

	common.WriteJSONResponse(w, r, http.StatusOK, deleteResp)
}
