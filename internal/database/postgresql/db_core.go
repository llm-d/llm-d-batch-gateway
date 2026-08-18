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

// This file implements the shared SQL logic for PostgreSQL database clients.
// It operates on BaseIndexes + BaseContents and supports extra indexed columns
// declared by the TableDescriptor.

package postgresql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/util/logging"
)

const (
	colID       = "id"
	colTenantID = "tenant_id"
	colExpiry   = "expiry"
	colTags     = "tags"
	colSpec     = "spec"
	colStatus   = "status"

	// expiryCondition uses DB-side NOW() to avoid clock skew between app and database.
	expiryCondition = colExpiry + " > 0 AND " + colExpiry + " <= EXTRACT(EPOCH FROM NOW())::BIGINT"
)

// TableDescriptor describes the table layout for a concrete item type.
// The core uses this to build SQL dynamically.
type TableDescriptor interface {
	// TableName returns the PostgreSQL table name.
	TableName() string
	// Schema returns the DDL SQL to create the table and indexes.
	// Must be idempotent (use IF NOT EXISTS).
	Schema() string
	// ExtraColumns returns names of additional indexed columns
	// beyond the common set (id, tenant_id, expiry, tags).
	ExtraColumns() []string
}

// pgCore contains the shared state and SQL logic for all PostgreSQL clients.
type pgCore struct {
	pool      pgxPool
	desc      TableDescriptor
	closeOnce sync.Once
}

func newPgCore(ctx context.Context, config *PostgreSQLConfig, tableDescriptor TableDescriptor) (*pgCore, error) {
	pool, err := newPool(ctx, config)
	if err != nil {
		return nil, err
	}

	pgCore := &pgCore{
		pool: pool,
		desc: tableDescriptor,
	}

	if err := pgCore.ensureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pgCore, nil
}

func packTags(tags api.Tags) (string, error) {
	if len(tags) == 0 {
		return "{}", nil
	}

	data, err := json.Marshal(tags)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func unpackTags(tagsStr string) (api.Tags, error) {
	if tagsStr == "" {
		return nil, nil
	}

	var tags api.Tags
	if err := json.Unmarshal([]byte(tagsStr), &tags); err != nil {
		return nil, err
	}

	return tags, nil
}

// allColumns returns the full column list for INSERT.
func (c *pgCore) allColumns() []string {
	cols := []string{colID, colTenantID, colExpiry, colTags}
	cols = append(cols, c.desc.ExtraColumns()...)
	cols = append(cols, colSpec, colStatus)
	return cols
}

// selectColumns returns the column list for a SELECT query.
func (c *pgCore) selectColumns(includeStatic bool) []string {
	cols := []string{colID, colTenantID, colExpiry, colTags}
	cols = append(cols, c.desc.ExtraColumns()...)
	if includeStatic {
		cols = append(cols, colStatus, colSpec)
	} else {
		cols = append(cols, colStatus)
	}
	return cols
}

// store persists an item with its base indexes, contents, and any extra column values.
// extraValues is keyed by column name and must contain entries for all ExtraColumns().
func (c *pgCore) store(ctx context.Context, idx *api.BaseIndexes, contents *api.BaseContents, extraValues map[string]any) error {
	if err := idx.Validate(); err != nil {
		return err
	}

	tags, err := packTags(idx.Tags)
	if err != nil {
		return fmt.Errorf("DBStore: failed to pack tags: %w", err)
	}

	cols := c.allColumns()
	placeholders := make([]string, len(cols))
	for i := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	args := []any{idx.ID, idx.TenantID, idx.Expiry, tags}
	for _, col := range c.desc.ExtraColumns() {
		val, ok := extraValues[col]
		if !ok {
			return fmt.Errorf("DBStore: missing required extra column %q", col)
		}
		args = append(args, val)
	}
	args = append(args, contents.Spec, contents.Status)

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		c.desc.TableName(), strings.Join(cols, ", "), strings.Join(placeholders, ", "))

	_, err = c.pool.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("DBStore: succeeded", colID, idx.ID)

	return nil
}

// buildGetQuery constructs the SELECT SQL and args from the given query parameters.
// extraFilters is keyed by column name and adds equality conditions for each entry.
// rawConditions are appended verbatim to the WHERE clause (no parameterization).
func (c *pgCore) buildGetQuery(
	bq *api.BaseQuery, includeStatic bool, start, limit int,
	extraFilters map[string]any, rawConditions []string,
) (string, []any) {
	cols := c.selectColumns(includeStatic)

	var conditions []string
	var args []any
	argIdx := 1

	if len(bq.IDs) > 0 {
		conditions = append(conditions, fmt.Sprintf(colID+" = ANY($%d)", argIdx))
		args = append(args, bq.IDs)
		argIdx++
	}

	if bq.TenantID != "" {
		conditions = append(conditions, fmt.Sprintf(colTenantID+" = $%d", argIdx))
		args = append(args, bq.TenantID)
		argIdx++
	}

	if len(bq.TagSelectors) > 0 {
		var tagConds []string

		for k, v := range bq.TagSelectors {
			tagJSON, _ := json.Marshal(map[string]string{k: v})
			tagConds = append(tagConds, fmt.Sprintf(colTags+" @> $%d::jsonb", argIdx))
			args = append(args, string(tagJSON))
			argIdx++
		}

		joiner := " AND "
		if bq.TagsLogicalCond == api.LogicalCondOr {
			joiner = " OR "
		}

		conditions = append(conditions, "("+strings.Join(tagConds, joiner)+")")
	}

	if bq.Expired {
		conditions = append(conditions, expiryCondition)
	}

	for col, val := range extraFilters {
		conditions = append(conditions, fmt.Sprintf(col+" = $%d", argIdx))
		args = append(args, val)
		argIdx++
	}

	conditions = append(conditions, rawConditions...)

	if len(conditions) == 0 {
		return "", nil
	}

	where := strings.Join(conditions, " AND ")

	offsetArg := argIdx
	limitArg := argIdx + 1

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s ORDER BY %s OFFSET $%d LIMIT $%d",
		strings.Join(cols, ", "), c.desc.TableName(), where, colID, offsetArg, limitArg)
	args = append(args, start, limit)

	return sql, args
}

// scanRow scans a single row into BaseIndexes + BaseContents + extra values.
// The returned map is keyed by column name for each ExtraColumns() entry.
func (c *pgCore) scanRow(rows pgx.Rows, includeStatic bool) (*api.BaseIndexes, *api.BaseContents, map[string]any, error) {
	var (
		id      string
		tenant  string
		expiry  int64
		tagsStr *string
	)

	extraCols := c.desc.ExtraColumns()

	scanArgs := []any{&id, &tenant, &expiry, &tagsStr}

	extraDest := make([]any, len(extraCols))
	for i := range extraDest {
		scanArgs = append(scanArgs, &extraDest[i])
	}

	var status []byte
	scanArgs = append(scanArgs, &status)

	var spec []byte
	if includeStatic {
		scanArgs = append(scanArgs, &spec)
	}

	if err := rows.Scan(scanArgs...); err != nil {
		return nil, nil, nil, err
	}

	var tags api.Tags
	if tagsStr != nil {
		var err error
		tags, err = unpackTags(*tagsStr)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("DBGet: failed to unpack tags for id %s: %w", id, err)
		}
	}

	indexes := &api.BaseIndexes{
		ID:       id,
		TenantID: tenant,
		Expiry:   expiry,
		Tags:     tags,
	}

	contents := &api.BaseContents{
		Spec:   spec,
		Status: status,
	}

	extras := make(map[string]any, len(extraCols))
	for i, col := range extraCols {
		extras[col] = extraDest[i]
	}

	return indexes, contents, extras, nil
}

// get retrieves items from the database.
// extraFilters adds type-specific equality conditions (e.g., purpose = "batch").
// rawConditions are appended verbatim to the WHERE clause.
func (c *pgCore) get(
	ctx context.Context, bq *api.BaseQuery, includeStatic bool, start, limit int,
	extraFilters map[string]any, rawConditions []string,
) (
	indexes []*api.BaseIndexes, contents []*api.BaseContents, extras []map[string]any,
	cursor int, expectMore bool, err error,
) {
	// Request one extra row beyond the limit to determine if more results exist.
	sql, args := c.buildGetQuery(bq, includeStatic, start, limit+1, extraFilters, rawConditions)
	if sql == "" {
		return nil, nil, nil, 0, false, nil
	}

	rows, err := c.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, nil, 0, false, err
	}
	defer rows.Close()

	for rows.Next() {
		idx, cnt, extra, err := c.scanRow(rows, includeStatic)
		if err != nil {
			return nil, nil, nil, 0, false, err
		}

		indexes = append(indexes, idx)
		contents = append(contents, cnt)
		extras = append(extras, extra)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, nil, 0, false, err
	}

	// If we got more rows than the requested limit, there are more results available.
	if len(indexes) > limit {
		indexes = indexes[:limit]
		contents = contents[:limit]
		extras = extras[:limit]
		expectMore = true
	}

	cursor = start + len(indexes)

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("DBGet: succeeded",
		"nItems", len(indexes), "cursor", cursor, "expectMore", expectMore)

	return indexes, contents, extras, cursor, expectMore, nil
}

// update updates the dynamic fields of an existing item.
// When expectedStatus is non-nil, the update is conditional (CAS): it only
// succeeds if the current status column matches expectedStatus exactly.
// extraConditions add additional equality checks to the WHERE clause (e.g.,
// epoch fencing). rawSetClauses are appended verbatim to the SET clause
// (e.g., "epoch = epoch + 1"). Returns api.ErrConflict when any condition
// prevents the update from matching.
func (c *pgCore) update(ctx context.Context, idx *api.BaseIndexes, contents *api.BaseContents, expectedStatus []byte, extraConditions map[string]any, rawSetClauses []string) error {
	if err := idx.Validate(); err != nil {
		return err
	}

	var setClauses []string
	var args []any
	argIdx := 1

	if len(contents.Status) > 0 {
		setClauses = append(setClauses, fmt.Sprintf(colStatus+" = $%d", argIdx))
		args = append(args, contents.Status)
		argIdx++
	}

	if len(idx.Tags) > 0 {
		tags, err := packTags(idx.Tags)
		if err != nil {
			return fmt.Errorf("DBUpdate: failed to pack tags: %w", err)
		}

		setClauses = append(setClauses, fmt.Sprintf(colTags+" = $%d", argIdx))
		args = append(args, tags)
		argIdx++
	}

	if len(setClauses) == 0 {
		return nil
	}

	args = append(args, idx.ID)
	whereClause := fmt.Sprintf(colID+" = $%d", argIdx)
	argIdx++

	hasConditions := false
	if len(expectedStatus) > 0 {
		args = append(args, expectedStatus)
		whereClause += fmt.Sprintf(" AND "+colStatus+" = $%d", argIdx)
		argIdx++
		hasConditions = true
	}

	for col, val := range extraConditions {
		args = append(args, val)
		whereClause += fmt.Sprintf(" AND %s = $%d", col, argIdx)
		argIdx++
		hasConditions = true
	}

	setClauses = append(setClauses, rawSetClauses...)

	sql := fmt.Sprintf(
		"UPDATE %s SET %s WHERE %s",
		c.desc.TableName(), strings.Join(setClauses, ", "), whereClause,
	)

	result, err := c.pool.Exec(ctx, sql, args...)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 0 {
		if hasConditions {
			return fmt.Errorf("DBUpdate: %w", api.ErrConflict)
		}
		return fmt.Errorf("DBUpdate: item %s not found", idx.ID)
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("DBUpdate: succeeded", colID, idx.ID)

	return nil
}

// delete removes items by their IDs and returns the IDs that were actually deleted.
func (c *pgCore) delete(ctx context.Context, ids []string) (deletedIDs []string, err error) {
	if len(ids) == 0 {
		return nil, nil
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE "+colID+" = ANY($1) RETURNING "+colID, c.desc.TableName())

	rows, err := c.pool.Query(ctx, sql, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	deletedIDs = make([]string, 0, len(ids))
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}

		deletedIDs = append(deletedIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	logr.FromContextOrDiscard(ctx).V(logging.INFO).Info("DBDelete: succeeded",
		"nDeleted", len(deletedIDs), "ids", deletedIDs)

	return deletedIDs, nil
}

// ensureSchema applies the table DDL.
func (c *pgCore) ensureSchema(ctx context.Context) error {
	var err error
	for attempt := 0; attempt < 3; attempt++ {
		if _, err = c.pool.Exec(ctx, c.desc.Schema()); err == nil {
			return nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 100 * time.Millisecond):
		}
	}
	return err
}

func (c *pgCore) close() error {
	c.closeOnce.Do(func() {
		if c.pool != nil {
			c.pool.Close()
		}
	})
	return nil
}
