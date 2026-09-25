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
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

//go:embed batch_schema.sql
var batchSchemaSql string

// nonTerminalCondition is the SQL WHERE clause fragment that filters for
// non-terminal batch statuses. Computed once since terminal statuses are fixed.
var nonTerminalCondition = buildNonTerminalCondition()

func buildNonTerminalCondition() string {
	statuses := openai.TerminalStatuses()
	quoted := make([]string, len(statuses))
	for i, s := range statuses {
		quoted[i] = "'" + string(s) + "'"
	}
	return colStatus + `::jsonb->>'status' NOT IN (` + strings.Join(quoted, ",") + `)`
}

const (
	colProcessorID      = "processor_id"
	colPriority         = "priority"
	colEpoch            = "epoch"
	colRecoveryAttempts = "recovery_attempts"
)

// Compile-time checks.
var _ api.BatchProgressDBClient = (*PostgresBatchDBClient)(nil)

// Compile-time check: batchDescriptor implements TableDescriptor.
var _ TableDescriptor = (*batchDescriptor)(nil)

// batchDescriptor implements TableDescriptor for batch items.
type batchDescriptor struct{}

func (batchDescriptor) TableName() string { return "batch_items" }
func (batchDescriptor) Schema() string    { return batchSchemaSql }
func (batchDescriptor) ExtraColumns() []string {
	return []string{colProcessorID, colPriority, colEpoch, colRecoveryAttempts}
}

// PostgresBatchDBClient implements api.BatchDBClient using PostgreSQL.
type PostgresBatchDBClient struct {
	*pgCore
}

var _ api.BatchDBClient = (*PostgresBatchDBClient)(nil)

// NewPostgresBatchDBClient creates a new PostgreSQL batch database client.
func NewPostgresBatchDBClient(ctx context.Context, config *PostgreSQLConfig) (*PostgresBatchDBClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	pgCore, err := newPgCore(ctx, config, batchDescriptor{})
	if err != nil {
		return nil, err
	}

	logr.FromContextOrDiscard(ctx).Info("NewPostgresBatchDBClient: client created successfully")
	return &PostgresBatchDBClient{pgCore}, nil
}

func (c *PostgresBatchDBClient) Close() error {
	return c.close()
}

func (c *PostgresBatchDBClient) DBStore(ctx context.Context, item *api.BatchItem) (err error) {
	if item == nil {
		err = fmt.Errorf("item is nil")
		return
	}
	var processorID any = item.ProcessorID
	if item.ProcessorID == "" {
		processorID = nil
	}
	if err = c.store(ctx, &item.BaseIndexes, &item.BaseContents, map[string]any{
		colProcessorID:      processorID,
		colPriority:         item.Priority,
		colEpoch:            item.Epoch,
		colRecoveryAttempts: item.RecoveryAttempts,
	}); err != nil {
		return
	}
	return
}

func (c *PostgresBatchDBClient) DBGet(
	ctx context.Context, query *api.BatchQuery,
	includeStatic bool, start, limit int,
) (items []*api.BatchItem, cursor int, expectMore bool, err error) {
	if query == nil {
		return
	}

	var rawConditions []string
	if query.NonTerminal {
		rawConditions = append(rawConditions, nonTerminalCondition)
	}
	if query.HasProcessorID {
		rawConditions = append(rawConditions, colProcessorID+" IS NOT NULL")
	}

	var extraFilters map[string]any
	if query.ProcessorID != "" {
		extraFilters = map[string]any{colProcessorID: query.ProcessorID}
	}

	indexes, contents, extras, cursor, expectMore, err := c.get(
		ctx, &query.BaseQuery, includeStatic, start, limit, extraFilters, rawConditions)
	if err != nil {
		return
	}

	items = make([]*api.BatchItem, len(indexes))
	for i := range indexes {
		processorID, _ := extras[i][colProcessorID].(string)
		priority, _ := extras[i][colPriority].(int64)
		epoch, _ := extras[i][colEpoch].(int64)
		recoveryAttempts, _ := extras[i][colRecoveryAttempts].(int64)
		items[i] = &api.BatchItem{
			BaseIndexes:      *indexes[i],
			BaseContents:     *contents[i],
			ProcessorID:      processorID,
			Priority:         priority,
			Epoch:            epoch,
			RecoveryAttempts: recoveryAttempts,
		}
	}

	return
}

func (c *PostgresBatchDBClient) DBUpdate(ctx context.Context, item *api.BatchItem, expectedStatus []byte) (err error) {
	if item == nil {
		err = fmt.Errorf("item is nil")
		return
	}
	var epochFence map[string]any
	if item.Epoch > 0 {
		epochFence = map[string]any{colEpoch: item.Epoch}
	}
	var rawSets []string
	if item.BumpEpoch {
		rawSets = append(rawSets, colEpoch+" = "+colEpoch+" + 1")
	}
	if err = c.update(ctx, &item.BaseIndexes, &item.BaseContents, expectedStatus, epochFence, rawSets); err != nil {
		return
	}
	return
}

func (c *PostgresBatchDBClient) DBDelete(ctx context.Context, ids []string) (deletedIDs []string, err error) {
	if deletedIDs, err = c.delete(ctx, ids); err != nil {
		return
	}
	return
}

// DBUpdateProgress updates the in-flight request counts in the batch's status
// column, fenced by the batch's current epoch: a stale-epoch writer (a
// fenced-out processor incarnation) must not overwrite the current owner's
// counts, so a mismatched epoch matches no rows and is silently discarded.
func (c *PostgresBatchDBClient) DBUpdateProgress(ctx context.Context, id string, epoch int64, counts api.BatchRequestCounts) error {
	if id == "" {
		return fmt.Errorf("DBUpdateProgress: empty ID")
	}
	countsJSON, err := json.Marshal(counts)
	if err != nil {
		return fmt.Errorf("DBUpdateProgress: marshal counts: %w", err)
	}

	sql := fmt.Sprintf(
		"UPDATE %s SET status = jsonb_set(COALESCE(status, '{}'::jsonb), '{request_counts}', $1::jsonb, true) "+
			"WHERE id = $2 AND "+nonTerminalCondition+" AND "+colEpoch+" = $3",
		c.desc.TableName(),
	)

	result, err := c.pool.Exec(ctx, sql, string(countsJSON), id, epoch)
	if err != nil {
		return fmt.Errorf("DBUpdateProgress: %w", err)
	}
	if result.RowsAffected() == 0 {
		// The epoch fence matched no row: this processor is no longer the owner
		// (its epoch was bumped by a reclaimer). Surface it so the caller can
		// abort instead of dispatching against a job it no longer owns.
		return fmt.Errorf("DBUpdateProgress: %w", api.ErrConflict)
	}
	return nil
}
