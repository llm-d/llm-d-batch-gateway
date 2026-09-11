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
	colResumable        = "resumable"
)

// Compile-time check: batchDescriptor implements TableDescriptor.
var _ TableDescriptor = (*batchDescriptor)(nil)

// batchDescriptor implements TableDescriptor for batch items.
type batchDescriptor struct{}

func (batchDescriptor) TableName() string { return "batch_items" }
func (batchDescriptor) Schema() string    { return batchSchemaSql }
func (batchDescriptor) ExtraColumns() []string {
	return []string{colProcessorID, colPriority, colEpoch, colRecoveryAttempts, colResumable}
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
		colResumable:        item.Resumable,
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
		resumable, _ := extras[i][colResumable].(bool)
		items[i] = &api.BatchItem{
			BaseIndexes:      *indexes[i],
			BaseContents:     *contents[i],
			ProcessorID:      processorID,
			Priority:         priority,
			Epoch:            epoch,
			RecoveryAttempts: recoveryAttempts,
			Resumable:        resumable,
		}
	}

	return
}

func (c *PostgresBatchDBClient) DBUpdate(ctx context.Context, item *api.BatchItem, expectedStatus []byte) (err error) {
	if item == nil {
		err = fmt.Errorf("item is nil")
		return
	}
	var updateConditions map[string]any
	if item.Epoch > 0 || item.BumpEpoch {
		updateConditions = map[string]any{colEpoch: item.Epoch}
	}
	var rawSets []string
	if item.BumpEpoch {
		rawSets = append(rawSets, colEpoch+" = "+colEpoch+" + 1")
	}
	if item.ExpectedResumable != nil {
		if updateConditions == nil {
			updateConditions = make(map[string]any)
		}
		updateConditions[colResumable] = *item.ExpectedResumable
	}
	if err = c.update(ctx, &item.BaseIndexes, &item.BaseContents, expectedStatus, updateConditions, rawSets); err != nil {
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
