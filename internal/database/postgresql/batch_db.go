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
	"fmt"
	"strings"

	"github.com/go-logr/logr"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
	"github.com/llm-d/llm-d-batch-gateway/internal/shared/openai"
)

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

// Compile-time check: batchDescriptor implements TableDescriptor.
var _ TableDescriptor = (*batchDescriptor)(nil)

// batchDescriptor implements TableDescriptor for batch items.
type batchDescriptor struct{}

func (batchDescriptor) TableName() string      { return "batch_items" }
func (batchDescriptor) ExtraColumns() []string { return nil }

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
	if err = c.store(ctx, &item.BaseIndexes, &item.BaseContents, nil); err != nil {
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

	indexes, contents, _, cursor, expectMore, err := c.get(
		ctx, &query.BaseQuery, includeStatic, start, limit, nil, rawConditions)
	if err != nil {
		return
	}

	items = make([]*api.BatchItem, len(indexes))
	for i := range indexes {
		items[i] = &api.BatchItem{
			BaseIndexes:  *indexes[i],
			BaseContents: *contents[i],
		}
	}

	return
}

func (c *PostgresBatchDBClient) DBUpdate(ctx context.Context, item *api.BatchItem, expectedStatus []byte) (err error) {
	if item == nil {
		err = fmt.Errorf("item is nil")
		return
	}
	if err = c.update(ctx, &item.BaseIndexes, &item.BaseContents, expectedStatus); err != nil {
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
