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

	"github.com/go-logr/logr"
	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

const (
	colPurpose = "purpose"
)

// Compile-time check: fileTableDescriptor implements TableDescriptor.
var _ TableDescriptor = (*fileTableDescriptor)(nil)

// fileTableDescriptor implements TableDescriptor for file items.
type fileTableDescriptor struct{}

func (fileTableDescriptor) TableName() string      { return "file_items" }
func (fileTableDescriptor) ExtraColumns() []string { return []string{colPurpose} }

// PostgresFileDBClient implements api.FileDBClient using PostgreSQL.
type PostgresFileDBClient struct {
	*pgCore
}

var _ api.FileDBClient = (*PostgresFileDBClient)(nil)

// NewPostgresFileDBClient creates a new PostgreSQL file database client.
func NewPostgresFileDBClient(ctx context.Context, config *PostgreSQLConfig) (*PostgresFileDBClient, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	pgCore, err := newPgCore(ctx, config, fileTableDescriptor{})
	if err != nil {
		return nil, err
	}

	logr.FromContextOrDiscard(ctx).Info("NewPostgresFileDBClient: client created successfully")
	return &PostgresFileDBClient{pgCore}, nil
}

func (c *PostgresFileDBClient) Close() error {
	return c.close()
}

func (c *PostgresFileDBClient) DBStore(ctx context.Context, item *api.FileItem) (err error) {
	if item == nil {
		err = fmt.Errorf("item is nil")
		return
	}

	if err = c.store(ctx, &item.BaseIndexes, &item.BaseContents, map[string]any{
		colPurpose: item.Purpose,
	}); err != nil {
		return
	}
	return
}

func (c *PostgresFileDBClient) DBGet(
	ctx context.Context, query *api.FileQuery,
	includeStatic bool, start, limit int,
) (items []*api.FileItem, cursor int, expectMore bool, err error) {
	if query == nil {
		return
	}

	var extraFilters map[string]any
	if query.Purpose != "" {
		extraFilters = map[string]any{colPurpose: query.Purpose}
	}

	indexes, contents, extras, cursor, expectMore, err := c.get(
		ctx, &query.BaseQuery, includeStatic, start, limit, extraFilters, nil)
	if err != nil {
		return
	}

	items = make([]*api.FileItem, len(indexes))
	for i := range indexes {
		items[i] = &api.FileItem{
			BaseIndexes:  *indexes[i],
			BaseContents: *contents[i],
			Purpose:      extras[i][colPurpose],
		}
	}

	return
}

func (c *PostgresFileDBClient) DBUpdate(ctx context.Context, item *api.FileItem, expectedStatus []byte) (err error) {
	if item == nil {
		err = fmt.Errorf("item is nil")
		return
	}
	if err = c.update(ctx, &item.BaseIndexes, &item.BaseContents, expectedStatus); err != nil {
		return
	}
	return
}

func (c *PostgresFileDBClient) DBDelete(ctx context.Context, ids []string) (deletedIDs []string, err error) {
	if deletedIDs, err = c.delete(ctx, ids); err != nil {
		return
	}
	return
}
