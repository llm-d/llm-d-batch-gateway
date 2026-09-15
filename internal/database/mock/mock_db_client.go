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

// The file provides in-memory mock implementations for DBClient.
package mock

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

// MockDBClient is a generic in-memory implementation of api.DBClient[T, Q] for testing.
type MockDBClient[T any, Q any] struct {
	items           sync.Map
	idGetter        func(*T) string
	baseQueryGetter func(*Q) *api.BaseQuery

	// QueryFilter is an optional callback for query-specific filtering beyond BaseQuery.
	// When set, it is called for each item during DBGet. Return true to include the item.
	QueryFilter func(item *T, query *Q) bool
}

// NewMockDBClient creates a new mock DB client.
// idGetter extracts the ID from the item pointer.
// baseQueryGetter extracts the BaseQuery from the query pointer.
func NewMockDBClient[T any, Q any](idGetter func(*T) string, baseQueryGetter func(*Q) *api.BaseQuery) *MockDBClient[T, Q] {
	return &MockDBClient[T, Q]{
		idGetter:        idGetter,
		baseQueryGetter: baseQueryGetter,
	}
}

func (m *MockDBClient[T, Q]) DBStore(ctx context.Context, item *T) (err error) {
	if item == nil {
		return fmt.Errorf("item is nil")
	}
	id := m.idGetter(item)
	if id == "" {
		return fmt.Errorf("item has empty ID")
	}
	m.items.Store(id, item)
	return nil
}

func (m *MockDBClient[T, Q]) DBGet(
	ctx context.Context, query *Q,
	includeStatic bool, start, limit int) (
	items []*T, cursor int, expectMore bool, err error) {

	bq := m.baseQueryGetter(query)
	var allMatches []*T

	matchesAll := func(item *T) bool {
		if !m.matchesFilters(*item, bq) {
			return false
		}
		if m.QueryFilter != nil && !m.QueryFilter(item, query) {
			return false
		}
		return true
	}

	// If IDs are specified, get by IDs
	if len(bq.IDs) > 0 {
		for _, id := range bq.IDs {
			if value, ok := m.items.Load(id); ok {
				if item, ok := value.(*T); ok {
					if matchesAll(item) {
						allMatches = append(allMatches, item)
					}
				}
			}
		}
	} else {
		// Collect all items, applying filters
		m.items.Range(func(key, value any) bool {
			if item, ok := value.(*T); ok {
				if matchesAll(item) {
					allMatches = append(allMatches, item)
				}
			}
			return true
		})
	}

	// Handle pagination
	if start >= len(allMatches) {
		return
	}

	allMatches = allMatches[start:]

	if limit > 0 && len(allMatches) > limit {
		items = allMatches[:limit]
		expectMore = true
	} else {
		items = allMatches
	}

	cursor = start + len(items)

	return
}

func (m *MockDBClient[T, Q]) DBUpdate(ctx context.Context, item *T, expectedStatus []byte) (err error) {
	if item == nil {
		return fmt.Errorf("item is nil")
	}
	id := m.idGetter(item)
	if id == "" {
		return fmt.Errorf("item has empty ID")
	}
	existing, ok := m.items.Load(id)
	if !ok {
		return fmt.Errorf("cannot update item with ID '%s': item doesn't exist", id)
	}

	if existingItem, ok := existing.(*T); ok {
		val := reflect.ValueOf(*existingItem)

		// CAS: check expectedStatus matches current Status field.
		if expectedStatus != nil {
			statusField := val.FieldByName("Status")
			if statusField.IsValid() && statusField.Kind() == reflect.Slice {
				currentStatus, _ := statusField.Interface().([]byte)
				if !bytes.Equal(currentStatus, expectedStatus) {
					return fmt.Errorf("DBUpdate: %w", api.ErrConflict)
				}
			}
		}

		// Epoch fencing: if the update item has Epoch > 0, check it matches.
		updateVal := reflect.ValueOf(*item)
		epochField := updateVal.FieldByName("Epoch")
		if epochField.IsValid() && epochField.Kind() == reflect.Int64 && epochField.Int() > 0 {
			existingEpoch := val.FieldByName("Epoch")
			if existingEpoch.IsValid() && existingEpoch.Int() != epochField.Int() {
				return fmt.Errorf("DBUpdate: %w", api.ErrConflict)
			}
		}
	}

	m.items.Store(id, item)
	return nil
}

func (m *MockDBClient[T, Q]) DBDelete(ctx context.Context, ids []string) (deletedIDs []string, err error) {
	for _, id := range ids {
		if _, ok := m.items.LoadAndDelete(id); ok {
			deletedIDs = append(deletedIDs, id)
		}
	}
	return
}

func (m *MockDBClient[T, Q]) DBUpdateProgress(ctx context.Context, id string, epoch int64, counts api.BatchRequestCounts) error {
	if id == "" {
		return fmt.Errorf("DBUpdateProgress: empty ID")
	}
	existing, ok := m.items.Load(id)
	if !ok {
		return fmt.Errorf("cannot update progress for ID '%s': item doesn't exist", id)
	}

	if existingItem, ok := existing.(*T); ok {
		val := reflect.ValueOf(existingItem).Elem()
		// Mirror the Postgres epoch fence: items with an Epoch field are only
		// updated when the caller carries the current epoch; a stale-epoch write
		// matches no rows, so surface ErrConflict to the caller.
		if epochField := val.FieldByName("Epoch"); epochField.IsValid() && epochField.Kind() == reflect.Int64 && epochField.Int() != epoch {
			return fmt.Errorf("DBUpdateProgress: %w", api.ErrConflict)
		}
		statusField := val.FieldByName("Status")
		if statusField.IsValid() && statusField.CanSet() && statusField.Kind() == reflect.Slice {
			currentStatus, _ := statusField.Interface().([]byte)
			var statusMap map[string]any
			if len(currentStatus) > 0 {
				_ = json.Unmarshal(currentStatus, &statusMap)
			}
			if statusMap == nil {
				statusMap = make(map[string]any)
			}
			statusMap["request_counts"] = map[string]any{
				"total":     counts.Total,
				"completed": counts.Completed,
				"failed":    counts.Failed,
			}
			newStatus, err := json.Marshal(statusMap)
			if err != nil {
				return fmt.Errorf("DBUpdateProgress: marshal status: %w", err)
			}
			statusField.SetBytes(newStatus)
		}
	}
	return nil
}

func (m *MockDBClient[T, Q]) GetContext(parentCtx context.Context, timeLimit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parentCtx, timeLimit)
}

func (m *MockDBClient[T, Q]) Close() error {
	m.items.Clear()
	return nil
}

// matchesFilters checks if an item matches the query filters.
func (m *MockDBClient[T, Q]) matchesFilters(item T, query *api.BaseQuery) bool {
	val := reflect.ValueOf(item)

	// Filter by tenant if specified
	if query.TenantID != "" {
		tenantIDField := val.FieldByName("TenantID")
		if tenantIDField.IsValid() && tenantIDField.Kind() == reflect.String {
			if tenantIDField.String() != query.TenantID {
				return false
			}
		}
	}

	// Filter by expiry if specified
	if query.Expired {
		expiryField := val.FieldByName("Expiry")
		if expiryField.IsValid() && expiryField.Kind() == reflect.Int64 {
			expiry := expiryField.Int()
			if expiry == 0 || expiry > time.Now().Unix() {
				return false
			}
		}
	}

	// Filter by tag selectors if specified
	if len(query.TagSelectors) > 0 {
		tagsField := val.FieldByName("Tags")
		if tagsField.IsValid() && tagsField.Kind() == reflect.Map {
			itemTags, ok := tagsField.Interface().(api.Tags)
			if !ok {
				return false
			}
			for tagKey, tagValue := range query.TagSelectors {
				if itemTagValue, ok := itemTags[tagKey]; !ok || itemTagValue != tagValue {
					return false
				}
			}
		} else {
			return false
		}
	}

	return true
}
