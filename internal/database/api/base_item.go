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

package api

import "fmt"

// BaseQuery contains the common query parameters used to retrieve items from the database.
type BaseQuery struct {
	IDs             []string
	TenantID        string
	TagSelectors    Tags
	TagsLogicalCond LogicalCond
	Expired         bool

	// AfterID is a keyset pagination cursor. When set, only items that sort
	// strictly after this ID are returned and the 'start' offset passed to DBGet
	// is ignored. The ID does not have to exist.
	AfterID string
	// Descending returns items ordered by ID descending instead of ascending.
	// With AfterID set, "after" means smaller than AfterID.
	Descending bool
}

// BaseIndexes contains common indexed fields for all database items.
// These fields are used for querying and filtering.
type BaseIndexes struct {
	// ID is the item's unique ID.
	ID string
	// TenantID is the identifier for multi-tenancy support.
	TenantID string
	// Expiry is the Unix timestamp (in seconds) when the item expires.
	Expiry int64
	// Tags are key-value pairs that enable filtering items based on their contents.
	Tags Tags
}

// BaseContents contains the serialized data columns for a database item.
type BaseContents struct {
	// Spec contains the serialized specification/static data.
	Spec []byte
	// Status contains the serialized status/dynamic data.
	Status []byte
}

// Validate validates a BaseIndexes for required fields.
func (b *BaseIndexes) Validate() error {
	if b == nil {
		return fmt.Errorf("item is nil")
	}
	if len(b.ID) == 0 {
		return fmt.Errorf("ID is empty")
	}
	return nil
}
