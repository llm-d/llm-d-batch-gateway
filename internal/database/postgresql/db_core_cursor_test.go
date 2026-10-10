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
	"reflect"
	"strings"
	"testing"

	"github.com/llm-d/llm-d-batch-gateway/internal/database/api"
)

func TestBuildGetQuery_Ordering(t *testing.T) {
	tests := []struct {
		name     string
		query    api.BaseQuery
		start    int
		wantSQL  string // suffix of the generated statement
		wantArgs []any
	}{
		{
			name:     "offset ascending (default)",
			query:    api.BaseQuery{TenantID: "t"},
			start:    7,
			wantSQL:  "WHERE tenant_id = $1 ORDER BY id ASC OFFSET $2 LIMIT $3",
			wantArgs: []any{"t", 7, 10},
		},
		{
			name:     "offset descending",
			query:    api.BaseQuery{TenantID: "t", Descending: true},
			start:    7,
			wantSQL:  "WHERE tenant_id = $1 ORDER BY id DESC OFFSET $2 LIMIT $3",
			wantArgs: []any{"t", 7, 10},
		},
		{
			name:     "cursor ascending ignores offset",
			query:    api.BaseQuery{TenantID: "t", AfterID: "file_b"},
			start:    7,
			wantSQL:  "WHERE tenant_id = $1 AND id > $2 ORDER BY id ASC OFFSET $3 LIMIT $4",
			wantArgs: []any{"t", "file_b", 0, 10},
		},
		{
			name:     "cursor descending ignores offset",
			query:    api.BaseQuery{TenantID: "t", AfterID: "file_b", Descending: true},
			start:    7,
			wantSQL:  "WHERE tenant_id = $1 AND id < $2 ORDER BY id DESC OFFSET $3 LIMIT $4",
			wantArgs: []any{"t", "file_b", 0, 10},
		},
	}

	core := &pgCore{desc: batchDescriptor{}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sql, args := core.buildGetQuery(&tt.query, false, tt.start, 10, nil, nil)
			if !strings.HasSuffix(sql, tt.wantSQL) {
				t.Errorf("sql = %q, want suffix %q", sql, tt.wantSQL)
			}
			if !reflect.DeepEqual(args, tt.wantArgs) {
				t.Errorf("args = %v, want %v", args, tt.wantArgs)
			}
		})
	}
}
