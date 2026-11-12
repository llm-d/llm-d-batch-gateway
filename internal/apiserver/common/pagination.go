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

package common

import (
	"fmt"
	"strconv"
	"strings"
)

// ParseAfter interprets the "after" list query parameter.
//
// Following the OpenAI API, "after" is the ID of the last object of the previous
// page; idPrefix (e.g. "file_") is the prefix of valid IDs for the listed object.
// For backward compatibility a non-negative integer is still accepted and treated
// as an offset. Exactly one of offset and afterID is set in the result; an empty
// value yields zero values for both.
func ParseAfter(after, idPrefix string) (offset int, afterID string, err error) {
	if after == "" {
		return 0, "", nil
	}
	if isDigits(after) {
		offset, err = strconv.Atoi(after)
		if err != nil {
			return 0, "", fmt.Errorf("after offset %q is out of range", after)
		}
		return offset, "", nil
	}
	if strings.HasPrefix(after, idPrefix) && len(after) > len(idPrefix) {
		return 0, after, nil
	}
	return 0, "", fmt.Errorf("after must be an object ID starting with %q or a non-negative integer offset", idPrefix)
}

func isDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return s != ""
}
