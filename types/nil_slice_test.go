// Copyright 2024 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNilSliceMarshaling tests that nil slices are encoded as empty arrays
// instead of null. This is important for cross-language compatibility.
// See: https://github.com/coinbase/mesh-sdk-go/issues/62
func TestNilSliceMarshaling(t *testing.T) {
	// Test Allow type with nil slices
	allow := &Allow{
		OperationStatuses:     nil,
		OperationTypes:        nil,
		Errors:                nil,
		CallMethods:           nil,
		BalanceExemptions:     nil,
		HistoricalBalanceLookup: false,
	}

	j, err := json.Marshal(allow)
	assert.NoError(t, err)

	// Verify that nil slices are encoded as "[]" not "null"
	assert.Contains(t, string(j), `"operation_statuses":[]`)
	assert.Contains(t, string(j), `"operation_types":[]`)
	assert.Contains(t, string(j), `"errors":[]`)
	assert.Contains(t, string(j), `"call_methods":[]`)
	assert.Contains(t, string(j), `"balance_exemptions":[]`)

	// Verify no "null" values for slices
	assert.NotContains(t, string(j), `"operation_statuses":null`)
	assert.NotContains(t, string(j), `"operation_types":null`)
	assert.NotContains(t, string(j), `"errors":null`)
}

// TestEmptySliceMarshaling tests that empty slices are still encoded correctly
func TestEmptySliceMarshaling(t *testing.T) {
	allow := &Allow{
		OperationStatuses:     []*OperationStatus{},
		OperationTypes:        []string{},
		Errors:                []*Error{},
		CallMethods:           []string{},
		BalanceExemptions:     []*BalanceExemption{},
		HistoricalBalanceLookup: false,
	}

	j, err := json.Marshal(allow)
	assert.NoError(t, err)

	// Verify that empty slices are encoded as "[]"
	assert.Contains(t, string(j), `"operation_statuses":[]`)
	assert.Contains(t, string(j), `"operation_types":[]`)
	assert.Contains(t, string(j), `"errors":[]`)
}
