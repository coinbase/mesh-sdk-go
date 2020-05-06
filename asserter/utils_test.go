// Copyright 2020 Coinbase, Inc.
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

package asserter

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestJSONObject(t *testing.T) {
	var tests = map[string]struct {
		obj json.RawMessage
		err bool
	}{
		"nil": {
			obj: nil,
			err: false,
		},
		"json array": {
			obj: json.RawMessage(`["hello"]`),
			err: true,
		},
		"json number": {
			obj: json.RawMessage(`5`),
			err: true,
		},
		"json string": {
			obj: json.RawMessage(`"hello"`),
			err: true,
		},
		"json object": {
			obj: json.RawMessage(`{"hello":"cool", "bye":[1,2,3]}`),
			err: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			if test.err {
				assert.Error(t, JSONObject(test.obj))
			} else {
				assert.NoError(t, JSONObject(test.obj))
			}
		})
	}
}
