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

package types

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstructPartialBlockIdentifier(t *testing.T) {
	blockIdentifier := &BlockIdentifier{
		Index: 1,
		Hash:  "block 1",
	}

	partialBlockIdentifier := &PartialBlockIdentifier{
		Index: &blockIdentifier.Index,
		Hash:  &blockIdentifier.Hash,
	}

	assert.Equal(
		t,
		partialBlockIdentifier,
		ConstructPartialBlockIdentifier(blockIdentifier),
	)
}

func TestHash(t *testing.T) {
	var tests = map[string][]interface{}{
		"simple": []interface{}{
			1,
			1,
		},
		"complex": []interface{}{
			map[string]interface{}{
				"a":     "b",
				"b":     "c",
				"c":     "d",
				"blahz": json.RawMessage(`{"test":6, "wha":{"sweet":3, "nice":true}, "neat0":"hello"}`),
				"d": map[string]interface{}{
					"t":    "p",
					"e":    2,
					"k":    "l",
					"blah": json.RawMessage(`{"test":2, "neat":"hello", "cool":{"sweet":3, "nice":true}}`),
				},
			},
			map[string]interface{}{
				"b":     "c",
				"blahz": json.RawMessage(`{"wha":{"sweet":3, "nice":true},"test":6, "neat0":"hello"}`),
				"a":     "b",
				"d": map[string]interface{}{
					"e":    2,
					"k":    "l",
					"t":    "p",
					"blah": json.RawMessage(`{"test":2, "neat":"hello", "cool":{"nice":true, "sweet":3}}`),
				},
				"c": "d",
			},
			map[string]interface{}{
				"a": "b",
				"d": map[string]interface{}{
					"k":    "l",
					"t":    "p",
					"blah": json.RawMessage(`{"test":2, "cool":{"nice":true, "sweet":3}, "neat":"hello"}`),
					"e":    2,
				},
				"c":     "d",
				"blahz": json.RawMessage(`{"wha":{"nice":true, "sweet":3},"test":6, "neat0":"hello"}`),
				"b":     "c",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var val string
			for _, v := range test {
				if val == "" {
					val = Hash(v)
				} else {
					assert.Equal(t, val, Hash(v))
				}
			}
		})
	}
}
