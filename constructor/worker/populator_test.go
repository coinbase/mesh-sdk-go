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

package worker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPopulateInput(t *testing.T) {
	var tests = map[string]struct {
		state string
		input string

		output string
		err    error
	}{
		"no variables": {
			input:  `{"foo": "bar"}`,
			output: `{"foo": "bar"}`,
		},
		"single variable (string)": {
			state:  `{"network": "test"}`,
			input:  `{"foo": {{network}}}`,
			output: `{"foo": "test"}`,
		},
		"single variable (object)": {
			state: `{
				"network": {"network":"Testnet3", "blockchain":"Bitcoin"}
				}`,
			input:  `{"foo": {{network}}}`,
			output: `{"foo": {"network":"Testnet3", "blockchain":"Bitcoin"}}`,
		},
		"single variable used twice": {
			state: `{
				"network": {"network":"Testnet3", "blockchain":"Bitcoin"}
			}`,
			input:  `{"foo": {{network}}, "foo2": {{network}}}`,
			output: `{"foo": {"network":"Testnet3", "blockchain":"Bitcoin"}, "foo2": {"network":"Testnet3", "blockchain":"Bitcoin"}}`, // nolint
		},
		"multiple variables": {
			state: `{ 
				"network": {"network":"Testnet3", "blockchain":"Bitcoin"},
				"key": {"public_key":{"curve_type": "secp256k1", "hex_bytes": "03a6946b55ee2da05d57049a31df1bfd97ff2e5810057f4fb63e505622cdafd513"}}}`, // nolint
			input:  `{"foo": {{network}}, "bar": {{key.public_key}}}`,
			output: `{"foo": {"network":"Testnet3", "blockchain":"Bitcoin"}, "bar": {"curve_type": "secp256k1", "hex_bytes": "03a6946b55ee2da05d57049a31df1bfd97ff2e5810057f4fb63e505622cdafd513"}}`, // nolint
		},
		"single variable (doesn't exist)": {
			input: `{"foo": {{network}}}`,
			err:   errors.New("network is not present in state"),
		},
		"single variable path doesn't exist": {
			state: `{"network": {"network":"Testnet3", "blockchain":"Bitcoin"}}`,
			input: `{"foo": {{network.test}}}`,
			err:   errors.New("network.test is not present in state"),
		},
		"invalid json result": {
			state: `{"network": {"network":"Testnet3", "blockchain":"Bitcoin"}}`,
			input: `{{`,
			err:   errors.New("populated input is not valid JSON"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			output, err := PopulateInput(test.state, test.input)
			if test.err != nil {
				assert.Contains(t, err.Error(), test.err.Error())
				assert.Equal(t, "", output)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.output, output)
			}
		})
	}
}
