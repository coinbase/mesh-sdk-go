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

package asserter

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErr(t *testing.T) {
	var tests = map[string]struct {
		err    error
		is     bool
		source string
	}{
		"account balance error": {
			err:    ErrReturnedBlockHashMismatch,
			is:     true,
			source: "account balance error",
		},
		"block error": {
			err:    ErrBlockIdentifierIsNil,
			is:     true,
			source: "block error",
		},
		"coin error": {
			err:    ErrCoinChangeIsNil,
			is:     true,
			source: "coin error",
		},
		"construction error": {
			err:    ErrConstructionMetadataResponseIsNil,
			is:     true,
			source: "construction error",
		},
		"network error": {
			err:    ErrNetworkIdentifierIsNil,
			is:     true,
			source: "network error",
		},
		"server error": {
			err:    ErrNoSupportedNetworks,
			is:     true,
			source: "server error",
		},
		"not an assert error": {
			err:    errors.New("blah"),
			is:     false,
			source: "",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			is, source := Err(test.err)
			assert.Equal(t, test.is, is)
			assert.Equal(t, test.source, source)
		})
	}
}
