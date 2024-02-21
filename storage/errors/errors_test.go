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

package errors

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
		"balance storage error": {
			err:    ErrNegativeBalance,
			is:     true,
			source: "balance storage error",
		},
		"block storage error": {
			err:    ErrHeadBlockNotFound,
			is:     true,
			source: "block storage error",
		},
		"key storage error": {
			err:    ErrAddrExists,
			is:     true,
			source: "key storage error",
		},
		"not a storage error": {
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
