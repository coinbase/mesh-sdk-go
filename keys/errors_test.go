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

package keys

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestErr(t *testing.T) {
	var tests = map[string]struct {
		err error
		is  bool
	}{
		"is a keys error": {
			err: ErrPrivKeyLengthInvalid,
			is:  true,
		},
		"not a keys error": {
			err: errors.New("blah"),
			is:  false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			is := Err(test.err)
			assert.Equal(t, test.is, is)
		})
	}
}
