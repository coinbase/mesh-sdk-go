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

func TestErrParser(t *testing.T) {
	err1 := errors.New("error 1")
	err2 := errors.New("error 2")
	err3 := errors.New("error 3")

	var tests = map[string]struct {
		err     error
		errList []error
		found   bool
	}{
		"intent error": {
			err:     err1,
			errList: []error{err1, err2, err3},
			found:   true,
		},
		"match operations error": {
			err:     errors.New("this is not an error we expect to find"),
			errList: []error{err1, err2},
			found:   false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			found := FindError(test.errList, test.err)
			assert.Equal(t, test.found, found)
		})
	}
}
