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
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
)

// PopulateInput populates user defined variables in the input
// with their corresponding values from the execution state.
func PopulateInput(state string, input string) (string, error) {
	re := regexp.MustCompile(`\{\{[^\}]*\}\}`)

	var err error
	input = re.ReplaceAllStringFunc(input, func(match string) string {
		// remove special characters
		match = strings.Replace(match, "{{", "", 1)
		match = strings.Replace(match, "}}", "", 1)

		value := gjson.Get(state, match)
		if !value.Exists() {
			err = fmt.Errorf("%s is not present in state: %w", match, ErrVariableNotFound)
			return ""
		}

		return value.Raw
	})
	if err != nil {
		return "", fmt.Errorf("unable to insert variables: %w", err)
	}

	if !gjson.Valid(input) {
		log.Printf("invalid json: %s\n", input)
		return "", ErrInvalidJSON
	}

	return input, nil
}
