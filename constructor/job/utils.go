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

package job

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// UnmarshalInput attempts to strictly unmarshal some input
// into output.
func UnmarshalInput(input []byte, output interface{}) error {
	// To prevent silent erroring, we explicitly
	// reject any unknown fields.
	dec := json.NewDecoder(bytes.NewReader(input))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&output); err != nil {
		return fmt.Errorf("unable to decode: %w", err)
	}

	return nil
}
