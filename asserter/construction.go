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
	rosetta "github.com/coinbase/rosetta-sdk-go/gen"
)

// TransactionConstruction returns an error if
// the NetworkFee is not a valid rosetta.Amount.
func TransactionConstruction(
	response *rosetta.TransactionConstructionResponse,
) error {
	return Amount(response.NetworkFee)
}

// TransactionSubmit returns an error if
// the rosetta.TransactionIdentifier in the response is not
// valid or if the Submission.Status is not contained
// within the provided validStatuses slice.
func (a *Asserter) TransactionSubmit(
	response *rosetta.TransactionSubmitResponse,
) error {
	if err := TransactionIdentifier(response.TransactionIdentifier); err != nil {
		return err
	}

	return nil
}
