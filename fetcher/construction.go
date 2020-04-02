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

package fetcher

import (
	"context"
	"errors"

	"github.com/coinbase/rosetta-sdk-go/asserter"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"
)

// ConstructionMetadata returns the validated response
// from the ConstructionMetadata method.
func (f *Fetcher) ConstructionMetadata(
	ctx context.Context,
	network *rosetta.NetworkIdentifier,
	account *rosetta.AccountIdentifier,
	method *string,
) (*rosetta.Amount, *map[string]interface{}, error) {
	metadata, _, err := f.rosettaClient.ConstructionAPI.TransactionConstruction(ctx,
		rosetta.TransactionConstructionRequest{
			NetworkIdentifier: network,
			AccountIdentifier: account,
			Method:            method,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	if err := asserter.TransactionConstruction(metadata); err != nil {
		return nil, nil, err
	}

	return metadata.SuggestedFee, metadata.Metadata, nil
}

// ConstructionSubmit returns the validated response
// from the ConstructionSubmit method.
func (f *Fetcher) ConstructionSubmit(
	ctx context.Context,
	signedTransaction string,
) (*rosetta.TransactionIdentifier, *string, *map[string]interface{}, error) {
	if f.Asserter == nil {
		return nil, nil, nil, errors.New("asserter not initialized")
	}

	submitResponse, _, err := f.rosettaClient.ConstructionAPI.TransactionSubmit(ctx, rosetta.TransactionSubmitRequest{
		SignedTransaction: signedTransaction,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	if err := f.Asserter.TransactionSubmit(submitResponse); err != nil {
		return nil, nil, nil, err
	}

	return submitResponse.TransactionIdentifier, &submitResponse.Status, submitResponse.Metadata, nil
}
