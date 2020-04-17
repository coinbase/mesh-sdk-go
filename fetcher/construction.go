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

	"github.com/coinbase/rosetta-sdk-go/asserter"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// ConstructionMetadata returns the validated response
// from the ConstructionMetadata method.
func (f *Fetcher) ConstructionMetadata(
	ctx context.Context,
	network *types.NetworkIdentifier,
	options *map[string]interface{},
) (*map[string]interface{}, error) {
	metadata, _, err := f.rosettaClient.ConstructionAPI.ConstructionMetadata(ctx,
		&types.ConstructionMetadataRequest{
			NetworkIdentifier: network,
			Options:           options,
		},
	)
	if err != nil {
		return nil, err
	}

	if err := asserter.ConstructionMetadata(metadata); err != nil {
		return nil, err
	}

	return metadata.Metadata, nil
}

// ConstructionSubmit returns the validated response
// from the ConstructionSubmit method.
func (f *Fetcher) ConstructionSubmit(
	ctx context.Context,
	network *types.NetworkIdentifier,
	signedTransaction string,
) (*types.TransactionIdentifier, *map[string]interface{}, error) {
	submitResponse, _, err := f.rosettaClient.ConstructionAPI.ConstructionSubmit(
		ctx,
		&types.ConstructionSubmitRequest{
			NetworkIdentifier: network,
			SignedTransaction: signedTransaction,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	if err := asserter.ConstructionSubmit(submitResponse); err != nil {
		return nil, nil, err
	}

	return submitResponse.TransactionIdentifier, submitResponse.Metadata, nil
}
