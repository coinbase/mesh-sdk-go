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

// ConstructionCombine creates a network-specific transaction from an unsigned
// transaction and an array of provided signatures.
//
// The signed transaction returned from this method will be sent to the
// `/construction/submit` endpoint by the caller.
func (f *Fetcher) ConstructionCombine(
	ctx context.Context,
	network *types.NetworkIdentifier,
	unsignedTransaction string,
	signatures []*types.Signature,
) (string, error) {
	response, _, err := f.rosettaClient.ConstructionAPI.ConstructionCombine(ctx,
		&types.ConstructionCombineRequest{
			NetworkIdentifier:   network,
			UnsignedTransaction: unsignedTransaction,
			Signatures:          signatures,
		},
	)
	if err != nil {
		return "", err
	}

	if err := asserter.ConstructionCombine(response); err != nil {
		return "", err
	}

	return response.SignedTransaction, nil
}

// ConstructionDerive returns the network-specific address associated with a
// public key.
//
// Blockchains that require an on-chain action to create an
// account should not implement this method.
func (f *Fetcher) ConstructionDerive(
	ctx context.Context,
	network *types.NetworkIdentifier,
	publicKey *types.PublicKey,
	metadata map[string]interface{},
) (string, map[string]interface{}, error) {
	response, _, err := f.rosettaClient.ConstructionAPI.ConstructionDerive(ctx,
		&types.ConstructionDeriveRequest{
			NetworkIdentifier: network,
			PublicKey:         publicKey,
			Metadata:          metadata,
		},
	)
	if err != nil {
		return "", nil, err
	}

	if err := asserter.ConstructionDerive(response); err != nil {
		return "", nil, err
	}

	return response.Address, response.Metadata, nil
}

// ConstructionHash returns the network-specific transaction hash for
// a signed transaction.
func (f *Fetcher) ConstructionHash(
	ctx context.Context,
	network *types.NetworkIdentifier,
	signedTransaction string,
) (string, error) {
	response, _, err := f.rosettaClient.ConstructionAPI.ConstructionHash(ctx,
		&types.ConstructionHashRequest{
			NetworkIdentifier: network,
			SignedTransaction: signedTransaction,
		},
	)
	if err != nil {
		return "", err
	}

	if err := asserter.ConstructionHash(response); err != nil {
		return "", err
	}

	return response.TransactionHash, nil
}

// ConstructionMetadata returns the validated response
// from the ConstructionMetadata method.
func (f *Fetcher) ConstructionMetadata(
	ctx context.Context,
	network *types.NetworkIdentifier,
	options map[string]interface{},
) (map[string]interface{}, error) {
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

func (f *Fetcher) ConstructionParse()      {}
func (f *Fetcher) ConstructionPayloads()   {}
func (f *Fetcher) ConstructionPreprocess() {}

// ConstructionSubmit returns the validated response
// from the ConstructionSubmit method.
func (f *Fetcher) ConstructionSubmit(
	ctx context.Context,
	network *types.NetworkIdentifier,
	signedTransaction string,
) (*types.TransactionIdentifier, map[string]interface{}, error) {
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
