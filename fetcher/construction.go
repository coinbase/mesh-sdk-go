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
	"fmt"

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
) (string, *Error) {
	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionCombine(ctx,
		&types.ConstructionCombineRequest{
			NetworkIdentifier:   network,
			UnsignedTransaction: unsignedTransaction,
			Signatures:          signatures,
		},
	)
	if err != nil {
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /construction/combine %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return "", fetcherErr
	}

	if err := asserter.ConstructionCombineResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /construction/combine %s", ErrAssertionFailed, err.Error()),
		}
		return "", fetcherErr
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
) (string, map[string]interface{}, *Error) {
	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionDerive(ctx,
		&types.ConstructionDeriveRequest{
			NetworkIdentifier: network,
			PublicKey:         publicKey,
			Metadata:          metadata,
		},
	)
	if err != nil {
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /construction/derive %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return "", nil, fetcherErr
	}

	if err := asserter.ConstructionDeriveResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /construction/derive %s", ErrAssertionFailed, err.Error()),
		}
		return "", nil, fetcherErr
	}

	return response.Address, response.Metadata, nil
}

// ConstructionHash returns the network-specific transaction hash for
// a signed transaction.
func (f *Fetcher) ConstructionHash(
	ctx context.Context,
	network *types.NetworkIdentifier,
	signedTransaction string,
) (*types.TransactionIdentifier, *Error) {
	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionHash(ctx,
		&types.ConstructionHashRequest{
			NetworkIdentifier: network,
			SignedTransaction: signedTransaction,
		},
	)
	if err != nil {
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /construction/hash %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, fetcherErr
	}

	if err := asserter.TransactionIdentifierResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /construction/hash %s", ErrAssertionFailed, err.Error()),
		}
		return nil, fetcherErr
	}

	return response.TransactionIdentifier, nil
}

// ConstructionMetadata returns the validated response
// from the ConstructionMetadata method.
func (f *Fetcher) ConstructionMetadata(
	ctx context.Context,
	network *types.NetworkIdentifier,
	options map[string]interface{},
) (map[string]interface{}, *Error) {
	metadata, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionMetadata(ctx,
		&types.ConstructionMetadataRequest{
			NetworkIdentifier: network,
			Options:           options,
		},
	)
	if err != nil {
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /construction/metadata %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, fetcherErr
	}

	if err := asserter.ConstructionMetadataResponse(metadata); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /construction/metadata %s", ErrAssertionFailed, err.Error()),
		}
		return nil, fetcherErr
	}

	return metadata.Metadata, nil
}

// ConstructionParse is called on both unsigned and signed transactions to
// understand the intent of the formulated transaction.
//
// This is run as a sanity check before signing (after `/construction/payloads`)
// and before broadcast (after `/construction/combine`).
func (f *Fetcher) ConstructionParse(
	ctx context.Context,
	network *types.NetworkIdentifier,
	signed bool,
	transaction string,
) ([]*types.Operation, []string, map[string]interface{}, *Error) {
	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionParse(ctx,
		&types.ConstructionParseRequest{
			NetworkIdentifier: network,
			Signed:            signed,
			Transaction:       transaction,
		},
	)
	if err != nil {
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /construction/parse %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, nil, nil, fetcherErr
	}

	if err := f.Asserter.ConstructionParseResponse(response, signed); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /construction/parse %s", ErrAssertionFailed, err.Error()),
		}
		return nil, nil, nil, fetcherErr
	}

	return response.Operations, response.Signers, response.Metadata, nil
}

// ConstructionPayloads is called with an array of operations
// and the response from `/construction/metadata`. It returns an
// unsigned transaction blob and a collection of payloads that must
// be signed by particular addresses using a certain SignatureType.
//
// The array of operations provided in transaction construction often times
// can not specify all "effects" of a transaction (consider invoked transactions
// in Ethereum). However, they can deterministically specify the "intent"
// of the transaction, which is sufficient for construction. For this reason,
// parsing the corresponding transaction in the Data API (when it lands on chain)
// will contain a superset of whatever operations were provided during construction.
func (f *Fetcher) ConstructionPayloads(
	ctx context.Context,
	network *types.NetworkIdentifier,
	operations []*types.Operation,
	metadata map[string]interface{},
) (string, []*types.SigningPayload, *Error) {
	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionPayloads(ctx,
		&types.ConstructionPayloadsRequest{
			NetworkIdentifier: network,
			Operations:        operations,
			Metadata:          metadata,
		},
	)

	if err != nil {
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /construction/payloads %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return "", nil, fetcherErr
	}

	if err := asserter.ConstructionPayloadsResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /construction/payloads %s", ErrAssertionFailed, err.Error()),
		}
		return "", nil, fetcherErr
	}

	return response.UnsignedTransaction, response.Payloads, nil
}

// ConstructionPreprocess is called prior to `/construction/payloads` to construct a
// request for any metadata that is needed for transaction construction
// given (i.e. account nonce).
//
// The request returned from this method will be used by the caller (in a
// different execution environment) to call the `/construction/metadata`
// endpoint.
func (f *Fetcher) ConstructionPreprocess(
	ctx context.Context,
	network *types.NetworkIdentifier,
	operations []*types.Operation,
	metadata map[string]interface{},
) (map[string]interface{}, *Error) {
	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionPreprocess(ctx,
		&types.ConstructionPreprocessRequest{
			NetworkIdentifier: network,
			Operations:        operations,
			Metadata:          metadata,
		},
	)

	if err != nil {
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /construction/preprocess %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, fetcherErr
	}

	if err := asserter.ConstructionPreprocessResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /construction/preprocess %s", ErrAssertionFailed, err.Error()),
		}
		return nil, fetcherErr
	}

	return response.Options, nil
}

// ConstructionSubmit returns the validated response
// from the ConstructionSubmit method.
func (f *Fetcher) ConstructionSubmit(
	ctx context.Context,
	network *types.NetworkIdentifier,
	signedTransaction string,
) (*types.TransactionIdentifier, map[string]interface{}, *Error) {
	submitResponse, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionSubmit(
		ctx,
		&types.ConstructionSubmitRequest{
			NetworkIdentifier: network,
			SignedTransaction: signedTransaction,
		},
	)
	if err != nil {
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /construction/submit %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, nil, fetcherErr
	}

	if err := asserter.TransactionIdentifierResponse(submitResponse); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /construction/submit %s", ErrAssertionFailed, err.Error()),
		}
		return nil, nil, fetcherErr
	}

	return submitResponse.TransactionIdentifier, submitResponse.Metadata, nil
}
