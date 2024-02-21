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
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return "", &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionCombine(ctx,
		&types.ConstructionCombineRequest{
			NetworkIdentifier:   network,
			UnsignedTransaction: unsignedTransaction,
			Signatures:          signatures,
		},
	)
	if err != nil {
		return "", f.RequestFailedError(clientErr, err, "/construction/combine")
	}

	if err := asserter.ConstructionCombineResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/construction/combine response is invalid: %w", err),
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
) (*types.AccountIdentifier, map[string]interface{}, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionDerive(ctx,
		&types.ConstructionDeriveRequest{
			NetworkIdentifier: network,
			PublicKey:         publicKey,
			Metadata:          metadata,
		},
	)
	if err != nil {
		return nil, nil, f.RequestFailedError(clientErr, err, "/construction/derive")
	}

	if err := asserter.ConstructionDeriveResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/construction/derive response is invalid: %w", err),
		}
		return nil, nil, fetcherErr
	}

	return response.AccountIdentifier, response.Metadata, nil
}

// ConstructionHash returns the network-specific transaction hash for
// a signed transaction.
func (f *Fetcher) ConstructionHash(
	ctx context.Context,
	network *types.NetworkIdentifier,
	signedTransaction string,
) (*types.TransactionIdentifier, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionHash(ctx,
		&types.ConstructionHashRequest{
			NetworkIdentifier: network,
			SignedTransaction: signedTransaction,
		},
	)
	if err != nil {
		return nil, f.RequestFailedError(clientErr, err, "/construction/hash")
	}

	if err := asserter.TransactionIdentifierResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/construction/hash response is invalid: %w", err),
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
	publicKeys []*types.PublicKey,
) (map[string]interface{}, []*types.Amount, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	metadata, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionMetadata(ctx,
		&types.ConstructionMetadataRequest{
			NetworkIdentifier: network,
			Options:           options,
			PublicKeys:        publicKeys,
		},
	)
	if err != nil {
		return nil, nil, f.RequestFailedError(clientErr, err, "/construction/metadata")
	}

	if err := asserter.ConstructionMetadataResponse(metadata); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/construction/metadata response is invalid: %w", err),
		}
		return nil, nil, fetcherErr
	}

	return metadata.Metadata, metadata.SuggestedFee, nil
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
) ([]*types.Operation, []*types.AccountIdentifier, map[string]interface{}, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, nil, nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionParse(ctx,
		&types.ConstructionParseRequest{
			NetworkIdentifier: network,
			Signed:            signed,
			Transaction:       transaction,
		},
	)
	if err != nil {
		return nil, nil, nil, f.RequestFailedError(clientErr, err, "/construction/parse")
	}

	if err := f.Asserter.ConstructionParseResponse(response, signed); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/construction/parse response is invalid: %w", err),
		}
		return nil, nil, nil, fetcherErr
	}

	return response.Operations, response.AccountIdentifierSigners, response.Metadata, nil
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
	publicKeys []*types.PublicKey,
) (string, []*types.SigningPayload, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return "", nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionPayloads(ctx,
		&types.ConstructionPayloadsRequest{
			NetworkIdentifier: network,
			Operations:        operations,
			Metadata:          metadata,
			PublicKeys:        publicKeys,
		},
	)

	if err != nil {
		return "", nil, f.RequestFailedError(clientErr, err, "/construction/payloads")
	}

	if err := asserter.ConstructionPayloadsResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/construction/payloads response is invalid: %w", err),
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
) (map[string]interface{}, []*types.AccountIdentifier, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionPreprocess(ctx,
		&types.ConstructionPreprocessRequest{
			NetworkIdentifier: network,
			Operations:        operations,
			Metadata:          metadata,
		},
	)

	if err != nil {
		return nil, nil, f.RequestFailedError(clientErr, err, "/construction/preprocess")
	}

	if err := asserter.ConstructionPreprocessResponse(response); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/construction/preprocess response is invalid: %w", err),
		}
		return nil, nil, fetcherErr
	}

	return response.Options, response.RequiredPublicKeys, nil
}

// ConstructionSubmit returns the validated response
// from the ConstructionSubmit method.
func (f *Fetcher) ConstructionSubmit(
	ctx context.Context,
	network *types.NetworkIdentifier,
	signedTransaction string,
) (*types.TransactionIdentifier, map[string]interface{}, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	submitResponse, clientErr, err := f.rosettaClient.ConstructionAPI.ConstructionSubmit(
		ctx,
		&types.ConstructionSubmitRequest{
			NetworkIdentifier: network,
			SignedTransaction: signedTransaction,
		},
	)
	if err != nil {
		return nil, nil, f.RequestFailedError(clientErr, err, "/construction/submit")
	}

	if err := asserter.TransactionIdentifierResponse(submitResponse); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/construction/submit response is invalid: %w", err),
		}
		return nil, nil, fetcherErr
	}

	return submitResponse.TransactionIdentifier, submitResponse.Metadata, nil
}
