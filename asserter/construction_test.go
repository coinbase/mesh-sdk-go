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
	"errors"
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/asserter/errs"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestConstructionMetadataResponse(t *testing.T) {
	var tests = map[string]struct {
		response *types.ConstructionMetadataResponse
		err      error
	}{
		"valid response": {
			response: &types.ConstructionMetadataResponse{
				Metadata: map[string]interface{}{},
			},
			err: nil,
		},
		"with suggested fee": {
			response: &types.ConstructionMetadataResponse{
				Metadata: map[string]interface{}{},
				SuggestedFee: []*types.Amount{
					validAmount,
				},
			},
			err: nil,
		},
		"with duplicate suggested fee": {
			response: &types.ConstructionMetadataResponse{
				Metadata: map[string]interface{}{},
				SuggestedFee: []*types.Amount{
					validAmount,
					validAmount,
				},
			},
			err: fmt.Errorf("currency %+v used multiple times", validAmount.Currency),
		},
		"nil response": {
			err: errs.ErrConstructionMetadataResponseIsNil,
		},
		"invalid metadata": {
			response: &types.ConstructionMetadataResponse{},
			err:      errs.ErrConstructionMetadataResponseMetadataMissing,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionMetadataResponse(test.response)
			if test.err != nil {
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTransactionIdentifierResponse(t *testing.T) {
	var tests = map[string]struct {
		response *types.TransactionIdentifierResponse
		err      error
	}{
		"valid response": {
			response: &types.TransactionIdentifierResponse{
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "tx1",
				},
			},
			err: nil,
		},
		"nil response": {
			err: errs.ErrTxIdentifierResponseIsNil,
		},
		"invalid transaction identifier": {
			response: &types.TransactionIdentifierResponse{},
			err:      errs.ErrTxIdentifierIsNil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := TransactionIdentifierResponse(test.response)
			assert.True(t, errors.Is(err, test.err))
		})
	}
}

func TestConstructionCombineResponse(t *testing.T) {
	var tests = map[string]struct {
		response *types.ConstructionCombineResponse
		err      error
	}{
		"valid response": {
			response: &types.ConstructionCombineResponse{
				SignedTransaction: "signed tx",
			},
			err: nil,
		},
		"nil response": {
			err: errs.ErrConstructionCombineResponseIsNil,
		},
		"empty signed transaction": {
			response: &types.ConstructionCombineResponse{},
			err:      errs.ErrSignedTxEmpty,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionCombineResponse(test.response)
			assert.True(t, errors.Is(err, test.err))
		})
	}
}

func TestConstructionDeriveResponse(t *testing.T) {
	var tests = map[string]struct {
		response *types.ConstructionDeriveResponse
		err      error
	}{
		"valid response": {
			response: &types.ConstructionDeriveResponse{
				Address: "addr",
				Metadata: map[string]interface{}{
					"name": "hello",
				},
			},
			err: nil,
		},
		"nil response": {
			err: errs.ErrConstructionDeriveResponseIsNil,
		},
		"empty address": {
			response: &types.ConstructionDeriveResponse{
				Metadata: map[string]interface{}{
					"name": "hello",
				},
			},
			err: errs.ErrConstructionDeriveResponseAddrEmpty,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionDeriveResponse(test.response)
			assert.True(t, errors.Is(err, test.err))
		})
	}
}

func TestConstructionParseResponse(t *testing.T) {
	var tests = map[string]struct {
		response *types.ConstructionParseResponse
		signed   bool
		err      error
	}{
		"valid response": {
			response: &types.ConstructionParseResponse{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(0),
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(1),
						},
						RelatedOperations: []*types.OperationIdentifier{
							{Index: int64(0)},
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
				},
				Signers: []string{"account 1"},
				Metadata: map[string]interface{}{
					"extra": "stuff",
				},
			},
			signed: true,
			err:    nil,
		},
		"nil response": {
			err: errs.ErrConstructionParseResponseIsNil,
		},
		"no operations": {
			response: &types.ConstructionParseResponse{
				Signers: []string{"account 1"},
				Metadata: map[string]interface{}{
					"extra": "stuff",
				},
			},
			err: errs.ErrConstructionParseResponseOperationsEmpty,
		},
		"invalid operation ordering": {
			response: &types.ConstructionParseResponse{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(1),
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
				},
				Signers: []string{"account 1"},
				Metadata: map[string]interface{}{
					"extra": "stuff",
				},
			},
			err: errs.ErrOperationIdentifierIndexOutOfOrder,
		},
		"no signers": {
			response: &types.ConstructionParseResponse{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(0),
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(1),
						},
						RelatedOperations: []*types.OperationIdentifier{
							{Index: int64(0)},
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
				},
				Metadata: map[string]interface{}{
					"extra": "stuff",
				},
			},
			signed: true,
			err:    errs.ErrConstructionParseResponseSignersEmptyOnSignedTx,
		},
		"empty string signer": {
			response: &types.ConstructionParseResponse{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(0),
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(1),
						},
						RelatedOperations: []*types.OperationIdentifier{
							{Index: int64(0)},
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
				},
				Signers: []string{""},
				Metadata: map[string]interface{}{
					"extra": "stuff",
				},
			},
			signed: true,
			err:    errs.ErrConstructionParseResponseSignerEmpty,
		},
		"invalid signer unsigned": {
			response: &types.ConstructionParseResponse{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(0),
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(1),
						},
						RelatedOperations: []*types.OperationIdentifier{
							{Index: int64(0)},
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
				},
				Metadata: map[string]interface{}{
					"extra": "stuff",
				},
				Signers: []string{"account 1"},
			},
			signed: false,
			err:    errs.ErrConstructionParseResponseSignersNonEmptyOnUnsignedTx,
		},
		"valid response unsigned": {
			response: &types.ConstructionParseResponse{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(0),
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: int64(1),
						},
						RelatedOperations: []*types.OperationIdentifier{
							{Index: int64(0)},
						},
						Type:    "PAYMENT",
						Account: validAccount,
						Amount:  validAmount,
					},
				},
				Metadata: map[string]interface{}{
					"extra": "stuff",
				},
			},
			signed: false,
			err:    nil,
		},
	}

	asserter, err := NewClientWithResponses(
		&types.NetworkIdentifier{
			Blockchain: "hello",
			Network:    "world",
		},
		&types.NetworkStatusResponse{
			GenesisBlockIdentifier: &types.BlockIdentifier{
				Index: 0,
				Hash:  "block 0",
			},
			CurrentBlockIdentifier: &types.BlockIdentifier{
				Index: 100,
				Hash:  "block 100",
			},
			CurrentBlockTimestamp: MinUnixEpoch + 1,
			Peers: []*types.Peer{
				{
					PeerID: "peer 1",
				},
			},
		},
		&types.NetworkOptionsResponse{
			Version: &types.Version{
				RosettaVersion: "1.4.0",
				NodeVersion:    "1.0",
			},
			Allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					{
						Status:     "SUCCESS",
						Successful: true,
					},
					{
						Status:     "FAILURE",
						Successful: false,
					},
				},
				OperationTypes: []string{
					"PAYMENT",
				},
			},
		},
	)
	assert.NotNil(t, asserter)
	assert.NoError(t, err)

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := asserter.ConstructionParseResponse(test.response, test.signed)
			if test.err != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConstructionPayloadsResponse(t *testing.T) {
	var tests = map[string]struct {
		response *types.ConstructionPayloadsResponse
		err      error
	}{
		"valid response": {
			response: &types.ConstructionPayloadsResponse{
				UnsignedTransaction: "tx blob",
				Payloads: []*types.SigningPayload{
					{
						Address: "hello",
						Bytes:   []byte("48656c6c6f20476f7068657221"),
					},
				},
			},
			err: nil,
		},
		"nil response": {
			err: errs.ErrConstructionPayloadsResponseIsNil,
		},
		"empty unsigned transaction": {
			response: &types.ConstructionPayloadsResponse{
				Payloads: []*types.SigningPayload{
					{
						Address: "hello",
						Bytes:   []byte("48656c6c6f20476f7068657221"),
					},
				},
			},
			err: errs.ErrConstructionPayloadsResponseUnsignedTxEmpty,
		},
		"empty signing payloads": {
			response: &types.ConstructionPayloadsResponse{
				UnsignedTransaction: "tx blob",
			},
			err: errs.ErrConstructionPayloadsResponsePayloadsEmpty,
		},
		"invalid signing payload": {
			response: &types.ConstructionPayloadsResponse{
				UnsignedTransaction: "tx blob",
				Payloads: []*types.SigningPayload{
					{
						Bytes: []byte("48656c6c6f20476f7068657221"),
					},
				},
			},
			err: errs.ErrSigningPayloadAddrEmpty,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionPayloadsResponse(test.response)
			if test.err != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestPublicKey(t *testing.T) {
	var tests = map[string]struct {
		publicKey *types.PublicKey
		err       error
	}{
		"valid public key": {
			publicKey: &types.PublicKey{
				Bytes:     []byte("blah"),
				CurveType: types.Secp256k1,
			},
		},
		"nil public key": {
			err: errs.ErrPublicKeyIsNil,
		},
		"invalid bytes": {
			publicKey: &types.PublicKey{
				CurveType: types.Secp256k1,
			},
			err: errs.ErrPublicKeyBytesEmpty,
		},
		"invalid curve": {
			publicKey: &types.PublicKey{
				Bytes:     []byte("hello"),
				CurveType: "test",
			},
			err: errs.ErrCurveTypeNotSupported,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := PublicKey(test.publicKey)
			if test.err != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSigningPayload(t *testing.T) {
	var tests = map[string]struct {
		signingPayload *types.SigningPayload
		err            error
	}{
		"valid signing payload": {
			signingPayload: &types.SigningPayload{
				Address: "hello",
				Bytes:   []byte("blah"),
			},
		},
		"valid signing payload with signature type": {
			signingPayload: &types.SigningPayload{
				Address:       "hello",
				Bytes:         []byte("blah"),
				SignatureType: types.Ed25519,
			},
		},
		"nil signing payload": {
			err: errs.ErrSigningPayloadIsNil,
		},
		"empty address": {
			signingPayload: &types.SigningPayload{
				Bytes: []byte("blah"),
			},
			err: errs.ErrSigningPayloadAddrEmpty,
		},
		"empty bytes": {
			signingPayload: &types.SigningPayload{
				Address: "hello",
			},
			err: errs.ErrSigningPayloadBytesEmpty,
		},
		"invalid signature": {
			signingPayload: &types.SigningPayload{
				Address:       "hello",
				Bytes:         []byte("blah"),
				SignatureType: "blah",
			},
			err: errs.ErrSignatureTypeNotSupported,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := SigningPayload(test.signingPayload)
			if test.err != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestSignatures(t *testing.T) {
	var tests = map[string]struct {
		signatures []*types.Signature
		err        error
	}{
		"valid signatures": {
			signatures: []*types.Signature{
				{
					SigningPayload: &types.SigningPayload{
						Address: validAccount.Address,
						Bytes:   []byte("blah"),
					},
					PublicKey:     validPublicKey,
					SignatureType: types.Ed25519,
					Bytes:         []byte("hello"),
				},
				{
					SigningPayload: &types.SigningPayload{
						Address: validAccount.Address,
						Bytes:   []byte("blah"),
					},
					PublicKey:     validPublicKey,
					SignatureType: types.EcdsaRecovery,
					Bytes:         []byte("hello"),
				},
			},
		},
		"signature type match": {
			signatures: []*types.Signature{
				{
					SigningPayload: &types.SigningPayload{
						Address:       validAccount.Address,
						Bytes:         []byte("blah"),
						SignatureType: types.Ed25519,
					},
					PublicKey:     validPublicKey,
					SignatureType: types.Ed25519,
					Bytes:         []byte("hello"),
				},
			},
		},
		"nil signatures": {
			err: errs.ErrSignaturesEmpty,
		},
		"empty signature": {
			signatures: []*types.Signature{
				{
					SigningPayload: &types.SigningPayload{
						Address: validAccount.Address,
						Bytes:   []byte("blah"),
					},
					PublicKey:     validPublicKey,
					SignatureType: types.EcdsaRecovery,
					Bytes:         []byte("hello"),
				},
				{
					SigningPayload: &types.SigningPayload{
						Address:       validAccount.Address,
						Bytes:         []byte("blah"),
						SignatureType: types.Ed25519,
					},
					PublicKey:     validPublicKey,
					SignatureType: types.Ed25519,
				},
			},
			err: errs.ErrSignatureBytesEmpty,
		},
		"signature type mismatch": {
			signatures: []*types.Signature{
				{
					SigningPayload: &types.SigningPayload{
						Address:       validAccount.Address,
						Bytes:         []byte("blah"),
						SignatureType: types.EcdsaRecovery,
					},
					PublicKey:     validPublicKey,
					SignatureType: types.Ed25519,
					Bytes:         []byte("hello"),
				},
			},
			err: errs.ErrSignaturesReturnedSigMismatch,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Signatures(test.signatures)
			if test.err != nil {
				assert.Error(t, err)
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
