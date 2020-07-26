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
	"testing"

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
		"nil response": {
			err: errors.New("construction metadata response cannot be nil"),
		},
		"invalid metadata": {
			response: &types.ConstructionMetadataResponse{},
			err:      errors.New("Metadata is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionMetadataResponse(test.response)
			assert.Equal(t, test.err, err)
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
			err: errors.New("construction submit response cannot be nil"),
		},
		"invalid transaction identifier": {
			response: &types.TransactionIdentifierResponse{},
			err:      errors.New("TransactionIdentifier is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := TransactionIdentifierResponse(test.response)
			assert.Equal(t, test.err, err)
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
			err: errors.New("construction combine response cannot be nil"),
		},
		"empty signed transaction": {
			response: &types.ConstructionCombineResponse{},
			err:      errors.New("signed transaction cannot be empty"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionCombineResponse(test.response)
			assert.Equal(t, test.err, err)
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
			err: errors.New("construction derive response cannot be nil"),
		},
		"empty address": {
			response: &types.ConstructionDeriveResponse{
				Metadata: map[string]interface{}{
					"name": "hello",
				},
			},
			err: errors.New("address cannot be empty"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionDeriveResponse(test.response)
			assert.Equal(t, test.err, err)
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
			err: errors.New("construction parse response cannot be nil"),
		},
		"no operations": {
			response: &types.ConstructionParseResponse{
				Signers: []string{"account 1"},
				Metadata: map[string]interface{}{
					"extra": "stuff",
				},
			},
			err: errors.New("operations cannot be empty"),
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
			err: errors.New("Operation.OperationIdentifier.Index 1 is out of order, expected 0"),
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
			err:    errors.New("signers cannot be empty"),
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
			err:    errors.New("signer 0 cannot be empty"),
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
			err:    errors.New("signers should be empty for unsigned txs"),
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
				assert.Contains(t, err.Error(), test.err.Error())
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
			err: errors.New("construction payloads response cannot be nil"),
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
			err: errors.New("unsigned transaction cannot be empty"),
		},
		"empty signing payloads": {
			response: &types.ConstructionPayloadsResponse{
				UnsignedTransaction: "tx blob",
			},
			err: errors.New("signing payloads cannot be empty"),
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
			err: errors.New("signing payload address cannot be empty"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionPayloadsResponse(test.response)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
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
			err: errors.New("PublicKey cannot be nil"),
		},
		"invalid bytes": {
			publicKey: &types.PublicKey{
				CurveType: types.Secp256k1,
			},
			err: errors.New("public key bytes cannot be empty"),
		},
		"invalid curve": {
			publicKey: &types.PublicKey{
				Bytes:     []byte("hello"),
				CurveType: "test",
			},
			err: errors.New("test is not a supported CurveType"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := PublicKey(test.publicKey)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
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
			err: errors.New("signing payload cannot be nil"),
		},
		"empty address": {
			signingPayload: &types.SigningPayload{
				Bytes: []byte("blah"),
			},
			err: errors.New("signing payload address cannot be empty"),
		},
		"empty bytes": {
			signingPayload: &types.SigningPayload{
				Address: "hello",
			},
			err: errors.New("signing payload bytes cannot be empty"),
		},
		"invalid signature": {
			signingPayload: &types.SigningPayload{
				Address:       "hello",
				Bytes:         []byte("blah"),
				SignatureType: "blah",
			},
			err: errors.New("blah is not a supported SignatureType"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := SigningPayload(test.signingPayload)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
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
			err: errors.New("signatures cannot be empty"),
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
			err: errors.New("signature 1 bytes cannot be empty"),
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
			err: errors.New("requested signature type does not match"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Signatures(test.signatures)
			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
