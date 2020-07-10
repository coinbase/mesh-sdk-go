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

func TestConstructionMetadata(t *testing.T) {
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
			err := ConstructionMetadata(test.response)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestConstructionSubmit(t *testing.T) {
	var tests = map[string]struct {
		response *types.ConstructionSubmitResponse
		err      error
	}{
		"valid response": {
			response: &types.ConstructionSubmitResponse{
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
			response: &types.ConstructionSubmitResponse{},
			err:      errors.New("TransactionIdentifier is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionSubmit(test.response)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestConstructionCombine(t *testing.T) {
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
			err := ConstructionCombine(test.response)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestConstructionDerive(t *testing.T) {
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
			err := ConstructionDerive(test.response)
			assert.Equal(t, test.err, err)
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
