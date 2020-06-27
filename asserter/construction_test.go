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

func TestPublicKey(t *testing.T) {
	var tests = map[string]struct {
		publicKey *types.PublicKey
		err       error
	}{
		"valid public key": {
			publicKey: &types.PublicKey{
				HexBytes:  "48656c6c6f20476f7068657221",
				CurveType: types.Secp256k1,
			},
		},
		"nil public key": {
			err: errors.New("PublicKey cannot be nil"),
		},
		"invalid hex": {
			publicKey: &types.PublicKey{
				HexBytes:  "hello",
				CurveType: types.Secp256k1,
			},
			err: errors.New("hello is not a valid hex string"),
		},
		"invalid curve": {
			publicKey: &types.PublicKey{
				HexBytes:  "48656c6c6f20476f7068657221",
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
				Address:  "hello",
				HexBytes: "48656c6c6f20476f7068657221",
			},
		},
		"valid signing payload with signature type": {
			signingPayload: &types.SigningPayload{
				Address:       "hello",
				HexBytes:      "48656c6c6f20476f7068657221",
				SignatureType: types.Ed25519,
			},
		},
		"nil signing payload": {
			err: errors.New("signing payload cannot be nil"),
		},
		"empty address": {
			signingPayload: &types.SigningPayload{
				HexBytes: "48656c6c6f20476f7068657221",
			},
			err: errors.New("signing payload address cannot be empty"),
		},
		"empty hex": {
			signingPayload: &types.SigningPayload{
				Address: "hello",
			},
			err: errors.New("hex string cannot be empty"),
		},
		"invalid signature": {
			signingPayload: &types.SigningPayload{
				Address:       "hello",
				HexBytes:      "48656c6c6f20476f7068657221",
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
