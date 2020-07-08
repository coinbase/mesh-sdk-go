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

package keys

import (
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

var signerEd25519 Signer

func init() {
	keypair, _ := GenerateKeypair(types.Edwards25519)
	signerEd25519 = Signer(SignerEd25519{*keypair})
}

func mockPayload(msg []byte, signatureType types.SignatureType) *types.SigningPayload {
	payload := &types.SigningPayload{
		Address:       "test",
		Bytes:         msg,
		SignatureType: signatureType,
	}

	return payload
}

func TestSignEd25519(t *testing.T) {
	type payloadTest struct {
		payload *types.SigningPayload
		err     bool
		errMsg  string
	}

	var payloadTests = []payloadTest{
		{mockPayload(make([]byte, 32), types.Ed25519), false, ""},
		{mockPayload(make([]byte, 32), ""), false, ""},
		{mockPayload(make([]byte, 33), types.Ecdsa), true, "payload signature type is not ed25519"},
		{mockPayload(make([]byte, 34), types.EcdsaRecovery), true, "payload signature type is not ed25519"},
	}

	for _, test := range payloadTests {
		signature, err := signerEd25519.Sign(test.payload)

		if !test.err {
			assert.NoError(t, err)
			assert.Len(t, signature.Bytes, 64)
		} else {
			assert.Contains(t, err.Error(), test.errMsg)
		}
	}
}

func mockSignature(sigType types.SignatureType, pubkey *types.PublicKey, msg, sig []byte) *types.Signature {
	payload := &types.SigningPayload{
		Address:       "test",
		Bytes:         msg,
		SignatureType: types.Ed25519,
	}

	mockSig := &types.Signature{
		SigningPayload: payload,
		PublicKey:      pubkey,
		SignatureType:  sigType,
		Bytes:          sig,
	}
	return mockSig
}

func TestVerifyEd25519(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    string
	}

	payload := &types.SigningPayload{
		Address:       "test",
		Bytes:         make([]byte, 32),
		SignatureType: types.Ed25519,
	}
	testSignature, _ := signerEd25519.Sign(payload)

	var signatureTests = []signatureTest{
		{mockSignature(
			types.Ecdsa,
			signerEd25519.PublicKey(),
			make([]byte, 32),
			make([]byte, 32)), "payload signature type is not ed25519"},
		{mockSignature(
			types.EcdsaRecovery,
			signerEd25519.PublicKey(),
			make([]byte, 32),
			make([]byte, 32)), "payload signature type is not ed25519"},
		{mockSignature(
			types.Ed25519,
			signerEd25519.PublicKey(),
			make([]byte, 40),
			testSignature.Bytes), "verify returned false"},
	}

	for _, test := range signatureTests {
		err := signerEd25519.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg)
	}

	goodSignature := mockSignature(types.Ed25519, signerEd25519.PublicKey(), make([]byte, 32), testSignature.Bytes)
	assert.Equal(t, nil, signerEd25519.Verify(goodSignature))
}
