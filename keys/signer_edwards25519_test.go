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

package keys

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var signerEdwards25519 Signer

func init() {
	keypair, _ := GenerateKeypair(types.Edwards25519)
	signerEdwards25519, _ = keypair.Signer()
}

func mockPayload(msg []byte, signatureType types.SignatureType) *types.SigningPayload {
	payload := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             msg,
		SignatureType:     signatureType,
	}

	return payload
}

func TestSignEdwards25519(t *testing.T) {
	type payloadTest struct {
		payload *types.SigningPayload
		err     bool
		errMsg  error
	}

	var payloadTests = []payloadTest{
		{mockPayload(make([]byte, 32), types.Ed25519), false, nil},
		{mockPayload(make([]byte, 32), ""), false, nil},
		{mockPayload(make([]byte, 33), types.Ecdsa), true, ErrSignUnsupportedPayloadSignatureType},
		{
			mockPayload(make([]byte, 34), types.EcdsaRecovery),
			true,
			ErrSignUnsupportedPayloadSignatureType,
		},
	}

	for _, test := range payloadTests {
		signature, err := signerEdwards25519.Sign(test.payload, types.Ed25519)

		if !test.err {
			assert.NoError(t, err)
			assert.Len(t, signature.Bytes, 64)
			assert.Equal(t, signerEdwards25519.PublicKey(), signature.PublicKey)
		} else {
			assert.Contains(t, err.Error(), test.errMsg.Error())
		}
	}
}

func mockSignature(
	sigType types.SignatureType,
	pubkey *types.PublicKey,
	msg, sig []byte,
) *types.Signature {
	payload := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             msg,
		SignatureType:     sigType,
	}

	mockSig := &types.Signature{
		SigningPayload: payload,
		PublicKey:      pubkey,
		SignatureType:  sigType,
		Bytes:          sig,
	}
	return mockSig
}

func TestVerifyEdwards25519(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    error
	}

	simpleBytes := make([]byte, 32)
	copy(simpleBytes, "hello")

	payload := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             simpleBytes,
		SignatureType:     types.Ed25519,
	}
	testSignature, err := signerEdwards25519.Sign(payload, types.Ed25519)
	assert.NoError(t, err)

	var signatureTests = []signatureTest{
		{mockSignature(
			types.Ecdsa,
			signerEdwards25519.PublicKey(),
			simpleBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.EcdsaRecovery,
			signerEdwards25519.PublicKey(),
			simpleBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.Ed25519,
			signerEdwards25519.PublicKey(),
			func() []byte {
				b := make([]byte, 40)
				copy(b, "hello")

				return b
			}(),
			testSignature.Bytes), ErrVerifyFailed},
	}

	for _, test := range signatureTests {
		err := signerEdwards25519.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg.Error())
	}

	goodSignature := mockSignature(
		types.Ed25519,
		signerEdwards25519.PublicKey(),
		simpleBytes,
		testSignature.Bytes,
	)
	assert.Equal(t, nil, signerEdwards25519.Verify(goodSignature))
}
