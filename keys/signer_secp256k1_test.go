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

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func hash(message string) []byte {
	messageHashBytes := common.BytesToHash([]byte(message)).Bytes()
	return messageHashBytes
}

var signerSecp256k1 Signer

func init() {
	keypair, _ := GenerateKeypair(types.Secp256k1)
	signerSecp256k1, _ = keypair.Signer()
}

func TestSignSecp256k1(t *testing.T) {
	type payloadTest struct {
		payload *types.SigningPayload
		sigType types.SignatureType
		sigLen  int
		err     bool
		errMsg  error
	}

	var payloadTests = []payloadTest{
		{mockPayload(hash("hello123"), types.Ecdsa), types.Ecdsa, 64, false, nil},
		{mockPayload(hash("hello1234"), types.EcdsaRecovery), types.EcdsaRecovery, 65, false, nil},
		{
			mockPayload(hash("hello123"), types.Ed25519),
			types.Ed25519,
			64,
			true,
			ErrSignUnsupportedSignatureType,
		},
	}

	for _, test := range payloadTests {
		signature, err := signerSecp256k1.Sign(test.payload, test.sigType)

		if !test.err {
			assert.NoError(t, err)
			assert.Equal(t, len(signature.Bytes), test.sigLen)
			assert.Equal(t, signerSecp256k1.PublicKey(), signature.PublicKey)
		} else {
			assert.Contains(t, err.Error(), test.errMsg.Error())
		}
	}
}

func mockSecpSignature(
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

func TestVerifySecp256k1(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    error
	}

	payloadEcdsa := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             hash("hello"),
		SignatureType:     types.Ecdsa,
	}
	payloadEcdsaRecovery := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             hash("hello"),
		SignatureType:     types.EcdsaRecovery,
	}
	testSignatureEcdsa, _ := signerSecp256k1.Sign(payloadEcdsa, types.Ecdsa)
	testSignatureEcdsaRecovery, _ := signerSecp256k1.Sign(payloadEcdsaRecovery, types.EcdsaRecovery)

	simpleBytes := make([]byte, 33)
	copy(simpleBytes, "hello")

	var signatureTests = []signatureTest{
		{mockSecpSignature(
			types.Ed25519,
			signerSecp256k1.PublicKey(),
			hash("hello"),
			simpleBytes), ErrVerifyUnsupportedSignatureType},
		{mockSecpSignature(
			types.Ecdsa,
			signerSecp256k1.PublicKey(),
			hash("hello"),
			simpleBytes), ErrVerifyFailed},
	}

	for _, test := range signatureTests {
		err := signerSecp256k1.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg.Error())
	}

	goodEcdsaSignature := mockSecpSignature(
		types.Ecdsa,
		signerSecp256k1.PublicKey(),
		hash("hello"),
		testSignatureEcdsa.Bytes)
	goodEcdsaRecoverySignature := mockSecpSignature(
		types.EcdsaRecovery,
		signerSecp256k1.PublicKey(),
		hash("hello"),
		testSignatureEcdsaRecovery.Bytes)
	assert.Equal(t, nil, signerSecp256k1.Verify(goodEcdsaSignature))
	assert.Equal(t, nil, signerSecp256k1.Verify(goodEcdsaRecoverySignature))
}
