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

	"github.com/coinbase/kryptology/pkg/signatures/schnorr/mina"
	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var signerPallas Signer
var keypair *KeyPair
var txnBytes []byte

func init() {
	keypair, _ = GenerateKeypair(types.PALLAS)
	signerPallas, _ = keypair.Signer()

	privKey := &mina.SecretKey{}
	_ = privKey.UnmarshalBinary(keypair.PrivateKey)
	pubKey := privKey.GetPublicKey()

	_, sourceSecretKey, _ := mina.NewKeys()

	txn := &mina.Transaction{
		Fee:        3,
		FeeToken:   1,
		Nonce:      200,
		ValidUntil: 1000,
		Memo:       "this is a memo",
		FeePayerPk: pubKey,
		SourcePk:   pubKey,
		ReceiverPk: sourceSecretKey.GetPublicKey(),
		TokenId:    1,
		Amount:     42,
		Locked:     false,
		Tag:        [3]bool{false, false, false},
		NetworkId:  mina.TestNet,
	}
	txnBytes, _ = txn.MarshalBinary()
}

func TestSignPallas(t *testing.T) {
	type payloadTest struct {
		payload *types.SigningPayload
		err     bool
		errMsg  error
	}

	var payloadTests = []payloadTest{
		{mockPayload(txnBytes, types.SchnorrPoseidon), false, nil},
		{mockPayload(txnBytes, ""), false, nil},
		{mockPayload(txnBytes, types.Ecdsa), true, ErrSignUnsupportedPayloadSignatureType},
		{
			mockPayload(txnBytes, types.EcdsaRecovery),
			true,
			ErrSignUnsupportedPayloadSignatureType,
		},
	}
	for _, test := range payloadTests {
		signature, err := signerPallas.Sign(test.payload, types.SchnorrPoseidon)

		if !test.err {
			assert.NoError(t, err)
			assert.Len(t, signature.Bytes, 64)
			assert.Equal(t, signerPallas.PublicKey(), signature.PublicKey)
		} else {
			assert.Contains(t, err.Error(), test.errMsg.Error())
		}
	}
}

func TestVerifyPallas(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    error
	}

	payload := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             txnBytes,
		SignatureType:     types.SchnorrPoseidon,
	}
	testSignature, err := signerPallas.Sign(payload, types.SchnorrPoseidon)
	assert.NoError(t, err)

	simpleBytes := make([]byte, 32)
	copy(simpleBytes, "hello")

	var signatureTests = []signatureTest{
		{mockSignature(
			types.Ecdsa,
			signerPallas.PublicKey(),
			txnBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.EcdsaRecovery,
			signerPallas.PublicKey(),
			txnBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.SchnorrPoseidon,
			signerPallas.PublicKey(),
			simpleBytes,
			testSignature.Bytes), ErrVerifyFailed},
	}

	for _, test := range signatureTests {
		err := signerPallas.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg.Error())
	}

	// happy path
	goodSignature := mockSignature(
		types.SchnorrPoseidon,
		signerPallas.PublicKey(),
		txnBytes,
		testSignature.Bytes,
	)
	assert.Equal(t, nil, signerPallas.Verify(goodSignature))
}
