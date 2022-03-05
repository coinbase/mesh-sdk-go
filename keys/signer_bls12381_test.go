// Copyright 2022 Coinbase, Inc.
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

var signerBls12381 Signer

func init() {
	bls12381Keypair, _ := GenerateKeypair(types.Bls12381)
	signerBls12381, _ = bls12381Keypair.Signer()

	unsignedTxStr := "a7ca4bce10200d073ef10c46e9d27c3b4e31263d4c07fbec447650fcc1b286" +
		"300e8ecf25c0560f9cb5aa673247fb6a6f95fb73164d1bf5d41288f57ea40517d9da86c44a289ed" +
		"2b6f9d3bf9a750ee96dbf905073f8ae56c80100ef47f5585acf70baff8c72c3b8a833181fb3edf4" +
		"328fccd5bb71183532bff220ba46c268991a3ff07eb358e8255a65c30a2dce0e5fbb"
	txnBytes = []byte(unsignedTxStr)
}

func TestSignBls12381(t *testing.T) {
	type payloadTest struct {
		payload *types.SigningPayload
		err     bool
		errMsg  error
	}

	var payloadTests = []payloadTest{
		{mockPayload(txnBytes, types.BlsG2Element), false, nil},
		{mockPayload(txnBytes, ""), false, nil},
		{mockPayload(txnBytes, types.Ecdsa), true, ErrSignUnsupportedPayloadSignatureType},
		{
			mockPayload(txnBytes, types.EcdsaRecovery),
			true,
			ErrSignUnsupportedPayloadSignatureType,
		},
	}
	for _, test := range payloadTests {
		signature, err := signerBls12381.Sign(test.payload, types.BlsG2Element)

		if !test.err {
			assert.NoError(t, err)
			assert.Len(t, signature.Bytes, 96)
			assert.Equal(t, signerBls12381.PublicKey(), signature.PublicKey)
		} else {
			assert.Contains(t, err.Error(), test.errMsg.Error())
		}
	}
}

func TestVerifyBls(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    error
	}

	payload := mockPayload(txnBytes, types.BlsG2Element)
	testSignature, err := signerBls12381.Sign(payload, types.BlsG2Element)
	assert.NoError(t, err)

	simpleBytes := make([]byte, 32)
	copy(simpleBytes, "hello")

	var signatureTests = []signatureTest{
		{mockSignature(
			types.Ecdsa,
			signerBls12381.PublicKey(),
			txnBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.EcdsaRecovery,
			signerBls12381.PublicKey(),
			txnBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.BlsG2Element,
			signerBls12381.PublicKey(),
			simpleBytes,
			testSignature.Bytes), ErrVerifyFailed},
	}

	for _, test := range signatureTests {
		err := signerBls12381.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg.Error())
	}

	// happy path
	goodSignature := mockSignature(
		types.BlsG2Element,
		signerBls12381.PublicKey(),
		txnBytes,
		testSignature.Bytes,
	)

	assert.Equal(t, nil, signerBls12381.Verify(goodSignature))
}
