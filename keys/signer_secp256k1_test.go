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
	"encoding/hex"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
)

func hashAndHexEncode(message string) string {
	messageHashBytes := common.BytesToHash([]byte(message)).Bytes()
	return hex.EncodeToString(messageHashBytes)
}

var signerSecp256k1 Signer

func init() {
	keypair, _ := GenerateKeypair(types.Secp256k1)
	signerSecp256k1 = Signer(SignerSecp256k1{*keypair})
}

func TestSignSecp256k1(t *testing.T) {
	type payloadTest struct {
		payload *types.SigningPayload
		sigLen  int
		err     bool
		errMsg  string
	}

	var payloadTests = []payloadTest{
		{mockPayload(hashAndHexEncode("hello"), types.Ed25519), 64, true, "unsupported signature type in payload."},
		{mockPayload(hashAndHexEncode("hello123"), types.Ecdsa), 64, false, ""},
		{mockPayload(hashAndHexEncode("hello1234"), types.EcdsaRecovery), 65, false, ""},
		{mockPayload("asd", types.Ecdsa), 64, true, "unable to decode message"},
	}

	for _, test := range payloadTests {
		signature, err := signerSecp256k1.Sign(test.payload)

		if !test.err {
			assert.NoError(t, err)
			signatureBytes, _ := hex.DecodeString(signature.HexBytes)
			assert.Equal(t, len(signatureBytes), test.sigLen)
		} else {
			assert.Contains(t, err.Error(), test.errMsg)
		}
	}
}

func mockSecpSignature(sigType types.SignatureType, pubkey *types.PublicKey, msg, sig string) *types.Signature {
	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      msg,
		SignatureType: sigType,
	}

	mockSig := &types.Signature{
		SigningPayload: payload,
		PublicKey:      pubkey,
		SignatureType:  sigType,
		HexBytes:       sig,
	}
	return mockSig
}

func TestVerifySecp256k1(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    string
	}

	badPubkey := &types.PublicKey{
		HexBytes:  "asd",
		CurveType: types.Secp256k1,
	}

	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      hashAndHexEncode("hello"),
		SignatureType: types.Ecdsa,
	}
	testSignatureEcdsa, _ := signerSecp256k1.Sign(payload)

	var signatureTests = []signatureTest{
		{mockSecpSignature(
			types.Ed25519,
			signerSecp256k1.PublicKey(),
			hashAndHexEncode("hello"),
			"68656C6C6F313233"), "ed25519 is not supported"},
		{mockSecpSignature(
			types.Ecdsa,
			signerSecp256k1.PublicKey(),
			"hello",
			"68656C6C6F313233"), "unable to decode message"},
		{mockSecpSignature(
			types.Ecdsa,
			signerSecp256k1.PublicKey(),
			hashAndHexEncode("hello"),
			"asd"), "unable to decode signature"},
		{mockSecpSignature(
			types.Ecdsa,
			badPubkey,
			hashAndHexEncode("hello"),
			"asd"), "unable to decode pubkey"},
		{mockSecpSignature(
			types.Ecdsa,
			signerSecp256k1.PublicKey(),
			hashAndHexEncode("hello"),
			"68656C6C6F313233"), "verify returned false"},
	}

	for _, test := range signatureTests {
		err := signerSecp256k1.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg)
	}

	goodEcdsaSignature := mockSecpSignature(
		types.Ecdsa,
		signerSecp256k1.PublicKey(),
		hashAndHexEncode("hello"),
		testSignatureEcdsa.HexBytes)
	goodEcdsaRecoverySignature := mockSecpSignature(
		types.EcdsaRecovery,
		signerSecp256k1.PublicKey(),
		hashAndHexEncode("hello"),
		testSignatureEcdsa.HexBytes)
	assert.Equal(t, nil, signerSecp256k1.Verify(goodEcdsaSignature))
	assert.Equal(t, nil, signerSecp256k1.Verify(goodEcdsaRecoverySignature))
}
