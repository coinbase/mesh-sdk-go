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
	"github.com/stretchr/testify/assert"
)

var signerEd25519 Signer

func init() {
	keypair, _ := GenerateKeypair(types.Edwards25519)
	signerEd25519 = Signer(SignerEd25519{*keypair})
}

func mockPayload(msg string, signatureType types.SignatureType) *types.SigningPayload {
	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      msg,
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
		{mockPayload("68656C6C6F313233", types.Ed25519), false, ""},
		{mockPayload("68656C6C6F313233", ""), false, ""},
		{mockPayload("68656C6C6F313233", types.Ecdsa), true, "payload signature type is not ed25519"},
		{mockPayload("68656C6C6F313233", types.EcdsaRecovery), true, "payload signature type is not ed25519"},
		{mockPayload("asd", types.Ed25519), true, "unable to decode message"},
	}

	for _, test := range payloadTests {
		signature, err := signerEd25519.Sign(test.payload)

		if !test.err {
			assert.NoError(t, err)
			signatureBytes, _ := hex.DecodeString(signature.HexBytes)
			assert.Len(t, signatureBytes, 64)
		} else {
			assert.Contains(t, err.Error(), test.errMsg)
		}
	}
}

func mockSignature(sigType types.SignatureType, pubkey *types.PublicKey, msg, sig string) *types.Signature {
	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      msg,
		SignatureType: types.Ed25519,
	}

	mockSig := &types.Signature{
		SigningPayload: payload,
		PublicKey:      pubkey,
		SignatureType:  sigType,
		HexBytes:       sig,
	}
	return mockSig
}

func TestVerifyEd25519(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    string
	}

	badPubkey := &types.PublicKey{
		HexBytes:  "asd",
		CurveType: types.Edwards25519,
	}

	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      "68656C6C6F313233",
		SignatureType: types.Ed25519,
	}
	testSignature, _ := signerEd25519.Sign(payload)

	var signatureTests = []signatureTest{
		{mockSignature(
			types.Ecdsa,
			signerEd25519.PublicKey(),
			"68656C6C6F313233",
			""), "payload signature type is not ed25519"},
		{mockSignature(
			types.EcdsaRecovery,
			signerEd25519.PublicKey(),
			"68656C6C6F313233",
			""), "payload signature type is not ed25519"},
		{mockSignature(
			types.Ed25519,
			badPubkey,
			"68656C6C6F313233",
			""), "unable to decode pubkey"},
		{mockSignature(
			types.Ed25519,
			signerEd25519.PublicKey(),
			"asd",
			""), "unable to decode message"},
		{mockSignature(
			types.Ed25519,
			signerEd25519.PublicKey(),
			"617364",
			testSignature.HexBytes), "verify returned false"},
	}

	for _, test := range signatureTests {
		err := signerEd25519.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg)
	}

	goodSignature := mockSignature(types.Ed25519, signerEd25519.PublicKey(), "68656C6C6F313233", testSignature.HexBytes)
	assert.Equal(t, nil, signerEd25519.Verify(goodSignature))
}
