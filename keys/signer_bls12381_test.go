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
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/kryptology/pkg/signatures/bls/bls_sig"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var bls12381Keypair *KeyPair
var signerBls12381 Signer
var payloadBytes []byte
var augSigner Signer
var augPayload []byte

func init() {
	bls12381Keypair, _ = GenerateKeypair(types.Bls12381)
	signerBls12381, _ = bls12381Keypair.Signer()

	unsignedPayloadStr := "a7ca4bce10200d073ef10c46e9d27c3b4e31263d4c07fbec447650fcc1b286" +
		"300e8ecf25c0560f9cb5aa673247fb6a6f95fb73164d1bf5d41288f57ea40517d9da86c44a289ed" +
		"2b6f9d3bf9a750ee96dbf905073f8ae56c80100ef47f5585acf70baff8c72c3b8a833181fb3edf4" +
		"328fccd5bb71183532bff220ba46c268991a3ff07eb358e8255a65c30a2dce0e5fbb"
	payloadBytes, _ = hex.DecodeString(unsignedPayloadStr)
}

func TestSignBls12381Basic(t *testing.T) {
	type payloadTest struct {
		payload *types.SigningPayload
		err     bool
		errMsg  error
	}

	var payloadTests = []payloadTest{
		{mockPayload(payloadBytes, types.Bls12381BasicMpl), false, nil},
		{mockPayload(payloadBytes, ""), false, nil},
		{mockPayload(payloadBytes, types.Ecdsa), true, ErrSignUnsupportedPayloadSignatureType},
		{
			mockPayload(payloadBytes, types.EcdsaRecovery),
			true,
			ErrSignUnsupportedPayloadSignatureType,
		},
	}
	for _, test := range payloadTests {
		signature, err := signerBls12381.Sign(test.payload, types.Bls12381BasicMpl)

		if !test.err {
			assert.NoError(t, err)
			assert.Len(t, signature.Bytes, 96)
			assert.Equal(t, signerBls12381.PublicKey(), signature.PublicKey)
		} else {
			assert.Contains(t, err.Error(), test.errMsg.Error())
		}
	}
}

func TestVerifyBls12381Basic(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    error
	}

	payload := mockPayload(payloadBytes, types.Bls12381BasicMpl)
	testSignature, err := signerBls12381.Sign(payload, types.Bls12381BasicMpl)
	assert.NoError(t, err)

	simpleBytes := make([]byte, 32)
	copy(simpleBytes, "hello")

	var signatureTests = []signatureTest{
		{mockSignature(
			types.Ecdsa,
			signerBls12381.PublicKey(),
			payloadBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.EcdsaRecovery,
			signerBls12381.PublicKey(),
			payloadBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.Bls12381BasicMpl,
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
		types.Bls12381BasicMpl,
		signerBls12381.PublicKey(),
		payloadBytes,
		testSignature.Bytes,
	)

	assert.Equal(t, nil, signerBls12381.Verify(goodSignature))
}

func mockPublicKey(
	pubkey_bytes []byte,
	curveType types.CurveType,
) *types.PublicKey {
	mockPubKey := &types.PublicKey{
		Bytes:     pubkey_bytes,
		CurveType: curveType,
	}

	return mockPubKey
}

func mockKP(
	pubkey *types.PublicKey,
	pk_bytes []byte,
) *KeyPair {
	mockKP := &KeyPair{
		PublicKey:  pubkey,
		PrivateKey: pk_bytes,
	}

	return mockKP
}

func TestBls12381AugPrepend(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    error
	}

	sk1_bytes, _ := hex.DecodeString("1603e1217d13437657e2716bd51ecb84e803170368e0dcbf2b6eb704d6914d1c")
	synthetic_sk_bytes, _ := hex.DecodeString("0c0dd789a90f993feb20dc4ce7d9d689ce9a669341d06d6638696cba1eea3177")
	synthetic_offset_sk_bytes, _ := hex.DecodeString("1d0baa87e45f286a6a1cde0566191e6a0e6a6fa7223764f894c4836565207360")

	payload := mockPayload(payloadBytes, types.Bls12381AugMpl)

	sk1 := new(bls_sig.SecretKey)
	sk1.UnmarshalBinary(sk1_bytes)
	pk1, _ := sk1.GetPublicKey()
	pk1_bytes, _ := pk1.MarshalBinary()
	pubKey := mockPublicKey(pk1_bytes, types.Bls12381)
	keyPair := mockKP(pubKey, sk1_bytes)
	signer, _ := keyPair.Signer()

	sig1, _ := signer.Sign(payload, types.Bls12381AugMpl)
	assert.Equal(t, nil, signer.Verify(sig1))

	synthetic_sk := new(bls_sig.SecretKey)
	synthetic_sk.UnmarshalBinary(synthetic_sk_bytes)
	synthetic_pk, _ := synthetic_sk.GetPublicKey()
	synthetic_pk_bytes, _ := synthetic_pk.MarshalBinary()
	syntheticPubKey := mockPublicKey(synthetic_pk_bytes, types.Bls12381)
	syntheticKeyPair := mockKP(syntheticPubKey, synthetic_sk_bytes)
	syntheticSigner, _ := syntheticKeyPair.Signer()

	sig2, _ := syntheticSigner.Sign(payload, types.Bls12381AugMpl)
	assert.Equal(t, nil, syntheticSigner.Verify(sig2))

	synthetic_offset_sk := new(bls_sig.SecretKey)
	synthetic_offset_sk.UnmarshalBinary(synthetic_offset_sk_bytes)
	synthetic_offset_pk, _ := synthetic_offset_sk.GetPublicKey()
	synthetic_offset_pk_bytes, _ := synthetic_offset_pk.MarshalBinary()
	syntheticOffsetPubKey := mockPublicKey(synthetic_offset_pk_bytes, types.Bls12381)
	syntheticOffsetKeyPair := mockKP(syntheticOffsetPubKey, synthetic_offset_sk_bytes)
	syntheticOffsetSigner, _ := syntheticOffsetKeyPair.Signer()

	prependedPayload := mockPayload(append(syntheticPubKey.Bytes, payloadBytes...), types.Bls12381AugMpl)

	offset_sig, _ := syntheticOffsetSigner.Sign(prependedPayload, types.Bls12381AugMpl)
	assert.Equal(t, nil, syntheticOffsetSigner.Verify(offset_sig))

	sig3, _ := signer.Sign(prependedPayload, types.Bls12381AugMpl)

	assert.Equal(t, nil, signer.Verify(sig3))
	assert.Equal(t, nil, syntheticOffsetSigner.Verify(sig3))
	assert.Equal(t, nil, syntheticSigner.Verify(sig3))
}
