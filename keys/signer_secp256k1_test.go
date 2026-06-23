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
	"encoding/hex"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
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
		{mockPayload(hash("hello1234"), types.Schnorr1), types.Schnorr1, 64, false, nil},
		{
			mockPayload(hash("hello1234"), types.SchnorrBip340),
			types.SchnorrBip340,
			64,
			false,
			nil,
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
	payloadSchnorr1 := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             hash("hello"),
		SignatureType:     types.Schnorr1,
	}
	testSignatureEcdsa, _ := signerSecp256k1.Sign(payloadEcdsa, types.Ecdsa)
	testSignatureEcdsaRecovery, _ := signerSecp256k1.Sign(payloadEcdsaRecovery, types.EcdsaRecovery)
	testSignatureSchnorr1, _ := signerSecp256k1.Sign(payloadSchnorr1, types.Schnorr1)

	simpleBytes := make([]byte, 33)
	copy(simpleBytes, "hello")

	var signatureTests = []signatureTest{
		{mockSignature(
			types.Ed25519,
			signerSecp256k1.PublicKey(),
			hash("hello"),
			simpleBytes), ErrVerifyUnsupportedSignatureType},
		{mockSignature(
			types.Ecdsa,
			signerSecp256k1.PublicKey(),
			hash("hello"),
			simpleBytes), ErrVerifyFailed},
		{mockSignature(
			types.Schnorr1,
			signerSecp256k1.PublicKey(),
			hash("hello"),
			simpleBytes), ErrVerifyFailed},
	}

	for _, test := range signatureTests {
		err := signerSecp256k1.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg.Error())
	}

	goodEcdsaSignature := mockSignature(
		types.Ecdsa,
		signerSecp256k1.PublicKey(),
		hash("hello"),
		testSignatureEcdsa.Bytes)
	goodEcdsaRecoverySignature := mockSignature(
		types.EcdsaRecovery,
		signerSecp256k1.PublicKey(),
		hash("hello"),
		testSignatureEcdsaRecovery.Bytes)
	goodSchnorr1Signature := mockSignature(
		types.Schnorr1,
		signerSecp256k1.PublicKey(),
		hash("hello"),
		testSignatureSchnorr1.Bytes)
	assert.Equal(t, nil, signerSecp256k1.Verify(goodEcdsaSignature))
	assert.Equal(t, nil, signerSecp256k1.Verify(goodEcdsaRecoverySignature))
	assert.Equal(t, nil, signerSecp256k1.Verify(goodSchnorr1Signature))

	// BIP-340 round-trip
	payloadBip340 := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             hash("hello"),
		SignatureType:     types.SchnorrBip340,
	}
	testSignatureBip340, _ := signerSecp256k1.Sign(payloadBip340, types.SchnorrBip340)
	goodBip340Signature := mockSignature(
		types.SchnorrBip340,
		signerSecp256k1.PublicKey(),
		hash("hello"),
		testSignatureBip340.Bytes)
	assert.Equal(t, nil, signerSecp256k1.Verify(goodBip340Signature))
}

// TestSchnorrBip340_Bip341KeyPathVector tests BIP-340 using the BIP-341 wallet
// test vectors (keyPathSpending[0], txinIndex=4, hashType=0 / SIGHASH_DEFAULT).
//
// Source: https://github.com/bitcoin/bips/blob/master/bip-0341/wallet-test-vectors.json
//
// Two checks:
//  1. Sign → Verify round-trip: sign the spec sighash with the spec tweaked key, verify
//     the produced signature is valid. The exact bytes are NOT compared against the spec
//     expected witness because btcec uses RFC6979 nonce generation while the spec test
//     vectors use aux_rand=0; the signatures differ but both are valid BIP-340.
//  2. Verify spec signature: verify the expected witness bytes from the spec using our
//     Verify path. This confirms interoperability with signatures produced by other
//     BIP-340 implementations.
func TestSchnorrBip340_Bip341KeyPathVector(t *testing.T) {
	// BIP-341 wallet-test-vectors.json → keyPathSpending[0] → inputSpending[txinIndex=4]
	const (
		tweakedPrivKeyHex = "a8e7aa924f0d58854185a490e6c41f6efb7b675c0f3331b7f14b549400b4d501"
		sighashHex        = "4f900a0bae3f1446fd48490c2958b5a023228f01661cda3496a11da502a7f7ef"
		// expected.witness[0] from the spec — produced by a BIP-340 implementation
		// using aux_rand=0; bytes differ from btcec (RFC6979) but must verify correctly.
		expectedWitnessHex = "b4010dd48a617db09926f729e79c33ae0b4e94b79f04a1ae93ede6315eb3669de185a17d2b0ac9ee09fd4c64b678a0b61a0a86fa888a273c8511be83bfd6810f"
	)

	kp, err := ImportPrivateKey(tweakedPrivKeyHex, types.Secp256k1)
	assert.NoError(t, err)
	signer, err := kp.Signer()
	assert.NoError(t, err)

	sighash, _ := hex.DecodeString(sighashHex)
	payload := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             sighash,
		SignatureType:     types.SchnorrBip340,
	}

	// Check 1: sign produces a valid 64-byte signature that self-verifies.
	sig, err := signer.Sign(payload, types.SchnorrBip340)
	assert.NoError(t, err)
	assert.Equal(t, 64, len(sig.Bytes))
	assert.NoError(t, signer.Verify(sig))

	// Check 2: our Verify accepts the spec's expected witness bytes, confirming
	// interoperability with external BIP-340 implementations.
	specSigBytes, _ := hex.DecodeString(expectedWitnessHex)
	specSig := mockSignature(types.SchnorrBip340, kp.PublicKey, sighash, specSigBytes)
	assert.NoError(t, signer.Verify(specSig), "Verify must accept the BIP-341 spec witness")
}

// TestSchnorrBip340_SignNegative tests that Sign rejects messages that are not exactly 32 bytes.
func TestSchnorrBip340_SignNegative(t *testing.T) {
	kp, _ := GenerateKeypair(types.Secp256k1)
	signer, _ := kp.Signer()

	for _, bad := range [][]byte{
		[]byte("short"),
		make([]byte, 31),
		make([]byte, 33),
		make([]byte, 64),
	} {
		payload := &types.SigningPayload{
			AccountIdentifier: &types.AccountIdentifier{Address: "test"},
			Bytes:             bad,
			SignatureType:     types.SchnorrBip340,
		}
		_, err := signer.Sign(payload, types.SchnorrBip340)
		assert.ErrorContains(t, err, "32-byte")
	}
}

// TestSchnorrBip340_VerifyNegative tests that Verify correctly rejects invalid inputs.
func TestSchnorrBip340_VerifyNegative(t *testing.T) {
	kp, _ := GenerateKeypair(types.Secp256k1)
	signer, _ := kp.Signer()

	msg := make([]byte, 32)
	copy(msg, []byte("test message for bip340"))

	payload := &types.SigningPayload{
		AccountIdentifier: &types.AccountIdentifier{Address: "test"},
		Bytes:             msg,
		SignatureType:     types.SchnorrBip340,
	}
	goodSig, _ := signer.Sign(payload, types.SchnorrBip340)

	// Garbage signature bytes (correct length, wrong content).
	garbageSig := make([]byte, 64)
	copy(garbageSig, []byte("this is not a valid bip340 signature at all padding padding!!!!"))
	err := signer.Verify(mockSignature(types.SchnorrBip340, kp.PublicKey, msg, garbageSig))
	assert.Error(t, err, "garbage signature must be rejected")

	// Correct signature, wrong message.
	wrongMsg := make([]byte, 32)
	copy(wrongMsg, []byte("different message"))
	err = signer.Verify(mockSignature(types.SchnorrBip340, kp.PublicKey, wrongMsg, goodSig.Bytes))
	assert.Error(t, err, "signature over different message must be rejected")

	// Correct signature, mismatched public key.
	otherKp, _ := GenerateKeypair(types.Secp256k1)
	err = signer.Verify(mockSignature(types.SchnorrBip340, otherKp.PublicKey, msg, goodSig.Bytes))
	assert.Error(t, err, "signature verified against wrong public key must be rejected")
}
