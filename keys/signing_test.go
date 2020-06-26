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
	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func hashAndHexEncode(message string) string {
	messageHashBytes := common.BytesToHash([]byte(message)).Bytes()
	return hex.EncodeToString(messageHashBytes)
}

func TestSignEd25519(t *testing.T) {
	curve := CurveType(Edwards25519Curve)
	keypair, err := GenerateKeypair(curve)
	assert.NoError(t, err)

	signatureType := SignatureType(Ed25519Signature)
	payload := &SigningPayload{
		Address:       "test",
		PayloadHex:    "68656C6C6F0D0A",
		SignatureType: signatureType,
	}

	signature, err := SignPayload(payload, keypair)
	assert.NoError(t, err)

	verify, err := Verify(signature)
	assert.NoError(t, err)
	assert.Equal(t, verify, true)
}

func TestSignSecp256k1EcdsaRecovery(t *testing.T) {
	curve := CurveType(Secp256k1Curve)
	keypair, err := GenerateKeypair(curve)
	assert.NoError(t, err)

	signatureType := SignatureType(EcdsaPubkeyRecoverySignature)

	payload := &SigningPayload{
		Address:       "test",
		PayloadHex:    hashAndHexEncode("hello"),
		SignatureType: signatureType,
	}

	signature, err := SignPayload(payload, keypair)
	assert.NoError(t, err)

	verify, err := Verify(signature)
	assert.NoError(t, err)
	assert.Equal(t, true, verify)
}

func TestSignSecp256k1Ecdsa(t *testing.T) {
	curve := CurveType(Secp256k1Curve)
	keypair, err := GenerateKeypair(curve)
	assert.NoError(t, err)

	signatureType := SignatureType(EcdsaSignature)

	payload := &SigningPayload{
		Address:       "test",
		PayloadHex:    hashAndHexEncode("hello"),
		SignatureType: signatureType,
	}

	signature, err := SignPayload(payload, keypair)
	assert.NoError(t, err)

	verify, err := Verify(signature)
	assert.NoError(t, err)
	assert.Equal(t, true, verify)
}

func TestSignInvalidPayloadEcdsa(t *testing.T) {
	curve := CurveType(Secp256k1Curve)
	keypair, _ := GenerateKeypair(curve)
	signatureType := SignatureType(EcdsaSignature)

	invalidPayload := make([]byte, 33)

	payload := &SigningPayload{
		Address:       "test",
		PayloadHex:    hex.EncodeToString(invalidPayload),
		SignatureType: signatureType,
	}

	_, err := SignPayload(payload, keypair)
	errorMsg := err.Error()
	assert.Contains(t, errorMsg, "unable to sign. invalid message length, need 32 bytes")
}
