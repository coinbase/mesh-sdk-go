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

func TestSignEd25519(t *testing.T) {
	curve := types.Edwards25519
	keypair, err := GenerateKeypair(curve)
	assert.NoError(t, err)

	signatureType := types.Ed25519
	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      "68656C6C6F0D0A",
		SignatureType: signatureType,
	}

	signature, err := SignPayload(payload, keypair)
	assert.NoError(t, err)
	signatureBytes, _ := hex.DecodeString(signature.HexBytes)
	assert.Equal(t, len(signatureBytes), 64)

	verify, err := Verify(signature)
	assert.NoError(t, err)
	assert.True(t, verify)
}

func TestSignSecp256k1EcdsaRecovery(t *testing.T) {
	curve := types.Secp256k1
	keypair, err := GenerateKeypair(curve)
	assert.NoError(t, err)

	signatureType := types.EcdsaRecovery

	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      hashAndHexEncode("hello"),
		SignatureType: signatureType,
	}

	signature, err := SignPayload(payload, keypair)
	assert.NoError(t, err)
	signatureBytes, _ := hex.DecodeString(signature.HexBytes)
	assert.Equal(t, len(signatureBytes), 65)

	verify, err := Verify(signature)
	assert.NoError(t, err)
	assert.True(t, verify)
}

func TestSignSecp256k1Ecdsa(t *testing.T) {
	curve := types.Secp256k1
	keypair, err := GenerateKeypair(curve)
	assert.NoError(t, err)

	signatureType := types.Ecdsa

	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      hashAndHexEncode("hello"),
		SignatureType: signatureType,
	}

	signature, err := SignPayload(payload, keypair)
	assert.NoError(t, err)
	signatureBytes, _ := hex.DecodeString(signature.HexBytes)
	assert.Equal(t, len(signatureBytes), 64)

	verify, err := Verify(signature)
	assert.NoError(t, err)
	assert.True(t, verify)
}

func TestSignInvalidPayload(t *testing.T) {
	keypair, _ := GenerateKeypair(types.Secp256k1)
	signatureType := types.Ecdsa

	invalidPayload := make([]byte, 33)

	payload := &types.SigningPayload{
		Address:       "test",
		HexBytes:      hex.EncodeToString(invalidPayload),
		SignatureType: signatureType,
	}

	_, err := SignPayload(payload, keypair)
	errorMsg := err.Error()
	assert.Contains(t, errorMsg, "unable to sign. invalid message length, need 32 bytes")
}
