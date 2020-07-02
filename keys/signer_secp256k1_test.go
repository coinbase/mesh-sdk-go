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

func TestSignSecp256k1EcdsaRecovery(t *testing.T) {
	signatureType := types.EcdsaRecovery

	payload := types.SigningPayload{
		Address:       "test",
		HexBytes:      hashAndHexEncode("hello"),
		SignatureType: signatureType,
	}

	signature, err := signerSecp256k1.Sign(payload)
	assert.NoError(t, err)
	signatureBytes, _ := hex.DecodeString(signature.HexBytes)
	assert.Equal(t, len(signatureBytes), 65)

	verify, err := signerSecp256k1.Verify(signature)
	assert.NoError(t, err)
	assert.True(t, verify)
}

func TestSignSecp256k1Ecdsa(t *testing.T) {
	signatureType := types.Ecdsa

	payload := types.SigningPayload{
		Address:       "test",
		HexBytes:      hashAndHexEncode("hello"),
		SignatureType: signatureType,
	}

	signature, err := signerSecp256k1.Sign(payload)
	assert.NoError(t, err)
	signatureBytes, _ := hex.DecodeString(signature.HexBytes)
	assert.Equal(t, len(signatureBytes), 64)

	verify, err := signerSecp256k1.Verify(signature)
	assert.NoError(t, err)
	assert.True(t, verify)
}

func TestSignInvalidPayload(t *testing.T) {
	signatureType := types.Ecdsa

	invalidPayload := make([]byte, 33)

	payload := types.SigningPayload{
		Address:       "test",
		HexBytes:      hex.EncodeToString(invalidPayload),
		SignatureType: signatureType,
	}

	_, err := signerSecp256k1.Sign(payload)
	errorMsg := err.Error()
	assert.Contains(t, errorMsg, "unable to sign. invalid message length, need 32 bytes")
}
