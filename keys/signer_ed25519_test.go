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

func TestSignEd25519(t *testing.T) {
	signatureType := types.EcdsaRecovery

	payload := types.SigningPayload{
		Address:       "test",
		HexBytes:      "68656C6C6F0D0A",
		SignatureType: signatureType,
	}

	signature, err := signerEd25519.Sign(payload)
	assert.NoError(t, err)
	signatureBytes, _ := hex.DecodeString(signature.HexBytes)
	assert.Equal(t, len(signatureBytes), 64)

	verify, err := signerEd25519.Verify(signature)
	assert.NoError(t, err)
	assert.True(t, verify)
}
