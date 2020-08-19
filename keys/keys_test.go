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
	"encoding/json"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestJSONEncoding(t *testing.T) {
	secp256k1Keypair, err := GenerateKeypair(types.Secp256k1)
	assert.NoError(t, err)

	edwards25519Keypair, err := GenerateKeypair(types.Edwards25519)
	assert.NoError(t, err)

	var keyPairs = []*KeyPair{secp256k1Keypair, edwards25519Keypair}
	for _, keypair := range keyPairs {
		kpb, err := json.Marshal(keypair)
		assert.NoError(t, err)

		// Simple Hex Check
		simpleType := struct {
			HexBytes string `json:"private_key"`
		}{}
		err = json.Unmarshal(kpb, &simpleType)
		assert.NoError(t, err)

		b, err := hex.DecodeString(simpleType.HexBytes)
		assert.NoError(t, err)
		assert.Equal(t, keypair.PrivateKey, b)

		var kp KeyPair
		err = json.Unmarshal(kpb, &kp)
		assert.NoError(t, err)
		assert.Equal(t, keypair.PrivateKey, kp.PrivateKey)
	}
}

func TestGenerateKeypairSecp256k1(t *testing.T) {
	curve := types.Secp256k1
	keypair, err := GenerateKeypair(curve)

	assert.NoError(t, err)
	assert.Equal(t, keypair.PublicKey.CurveType, curve)
	assert.Len(t, keypair.PrivateKey, PrivKeyBytesLen)
}

func TestGenerateKeypairEdwards25519(t *testing.T) {
	curve := types.Edwards25519
	keypair, err := GenerateKeypair(curve)

	assert.NoError(t, err)
	assert.Equal(t, keypair.PublicKey.CurveType, curve)
	assert.Len(t, keypair.PrivateKey, PrivKeyBytesLen)
}

func mockKeyPair(privKey []byte, curveType types.CurveType) *KeyPair {
	keypair, _ := GenerateKeypair(curveType)
	keypair.PrivateKey = privKey
	return keypair
}

func TestKeypairValidity(t *testing.T) {
	// invalid CurveType
	keyPair, err := GenerateKeypair(types.Edwards25519)
	assert.NoError(t, err)

	keyPair.PublicKey.CurveType = "blah"
	err = keyPair.IsValid()
	assert.Contains(t, err.Error(), "not a supported CurveType: blah")

	type privKeyTest struct {
		keypair *KeyPair
		errMsg  string
	}

	var privKeyTests = []privKeyTest{
		{mockKeyPair(make([]byte, 33), types.Secp256k1), "invalid privkey length"},
		{mockKeyPair(make([]byte, 31), types.Secp256k1), "invalid privkey length"},
		{mockKeyPair(make([]byte, 0), types.Secp256k1), "invalid privkey length"},
		{mockKeyPair(make([]byte, 33), types.Edwards25519), "invalid privkey length"},
		{mockKeyPair(make([]byte, 31), types.Edwards25519), "invalid privkey length"},
		{mockKeyPair(make([]byte, 0), types.Edwards25519), "invalid privkey length"},
	}

	for _, test := range privKeyTests {
		err = test.keypair.IsValid()
		assert.Contains(t, err.Error(), test.errMsg)
	}
}

func TestImportPrivKey(t *testing.T) {
	type importKeyTest struct {
		privKey   string
		curveType types.CurveType
		errMsg    string
	}

	importPrivKeyTests := []importKeyTest{
		{
			"aeb121b4c545f0f850e1480492508c65a250e9965b0d90176fab4d7506398ebb",
			types.Edwards25519,
			"",
		},
		{
			"01ea48249742650907004331e85536f868e2d3959434ba751d8aa230138a9707",
			types.Edwards25519,
			"",
		},
		{
			"f1821e051843bce17c1f31023609e9412ae4525d27447fc93afa4a271ee60550",
			types.Edwards25519,
			"",
		},
		{"0b188af56b25d007fbc4bbf2176cd2a54d876ce4774bb5df38b7c83349405b7a", types.Secp256k1, ""},
		{"0e842a16b2d39a4dff5c63688513cb2109e30c3c30bc4eb502cc54f4614493f6", types.Secp256k1, ""},
		{"42efc44bdf7b2d4d45ddd6ddb727ed498c91e7070914c9ed0d80af680ff42b3e", types.Secp256k1, ""},
		{"asd", types.Secp256k1, "could not decode privkey"},
		{"asd", types.Edwards25519, "could not decode privkey"},
	}

	for _, test := range importPrivKeyTests {
		_, err := ImportPrivKey(test.privKey, test.curveType)
		if err != nil {
			assert.Contains(t, err.Error(), test.errMsg)
		}
	}
}
