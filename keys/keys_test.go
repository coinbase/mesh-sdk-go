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
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
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

func TestGenerateKeypairPallas(t *testing.T) {
	curve := types.Pallas
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
	assert.True(t, errors.Is(err, asserter.ErrCurveTypeNotSupported))

	type privKeyTest struct {
		keypair *KeyPair
		err     error
	}

	var privKeyTests = []privKeyTest{
		{mockKeyPair(make([]byte, 33), types.Secp256k1), ErrPrivKeyLengthInvalid},
		{mockKeyPair(make([]byte, 31), types.Secp256k1), ErrPrivKeyLengthInvalid},
		{mockKeyPair(make([]byte, 0), types.Secp256k1), ErrPrivKeyLengthInvalid},
		{mockKeyPair(make([]byte, 33), types.Edwards25519), ErrPrivKeyLengthInvalid},
		{mockKeyPair(make([]byte, 31), types.Edwards25519), ErrPrivKeyLengthInvalid},
		{mockKeyPair(make([]byte, 0), types.Edwards25519), ErrPrivKeyLengthInvalid},
	}

	for _, test := range privKeyTests {
		err = test.keypair.IsValid()
		assert.True(t, errors.Is(err, test.err))
	}
}

func TestImportPrivateKey(t *testing.T) {
	importPrivKeyTests := map[string]struct {
		privKey   string
		curveType types.CurveType
		err       error
	}{
		"simple ed25519": {
			"aeb121b4c545f0f850e1480492508c65a250e9965b0d90176fab4d7506398ebb",
			types.Edwards25519,
			nil,
		},
		"simple Secp256k1": {
			"0b188af56b25d007fbc4bbf2176cd2a54d876ce4774bb5df38b7c83349405b7a",
			types.Secp256k1,
			nil,
		},
		"simple Pallas": {
			"92D872DA7B3C90CF69D347908C3D3D692EA033A1D6E4A1695FCDCF6BBED87F37",
			types.Pallas,
			nil,
		},
		"short ed25519":   {"asd", types.Secp256k1, ErrPrivKeyUndecodable},
		"short Secp256k1": {"asd", types.Edwards25519, ErrPrivKeyUndecodable},
		"short pallas":    {"asd", types.Pallas, ErrPrivKeyUndecodable},
		"long ed25519": {
			"aeb121b4c545f0f850e1480492508c65a250e9965b0d90176fab4d7506398ebbaeb121b4c545f0f850e1480492508c65a250e9965b0d90176fab4d7506398ebb", // nolint:lll
			types.Secp256k1,
			ErrPrivKeyLengthInvalid,
		},
		"long Secp256k1": {
			"0b188af56b25d007fbc4bbf2176cd2a54d876ce4774bb5df38b7c83349405b7a0b188af56b25d007fbc4bbf2176cd2a54d876ce4774bb5df38b7c83349405b7a", // nolint:lll
			types.Edwards25519,
			ErrPrivKeyLengthInvalid,
		},
		"long Pallas": {
			"92D872DA7B3C90CF69D347908C3D3D692EA033A1D6E4A1695FCDCF6BBED87F3792D872DA7B3C90CF69D347908C3D3D692EA033A1D6E4A1695FCDCF6BBED87F37", // nolint:lll
			types.Pallas,
			ErrPrivKeyLengthInvalid,
		},
	}

	for name, test := range importPrivKeyTests {
		t.Run(name, func(t *testing.T) {
			kp, err := ImportPrivateKey(test.privKey, test.curveType)
			if test.err != nil {
				assert.Nil(t, kp)
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, kp.IsValid())
				assert.NoError(t, err)
			}
		})
	}
}
