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
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func TestGenerateKeypairSecp256k1(t *testing.T) {
	curve := types.Secp256k1
	keypair, err := GenerateKeypair(curve)

	assert.NoError(t, err)
	assert.Equal(t, keypair.PublicKey.CurveType, curve)
	assert.Equal(t, keypair.PrivateKey.CurveType, curve)
}

func TestGenerateKeypairEd25519(t *testing.T) {
	curve := types.Edwards25519
	keypair, err := GenerateKeypair(curve)

	assert.NoError(t, err)
	assert.Equal(t, keypair.PublicKey.CurveType, curve)
	assert.Equal(t, keypair.PrivateKey.CurveType, curve)
}

func TestKeypairValidity(t *testing.T) {
	// Non matching curves
	keyPair, _ := GenerateKeypair(types.Edwards25519)
	keyPair.PublicKey.CurveType = types.Secp256k1
	valid, err := keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "do not match")

	// Privkey length too short
	keyPair, _ = GenerateKeypair(types.Edwards25519)
	keyPair.PrivateKey = &PrivateKey{
		CurveType: types.Edwards25519,
	}

	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid privkey length")

	// Privkey length too short
	keyPair, _ = GenerateKeypair(types.Secp256k1)
	keyPair.PrivateKey = &PrivateKey{
		CurveType: types.Secp256k1,
	}

	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid privkey length")
}
