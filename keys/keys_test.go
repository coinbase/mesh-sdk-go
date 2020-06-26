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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateKeypairSecp256k1(t *testing.T) {
	curve := CurveType(Secp256k1Curve)
	keypair, err := GenerateKeypair(curve)

	assert.NoError(t, err)
	assert.Equal(t, keypair.PublicKey.Curve, curve)
	assert.Equal(t, keypair.PrivateKey.Curve, curve)
}

func TestGenerateKeypairEd25519(t *testing.T) {
	curve := CurveType(Edwards25519Curve)
	keypair, err := GenerateKeypair(curve)

	assert.NoError(t, err)
	assert.Equal(t, keypair.PublicKey.Curve, curve)
	assert.Equal(t, keypair.PrivateKey.Curve, curve)
}

func TestKeypairValidity(t *testing.T) {
	// Non matching curves
	pubKey := &PublicKey{
		Curve: CurveType(Secp256k1Curve),
	}
	privKey := &PrivateKey{
		Curve: CurveType(Edwards25519Curve),
	}
	keyPair := &KeyPair{
		PublicKey:  pubKey,
		PrivateKey: privKey,
	}
	valid, err := keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "do not match")

	// Ed25519 pubkey invalid length
	keyPair, _ = GenerateKeypair(CurveType(Edwards25519Curve))
	keyPair.PublicKey = &PublicKey{
		Curve: CurveType(Edwards25519Curve),
	}
	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid pubkey length")

	// Secp256k1 pubkey invalid length
	keyPair, _ = GenerateKeypair(CurveType(Secp256k1Curve))
	keyPair.PublicKey = &PublicKey{
		Curve: CurveType(Secp256k1Curve),
	}
	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid pubkey length")

	// Privkey length too short
	keyPair, _ = GenerateKeypair(CurveType(Edwards25519Curve))
	keyPair.PrivateKey = &PrivateKey{
		Curve: CurveType(Edwards25519Curve),
	}

	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid privkey length")

	// Privkey length too short
	keyPair, _ = GenerateKeypair(CurveType(Secp256k1Curve))
	keyPair.PrivateKey = &PrivateKey{
		Curve: CurveType(Secp256k1Curve),
	}

	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid privkey length")
}
