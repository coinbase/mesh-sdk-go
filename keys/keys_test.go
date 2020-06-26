package keys

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGenerateKeypairSecp256k1(t *testing.T) {
	curve := CurveType(SECP256K1_CURVE)
	keypair, err := GenerateKeypair(curve)

	assert.NoError(t, err)
	assert.Equal(t, keypair.PublicKey.Curve, curve)
	assert.Equal(t, keypair.PrivateKey.Curve, curve)
}

func TestGenerateKeypairEd25519(t *testing.T) {
	curve := CurveType(EDWARDS25519_CURVE)
	keypair, err := GenerateKeypair(curve)

	assert.NoError(t, err)
	assert.Equal(t, keypair.PublicKey.Curve, curve)
	assert.Equal(t, keypair.PrivateKey.Curve, curve)
}

func TestKeypairValidity(t *testing.T) {
	// Non matching curves
	pubKey := &PublicKey{
		Curve: CurveType(SECP256K1_CURVE),
	}
	privKey := &PrivateKey{
		Curve: CurveType(EDWARDS25519_CURVE),
	}
	keyPair := &KeyPair{
		PublicKey:  pubKey,
		PrivateKey: privKey,
	}
	valid, err := keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "do not match")

	// Ed25519 pubkey invalid length
	keyPair, err = GenerateKeypair(CurveType(EDWARDS25519_CURVE))
	keyPair.PublicKey = &PublicKey{
		Curve: CurveType(EDWARDS25519_CURVE),
	}
	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid pubkey length")

	// Secp256k1 pubkey invalid length
	keyPair, err = GenerateKeypair(CurveType(SECP256K1_CURVE))
	keyPair.PublicKey = &PublicKey{
		Curve: CurveType(SECP256K1_CURVE),
	}
	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid pubkey length")

	// Privkey length too short
	keyPair, err = GenerateKeypair(CurveType(EDWARDS25519_CURVE))
	keyPair.PrivateKey = &PrivateKey{
		Curve: CurveType(EDWARDS25519_CURVE),
	}

	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid privkey length")

	// Privkey length too short
	keyPair, err = GenerateKeypair(CurveType(SECP256K1_CURVE))
	keyPair.PrivateKey = &PrivateKey{
		Curve: CurveType(SECP256K1_CURVE),
	}

	valid, err = keyPair.IsValid()
	assert.Equal(t, false, valid)
	assert.Contains(t, err.Error(), "invalid privkey length")
}
