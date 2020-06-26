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
