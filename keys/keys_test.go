package keys

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
