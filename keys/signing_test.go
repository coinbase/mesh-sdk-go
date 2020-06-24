package keys

import (
	"encoding/hex"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSignEd25519(t *testing.T) {
	curve := CurveType(EDWARDS25519_CURVE)
	keypair, err := GenerateKeypair(curve)
	assert.NoError(t, err)

	signatureType := SignatureType(ED25519_SIGNATURE)
	payload := &SigningPayload{
		Address:         "test",
		HexEncodedBytes: "68656C6C6F0D0A",
		SignatureType:   signatureType,
	}

	signature, err := SignPayload(payload, keypair)
	assert.NoError(t, err)

	verify, err := Verify(signature)
	assert.NoError(t, err)
	assert.Equal(t, verify, true)
}

func TestSignSecp256k1(t *testing.T) {
	curve := CurveType(SECP256K1_CURVE)
	keypair, err := GenerateKeypair(curve)
	assert.NoError(t, err)

	signatureType := SignatureType(ECDSA_PUBKEY_RECOVERY_SIGNATURE)

	messageHash := hex.EncodeToString(chainhash.DoubleHashB([]byte("hello")))

	payload := &SigningPayload{
		Address:         "test",
		HexEncodedBytes: messageHash,
		SignatureType:   signatureType,
	}

	signature, err := SignPayload(payload, keypair)
	assert.NoError(t, err)

	verify, err := Verify(signature)
	assert.NoError(t, err)
	assert.Equal(t, true, verify)
}
