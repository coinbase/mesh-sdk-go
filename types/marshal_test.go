package types

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCustomMarshal(t *testing.T) {
	// We only test PublicKey because the marshaling logic
	// is all codegen.
	s := &PublicKey{
		CurveType: Secp256k1,
		Bytes:     []byte("hsdjkfhkasjfhkjasdhfkjasdnfkjabsdfkjhakjsfdhjksadhfjk23478923645yhsdfn"),
	}

	j, err := json.Marshal(s)
	assert.NoError(t, err)

	// Simple Hex Check
	simpleType := struct {
		HexBytes string `json:"hex_bytes"`
	}{}

	err = json.Unmarshal(j, &simpleType)
	assert.NoError(t, err)

	b, err := hex.DecodeString(simpleType.HexBytes)
	assert.NoError(t, err)

	assert.Equal(t, s.Bytes, b)

	// Full Unmarshal Check
	s2 := &PublicKey{}
	err = json.Unmarshal(j, s2)
	assert.NoError(t, err)

	assert.Equal(t, s, s2)
}
