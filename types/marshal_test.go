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

package types

import (
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCustomMarshalPublicKey(t *testing.T) {
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

	// Invalid Hex Check
	s3 := &PublicKey{}
	err = json.Unmarshal([]byte(`{"hex_bytes":"hello"}`), s3)
	assert.Error(t, err)
	assert.Len(t, s3.Bytes, 0)
}

func TestCustomMarshalSignature(t *testing.T) {
	s := &Signature{
		SignatureType: Ecdsa,
		Bytes: []byte(
			"hsdjkfhkasjfhkjasdhfkjasdnfkjabsdfkjhakjsfdhjksadhfjk23478923645yhsdfn",
		),
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
	s2 := &Signature{}
	err = json.Unmarshal(j, s2)
	assert.NoError(t, err)

	assert.Equal(t, s, s2)

	// Invalid Hex Check
	s3 := &Signature{}
	err = json.Unmarshal([]byte(`{"hex_bytes":"hello"}`), s3)
	assert.Error(t, err)
	assert.Len(t, s3.Bytes, 0)
}

func TestCustomMarshalSigningPayload(t *testing.T) {
	s := &SigningPayload{
		AccountIdentifier: &AccountIdentifier{
			Address: "addr1",
		},
		Bytes: []byte("hsdjkfhkasjfhkjasdhfkjasdnfkjabsdfkjhakjsfdhjksadhfjk23478923645yhsdfn"),
	}

	j, err := json.Marshal(s)
	assert.NoError(t, err)

	// Hex and address Check
	simpleType := struct {
		Address  string `json:"address"`
		HexBytes string `json:"hex_bytes"`
	}{}

	err = json.Unmarshal(j, &simpleType)
	assert.NoError(t, err)

	b, err := hex.DecodeString(simpleType.HexBytes)
	assert.NoError(t, err)
	assert.Equal(t, s.Bytes, b)
	assert.Equal(t, s.AccountIdentifier.Address, simpleType.Address)

	// Full Unmarshal Check
	s2 := &SigningPayload{}
	err = json.Unmarshal(j, s2)
	assert.NoError(t, err)
	assert.Equal(t, s, s2)

	// Invalid Hex Check
	s3 := &SigningPayload{}
	err = json.Unmarshal([]byte(`{"hex_bytes":"hello"}`), s3)
	assert.Error(t, err)
	assert.Len(t, s3.Bytes, 0)

	// Unmarshal fields
	var s4 SigningPayload
	err = json.Unmarshal([]byte(`{"address":"hello", "hex_bytes":"74657374"}`), &s4)
	assert.NoError(t, err)
	assert.Equal(t, &AccountIdentifier{Address: "hello"}, s4.AccountIdentifier)
	assert.Equal(t, []byte("test"), s4.Bytes)

	// Unmarshal fields (empty address)
	var s5 SigningPayload
	err = json.Unmarshal([]byte(`{"hex_bytes":"74657374"}`), &s5)
	assert.NoError(t, err)
	assert.Nil(t, s5.AccountIdentifier)
	assert.Equal(t, []byte("test"), s5.Bytes)
}
