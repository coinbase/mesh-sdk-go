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

	"github.com/coinbase/rosetta-sdk-go/types"
)

// KeyPair contains a PrivateKey and its' associated PublicKey
type KeyPair struct {
	PublicKey  *types.PublicKey
	PrivateKey *PrivateKey
}

// PrivateKey contains the privkey bytes as well as the CurveType
type PrivateKey struct {
	Bytes     []byte          `json:"hex_bytes"`
	CurveType types.CurveType `json:"curve_type"`
}

// MarshalJSON overrides the default JSON marshaler
// and encodes bytes as hex instead of base64.
func (pk *PrivateKey) MarshalJSON() ([]byte, error) {
	type Alias PrivateKey
	j, err := json.Marshal(struct {
		Bytes string `json:"hex_bytes"`
		*Alias
	}{
		Bytes: hex.EncodeToString(pk.Bytes),
		Alias: (*Alias)(pk),
	})
	if err != nil {
		return nil, err
	}
	return j, nil
}

// UnmarshalJSON overrides the default JSON unmarshaler
// and decodes bytes from hex instead of base64.
func (pk *PrivateKey) UnmarshalJSON(b []byte) error {
	type Alias PrivateKey
	r := struct {
		Bytes string `json:"hex_bytes"`
		*Alias
	}{
		Alias: (*Alias)(pk),
	}
	err := json.Unmarshal(b, &r)
	if err != nil {
		return err
	}
	bytes, err := hex.DecodeString(r.Bytes)
	if err != nil {
		return err
	}
	pk.Bytes = bytes
	return nil
}
