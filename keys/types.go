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
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// KeyPair contains a PrivateKey and its associated PublicKey
type KeyPair struct {
	PublicKey  *types.PublicKey `json:"public_key"`
	PrivateKey []byte           `json:"private_key"`
}

// MarshalJSON overrides the default JSON marshaler
// and encodes bytes as hex instead of base64.
func (k *KeyPair) MarshalJSON() ([]byte, error) {
	type Alias KeyPair
	j, err := json.Marshal(struct {
		PrivateKey string `json:"private_key"`
		*Alias
	}{
		PrivateKey: hex.EncodeToString(k.PrivateKey),
		Alias:      (*Alias)(k),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal key pair: %w", err)
	}
	return j, nil
}

// UnmarshalJSON overrides the default JSON unmarshaler
// and decodes bytes from hex instead of base64.
func (k *KeyPair) UnmarshalJSON(b []byte) error {
	type Alias KeyPair
	r := struct {
		PrivateKey string `json:"private_key"`
		*Alias
	}{
		Alias: (*Alias)(k),
	}
	err := json.Unmarshal(b, &r)
	if err != nil {
		return fmt.Errorf("failed to unmarshal key pair: %w", err)
	}
	bytes, err := hex.DecodeString(r.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to decode private key: %w", err)
	}
	k.PrivateKey = bytes
	return nil
}
