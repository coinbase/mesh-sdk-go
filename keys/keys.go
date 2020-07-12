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
	"crypto/ed25519"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/btcsuite/btcd/btcec"
)

// PrivKeyBytesLen are 32-bytes for all supported curvetypes
const PrivKeyBytesLen = 32

// GenerateKeypair returns a Keypair of a specified CurveType
func GenerateKeypair(curve types.CurveType) (*KeyPair, error) {
	var keyPair *KeyPair

	switch curve {
	case types.Secp256k1:
		rawPrivKey, err := btcec.NewPrivateKey(btcec.S256())
		if err != nil {
			return nil, fmt.Errorf("keygen: new private key. %w", err)
		}
		rawPubKey := rawPrivKey.PubKey()

		pubKey := &types.PublicKey{
			Bytes:     rawPubKey.SerializeCompressed(),
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: rawPrivKey.Serialize(),
		}

	case types.Edwards25519:
		rawPubKey, rawPrivKey, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, fmt.Errorf("keygen: new keypair. %w", err)
		}

		pubKey := &types.PublicKey{
			Bytes:     rawPubKey,
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: rawPrivKey.Seed(),
		}

	default:
		return nil, fmt.Errorf("%s is not supported", curve)
	}

	return keyPair, nil
}

// IsValid checks the validity of a keypair
func (k *KeyPair) IsValid() error {
	// Checks if valid PublicKey and CurveType
	err := asserter.PublicKey(k.PublicKey)
	if err != nil {
		return err
	}

	// Will change if we support more CurveTypes with different privkey sizes
	if len(k.PrivateKey) != PrivKeyBytesLen {
		return fmt.Errorf(
			"invalid privkey length %v. Expected 32 bytes",
			len(k.PrivateKey),
		)
	}

	return nil
}

// Signer returns the constructs a Signer
// for the KeyPair.
func (k *KeyPair) Signer() (Signer, error) {
	switch k.PublicKey.CurveType {
	case types.Secp256k1:
		return &SignerSecp256k1{k}, nil
	case types.Edwards25519:
		return &SignerEdwards25519{k}, nil
	default:
		return nil, fmt.Errorf("curve %s not supported", k.PublicKey.CurveType)
	}
}
