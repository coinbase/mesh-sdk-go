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
	"encoding/hex"
	"fmt"

	"github.com/btcsuite/btcd/btcec"
	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

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
			HexBytes:  hex.EncodeToString(rawPubKey.SerializeCompressed()),
			CurveType: curve,
		}
		privKey := &PrivateKey{
			HexBytes:  hex.EncodeToString(rawPrivKey.Serialize()),
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: privKey,
		}

	case types.Edwards25519:
		rawPubKey, rawPrivKey, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, fmt.Errorf("keygen: new keypair. %w", err)
		}

		pubKey := &types.PublicKey{
			HexBytes:  hex.EncodeToString(rawPubKey),
			CurveType: curve,
		}

		privKey := &PrivateKey{
			HexBytes:  hex.EncodeToString(rawPrivKey.Seed()),
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: privKey,
		}

	default:
		return nil, fmt.Errorf("%s is not supported", curve)
	}

	return keyPair, nil
}

func (k KeyPair) IsValid() (bool, error) {
	sk, _ := hex.DecodeString(k.PrivateKey.HexBytes)
	pkCurve := k.PublicKey.CurveType
	skCurve := k.PrivateKey.CurveType

	// Checks if valid Public Key
	err := asserter.PublicKey(k.PublicKey)
	if err != nil {
		return false, err
	}

	// Checks if valid CurveType
	err = asserter.CurveType(k.PublicKey.CurveType)
	if err != nil {
		return false, err
	}

	// Checks if pk and sk have the same CurveType
	if pkCurve != skCurve {
		return false, fmt.Errorf("private key curve %s and public key curve %s do not match", skCurve, pkCurve)
	}

	// All privkeys are 32-bytes
	if len(sk) != btcec.PrivKeyBytesLen {
		return false, fmt.Errorf("invalid privkey length %v", len(sk))
	}

	return true, nil
}
