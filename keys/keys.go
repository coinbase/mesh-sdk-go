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
)

func GenerateKeypair(curve CurveType) (*KeyPair, error) {
	var keyPair *KeyPair

	switch {
	case curve.IsSecp256k1():
		rawPrivKey, err := btcec.NewPrivateKey(btcec.S256())
		if err != nil {
			return nil, fmt.Errorf("keygen: new private key. %w", err)
		}
		rawPubKey := rawPrivKey.PubKey()

		pubKey := &PublicKey{
			PubKeyHex: hex.EncodeToString(rawPubKey.SerializeCompressed()),
			Curve:     curve,
		}
		privKey := &PrivateKey{
			PrivKeyHex: hex.EncodeToString(rawPrivKey.Serialize()),
			Curve:      curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: privKey,
		}

	case curve.IsEdwards25519():
		rawPubKey, rawPrivKey, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, fmt.Errorf("keygen: new keypair. %w", err)
		}

		pubKey := &PublicKey{
			PubKeyHex: hex.EncodeToString(rawPubKey),
			Curve:     curve,
		}
		privKey := &PrivateKey{
			PrivKeyHex: hex.EncodeToString(rawPrivKey.Seed()),
			Curve:      curve,
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
	pk, _ := hex.DecodeString(k.PublicKey.PubKeyHex)
	sk, _ := hex.DecodeString(k.PrivateKey.PrivKeyHex)
	pkCurve := k.PublicKey.Curve
	skCurve := k.PrivateKey.Curve

	if pkCurve != skCurve {
		return false, fmt.Errorf("private key curve %s and public key curve %s do not match", skCurve, pkCurve)
	}

	// Secp256k1 Pubkeys are 33-bytes
	if pkCurve.IsSecp256k1() && len(pk) != btcec.PubKeyBytesLenCompressed {
		return false, fmt.Errorf("invalid pubkey length %v", len(pk))
	}

	// Ed25519 pubkeys are 32-bytes
	if pkCurve.IsEdwards25519() && len(pk) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid pubkey length %v", len(pk))
	}

	// All privkeys are 32-bytes
	if len(sk) != btcec.PrivKeyBytesLen {
		return false, fmt.Errorf("invalid privkey length %v", len(sk))
	}

	return true, nil
}
