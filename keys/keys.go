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
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/btcsuite/btcd/btcec"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// PrivKeyBytesLen are 32-bytes for all supported curvetypes
const PrivKeyBytesLen = 32

func privateKeyValid(privateKey []byte) error {
	// We will need to add a switch statement here if we add support
	// for CurveTypes that have a different private key length than
	// PrivKeyBytesLen.
	if len(privateKey) != PrivKeyBytesLen {
		return fmt.Errorf(
			"%w: expected %d bytes but got %v",
			ErrPrivKeyLengthInvalid,
			PrivKeyBytesLen,
			len(privateKey),
		)
	}

	if asserter.BytesArrayZero(privateKey) {
		return ErrPrivKeyZero
	}

	return nil
}

// ImportPrivateKey returns a Keypair from a hex-encoded privkey string
func ImportPrivateKey(privKeyHex string, curve types.CurveType) (*KeyPair, error) {
	privKey, err := hex.DecodeString(privKeyHex)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrPrivKeyUndecodable, privKeyHex)
	}

	// We check the parsed private key length to ensure we don't panic (most
	// crypto libraries panic with incorrect private key lengths instead of
	// throwing an error).
	if err := privateKeyValid(privKey); err != nil {
		return nil, err
	}

	var keyPair *KeyPair
	switch curve {
	case types.Secp256k1:
		rawPrivKey, rawPubKey := btcec.PrivKeyFromBytes(btcec.S256(), privKey)

		pubKey := &types.PublicKey{
			Bytes:     rawPubKey.SerializeCompressed(),
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: rawPrivKey.Serialize(),
		}
	case types.Edwards25519:
		rawPrivKey := ed25519.NewKeyFromSeed(privKey)

		pubKey := &types.PublicKey{
			Bytes:     rawPrivKey.Public().(ed25519.PublicKey),
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: rawPrivKey.Seed(),
		}
	case types.Secp256r1:
		crv := elliptic.P256()
		x, y := crv.ScalarBaseMult(privKey)

		// IsOnCurve will return false for the point at infinity (0, 0)
		// See:
		// https://github.com/golang/go/blob/3298300ddf45a0792b4d8ea5e05f0fbceec4c9f9/src/crypto/elliptic/elliptic.go#L24
		if !crv.IsOnCurve(x, y) {
			return nil, ErrPubKeyNotOnCurve
		}

		rawPubKey := ecdsa.PublicKey{X: x, Y: y, Curve: crv}
		rawPrivKey := ecdsa.PrivateKey{
			PublicKey: rawPubKey,
			D:         new(big.Int).SetBytes(privKey),
		}

		pubKey := &types.PublicKey{
			Bytes:     elliptic.Marshal(crv, rawPubKey.X, rawPubKey.Y),
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: rawPrivKey.D.Bytes(),
		}
	default:
		return nil, fmt.Errorf("%w: %s", ErrCurveTypeNotSupported, curve)
	}

	// We test for validity before returning
	// the new KeyPair.
	if err := keyPair.IsValid(); err != nil {
		return nil, err
	}

	return keyPair, nil
}

// GenerateKeypair returns a Keypair of a specified CurveType
func GenerateKeypair(curve types.CurveType) (*KeyPair, error) {
	var keyPair *KeyPair

	switch curve {
	case types.Secp256k1:
		rawPrivKey, err := btcec.NewPrivateKey(btcec.S256())
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrKeyGenSecp256k1Failed, err)
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
			return nil, fmt.Errorf("%w: %v", ErrKeyGenEdwards25519Failed, err)
		}

		pubKey := &types.PublicKey{
			Bytes:     rawPubKey,
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: rawPrivKey.Seed(),
		}
	case types.Secp256r1:
		crv := elliptic.P256()
		rawPrivKey, err := ecdsa.GenerateKey(crv, rand.Reader)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrKeyGenSecp256r1Failed, err)
		}
		rawPubKey := rawPrivKey.PublicKey
		pubKey := &types.PublicKey{
			Bytes:     elliptic.Marshal(crv, rawPubKey.X, rawPubKey.Y),
			CurveType: curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: rawPrivKey.D.Bytes(),
		}
	default:
		return nil, fmt.Errorf("%w: %s", ErrCurveTypeNotSupported, curve)
	}

	// We test for validity before returning
	// the new KeyPair.
	if err := keyPair.IsValid(); err != nil {
		return nil, err
	}

	return keyPair, nil
}

// IsValid checks the validity of a KeyPair.
func (k *KeyPair) IsValid() error {
	if err := asserter.PublicKey(k.PublicKey); err != nil {
		return err
	}

	if err := privateKeyValid(k.PrivateKey); err != nil {
		return err
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
	case types.Secp256r1:
		return &SignerSecp256r1{k}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrCurveTypeNotSupported, k.PublicKey.CurveType)
	}
}
