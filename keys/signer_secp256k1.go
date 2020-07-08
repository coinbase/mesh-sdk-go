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
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

// SignerSecp256k1 is initialized from a keypair
type SignerSecp256k1 struct {
	KeyPair *KeyPair
}

// PublicKey returns the PublicKey of the signer
func (s SignerSecp256k1) PublicKey() *types.PublicKey {
	return s.KeyPair.PublicKey
}

// Signs arbitrary payloads using a KeyPair
func (s SignerSecp256k1) Sign(payload *types.SigningPayload, sigType types.SignatureType) (*types.Signature, error) {
	err := s.KeyPair.IsValid()
	if err != nil {
		return nil, err
	}
	privKeyBytes := s.KeyPair.PrivateKey.Bytes

	var sig []byte
	switch sigType {
	case types.EcdsaRecovery:
		sig, err = secp256k1.Sign(payload.Bytes, privKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("sign: unable to sign. %w", err)
		}
	case types.Ecdsa:
		sig, err = secp256k1.Sign(payload.Bytes, privKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("sign: unable to sign. %w", err)
		}
		sig = sig[:64]
	default:
		return nil, fmt.Errorf("sign: unsupported signature type. %w", err)
	}

	return &types.Signature{
		SigningPayload: payload,
		PublicKey:      s.KeyPair.PublicKey,
		SignatureType:  payload.SignatureType,
		Bytes:          sig,
	}, nil
}

// Verify verifies a Signature, by checking the validity of a Signature,
// the SigningPayload, and the PublicKey of the Signature.
func (s SignerSecp256k1) Verify(signature *types.Signature) error {
	pubKey := signature.PublicKey.Bytes
	message := signature.SigningPayload.Bytes
	sig := signature.Bytes

	err := asserter.Signatures([]*types.Signature{signature})
	if err != nil {
		return err
	}

	var verify bool
	switch signature.SignatureType {
	case types.Ecdsa:
		verify = secp256k1.VerifySignature(pubKey, message, sig)
	case types.EcdsaRecovery:
		normalizedSig := sig[:64]
		verify = secp256k1.VerifySignature(pubKey, message, normalizedSig)
	default:
		return fmt.Errorf("%s is not supported", signature.SignatureType)
	}

	if !verify {
		return fmt.Errorf("verify: verify returned false")
	}
	return nil
}
