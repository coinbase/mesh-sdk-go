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
	"fmt"

	zil_schnorr "github.com/Zilliqa/gozilliqa-sdk/schnorr"
	"github.com/ethereum/go-ethereum/crypto/secp256k1"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// SignerSecp256k1 is initialized from a keypair
type SignerSecp256k1 struct {
	KeyPair *KeyPair
}

// EcdsaSignatureLen is 64 bytes
const EcdsaSignatureLen = 64

var _ Signer = (*SignerSecp256k1)(nil)

// PublicKey returns the PublicKey of the signer
func (s *SignerSecp256k1) PublicKey() *types.PublicKey {
	return s.KeyPair.PublicKey
}

// Sign arbitrary payloads using a KeyPair
func (s *SignerSecp256k1) Sign(
	payload *types.SigningPayload,
	sigType types.SignatureType,
) (*types.Signature, error) {
	err := s.KeyPair.IsValid()
	if err != nil {
		return nil, fmt.Errorf("key pair is invalid: %w", err)
	}
	privKeyBytes := s.KeyPair.PrivateKey

	if !(payload.SignatureType == sigType || payload.SignatureType == "") {
		return nil, fmt.Errorf(
			"signing payload signature type %v is invalid: %w",
			payload.SignatureType,
			ErrSignUnsupportedPayloadSignatureType,
		)
	}

	var sig []byte
	switch sigType {
	case types.EcdsaRecovery:
		sig, err = secp256k1.Sign(payload.Bytes, privKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to sign for %v: %w", types.EcdsaRecovery, err)
		}
	case types.Ecdsa:
		sig, err = secp256k1.Sign(payload.Bytes, privKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to sign for %v: %w", types.Ecdsa, err)
		}
		sig = sig[:EcdsaSignatureLen]
	case types.Schnorr1:
		sig, err = zil_schnorr.SignMessage(privKeyBytes, s.KeyPair.PublicKey.Bytes, payload.Bytes)
		if err != nil {
			return nil, fmt.Errorf("failed to sign for %v: %w", types.Schnorr1, err)
		}
	default:
		return nil, fmt.Errorf(
			"signature type %s is invalid: %w",
			types.PrintStruct(sigType),
			ErrSignUnsupportedSignatureType,
		)
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
func (s *SignerSecp256k1) Verify(signature *types.Signature) error {
	pubKey := signature.PublicKey.Bytes
	message := signature.SigningPayload.Bytes
	sig := signature.Bytes

	err := asserter.Signatures([]*types.Signature{signature})
	if err != nil {
		return fmt.Errorf("signature is invalid: %w", err)
	}

	var verify bool
	switch signature.SignatureType {
	case types.Ecdsa:
		verify = secp256k1.VerifySignature(pubKey, message, sig)
	case types.EcdsaRecovery:
		normalizedSig := sig[:EcdsaSignatureLen]
		verify = secp256k1.VerifySignature(pubKey, message, normalizedSig)
	case types.Schnorr1:
		verify = zil_schnorr.VerifySignature(pubKey, message, sig)
	default:
		return fmt.Errorf(
			"signature type %s is invalid: %w",
			types.PrintStruct(signature.SignatureType),
			ErrVerifyUnsupportedSignatureType,
		)
	}

	if !verify {
		return ErrVerifyFailed
	}
	return nil
}
