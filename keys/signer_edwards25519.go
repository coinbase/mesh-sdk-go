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
	"crypto/ed25519"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// SignerEdwards25519 is initialized from a keypair
type SignerEdwards25519 struct {
	KeyPair *KeyPair
}

var _ Signer = (*SignerEdwards25519)(nil)

// PublicKey returns the PublicKey of the signer
func (s *SignerEdwards25519) PublicKey() *types.PublicKey {
	return s.KeyPair.PublicKey
}

// Sign arbitrary payloads using a KeyPair
func (s *SignerEdwards25519) Sign(
	payload *types.SigningPayload,
	sigType types.SignatureType,
) (*types.Signature, error) {
	err := s.KeyPair.IsValid()
	if err != nil {
		return nil, fmt.Errorf("key pair is invalid: %w", err)
	}

	if !(payload.SignatureType == types.Ed25519 || payload.SignatureType == "") {
		return nil, fmt.Errorf(
			"expected signing payload signature type %v but got %v: %w",
			types.Ed25519,
			payload.SignatureType,
			ErrSignUnsupportedPayloadSignatureType,
		)
	}

	if sigType != types.Ed25519 {
		return nil, fmt.Errorf(
			"expected signature type %v but got %v: %w",
			types.Ed25519,
			sigType,
			ErrSignUnsupportedSignatureType,
		)
	}

	privKeyBytes := s.KeyPair.PrivateKey
	privKey := ed25519.NewKeyFromSeed(privKeyBytes)
	sig := ed25519.Sign(privKey, payload.Bytes)

	return &types.Signature{
		SigningPayload: payload,
		PublicKey:      s.KeyPair.PublicKey,
		SignatureType:  payload.SignatureType,
		Bytes:          sig,
	}, nil
}

// Verify verifies a Signature, by checking the validity of a Signature,
// the SigningPayload, and the PublicKey of the Signature.
func (s *SignerEdwards25519) Verify(signature *types.Signature) error {
	if signature.SignatureType != types.Ed25519 {
		return fmt.Errorf(
			"expected signing payload signature type %v but got %v: %w",
			types.Ed25519,
			signature.SignatureType,
			ErrVerifyUnsupportedPayloadSignatureType,
		)
	}

	pubKey := signature.PublicKey.Bytes
	message := signature.SigningPayload.Bytes
	sig := signature.Bytes
	err := asserter.Signatures([]*types.Signature{signature})
	if err != nil {
		return fmt.Errorf("signature is invalid: %w", err)
	}

	verify := ed25519.Verify(pubKey, message, sig)
	if !verify {
		return ErrVerifyFailed
	}

	return nil
}
