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

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// SignerEd25519 is initialized from a keypair
type SignerEd25519 struct {
	KeyPair KeyPair
}

// PublicKey returns the PublicKey of the signer
func (s SignerEd25519) PublicKey() *types.PublicKey {
	return s.KeyPair.PublicKey
}

// Signs arbitrary payloads using a KeyPair
func (s SignerEd25519) Sign(payload *types.SigningPayload) (*types.Signature, error) {
	err := s.KeyPair.IsValid()
	if err != nil {
		return nil, err
	}

	if !(payload.SignatureType == types.Ed25519 || payload.SignatureType == "") {
		return nil, fmt.Errorf("sign: payload signature type is not ed25519")
	}

	privKeyHexBytes, _ := hex.DecodeString(s.KeyPair.PrivateKey.HexBytes)
	decodedMessage, err := hex.DecodeString(payload.HexBytes)
	if err != nil {
		return nil, fmt.Errorf("sign: unable to decode message. %w", err)
	}

	privKey := ed25519.NewKeyFromSeed(privKeyHexBytes)
	sig := ed25519.Sign(privKey, decodedMessage)

	return &types.Signature{
		SigningPayload: payload,
		PublicKey:      s.KeyPair.PublicKey,
		SignatureType:  payload.SignatureType,
		HexBytes:       hex.EncodeToString(sig),
	}, nil
}

// Verify verifies a Signature, by checking the validity of a Signature,
// the SigningPayload, and the PublicKey of the Signature.
func (s SignerEd25519) Verify(signature *types.Signature) error {
	if signature.SignatureType != types.Ed25519 {
		return fmt.Errorf("verify: payload signature type is not ed25519")
	}

	pubKey, err := hex.DecodeString(signature.PublicKey.HexBytes)
	if err != nil {
		return fmt.Errorf("verify: unable to decode pubkey. %w", err)
	}
	decodedMessage, err := hex.DecodeString(signature.SigningPayload.HexBytes)
	if err != nil {
		return fmt.Errorf("verify: unable to decode message. %w", err)
	}
	decodedSignature, err := hex.DecodeString(signature.HexBytes)
	if err != nil {
		return fmt.Errorf("verify: unable to decode signature. %w", err)
	}

	err = asserter.Signatures([]*types.Signature{signature})
	if err != nil {
		return err
	}

	verify := ed25519.Verify(pubKey, decodedMessage, decodedSignature)
	if !verify {
		return fmt.Errorf("verify: verify returned false")
	}

	return nil
}
