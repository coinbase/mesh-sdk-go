// Copyright 2022 Coinbase, Inc.
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
	"errors"
	"fmt"

	"github.com/coinbase/kryptology/pkg/signatures/bls/bls_sig"
	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var ErrBlsTransactionValidationErr = errors.New("transaction with bls validation failed")

type SignerBls12381 struct {
	KeyPair *KeyPair
}

func (s *SignerBls12381) PublicKey() *types.PublicKey {
	return s.KeyPair.PublicKey
}

// Sign transaction payloads using a KeyPair
func (s *SignerBls12381) Sign(
	payload *types.SigningPayload,
	sigType types.SignatureType,
) (*types.Signature, error) {
	err := s.KeyPair.IsValid()
	if err != nil {
		return nil, err
	}

	if !(payload.SignatureType == types.BlsG2Element || payload.SignatureType == "") {
		return nil, fmt.Errorf(
			"%w: expected %v but got %v",
			ErrSignUnsupportedPayloadSignatureType,
			types.BlsG2Element,
			payload.SignatureType,
		)
	}

	if sigType != types.BlsG2Element {
		return nil, fmt.Errorf(
			"%w: expected %v but got %v",
			ErrSignUnsupportedSignatureType,
			types.BlsG2Element,
			sigType,
		)
	}

	// Generate private key bytes
	privKeyBytes := s.KeyPair.PrivateKey
	privKey := &bls_sig.SecretKey{}
	_ = privKey.UnmarshalBinary(privKeyBytes)

	bls := bls_sig.NewSigBasic()
	sig, err := bls.Sign(privKey, payload.Bytes)
	if err != nil {
		return nil, err
	}
	sigBytes, _ := sig.MarshalBinary()

	return &types.Signature{
		SigningPayload: payload,
		PublicKey:      s.KeyPair.PublicKey,
		SignatureType:  payload.SignatureType,
		Bytes:          sigBytes,
	}, nil
}

// Verify verifies a Signature, by checking the validity of a Signature,
// the SigningPayload, and the PublicKey of the Signature.
func (s *SignerBls12381) Verify(signature *types.Signature) error {
	if signature.SignatureType != types.BlsG2Element {
		return fmt.Errorf(
			"%w: expected %v but got %v",
			ErrVerifyUnsupportedPayloadSignatureType,
			types.BlsG2Element,
			signature.SignatureType,
		)
	}

	pubKeyBytes := signature.PublicKey.Bytes
	pubKey := &bls_sig.PublicKey{}
	_ = pubKey.UnmarshalBinary(pubKeyBytes)

	sigBytes := signature.Bytes
	sig := &bls_sig.Signature{}
	_ = sig.UnmarshalBinary(sigBytes)

	err := asserter.Signatures([]*types.Signature{signature})
	if err != nil {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, err)
	}

	bls := bls_sig.NewSigBasic()
	result, err := bls.Verify(pubKey, signature.SigningPayload.Bytes, sig)

	if err != nil {
		return err
	}

	if !result {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, "Verify failed")
	}

	return nil
}
