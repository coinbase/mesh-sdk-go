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

	if !(payload.SignatureType == types.Bls12381BasicMpl || payload.SignatureType == types.Bls12381AugMpl || payload.SignatureType == "") {
		return nil, fmt.Errorf(
			"%w: expected %v or %v but got %v",
			ErrSignUnsupportedPayloadSignatureType,
			types.Bls12381BasicMpl,
			types.Bls12381AugMpl,
			payload.SignatureType,
		)
	}

	// Generate private key bytes
	privKeyBytes := s.KeyPair.PrivateKey
	privKey := &bls_sig.SecretKey{}
	_ = privKey.UnmarshalBinary(privKeyBytes)

	switch sigType {
	case types.Bls12381BasicMpl:
		sigBytes, _ := signBasic(privKey, payload)

		return &types.Signature{
			SigningPayload: payload,
			PublicKey:      s.KeyPair.PublicKey,
			SignatureType:  payload.SignatureType,
			Bytes:          sigBytes,
		}, nil
	case types.Bls12381AugMpl:
		sigBytes, _ := signAug(privKey, payload)

		return &types.Signature{
			SigningPayload: payload,
			PublicKey:      s.KeyPair.PublicKey,
			SignatureType:  payload.SignatureType,
			Bytes:          sigBytes,
		}, nil
	default:
		return nil, fmt.Errorf(
			"%w: expected %v or %v but got %v",
			ErrSignUnsupportedSignatureType,
			types.Bls12381BasicMpl,
			types.Bls12381AugMpl,
			sigType,
		)
	}
}

func signBasic(sk *bls_sig.SecretKey, payload *types.SigningPayload) ([]byte, error) {
	bls := bls_sig.NewSigBasic()

	sig, err := bls.Sign(sk, payload.Bytes)

	if err != nil {
		return nil, err
	}
	sigBytes, _ := sig.MarshalBinary()

	return sigBytes, nil
}

func signAug(sk *bls_sig.SecretKey, payload *types.SigningPayload) ([]byte, error) {
	// Domain separation tag for aug scheme
	// according to section 4.2.2 in
	// https://tools.ietf.org/html/draft-irtf-cfrg-bls-signature-03
	bls := bls_sig.NewSigBasicWithDst("BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_AUG_")

	sig, err := bls.Sign(sk, payload.Bytes)

	if err != nil {
		return nil, err
	}
	sigBytes, _ := sig.MarshalBinary()

	return sigBytes, nil
}

// Verify verifies a Signature, by checking the validity of a Signature,
// the SigningPayload, and the PublicKey of the Signature.
func (s *SignerBls12381) Verify(signature *types.Signature) error {
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

	switch signature.SignatureType {
	case types.Bls12381BasicMpl:
		return verifyBasic(pubKey, signature.SigningPayload.Bytes, sig)
	case types.Bls12381AugMpl:
		return verifyAug(pubKey, signature.SigningPayload.Bytes, sig)
	default:
		return fmt.Errorf(
			"%w: expected %v or %v but got %v",
			ErrVerifyUnsupportedPayloadSignatureType,
			types.Bls12381BasicMpl,
			types.Bls12381AugMpl,
			signature.SignatureType,
		)
	}
}

func verifyBasic(pubKey *bls_sig.PublicKey, payload []byte, signature *bls_sig.Signature) error {
	bls := bls_sig.NewSigBasic()
	result, err := bls.Verify(pubKey, payload, signature)

	if err != nil {
		return err
	}

	if !result {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, "Verify failed")
	}

	return nil
}

func verifyAug(pubKey *bls_sig.PublicKey, payload []byte, signature *bls_sig.Signature) error {
	bls := bls_sig.NewSigAug()
	result, err := bls.Verify(pubKey, payload, signature)

	if err != nil {
		return err
	}

	if !result {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, "Verify failed")
	}

	return nil
}
