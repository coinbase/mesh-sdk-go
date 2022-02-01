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
	"errors"
	"fmt"

	"github.com/coinbase/kryptology/pkg/signatures/schnorr/mina"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var ErrPallasTransactionValidationErr = errors.New("transaction with pallas validation failed")

type SignerPallas struct {
	KeyPair *KeyPair
}

func (s *SignerPallas) PublicKey() *types.PublicKey {
	return s.KeyPair.PublicKey
}

// Sign transaction payloads using a KeyPair
func (s *SignerPallas) Sign(
	payload *types.SigningPayload,
	sigType types.SignatureType,
) (*types.Signature, error) {
	err := s.KeyPair.IsValid()
	if err != nil {
		return nil, err
	}

	if !(payload.SignatureType == types.SchnorrPoseidon || payload.SignatureType == "") {
		return nil, fmt.Errorf(
			"%w: expected %v but got %v",
			ErrSignUnsupportedPayloadSignatureType,
			types.SchnorrPoseidon,
			payload.SignatureType,
		)
	}

	if sigType != types.SchnorrPoseidon {
		return nil, fmt.Errorf(
			"%w: expected %v but got %v",
			ErrSignUnsupportedSignatureType,
			types.SchnorrPoseidon,
			sigType,
		)
	}

	// Generate private key bytes
	privKeyBytes := s.KeyPair.PrivateKey
	privKey := &mina.SecretKey{}
	_ = privKey.UnmarshalBinary(privKeyBytes)

	// Convert payload to transaction
	tx := &mina.Transaction{}
	_ = tx.UnmarshalBinary(payload.Bytes)

	sig, err := privKey.SignTransaction(tx)
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
func (s *SignerPallas) Verify(signature *types.Signature) error {
	if signature.SignatureType != types.SchnorrPoseidon {
		return fmt.Errorf(
			"%w: expected %v but got %v",
			ErrVerifyUnsupportedPayloadSignatureType,
			types.SchnorrPoseidon,
			signature.SignatureType,
		)
	}

	pubKeyBytes := signature.PublicKey.Bytes
	pubKey := &mina.PublicKey{}
	_ = pubKey.UnmarshalBinary(pubKeyBytes)

	sigBytes := signature.Bytes
	sig := &mina.Signature{}
	_ = sig.UnmarshalBinary(sigBytes)

	err := asserter.Signatures([]*types.Signature{signature})
	if err != nil {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, err)
	}

	txnBytes := signature.SigningPayload.Bytes
	txn := &mina.Transaction{}
	err = txn.UnmarshalBinary(txnBytes)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, err)
	}

	verifyErr := pubKey.VerifyTransaction(sig, txn)
	if verifyErr != nil {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, verifyErr)
	}

	return nil
}
