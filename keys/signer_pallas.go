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
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

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

	tx, err := ParseSigningPayload(payload)
	if err != nil {
		return nil, err
	}

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

	transaction, err := ParseSigningPayload(signature.SigningPayload)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, err)
	}

	verifyErr := pubKey.VerifyTransaction(sig, transaction)
	if verifyErr != nil {
		return fmt.Errorf("%w: %s", ErrVerifyFailed, verifyErr)
	}

	return nil
}

type PayloadFields struct {
	To         string  `json:"to"`
	From       string  `json:"from"`
	Fee        string  `json:"fee"`
	Amount     *string `json:"amount,omitempty"`
	Nonce      string  `json:"nonce"`
	ValidUntil *string `json:"valid_until,omitempty"`
	Memo       *string `json:"memo,omitempty"`
}

type SigningPayload struct {
	Payment *PayloadFields `json:"payment"`
}

func ParseSigningPayload(rawPayload *types.SigningPayload) (*mina.Transaction, error) {
	var signingPayload SigningPayload
	var payloadFields PayloadFields

	err := json.Unmarshal(rawPayload.Bytes, &signingPayload)
	if err != nil {
		return nil, err
	}

	if signingPayload.Payment != nil {
		payloadFields = *signingPayload.Payment
	} else {
		return nil, errors.New(
			"payment not found in signingPayload",
		)
	}

	transaction, err := constructTransaction(&payloadFields)
	if err != nil {
		return nil, err
	}
	return transaction, nil
}

func constructTransaction(p *PayloadFields) (*mina.Transaction, error) {
	var fromPublicKey mina.PublicKey
	if err := fromPublicKey.ParseAddress(p.From); err != nil {
		return nil, err
	}

	var toPublicKey mina.PublicKey
	if err := toPublicKey.ParseAddress(p.To); err != nil {
		return nil, err
	}

	fee, err := strconv.ParseUint(p.Fee, 10, 64)
	if err != nil {
		return nil, err
	}

	// amount is a field that only exists in a Payment transaction
	amount := uint64(0)
	if p.Amount != nil {
		amount, err = strconv.ParseUint(*p.Amount, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	nonce, err := strconv.ParseUint(p.Nonce, 10, 32)
	if err != nil {
		return nil, err
	}

	validUntil := uint64(0)
	if p.ValidUntil != nil {
		validUntil, err = strconv.ParseUint(*p.ValidUntil, 10, 32)
		if err != nil {
			return nil, err
		}
	}

	memo := ""
	if p.Memo != nil {
		memo = *p.Memo
	}

	txn := &mina.Transaction{
		Fee:        fee,
		FeeToken:   1,
		FeePayerPk: &fromPublicKey,
		Nonce:      uint32(nonce),
		ValidUntil: uint32(validUntil),
		Memo:       memo,
		// Tag is a forwards compatible API, at the moment it is set to an array of 3 false's
		Tag:        [3]bool{false, false, false},
		SourcePk:   &fromPublicKey,
		ReceiverPk: &toPublicKey,
		TokenId:    1,
		Amount:     amount,
		Locked:     false,
		NetworkId:  mina.TestNet,
	}

	return txn, nil
}
