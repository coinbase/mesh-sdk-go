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
	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

func signECDSA(privKeyBytes, payload []byte, signatureType types.SignatureType) ([]byte, error) {
	var sig []byte
	var err error

	switch signatureType {
	case types.EcdsaRecovery:
		sig, err = secp256k1.Sign(payload, privKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("sign: unable to sign. %w", err)
		}
	case types.Ecdsa:
		sig, err = secp256k1.Sign(payload, privKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("sign: unable to sign. %w", err)
		}
		sig = sig[:64]
	}
	return sig, nil
}

func signEd25519(privKeyBytes, payload []byte) []byte {
	privKey := ed25519.NewKeyFromSeed(privKeyBytes)
	sig := ed25519.Sign(privKey, payload)

	return sig
}

func SignPayload(payload *types.SigningPayload, keypair *KeyPair) (*types.Signature, error) {
	privKeyBytes, err := hex.DecodeString(keypair.PrivateKey.HexBytes)
	if err != nil {
		return nil, fmt.Errorf("sign: unable to decode private key. %w", err)
	}

	decodedMessage, err := hex.DecodeString(payload.HexBytes)
	if err != nil {
		return nil, fmt.Errorf("sign: unable to decode message. %w", err)
	}

	curve := keypair.PrivateKey.CurveType
	switch curve {
	case types.Secp256k1:
		sig, err := signECDSA(privKeyBytes, decodedMessage, payload.SignatureType)
		if err != nil {
			return nil, err
		}

		return &types.Signature{
			SigningPayload: payload,
			PublicKey:      keypair.PublicKey,
			SignatureType:  payload.SignatureType,
			HexBytes:       hex.EncodeToString(sig),
		}, nil
	case types.Edwards25519:
		sig := signEd25519(privKeyBytes, decodedMessage)

		return &types.Signature{
			SigningPayload: payload,
			PublicKey:      keypair.PublicKey,
			SignatureType:  payload.SignatureType,
			HexBytes:       hex.EncodeToString(sig),
		}, nil
	default:
		return nil, fmt.Errorf("%s is not supported", curve)
	}
}

func verifyEd25519(pubKey, decodedMessage, decodedSignature []byte) bool {
	return ed25519.Verify(pubKey, decodedMessage, decodedSignature)
}

func verifyECDSA(pubKey, decodedMessage, decodedSignature []byte) bool {
	return secp256k1.VerifySignature(pubKey, decodedMessage, decodedSignature)
}

func verifyECDSARecovery(pubKey, decodedMessage, decodedSignature []byte) bool {
	normalizedSig := decodedSignature[:64]
	return secp256k1.VerifySignature(pubKey, decodedMessage, normalizedSig)
}

func Verify(signature *types.Signature) (bool, error) {
	curve := signature.PublicKey.CurveType
	pubKey, err := hex.DecodeString(signature.PublicKey.HexBytes)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode pubkey. %w", err)
	}
	decodedMessage, err := hex.DecodeString(signature.SigningPayload.HexBytes)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode message. %w", err)
	}
	decodedSignature, err := hex.DecodeString(signature.HexBytes)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode signature. %w", err)
	}

	err = asserter.Signatures([]*types.Signature{signature})
	if err != nil {
		return false, err
	}

	switch signature.SignatureType {
	case types.Ecdsa:
		verify := verifyECDSA(pubKey, decodedMessage, decodedSignature)
		return verify, nil
	case types.EcdsaRecovery:
		verify := verifyECDSARecovery(pubKey, decodedMessage, decodedSignature)
		return verify, nil
	case types.Ed25519:
		verify := verifyEd25519(pubKey, decodedMessage, decodedSignature)
		return verify, nil
	default:
		return false, fmt.Errorf("%s is not supported", curve)
	}
}
