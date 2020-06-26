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
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

func signECDSA(privKeyBytes, payload []byte, signatureType SignatureType) ([]byte, error) {
	var sig []byte
	var err error

	if signatureType.IsEcdsaPubkeyRecovery() {
		sig, err = secp256k1.Sign(payload, privKeyBytes)
		if err != nil {
			return nil, fmt.Errorf("sign: unable to sign. %w", err)
		}
	} else {
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

func SignPayload(payload *SigningPayload, keypair *KeyPair) (*Signature, error) {
	privKeyBytes, err := hex.DecodeString(keypair.PrivateKey.PrivKeyHex)
	if err != nil {
		return nil, fmt.Errorf("sign: unable to decode private key. %w", err)
	}

	decodedMessage, err := hex.DecodeString(payload.PayloadHex)
	if err != nil {
		return nil, fmt.Errorf("sign: unable to decode message. %w", err)
	}

	curve := keypair.PrivateKey.Curve
	switch {
	case curve.IsSecp256k1():
		sig, err := signECDSA(privKeyBytes, decodedMessage, payload.SignatureType)
		if err != nil {
			return nil, err
		}

		return &Signature{
			Payload:       payload,
			Signature:     hex.EncodeToString(sig),
			PublicKey:     keypair.PublicKey,
			SignatureType: payload.SignatureType,
		}, nil
	case curve.IsEdwards25519():
		sig := signEd25519(privKeyBytes, decodedMessage)

		return &Signature{
			Payload:       payload,
			Signature:     hex.EncodeToString(sig),
			PublicKey:     keypair.PublicKey,
			SignatureType: payload.SignatureType,
		}, nil
	default:
		return nil, fmt.Errorf("%s is not supported", curve)
	}
}

func verifyEd25519(pubKey, decodedMessage, decodedSignature []byte) bool {
	return ed25519.Verify(pubKey, decodedMessage, decodedSignature)
}

func verifyECDSA(pubKey, decodedMessage, decodedSignature []byte) (bool, error) {
	var normalizedSig []byte
	switch len(decodedSignature) {
	// ECDSA-PubkeyRecovery Signature
	case crypto.SignatureLength:
		normalizedSig = decodedSignature[:64]
	// Regular ECDSA Signature
	case crypto.RecoveryIDOffset:
		normalizedSig = decodedSignature
	default:
		return false, fmt.Errorf("signature length %d is invalid", len(decodedSignature))
	}

	return secp256k1.VerifySignature(pubKey, decodedMessage, normalizedSig), nil
}

func Verify(signature *Signature) (bool, error) {
	curve := signature.PublicKey.Curve
	pubKey, err := hex.DecodeString(signature.PublicKey.PubKeyHex)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode pubkey. %w", err)
	}
	decodedMessage, err := hex.DecodeString(signature.Payload.PayloadHex)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode message. %w", err)
	}
	decodedSignature, err := hex.DecodeString(signature.Signature)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode signature. %w", err)
	}

	switch {
	case curve.IsSecp256k1():
		verify, _ := verifyECDSA(pubKey, decodedMessage, decodedSignature)
		return verify, nil
	case curve.IsEdwards25519():
		verify := verifyEd25519(pubKey, decodedMessage, decodedSignature)
		return verify, nil
	default:
		return false, fmt.Errorf("%s is not supported", curve)
	}
}
