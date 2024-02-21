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
// limitations under the License

package keys

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// SignerSecp256r1 is initialized from a keypair
type SignerSecp256r1 struct {
	KeyPair *KeyPair
}

// The Ecdsa signature is the couple (R, S), both R and S are 32 bytes
const (
	EcdsaRLen   = 32
	EcdsaSLen   = 32
	EcdsaMsgLen = 32
)

// Verify interface compliance at compile time
var _ Signer = (*SignerSecp256r1)(nil)

// PublicKey returns the PublicKey of the signer
func (s *SignerSecp256r1) PublicKey() *types.PublicKey {
	return s.KeyPair.PublicKey
}

// Sign arbitrary payloads using a KeyPair with specific sigType.
// Currently, we only support sigType types.Ecdsa for secp256r1 and the signature format is R || S.
func (s *SignerSecp256r1) Sign(
	payload *types.SigningPayload,
	sigType types.SignatureType,
) (*types.Signature, error) {
	if err := s.KeyPair.IsValid(); err != nil {
		return nil, fmt.Errorf("key pair is invalid: %w", err)
	}

	if !(payload.SignatureType == sigType || payload.SignatureType == "") {
		return nil, fmt.Errorf(
			"signing payload signature type %v is invalid: %w",
			payload.SignatureType,
			ErrSignUnsupportedPayloadSignatureType,
		)
	}

	if sigType != types.Ecdsa {
		return nil, fmt.Errorf(
			"expected signature type %v but got %v: %w",
			types.Ecdsa,
			sigType,
			ErrSignUnsupportedSignatureType,
		)
	}

	crv := elliptic.P256()
	x, y := crv.ScalarBaseMult(s.KeyPair.PrivateKey)

	// IsOnCurve will return false for the point at infinity (0, 0)
	// See:
	// https://github.com/golang/go/blob/3298300ddf45a0792b4d8ea5e05f0fbceec4c9f9/src/crypto/elliptic/elliptic.go#L24
	if !crv.IsOnCurve(x, y) {
		return nil, ErrPubKeyNotOnCurve
	}

	pubKey := ecdsa.PublicKey{X: x, Y: y, Curve: crv}
	privKey := ecdsa.PrivateKey{
		PublicKey: pubKey,
		D:         new(big.Int).SetBytes(s.KeyPair.PrivateKey),
	}

	sigR, sigS, err := ecdsa.Sign(rand.Reader, &privKey, payload.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}
	sig := sigR.Bytes()
	sig = append(sig, sigS.Bytes()...)

	return &types.Signature{
		SigningPayload: payload,
		PublicKey:      s.KeyPair.PublicKey,
		SignatureType:  payload.SignatureType,
		Bytes:          sig,
	}, nil
}

// Verify verifies a Signature, by checking the validity of a Signature,
// the SigningPayload, and the PublicKey of the Signature.
func (s *SignerSecp256r1) Verify(signature *types.Signature) error {
	if signature.SignatureType != types.Ecdsa {
		return fmt.Errorf(
			"expected signing payload signature type %v but got %v: %w",
			types.Ecdsa,
			signature.SignatureType,
			ErrVerifyUnsupportedSignatureType,
		)
	}

	if err := asserter.Signatures([]*types.Signature{signature}); err != nil {
		return fmt.Errorf("signature is invalid: %w", err)
	}

	message := signature.SigningPayload.Bytes

	if len(message) != EcdsaMsgLen {
		return ErrVerifyFailed
	}

	sig := signature.Bytes

	crv := elliptic.P256()
	x, y := elliptic.Unmarshal(elliptic.P256(), signature.PublicKey.Bytes)

	// IsOnCurve will return false for the point at infinity (0, 0)
	// See:
	// https://github.com/golang/go/blob/3298300ddf45a0792b4d8ea5e05f0fbceec4c9f9/src/crypto/elliptic/elliptic.go#L24
	if !crv.IsOnCurve(x, y) {
		return ErrPubKeyNotOnCurve
	}
	publicKey := ecdsa.PublicKey{X: x, Y: y, Curve: elliptic.P256()}

	sigR := new(big.Int).SetBytes(sig[:EcdsaRLen])
	sigS := new(big.Int).SetBytes(sig[EcdsaRLen:])

	verify := ecdsa.Verify(&publicKey, message, sigR, sigS)

	if !verify {
		return ErrVerifyFailed
	}
	return nil
}
