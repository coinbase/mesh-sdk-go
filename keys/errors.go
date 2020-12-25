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

	utils "github.com/coinbase/rosetta-sdk-go/errors"
)

// Named error types for Keys errors
var (
	ErrPrivKeyUndecodable   = errors.New("could not decode privkey")
	ErrPrivKeyLengthInvalid = errors.New("invalid privkey length")
	ErrPrivKeyZero          = errors.New("privkey cannot be 0")
	ErrPubKeyNotOnCurve     = errors.New("pubkey is not on the curve")

	ErrKeyGenSecp256k1Failed = errors.New(
		"keygen: error generating key pair for secp256k1 curve type",
	)
	ErrKeyGenEdwards25519Failed = errors.New(
		"keygen: error generating key pair for edwards25519 curve type",
	)
	ErrKeyGenSecp256r1Failed = errors.New(
		"keygen: error generating key pair for secp256r1 curve type",
	)
	ErrCurveTypeNotSupported = errors.New("not a supported CurveType")

	ErrSignUnsupportedPayloadSignatureType = errors.New(
		"sign: unexpected payload.SignatureType while signing",
	)
	ErrSignUnsupportedSignatureType = errors.New(
		"sign: unexpected Signature type while signing",
	)
	ErrSignFailed = errors.New("sign: unable to sign")

	ErrVerifyUnsupportedPayloadSignatureType = errors.New(
		"verify: unexpected payload.SignatureType while verifying",
	)
	ErrVerifyUnsupportedSignatureType = errors.New(
		"verify: unexpected Signature type while verifying",
	)
	ErrVerifyFailed = errors.New("verify: verify returned false")
)

// Err takes an error as an argument and returns
// whether or not the error is one thrown by the keys package
func Err(err error) bool {
	keyErrors := []error{
		ErrPrivKeyUndecodable,
		ErrPrivKeyLengthInvalid,
		ErrPrivKeyZero,
		ErrPubKeyNotOnCurve,
		ErrKeyGenSecp256k1Failed,
		ErrKeyGenSecp256r1Failed,
		ErrKeyGenEdwards25519Failed,
		ErrCurveTypeNotSupported,
		ErrSignUnsupportedPayloadSignatureType,
		ErrSignUnsupportedSignatureType,
		ErrSignFailed,
		ErrVerifyUnsupportedPayloadSignatureType,
		ErrVerifyUnsupportedSignatureType,
		ErrVerifyFailed,
	}

	return utils.FindError(keyErrors, err)
}
