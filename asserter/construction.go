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

package asserter

import (
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// ConstructionMetadataResponse returns an error if
// the metadata is not a JSON object.
func ConstructionMetadataResponse(
	response *types.ConstructionMetadataResponse,
) error {
	if response == nil {
		return errors.New("construction metadata response cannot be nil")
	}

	if response.Metadata == nil {
		return errors.New("Metadata is nil")
	}

	return nil
}

// ConstructionSubmitResponse returns an error if
// the types.TransactionIdentifier in the response is not
// valid or if the Submission.Status is not contained
// within the provided validStatuses slice.
func ConstructionSubmitResponse(
	response *types.ConstructionSubmitResponse,
) error {
	if response == nil {
		return errors.New("construction submit response cannot be nil")
	}

	if err := TransactionIdentifier(response.TransactionIdentifier); err != nil {
		return err
	}

	return nil
}

// ConstructionCombineResponse returns an error if
// a *types.ConstructionCombineResponse does
// not have a populated SignedTransaction.
func ConstructionCombineResponse(
	response *types.ConstructionCombineResponse,
) error {
	if response == nil {
		return errors.New("construction combine response cannot be nil")
	}

	if len(response.SignedTransaction) == 0 {
		return errors.New("signed transaction cannot be empty")
	}

	return nil
}

// ConstructionDeriveResponse returns an error if
// a *types.ConstructionDeriveResponse does
// not have a populated Address.
func ConstructionDeriveResponse(
	response *types.ConstructionDeriveResponse,
) error {
	if response == nil {
		return errors.New("construction derive response cannot be nil")
	}

	if len(response.Address) == 0 {
		return errors.New("address cannot be empty")
	}

	return nil
}

// ConstructionHashResponse returns an error if
// a *types.ConstructionHashResponse does
// not have a populated transaction hash.
func ConstructionHashResponse(
	response *types.ConstructionHashResponse,
) error {
	if response == nil {
		return errors.New("construction hash response cannot be nil")
	}

	if len(response.TransactionHash) == 0 {
		return errors.New("transaction hash cannot be empty")
	}

	return nil
}

// ConstructionParseResponse returns an error if
// a *types.ConstructionParseResponse does
// not have a valid set of operations or
// if the signers is empty.
func (a *Asserter) ConstructionParseResponse(
	response *types.ConstructionParseResponse,
	signed bool,
) error {
	if a == nil {
		return ErrAsserterNotInitialized
	}

	if response == nil {
		return errors.New("construction parse response cannot be nil")
	}

	if len(response.Operations) == 0 {
		return errors.New("operations cannot be empty")
	}

	if err := a.Operations(response.Operations, true); err != nil {
		return fmt.Errorf("%w unable to parse operations", err)
	}

	if signed && len(response.Signers) == 0 {
		return errors.New("signers cannot be empty")
	}

	if !signed && len(response.Signers) > 0 {
		return errors.New("signers should be empty for unsigned txs")
	}

	for i, signer := range response.Signers {
		if len(signer) == 0 {
			return fmt.Errorf("signer %d cannot be empty string", i)
		}
	}

	return nil
}

// ConstructionPayloadsResponse returns an error if
// a *types.ConstructionPayloadsResponse does
// not have an UnsignedTransaction or has no
// valid *SigningPaylod.
func ConstructionPayloadsResponse(
	response *types.ConstructionPayloadsResponse,
) error {
	if response == nil {
		return errors.New("construction payloads response cannot be nil")
	}

	if len(response.UnsignedTransaction) == 0 {
		return errors.New("unsigned transaction cannot be empty")
	}

	if len(response.Payloads) == 0 {
		return errors.New("signing payloads cannot be empty")
	}

	for i, payload := range response.Payloads {
		if err := SigningPayload(payload); err != nil {
			return fmt.Errorf("%w: signing payload %d is invalid", err, i)
		}
	}

	return nil
}

// PublicKey returns an error if
// the *types.PublicKey is nil, is not
// valid hex, or has an undefined CurveType.
func PublicKey(
	publicKey *types.PublicKey,
) error {
	if publicKey == nil {
		return errors.New("PublicKey cannot be nil")
	}

	if len(publicKey.Bytes) == 0 {
		return errors.New("public key bytes cannot be empty")
	}

	if err := CurveType(publicKey.CurveType); err != nil {
		return fmt.Errorf("%w public key curve type is not supported", err)
	}

	return nil
}

// CurveType returns an error if
// the curve is not a valid types.CurveType.
func CurveType(
	curve types.CurveType,
) error {
	switch curve {
	case types.Secp256k1, types.Edwards25519:
		return nil
	default:
		return fmt.Errorf("%s is not a supported CurveType", curve)
	}
}

// SigningPayload returns an error
// if a *types.SigningPayload is nil,
// has an empty address, has invlaid hex,
// or has an invalid SignatureType (if populated).
func SigningPayload(
	signingPayload *types.SigningPayload,
) error {
	if signingPayload == nil {
		return errors.New("signing payload cannot be nil")
	}

	if len(signingPayload.Address) == 0 {
		return errors.New("signing payload address cannot be empty")
	}

	if len(signingPayload.Bytes) == 0 {
		return errors.New("signing payload bytes cannot be empty")
	}

	// SignatureType can be optionally populated
	if len(signingPayload.SignatureType) == 0 {
		return nil
	}

	if err := SignatureType(signingPayload.SignatureType); err != nil {
		return fmt.Errorf("%w signature payload signature type is not valid", err)
	}

	return nil
}

// Signatures returns an error if any
// *types.Signature is invalid.
func Signatures(
	signatures []*types.Signature,
) error {
	if len(signatures) == 0 {
		return errors.New("signatures cannot be empty")
	}

	for i, signature := range signatures {
		if err := SigningPayload(signature.SigningPayload); err != nil {
			return fmt.Errorf("%w: signature %d has invalid signing payload", err, i)
		}

		if err := PublicKey(signature.PublicKey); err != nil {
			return fmt.Errorf("%w: signature %d has invalid public key", err, i)
		}

		if err := SignatureType(signature.SignatureType); err != nil {
			return fmt.Errorf("%w: signature %d has invalid signature type", err, i)
		}

		// Return an error if the requested signature type does not match the
		// signture type in the returned signature.
		if len(signature.SigningPayload.SignatureType) > 0 &&
			signature.SigningPayload.SignatureType != signature.SignatureType {
			return fmt.Errorf("requested signature type does not match returned signature type")
		}

		if len(signature.Bytes) == 0 {
			return fmt.Errorf("signature %d bytes cannot be empty", i)
		}
	}

	return nil
}

// SignatureType returns an error if
// signature is not a valid types.SignatureType.
func SignatureType(
	signature types.SignatureType,
) error {
	switch signature {
	case types.Ecdsa, types.EcdsaRecovery, types.Ed25519:
		return nil
	default:
		return fmt.Errorf("%s is not a supported SignatureType", signature)
	}
}
