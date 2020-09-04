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

package parser

import (
	"errors"

	"github.com/coinbase/rosetta-sdk-go/utils"
)

// Named error types for Parser errors
var (
	///////////////////
	/* INTENT ERRORS */
	///////////////////
	ErrExpectedOperationAccountMismatch = errors.New(
		"intended account did not match observed account",
	)
	ErrExpectedOperationAmountMismatch = errors.New(
		"intended amount did not match observed amount",
	)
	ErrExpectedOperationTypeMismatch = errors.New(
		"intended type did not match observed type",
	)
	ErrExpectedOperationsUnexpectedOperation = errors.New("found extra operation")
	ErrExpectedSignerDuplicateSigner         = errors.New("found duplicate signer")
	ErrExpectedSignerUnexpectedSigner        = errors.New("found unexpected signers")

	IntentErrs = []error{
		ErrExpectedOperationAccountMismatch,
		ErrExpectedOperationAmountMismatch,
		ErrExpectedOperationTypeMismatch,
		ErrExpectedOperationsUnexpectedOperation,
		ErrExpectedSignerDuplicateSigner,
		ErrExpectedSignerUnexpectedSigner,
	}

	/////////////////////////////
	/* MATCH OPERATIONS ERRORS */
	/////////////////////////////
	ErrAccountMatchAccountMissing           = errors.New("account is missing")
	ErrAccountMatchSubAccountMissing        = errors.New("SubAccountIdentifier.Address is missing")
	ErrAccountMatchSubAccountPopulated      = errors.New("SubAccount is populated")
	ErrAccountMatchUnexpectedSubAccountAddr = errors.New("unexpected SubAccountIdentifier.Address")

	ErrMetadataMatchKeyNotFound       = errors.New("key is not present in metadata")
	ErrMetadataMatchValueTypeMismatch = errors.New("unexpected value associated with key")

	ErrAmountMatchAmountMissing      = errors.New("amount is missing")
	ErrAmountMatchAmountPopulated    = errors.New("amount is populated")
	ErrAmountMatchUnexpectedSign     = errors.New("unexpected amount sign")
	ErrAmountMatchUnexpectedCurrency = errors.New("unexpected currency")

	ErrCoinActionMatchCoinChangeIsNil      = errors.New("coin change is nil")
	ErrCoinActionMatchUnexpectedCoinAction = errors.New("unexpected coin action")

	ErrEqualAmountsNoOperations = errors.New("cannot check equality of 0 operations")
	ErrEqualAmountsNotEqual     = errors.New("amounts are not equal")

	ErrOppositeAmountsSameSign       = errors.New("operations have the same sign")
	ErrOppositeAmountsAbsValMismatch = errors.New("operation absolute values are not equal")

	ErrEqualAddressesTooFewOperations = errors.New("cannot check equality of <= 1 operations")
	ErrEqualAddressesAccountIsNil     = errors.New("account is nil")
	ErrEqualAddressesAddrMismatch     = errors.New("addresses do not match")

	ErrMatchIndexValidIndexOutOfRange = errors.New("match index out of range")
	ErrMatchIndexValidIndexIsNil      = errors.New("match index is nil")

	ErrMatchOperationsNoOperations          = errors.New("unable to match anything to 0 operations")
	ErrMatchOperationsDescriptionsMissing   = errors.New("no descriptions to match")
	ErrMatchOperationsMatchNotFound         = errors.New("unable to find match for operation")
	ErrMatchOperationsDescriptionNotMatched = errors.New("could not find match for description")

	MatchOpsErrs = []error{
		ErrAccountMatchAccountMissing,
		ErrAccountMatchSubAccountMissing,
		ErrAccountMatchSubAccountPopulated,
		ErrAccountMatchUnexpectedSubAccountAddr,
		ErrMetadataMatchKeyNotFound,
		ErrMetadataMatchValueTypeMismatch,
		ErrAmountMatchAmountMissing,
		ErrAmountMatchAmountPopulated,
		ErrAmountMatchUnexpectedSign,
		ErrAmountMatchUnexpectedCurrency,
		ErrCoinActionMatchCoinChangeIsNil,
		ErrCoinActionMatchUnexpectedCoinAction,
		ErrEqualAmountsNoOperations,
		ErrEqualAmountsNotEqual,
		ErrOppositeAmountsSameSign,
		ErrOppositeAmountsAbsValMismatch,
		ErrEqualAddressesTooFewOperations,
		ErrEqualAddressesAccountIsNil,
		ErrEqualAddressesAddrMismatch,
		ErrMatchIndexValidIndexOutOfRange,
		ErrMatchIndexValidIndexIsNil,
		ErrMatchOperationsNoOperations,
		ErrMatchOperationsDescriptionsMissing,
		ErrMatchOperationsMatchNotFound,
		ErrMatchOperationsDescriptionNotMatched,
	}
)

// ErrParser takes an error as an argument and returns
// whether or not the error is one thrown by the asserter
// along with the specific source of the error
func ErrParser(err error) (bool, string) {
	parserErrs := map[string][]error{
		"intent error":           IntentErrs,
		"match operations error": MatchOpsErrs,
	}

	for key, val := range parserErrs {
		if utils.FindError(val, err) {
			return true, key
		}
	}
	return false, ""
}
