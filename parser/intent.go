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
// limitations under the License.

package parser

import (
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// ExpectedOperation returns an error if an observed operation
// differs from the intended operation. An operation is considered
// to be different from the intent if the AccountIdentifier,
// Amount, or Type has changed.
func ExpectedOperation(intent *types.Operation, observed *types.Operation) error {
	if types.Hash(intent.Account) != types.Hash(observed.Account) {
		return fmt.Errorf(
			"expected operation account identifier %s but got %s: %w",
			types.PrettyPrintStruct(intent.Account),
			types.PrettyPrintStruct(observed.Account),
			ErrExpectedOperationAccountMismatch,
		)
	}

	if types.Hash(intent.Amount) != types.Hash(observed.Amount) {
		return fmt.Errorf(
			"expected operation amount %s but got %s: %w",
			types.PrettyPrintStruct(intent.Amount),
			types.PrettyPrintStruct(observed.Amount),
			ErrExpectedOperationAmountMismatch,
		)
	}

	if intent.Type != observed.Type {
		return fmt.Errorf(
			"expected operation type %s but got %s: %w",
			intent.Type,
			observed.Type,
			ErrExpectedOperationTypeMismatch,
		)
	}

	return nil
}

// ExpectedOperations returns an error if a slice of intended
// operations differ from observed operations. Optionally,
// it is possible to error if any extra observed opertions
// are found or if operations matched are not considered
// successful.
func (p *Parser) ExpectedOperations(
	intent []*types.Operation,
	observed []*types.Operation,
	errExtra bool,
	confirmSuccess bool,
) error {
	matches := make(map[int]struct{})
	failedMatches := []*types.Operation{}

	for _, obs := range observed {
		foundMatch := false
		for i, in := range intent {
			if _, exists := matches[i]; exists {
				continue
			}

			// Any error returned here only indicated that intent
			// does not match observed. For ExpectedOperations,
			// we don't care about the content of the error, we
			// just care if it errors so we can evaluate the next
			// operation for a match.
			if err := ExpectedOperation(in, obs); err != nil {
				continue
			}

			if confirmSuccess {
				obsSuccess, err := p.Asserter.OperationSuccessful(obs)
				if err != nil {
					return fmt.Errorf(
						"failed to check the status of operation %s: %w",
						types.PrintStruct(obs),
						err,
					)
				}

				if !obsSuccess {
					failedMatches = append(failedMatches, obs)
					continue
				}
			}

			matches[i] = struct{}{}
			foundMatch = true
			break
		}

		if !foundMatch && errExtra {
			return fmt.Errorf(
				"operation %s: %w",
				types.PrintStruct(obs),
				ErrExpectedOperationsExtraOperation,
			)
		}
	}

	missingIntent := []int{}
	for i := 0; i < len(intent); i++ {
		if _, exists := matches[i]; !exists {
			missingIntent = append(missingIntent, i)
		}
	}

	if len(missingIntent) > 0 {
		errString := fmt.Sprintf(
			"could not intent match %v",
			missingIntent,
		)

		if len(failedMatches) > 0 {
			errString += fmt.Sprintf(
				"found matching ops with unsuccessful status %s: %s",
				types.PrettyPrintStruct(failedMatches),
				errString,
			)
		}

		return errors.New(errString)
	}

	return nil
}

// ExpectedSigners returns an error if a slice of SigningPayload
// has different signers than what was observed (typically populated
// using the signers returned from parsing a transaction).
func ExpectedSigners(intent []*types.SigningPayload, observed []*types.AccountIdentifier) error {
	// De-duplicate required signers (ex: multiple UTXOs from same address)
	intendedSigners := make(map[string]struct{})
	for _, payload := range intent {
		intendedSigners[types.Hash(payload.AccountIdentifier)] = struct{}{}
	}

	// Could exit here if len(intent) != len(observed) but
	// more useful to print out a detailed error message.
	seenSigners := make(map[string]struct{})
	unmatched := []*types.AccountIdentifier{} // observed
	for _, signer := range observed {
		_, exists := intendedSigners[types.Hash(signer)]
		if !exists {
			unmatched = append(unmatched, signer)
		} else {
			seenSigners[types.Hash(signer)] = struct{}{}
		}
	}

	// Check to see if there are any expected
	// signers that we could not find.
	for _, payload := range intent {
		hash := types.Hash(payload.AccountIdentifier)
		if _, exists := seenSigners[hash]; !exists {
			return fmt.Errorf(
				"payload account identifier %s is invalid: %w",
				types.PrintStruct(payload.AccountIdentifier),
				ErrExpectedSignerMissing,
			)
		}
	}

	// Return an error if any observed signatures
	// were not expected.
	if len(unmatched) != 0 {
		return fmt.Errorf(
			"unmatched signers are %s: %w",
			types.PrintStruct(unmatched),
			ErrExpectedSignerUnexpectedSigner,
		)
	}

	return nil
}
