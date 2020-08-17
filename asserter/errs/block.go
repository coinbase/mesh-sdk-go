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

package errs

import "errors"

var (
	ErrAmountValueMissing = errors.New("Amount.Value is missing")

	ErrAmountIsNotInt = errors.New("Amount.Value is not an integer")

	ErrAmountCurrencyIsNil = errors.New("Amount.Currency is nil")

	ErrAmountCurrencySymbolEmpty = errors.New("Amount.Currency.Symbol is empty")

	ErrAmountCurrencyHasNegDecimals = errors.New("Amount.Currency.Decimals must be >= 0")

	ErrOperationIdentifierIndexIsNil = errors.New("Operation.OperationIdentifier.Index is invalid")

	ErrOperationIdentifierIndexOutOfOrder = errors.New(
		"Operation.OperationIdentifier.Index is out of order",
	)

	ErrOperationIdentifierNetworkIndexInvalid = errors.New(
		"Operation.OperationIdentifier.NetworkIndex is invalid",
	)

	ErrAccountIsNil = errors.New("Account is nil")

	ErrAccountAddrMissing = errors.New("Account.Address is missing")

	ErrAccountSubAccountAddrMissing = errors.New("Account.SubAccount.Address is missing")

	ErrOperationStatusMissing = errors.New("Operation.Status is missing")

	ErrOperationStatusInvalid = errors.New("Operation.Status is invalid")

	ErrOperationTypeInvalid = errors.New("Operation.Type is invalid")

	ErrOperationIsNil = errors.New("Operation is nil")

	ErrOperationStatusNotEmptyForConstruction = errors.New(
		"Operation.Status must be empty for construction",
	)

	ErrRelatedOperationIndexOutOfOrder = errors.New(
		"related operation has index greater than operation",
	)

	ErrRelatedOperationIndexDuplicate = errors.New("found duplicate related operation index")

	ErrBlockIdentifierIsNil = errors.New("BlockIdentifier is nil")

	ErrBlockIdentifierHashMissing = errors.New("BlockIdentifier.Hash is missing")

	ErrBlockIdentifierIndexIsNeg = errors.New("BlockIdentifier.Index is negative")

	ErrPartialBlockIdentifierIsNil = errors.New("PartialBlockIdentifier is nil")

	ErrPartialBlockIdentifierFieldsNotSet = errors.New(
		"neither PartialBlockIdentifier.Hash nor PartialBlockIdentifier.Index is set",
	)

	ErrTxIdentifierIsNil = errors.New("TransactionIdentifier is nil")

	ErrTxIdentifierHashMissing = errors.New("TransactionIdentifier.Hash is missing")

	ErrNoOperationsForConstruction = errors.New("operations cannot be empty for construction")

	ErrTxIsNil = errors.New("Transaction is nil")

	ErrTimestampBeforeMin = errors.New("timestamp is before 01/01/2000")

	ErrTimestampAfterMax = errors.New("timestamp is after 01/01/2040")

	ErrBlockIsNil = errors.New("Block is nil")

	ErrBlockHashEqualsParentBlockHash = errors.New(
		"BlockIdentifier.Hash == ParentBlockIdentifier.Hash",
	)

	ErrBlockIndexPrecedesParentBlockIndex = errors.New(
		"BlockIdentifier.Index <= ParentBlockIdentifier.Index",
	)
)
