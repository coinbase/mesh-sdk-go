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
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/asserter/errs"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// MinUnixEpoch is the unix epoch time in milliseconds of
	// 01/01/2000 at 12:00:00 AM.
	MinUnixEpoch = 946713600000

	// MaxUnixEpoch is the unix epoch time in milliseconds of
	// 01/01/2040 at 12:00:00 AM.
	MaxUnixEpoch = 2209017600000
)

// Amount ensures a types.Amount has an
// integer value, specified precision, and symbol.
func Amount(amount *types.Amount) error {
	if amount == nil || amount.Value == "" {
		return errs.ErrAmountValueMissing
	}

	_, ok := new(big.Int).SetString(amount.Value, 10)
	if !ok {
		return fmt.Errorf("%w: %s", errs.ErrAmountIsNotInt, amount.Value)
	}

	if amount.Currency == nil {
		return errs.ErrAmountCurrencyIsNil
	}

	if amount.Currency.Symbol == "" {
		return errs.ErrAmountCurrencySymbolEmpty
	}

	if amount.Currency.Decimals < 0 {
		return errs.ErrAmountCurrencyHasNegDecimals
	}

	return nil
}

// OperationIdentifier returns an error if index of the
// types.Operation is out-of-order or if the NetworkIndex is
// invalid.
func OperationIdentifier(
	identifier *types.OperationIdentifier,
	index int64,
) error {
	if identifier == nil {
		return errs.ErrOperationIdentifierIndexIsNil
	}

	if identifier.Index != index {
		return fmt.Errorf(
			"%w: expected %d but got %d",
			errs.ErrOperationIdentifierIndexOutOfOrder,
			index,
			identifier.Index,
		)
	}

	if identifier.NetworkIndex != nil && *identifier.NetworkIndex < 0 {
		return errs.ErrOperationIdentifierNetworkIndexInvalid
	}

	return nil
}

// AccountIdentifier returns an error if a types.AccountIdentifier
// is missing an address or a provided SubAccount is missing an identifier.
func AccountIdentifier(account *types.AccountIdentifier) error {
	if account == nil {
		return errs.ErrAccountIsNil
	}

	if account.Address == "" {
		return errs.ErrAccountAddrMissing
	}

	if account.SubAccount == nil {
		return nil
	}

	if account.SubAccount.Address == "" {
		return errs.ErrAccountSubAccountAddrMissing
	}

	return nil
}

// containsString checks if an string is contained in a slice
// of strings.
func containsString(valid []string, value string) bool {
	for _, v := range valid {
		if v == value {
			return true
		}
	}

	return false
}

// containsInt64 checks if an int64 is contained in a slice
// of Int64.
func containsInt64(valid []int64, value int64) bool {
	for _, v := range valid {
		if v == value {
			return true
		}
	}

	return false
}

// OperationStatus returns an error if an operation.Status
// is not valid.
func (a *Asserter) OperationStatus(status string) error {
	if a == nil {
		return ErrAsserterNotInitialized
	}

	if status == "" {
		return errs.ErrOperationStatusMissing
	}

	if _, ok := a.operationStatusMap[status]; !ok {
		return fmt.Errorf("%w: %s", errs.ErrOperationStatusInvalid, status)
	}

	return nil
}

// OperationType returns an error if an operation.Type
// is not valid.
func (a *Asserter) OperationType(t string) error {
	if a == nil {
		return ErrAsserterNotInitialized
	}

	if t == "" || !containsString(a.operationTypes, t) {
		return fmt.Errorf("%w: %s", errs.ErrOperationTypeInvalid, t)
	}

	return nil
}

// Operation ensures a types.Operation has a valid
// type, status, and amount.
func (a *Asserter) Operation(
	operation *types.Operation,
	index int64,
	construction bool,
) error {
	if a == nil {
		return ErrAsserterNotInitialized
	}

	if operation == nil {
		return errs.ErrOperationIsNil
	}

	if err := OperationIdentifier(operation.OperationIdentifier, index); err != nil {
		return fmt.Errorf("%w: Operation identifier is invalid in operation %d", err, index)
	}

	if err := a.OperationType(operation.Type); err != nil {
		return fmt.Errorf("%w: operation type is invalid in operation %d", err, index)
	}

	if construction && len(operation.Status) != 0 {
		return errs.ErrOperationStatusNotEmptyForConstruction
	}

	if err := a.OperationStatus(operation.Status); err != nil && !construction {
		return fmt.Errorf("%w: operation status is invalid in operation %d", err, index)
	}

	if operation.Amount == nil {
		return nil
	}

	if err := AccountIdentifier(operation.Account); err != nil {
		return fmt.Errorf("%w: account identifier is invalid in operation %d", err, index)
	}

	if err := Amount(operation.Amount); err != nil {
		return fmt.Errorf("%w: amount is invalid in operation %d", err, index)
	}

	if operation.CoinChange == nil {
		return nil
	}

	if err := CoinChange(operation.CoinChange); err != nil {
		return fmt.Errorf("%w: coin change is invalid in operation %d", err, index)
	}

	return nil
}

// BlockIdentifier ensures a types.BlockIdentifier
// is well-formatted.
func BlockIdentifier(blockIdentifier *types.BlockIdentifier) error {
	if blockIdentifier == nil {
		return errs.ErrBlockIdentifierIsNil
	}

	if blockIdentifier.Hash == "" {
		return errs.ErrBlockIdentifierHashMissing
	}

	if blockIdentifier.Index < 0 {
		return errs.ErrBlockIdentifierIndexIsNeg
	}

	return nil
}

// PartialBlockIdentifier ensures a types.PartialBlockIdentifier
// is well-formatted.
func PartialBlockIdentifier(blockIdentifier *types.PartialBlockIdentifier) error {
	if blockIdentifier == nil {
		return errs.ErrPartialBlockIdentifierIsNil
	}

	if blockIdentifier.Hash != nil && *blockIdentifier.Hash != "" {
		return nil
	}

	if blockIdentifier.Index != nil && *blockIdentifier.Index >= 0 {
		return nil
	}

	return errs.ErrPartialBlockIdentifierFieldsNotSet
}

// TransactionIdentifier returns an error if a
// types.TransactionIdentifier has an invalid hash.
func TransactionIdentifier(
	transactionIdentifier *types.TransactionIdentifier,
) error {
	if transactionIdentifier == nil {
		return errs.ErrTxIdentifierIsNil
	}

	if transactionIdentifier.Hash == "" {
		return errs.ErrTxIdentifierHashMissing
	}

	return nil
}

// Operations returns an error if any *types.Operation
// in a []*types.Operation is invalid.
func (a *Asserter) Operations(
	operations []*types.Operation,
	construction bool,
) error {
	if len(operations) == 0 && construction {
		return errs.ErrNoOperationsForConstruction
	}

	for i, op := range operations {
		// Ensure operations are sorted
		if err := a.Operation(op, int64(i), construction); err != nil {
			return err
		}

		// Ensure an operation's related_operations are only
		// operations with an index less than the operation
		// and that there are no duplicates.
		relatedIndexes := []int64{}
		for _, relatedOp := range op.RelatedOperations {
			if relatedOp.Index >= op.OperationIdentifier.Index {
				return fmt.Errorf(
					"%w: related operation index %d >= operation index %d",
					errs.ErrRelatedOperationIndexOutOfOrder,
					relatedOp.Index,
					op.OperationIdentifier.Index,
				)
			}

			if containsInt64(relatedIndexes, relatedOp.Index) {
				return fmt.Errorf(
					"%w: related operation index %d found for operation index %d",
					errs.ErrRelatedOperationIndexDuplicate,
					relatedOp.Index,
					op.OperationIdentifier.Index,
				)
			}
			relatedIndexes = append(relatedIndexes, relatedOp.Index)
		}
	}

	return nil
}

// Transaction returns an error if the types.TransactionIdentifier
// is invalid, if any types.Operation within the types.Transaction
// is invalid, or if any operation index is reused within a transaction.
func (a *Asserter) Transaction(
	transaction *types.Transaction,
) error {
	if a == nil {
		return ErrAsserterNotInitialized
	}

	if transaction == nil {
		return errs.ErrTxIsNil
	}

	if err := TransactionIdentifier(transaction.TransactionIdentifier); err != nil {
		return err
	}

	if err := a.Operations(transaction.Operations, false); err != nil {
		return fmt.Errorf(
			"%w invalid operation in transaction %s",
			err,
			transaction.TransactionIdentifier.Hash,
		)
	}

	return nil
}

// Timestamp returns an error if the timestamp
// on a block is less than or equal to 0.
func Timestamp(timestamp int64) error {
	switch {
	case timestamp < MinUnixEpoch:
		return fmt.Errorf("%w: %d", errs.ErrTimestampBeforeMin, timestamp)
	case timestamp > MaxUnixEpoch:
		return fmt.Errorf("%w: %d", errs.ErrTimestampAfterMax, timestamp)
	default:
		return nil
	}
}

// Block runs a basic set of assertions for each returned block.
func (a *Asserter) Block(
	block *types.Block,
) error {
	if a == nil {
		return ErrAsserterNotInitialized
	}

	if block == nil {
		return errs.ErrBlockIsNil
	}

	if err := BlockIdentifier(block.BlockIdentifier); err != nil {
		return err
	}

	if err := BlockIdentifier(block.ParentBlockIdentifier); err != nil {
		return err
	}

	// Only apply some assertions if the block index is not the
	// genesis index.
	if a.genesisBlock.Index != block.BlockIdentifier.Index {
		if block.BlockIdentifier.Hash == block.ParentBlockIdentifier.Hash {
			return errs.ErrBlockHashEqualsParentBlockHash
		}

		if block.BlockIdentifier.Index <= block.ParentBlockIdentifier.Index {
			return errs.ErrBlockIndexPrecedesParentBlockIndex
		}

		if err := Timestamp(block.Timestamp); err != nil {
			return err
		}
	}

	for _, transaction := range block.Transactions {
		if err := a.Transaction(transaction); err != nil {
			return err
		}
	}

	return nil
}
