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
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/models"
)

// Amount ensures a models.Amount has an
// integer value, specified precision, and symbol.
func Amount(amount *models.Amount) error {
	if amount == nil || amount.Value == "" {
		return errors.New("Amount.Value is missing")
	}

	_, ok := new(big.Int).SetString(amount.Value, 10)
	if !ok {
		return fmt.Errorf("Amount.Value not an integer %s", amount.Value)
	}

	if amount.Currency == nil {
		return errors.New("Amount.Currency is nil")
	}

	if amount.Currency.Symbol == "" {
		return errors.New("Amount.Currency.Symbol is empty")
	}

	if amount.Currency.Decimals <= 0 {
		return errors.New("Amount.Currency.Decimals must be > 0")
	}

	return nil
}

// contains checks if a string is contained in a slice
// of strings.
func contains(valid []string, value string) bool {
	for _, v := range valid {
		if v == value {
			return true
		}
	}

	return false
}

// OperationIdentifier returns an error if index of the
// models.Operation is out-of-order or if the NetworkIndex is
// invalid.
func OperationIdentifier(
	identifier *models.OperationIdentifier,
	index int64,
) error {
	if identifier == nil || identifier.Index != index {
		return errors.New("Operation.OperationIdentifier.Index invalid")
	}

	if identifier.NetworkIndex != nil && *identifier.NetworkIndex < 0 {
		return errors.New("Operation.OperationIdentifier.NetworkIndex invalid")
	}

	return nil
}

// AccountIdentifier returns an error if a models.AccountIdentifier
// is missing an address or a provided SubAccount is missing an identifier.
func AccountIdentifier(account *models.AccountIdentifier) error {
	if account == nil {
		return errors.New("Account is nil")
	}

	if account.Address == "" {
		return errors.New("Account.Address is missing")
	}

	if account.SubAccount == nil {
		return nil
	}

	if account.SubAccount.Address == "" {
		return errors.New("Account.SubAccount.Address is missing")
	}

	return nil
}

// OperationSuccessful returns a boolean indicating if a models.Operation is
// successful and should be applied in a transaction. This should only be called
// AFTER an operation has been validated.
func (a *Asserter) OperationSuccessful(operation *models.Operation) (bool, error) {
	val, ok := a.operationStatusMap[operation.Status]
	if !ok {
		return false, fmt.Errorf("%s not found", operation.Status)
	}

	return val, nil
}

// Operation ensures a models.Operation has a valid
// type, status, and amount.
func (a *Asserter) Operation(
	operation *models.Operation,
	index int64,
) error {
	if operation == nil {
		return errors.New("Operation is nil")
	}

	if err := OperationIdentifier(operation.OperationIdentifier, index); err != nil {
		return err
	}

	if operation.Type == "" || !contains(a.operationTypes, operation.Type) {
		return fmt.Errorf("Operation.Type %s is invalid", operation.Type)
	}

	if operation.Status == "" || !contains(a.operationStatuses(), operation.Status) {
		return fmt.Errorf("Operation.Status %s is invalid", operation.Status)
	}

	if operation.Amount == nil {
		return nil
	}

	if err := AccountIdentifier(operation.Account); err != nil {
		return err
	}

	return Amount(operation.Amount)
}

// BlockIdentifier ensures a models.BlockIdentifier
// is well-formatted.
func BlockIdentifier(blockIdentifier *models.BlockIdentifier) error {
	if blockIdentifier == nil || blockIdentifier.Hash == "" {
		return errors.New("BlockIdentifier.Hash is missing")
	}

	if blockIdentifier.Index < 0 {
		return errors.New("BlockIdentifier.Index is negative")
	}

	return nil
}

// PartialBlockIdentifier ensures a models.PartialBlockIdentifier
// is well-formatted.
func PartialBlockIdentifier(blockIdentifier *models.PartialBlockIdentifier) error {
	if blockIdentifier == nil {
		return errors.New("PartialBlockIdentifier is nil")
	}

	if blockIdentifier.Hash != nil && *blockIdentifier.Hash == "" {
		return nil
	}

	if blockIdentifier.Index != nil && *blockIdentifier.Index > 0 {
		return nil
	}

	return errors.New("neither PartialBlockIdentifier.Hash nor PartialBlockIdentifier.Index is set")
}

// TransactionIdentifier returns an error if a
// models.TransactionIdentifier has an invalid hash.
func TransactionIdentifier(
	transactionIdentifier *models.TransactionIdentifier,
) error {
	if transactionIdentifier == nil || transactionIdentifier.Hash == "" {
		return errors.New("Transaction.TransactionIdentifier.Hash is missing")
	}

	return nil
}

// Transaction returns an error if the models.TransactionIdentifier
// is invalid, if any models.Operation within the models.Transaction
// is invalid, or if any operation index is reused within a transaction.
func (a *Asserter) Transaction(
	transaction *models.Transaction,
) error {
	if transaction == nil {
		return errors.New("transaction is nil")
	}

	if err := TransactionIdentifier(transaction.TransactionIdentifier); err != nil {
		return err
	}

	for i, op := range transaction.Operations {
		if err := a.Operation(op, int64(i)); err != nil {
			return err
		}
	}

	return nil
}

// Timestamp returns an error if the timestamp
// on a block is less than or equal to 0.
func Timestamp(timestamp int64) error {
	if timestamp <= 0 {
		return fmt.Errorf("Timestamp is invalid %d", timestamp)
	}

	return nil
}

// Block runs a basic set of assertions for each returned block.
func (a *Asserter) Block(
	ctx context.Context,
	block *models.Block,
) error {
	if block == nil {
		return errors.New("block is nil")
	}

	if err := BlockIdentifier(block.BlockIdentifier); err != nil {
		return err
	}

	if err := BlockIdentifier(block.ParentBlockIdentifier); err != nil {
		return err
	}

	// Only apply some assertions if the block index is not the
	// genesis index.
	if a.genesisIndex != block.BlockIdentifier.Index {
		if block.BlockIdentifier.Hash == block.ParentBlockIdentifier.Hash {
			return errors.New("Block.BlockIdentifier.Hash == Block.ParentBlockIdentifier.Hash")
		}

		if block.BlockIdentifier.Index <= block.ParentBlockIdentifier.Index {
			return errors.New("Block.BlockIdentifier.Index <= Block.ParentBlockIdentifier.Index")
		}
	}

	if err := Timestamp(block.Timestamp); err != nil {
		return err
	}

	for _, transaction := range block.Transactions {
		if err := a.Transaction(transaction); err != nil {
			return err
		}
	}

	return nil
}
