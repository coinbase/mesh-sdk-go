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
	"fmt"
	"math/big"
	"reflect"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// AmountSign is used to represent possible signedness
// of an amount.
type AmountSign int

const (
	// AnyAmountSign is a positive or negative amount.
	AnyAmountSign = 0

	// NegativeAmountSign is a negative amount.
	NegativeAmountSign = 1

	// PositiveAmountSign is a positive amount.
	PositiveAmountSign = 2

	// PositiveOrZeroAmountSign is a positive or zero amount.
	PositiveOrZeroAmountSign = 3

	// NegativeOrZeroAmountSign is a positive or zero amount.
	NegativeOrZeroAmountSign = 4

	// oppositesLength is the only allowed number of
	// operations to compare as opposites.
	oppositesLength = 2
)

// Match returns a boolean indicating if a *types.Amount
// has an AmountSign.
func (s AmountSign) Match(amount *types.Amount) bool {
	if s == AnyAmountSign {
		return true
	}

	numeric, err := types.AmountValue(amount)
	if err != nil {
		return false
	}

	if s == NegativeAmountSign && numeric.Sign() == -1 {
		return true
	}

	if s == PositiveAmountSign && numeric.Sign() == 1 {
		return true
	}

	if s == PositiveOrZeroAmountSign && (numeric.Sign() == 1 || amount.Value == "0") {
		return true
	}

	if s == NegativeOrZeroAmountSign && (numeric.Sign() == -1 || amount.Value == "0") {
		return true
	}

	return false
}

// String returns a description of an AmountSign.
func (s AmountSign) String() string {
	switch s {
	case AnyAmountSign:
		return "any"
	case NegativeAmountSign:
		return "negative"
	case PositiveAmountSign:
		return "positive"
	case PositiveOrZeroAmountSign:
		return "positive or zero"
	case NegativeOrZeroAmountSign:
		return "negative or zero"
	default:
		return "invalid"
	}
}

// MetadataDescription is used to check if a map[string]interface{}
// has certain keys and values of a certain kind.
type MetadataDescription struct {
	Key       string
	ValueKind reflect.Kind // ex: reflect.String
}

// AccountDescription is used to describe a *types.AccountIdentifier.
type AccountDescription struct {
	Exists                 bool
	SubAccountExists       bool
	SubAccountAddress      string
	SubAccountMetadataKeys []*MetadataDescription
}

// AmountDescription is used to describe a *types.Amount.
type AmountDescription struct {
	Exists   bool
	Sign     AmountSign
	Currency *types.Currency
}

// OperationDescription is used to describe a *types.Operation.
type OperationDescription struct {
	Account  *AccountDescription
	Amount   *AmountDescription
	Metadata []*MetadataDescription

	// Type is the operation.Type that must match. If this is left empty,
	// any type is considered a match.
	Type string

	// AllowRepeats indicates that multiple operations can be matched
	// to a particular description.
	AllowRepeats bool

	// Optional indicates that not finding any operations that meet
	// the description should not trigger an error.
	Optional bool

	// CoinAction indicates that an operation should have a CoinChange
	// and that it should have the CoinAction. If this is not populated,
	// CoinChange is not checked.
	CoinAction types.CoinAction
}

// Descriptions contains a slice of OperationDescriptions and
// high-level requirements enforced across multiple *types.Operations.
type Descriptions struct {
	OperationDescriptions []*OperationDescription

	// EqualAmounts are specified using the operation indices of
	// OperationDescriptions to handle out of order matches. MatchOperations
	// will error if all groups of operations aren't equal.
	EqualAmounts [][]int

	// OppositeAmounts are specified using the operation indices of
	// OperationDescriptions to handle out of order matches. MatchOperations
	// will error if all groups of operations aren't opposites.
	OppositeAmounts [][]int

	// OppositeZeroAmounts are specified using the operation indices of
	// OperationDescriptions to handle out of order matches. MatchOperations
	// will error if all groups of operations aren't 0 or opposites.
	OppositeOrZeroAmounts [][]int

	// EqualAddresses are specified using the operation indices of
	// OperationDescriptions to handle out of order matches. MatchOperations
	// will error if all groups of operations addresses aren't equal.
	EqualAddresses [][]int

	// ErrUnmatched indicates that an error should be returned
	// if all operations cannot be matched to a description.
	ErrUnmatched bool
}

// metadataMatch returns an error if a map[string]interface does not meet
// a slice of *MetadataDescription.
func metadataMatch(reqs []*MetadataDescription, metadata map[string]interface{}) error {
	if len(reqs) == 0 {
		return nil
	}

	for _, req := range reqs {
		val, ok := metadata[req.Key]
		if !ok {
			return fmt.Errorf("%w: %s", ErrMetadataMatchKeyNotFound, req.Key)
		}

		if reflect.TypeOf(val).Kind() != req.ValueKind {
			return fmt.Errorf(
				"%w: value of %s is not of type %s",
				ErrMetadataMatchKeyValueMismatch,
				req.Key,
				req.ValueKind,
			)
		}
	}

	return nil
}

// accountMatch returns an error if a *types.AccountIdentifier does not meet
// an *AccountDescription.
func accountMatch(req *AccountDescription, account *types.AccountIdentifier) error {
	if req == nil { // anything is ok
		return nil
	}

	if account == nil {
		if req.Exists {
			return ErrAccountMatchAccountMissing
		}

		return nil
	}

	if account.SubAccount == nil {
		if req.SubAccountExists {
			return ErrAccountMatchSubAccountMissing
		}

		return nil
	}

	if !req.SubAccountExists {
		return ErrAccountMatchSubAccountPopulated
	}

	// Optionally can require a certain subaccount address
	if len(req.SubAccountAddress) > 0 && account.SubAccount.Address != req.SubAccountAddress {
		return fmt.Errorf(
			"%w: expected %s but got %s",
			ErrAccountMatchUnexpectedSubAccountAddr,
			req.SubAccountAddress,
			account.SubAccount.Address,
		)
	}

	if err := metadataMatch(req.SubAccountMetadataKeys, account.SubAccount.Metadata); err != nil {
		return fmt.Errorf("%w: account metadata keys mismatch", err)
	}

	return nil
}

// amountMatch returns an error if a *types.Amount does not meet an
// *AmountDescription.
func amountMatch(req *AmountDescription, amount *types.Amount) error {
	if req == nil { // anything is ok
		return nil
	}

	if amount == nil {
		if req.Exists {
			return ErrAmountMatchAmountMissing
		}

		return nil
	}

	if !req.Exists {
		return ErrAmountMatchAmountPopulated
	}

	if !req.Sign.Match(amount) {
		return fmt.Errorf("%w: expected %s", ErrAmountMatchUnexpectedSign, req.Sign.String())
	}

	// If no currency is provided, anything is ok.
	if req.Currency == nil {
		return nil
	}

	if amount.Currency == nil || types.Hash(amount.Currency) != types.Hash(req.Currency) {
		return fmt.Errorf(
			"%w: expected %+v but got %+v",
			ErrAmountMatchUnexpectedCurrency,
			req.Currency,
			amount.Currency,
		)
	}

	return nil
}

func coinActionMatch(requiredAction types.CoinAction, coinChange *types.CoinChange) error {
	if len(requiredAction) == 0 {
		return nil
	}

	if coinChange == nil {
		return fmt.Errorf("%w: expected %s", ErrCoinActionMatchCoinChangeIsNil, requiredAction)
	}

	if coinChange.CoinAction != requiredAction {
		return fmt.Errorf(
			"%w: expected %s but got %s",
			ErrCoinActionMatchUnexpectedCoinAction,
			requiredAction,
			coinChange.CoinAction,
		)
	}

	return nil
}

// operationMatch returns an error if a *types.Operation does not match a
// *OperationDescription.
func operationMatch(
	operation *types.Operation,
	descriptions []*OperationDescription,
	matches []*Match,
) bool {
	for i, des := range descriptions {
		if matches[i] != nil && !des.AllowRepeats { // already matched
			continue
		}

		if len(des.Type) > 0 && des.Type != operation.Type {
			continue
		}

		if err := accountMatch(des.Account, operation.Account); err != nil {
			continue
		}

		if err := amountMatch(des.Amount, operation.Amount); err != nil {
			continue
		}

		if err := metadataMatch(des.Metadata, operation.Metadata); err != nil {
			continue
		}

		if err := coinActionMatch(des.CoinAction, operation.CoinChange); err != nil {
			continue
		}

		if matches[i] == nil {
			matches[i] = &Match{
				Operations: []*types.Operation{},
				Amounts:    []*big.Int{},
			}
		}

		if operation.Amount != nil {
			val, err := types.AmountValue(operation.Amount)
			if err != nil {
				continue
			}
			matches[i].Amounts = append(matches[i].Amounts, val)
		} else {
			matches[i].Amounts = append(matches[i].Amounts, nil)
		}

		// Wait to add operation to matches in case that we "continue" when
		// parsing operation.Amount.
		matches[i].Operations = append(matches[i].Operations, operation)
		return true
	}

	return false
}

// equalAmounts returns an error if a slice of operations do not have
// equal amounts.
func equalAmounts(ops []*types.Operation) error {
	if len(ops) == 0 {
		return ErrEqualAmountsNoOperations
	}

	val, err := types.AmountValue(ops[0].Amount)
	if err != nil {
		return err
	}

	for _, op := range ops {
		otherVal, err := types.AmountValue(op.Amount)
		if err != nil {
			return err
		}

		if val.Cmp(otherVal) != 0 {
			return fmt.Errorf(
				"%w: %s is not equal to %s",
				ErrEqualAmountsNotEqual,
				val.String(),
				otherVal.String(),
			)
		}
	}

	return nil
}

// oppositeAmounts returns an error if two operations do not have opposite
// amounts.
func oppositeAmounts(a *types.Operation, b *types.Operation) error {
	aVal, err := types.AmountValue(a.Amount)
	if err != nil {
		return err
	}

	bVal, err := types.AmountValue(b.Amount)
	if err != nil {
		return err
	}

	if aVal.Sign() == bVal.Sign() {
		return fmt.Errorf("%w: %s and %s", ErrOppositeAmountsSameSign, aVal.String(), bVal.String())
	}

	if aVal.CmpAbs(bVal) != 0 {
		return fmt.Errorf(
			"%w: %s and %s",
			ErrOppositeAmountsAbsValMismatch,
			aVal.String(),
			bVal.String(),
		)
	}

	return nil
}

// oppositeOrZeroAmounts returns an error if two operations do not have opposite
// amounts and both amounts are not zero.
func oppositeOrZeroAmounts(a *types.Operation, b *types.Operation) error {
	aVal, err := types.AmountValue(a.Amount)
	if err != nil {
		return err
	}

	bVal, err := types.AmountValue(b.Amount)
	if err != nil {
		return err
	}

	zero := big.NewInt(0)
	if aVal.Cmp(zero) == 0 && bVal.Cmp(zero) == 0 {
		return nil
	}

	if aVal.Sign() == bVal.Sign() {
		return fmt.Errorf("%w: %s and %s", ErrOppositeAmountsSameSign, aVal.String(), bVal.String())
	}

	if aVal.CmpAbs(bVal) != 0 {
		return fmt.Errorf(
			"%w: %s and %s",
			ErrOppositeAmountsAbsValMismatch,
			aVal.String(),
			bVal.String(),
		)
	}

	return nil
}

// equalAddresses returns an error if a slice of operations do not have
// equal addresses.
func equalAddresses(ops []*types.Operation) error {
	if len(ops) <= 1 {
		return fmt.Errorf("%w: got %d operations", ErrEqualAddressesTooFewOperations, len(ops))
	}

	base := ""

	for _, op := range ops {
		if op.Account == nil {
			return ErrEqualAddressesAccountIsNil
		}

		if len(base) == 0 {
			base = op.Account.Address
			continue
		}

		if base != op.Account.Address {
			return fmt.Errorf(
				"%w: %s is not equal to %s",
				ErrEqualAddressesAddrMismatch,
				base,
				op.Account.Address,
			)
		}
	}

	return nil
}

func matchIndexValid(matches []*Match, index int) error {
	if index >= len(matches) {
		return fmt.Errorf(
			"%w: at index %d",
			ErrMatchIndexValidIndexOutOfRange,
			index,
		)
	}
	if matches[index] == nil {
		return fmt.Errorf(
			"%w: at index %d",
			ErrMatchIndexValidIndexIsNil,
			index,
		)
	}

	return nil
}

func checkOps(requests [][]int, matches []*Match, valid func([]*types.Operation) error) error {
	for _, batch := range requests {
		ops := []*types.Operation{}
		for _, reqIndex := range batch {
			if err := matchIndexValid(matches, reqIndex); err != nil {
				return fmt.Errorf("%w: index %d not valid", err, reqIndex)
			}

			ops = append(ops, matches[reqIndex].Operations...)
		}

		if err := valid(ops); err != nil {
			return fmt.Errorf("%w operations not valid", err)
		}
	}

	return nil
}

// compareOppositeMatches ensures collections of *types.Operation
// that may have opposite amounts contain valid matching amounts
func compareOppositeMatches(
	amountPairs [][]int,
	matches []*Match,
	amountChecker func(*types.Operation, *types.Operation) error,
) error {
	for _, amountMatch := range amountPairs {
		if len(amountMatch) != oppositesLength { // cannot have opposites without exactly 2
			return fmt.Errorf("cannot check opposites of %d operations", len(amountMatch))
		}

		// compare all possible pairs
		if err := matchIndexValid(matches, amountMatch[0]); err != nil {
			return fmt.Errorf("%w: amounts comparison error", err)
		}
		if err := matchIndexValid(matches, amountMatch[1]); err != nil {
			return fmt.Errorf("%w: amounts comparison error", err)
		}

		match0Ops := matches[amountMatch[0]].Operations
		match1Ops := matches[amountMatch[1]].Operations
		if err := equalAmounts(match0Ops); err != nil {
			return fmt.Errorf(
				"%w: amounts comparison error for match index %d",
				err,
				amountMatch[0],
			)
		}
		if err := equalAmounts(match1Ops); err != nil {
			return fmt.Errorf(
				"%w: amounts comparison error for match index %d",
				err,
				amountMatch[1],
			)
		}

		// only need to check amount for the very first operation from each
		// matched operations group since we made sure all amounts within the same
		// matched operation group are the same
		if err := amountChecker(
			match0Ops[0],
			match1Ops[0],
		); err != nil {
			return fmt.Errorf("%w: amounts do not match the amountChecker function", err)
		}
	}

	return nil
}

// comparisonMatch ensures collections of *types.Operation
// have either equal or opposite amounts.
func comparisonMatch(
	descriptions *Descriptions,
	matches []*Match,
) error {
	if err := checkOps(descriptions.EqualAmounts, matches, equalAmounts); err != nil {
		return fmt.Errorf("%w: operation amounts not equal", err)
	}

	if err := checkOps(descriptions.EqualAddresses, matches, equalAddresses); err != nil {
		return fmt.Errorf("%w: operation addresses not equal", err)
	}

	if err := compareOppositeMatches(descriptions.OppositeAmounts, matches, oppositeAmounts); err != nil {
		return fmt.Errorf("%w: operation amounts not opposite", err)
	}
	if err := compareOppositeMatches(descriptions.OppositeOrZeroAmounts, matches, oppositeOrZeroAmounts); err != nil {
		return fmt.Errorf("%w: both operation amounts not opposite and not zero", err)
	}

	return nil
}

// Match contains all *types.Operation matching a given OperationDescription and
// their parsed *big.Int amounts (if populated).
type Match struct {
	Operations []*types.Operation

	// Amounts has the same length as Operations. If an operation has
	// a populate Amount, its corresponding *big.Int will be non-nil.
	Amounts []*big.Int
}

// First is a convenience method that returns the first matched operation
// and amount (if they exist). This is used when parsing matches when
// AllowRepeats is set to false.
func (m *Match) First() (*types.Operation, *big.Int) {
	if m == nil {
		return nil, nil
	}

	if len(m.Operations) > 0 {
		return m.Operations[0], m.Amounts[0]
	}

	return nil, nil
}

// MatchOperations attempts to match a slice of operations with a slice of
// OperationDescriptions (high-level descriptions of what operations are
// desired). If matching succeeds, a slice of matching operations in the
// mapped to the order of the descriptions is returned.
func MatchOperations(
	descriptions *Descriptions,
	operations []*types.Operation,
) ([]*Match, error) {
	if len(operations) == 0 {
		return nil, ErrMatchOperationsNoOperations
	}

	if len(descriptions.OperationDescriptions) == 0 {
		return nil, ErrMatchOperationsDescriptionsMissing
	}

	operationDescriptions := descriptions.OperationDescriptions
	matches := make([]*Match, len(operationDescriptions))

	// Match a *types.Operation to each *OperationDescription
	for i, op := range operations {
		matchFound := operationMatch(op, operationDescriptions, matches)
		if !matchFound && descriptions.ErrUnmatched {
			return nil, fmt.Errorf(
				"%w: at index %d",
				ErrMatchOperationsMatchNotFound,
				i,
			)
		}
	}

	// Error if any *OperationDescription is not matched
	for i := 0; i < len(matches); i++ {
		if matches[i] == nil && !descriptions.OperationDescriptions[i].Optional {
			return nil, fmt.Errorf("%w: %d", ErrMatchOperationsDescriptionNotMatched, i)
		}
	}

	// Once matches are found, assert high-level descriptions between
	// *types.Operations
	if err := comparisonMatch(descriptions, matches); err != nil {
		return nil, fmt.Errorf("%w: group descriptions not met", err)
	}

	return matches, nil
}
