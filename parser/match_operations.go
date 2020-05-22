package parser

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// AmountSign is used to represent possible signedness
// of an amount.
type AmountSign int

const (
	// AnyAmountSign is a positive or negative amount.
	AnyAmountSign = iota

	// NegativeAmountSign is a negative amount.
	NegativeAmountSign

	// PositiveAmountSign is a positive amount.
	PositiveAmountSign

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
}

// Descriptions contains a slice of OperationDescriptions and
// high-level requirements enforced across multiple *types.Operations.
type Descriptions struct {
	// EqualAmounts are specified using the operation indicies of
	// OperationDescriptions to handle out of order matches.
	EqualAmounts [][]int

	// OppositeAmounts are specified using the operation indicies of
	// OperationDescriptions to handle out of order matches.
	OppositeAmounts [][]int

	RejectExtraOperations bool
	OperationDescriptions []*OperationDescription
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
			return fmt.Errorf("%s is not present in metadata", req.Key)
		}

		if reflect.TypeOf(val).Kind() != req.ValueKind {
			return fmt.Errorf("%s value is not %s", req.Key, req.ValueKind)
		}
	}

	return nil
}

// accountMatch returns an error if a *types.AccountIdentifier does not meet
// an *AccountDescription.
func accountMatch(req *AccountDescription, account *types.AccountIdentifier) error {
	if req == nil { //anything is ok
		return nil
	}

	if account == nil {
		if req.Exists {
			return errors.New("account is missing")
		}

		return nil
	}

	if account.SubAccount == nil {
		if req.SubAccountExists {
			return errors.New("SubAccountIdentifier.Address is missing")
		}

		return nil
	}

	if !req.SubAccountExists {
		return errors.New("SubAccount is populated")
	}

	// Optionally can require a certain subaccount address
	if len(req.SubAccountAddress) > 0 && account.SubAccount.Address != req.SubAccountAddress {
		return fmt.Errorf("SubAccountIdentifier.Address is %s not %s", account.SubAccount.Address, req.SubAccountAddress)
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
			return errors.New("amount is missing")
		}

		return nil
	}

	if !req.Exists {
		return errors.New("amount is populated")
	}

	if !req.Sign.Match(amount) {
		return fmt.Errorf("amount sign was not %s", req.Sign.String())
	}

	// If no currency is provided, anything is ok.
	if req.Currency == nil {
		return nil
	}

	if amount.Currency == nil || types.Hash(amount.Currency) != types.Hash(req.Currency) {
		return fmt.Errorf("currency %+v is not %+v", amount.Currency, req.Currency)
	}

	return nil
}

// operationMatch returns an error if a *types.Operation does not match a
// *OperationDescription.
func operationMatch(groupIndex int, operation *types.Operation, descriptions []*OperationDescription, matches []int) {
	for i, req := range descriptions {
		if matches[i] != -1 { // already matched
			continue
		}

		if err := accountMatch(req.Account, operation.Account); err != nil {
			continue
		}

		if err := amountMatch(req.Amount, operation.Amount); err != nil {
			continue
		}

		if err := metadataMatch(req.Metadata, operation.Metadata); err != nil {
			continue
		}

		matches[i] = groupIndex
		return
	}
}

// equalAmounts returns an error if a slice of operations do not have
// equal amounts.
func equalAmounts(ops []*types.Operation) error {
	if len(ops) <= 1 {
		return fmt.Errorf("cannot check equality of %d operations", len(ops))
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
			return fmt.Errorf("%s is not equal to %s", val.String(), otherVal.String())
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
		return fmt.Errorf("%s and %s have the same sign", aVal.String(), bVal.String())
	}

	if aVal.CmpAbs(bVal) != 0 {
		return fmt.Errorf("|%s| and |%s| are not equal", aVal.String(), bVal.String())
	}

	return nil
}

// comparisonMatch ensures collections of *types.Operations
// have either equal or opposite amounts.
func comparisonMatch(
	matches []int,
	descriptions *Descriptions,
	operations []*types.Operation,
) error {
	for _, amountMatch := range descriptions.EqualAmounts {
		ops := make([]*types.Operation, len(amountMatch))
		for j, reqIndex := range amountMatch {
			ops[j] = operations[matches[reqIndex]]
		}

		if err := equalAmounts(ops); err != nil {
			return fmt.Errorf("%w: operations not equal", err)
		}
	}

	for _, amountMatch := range descriptions.OppositeAmounts {
		if len(amountMatch) != oppositesLength { // cannot have opposites without exactly 2
			return fmt.Errorf("cannot check opposites of %d operations", len(amountMatch))
		}

		if err := oppositeAmounts(
			operations[matches[amountMatch[0]]],
			operations[matches[amountMatch[1]]],
		); err != nil {
			return fmt.Errorf("%w: amounts not opposites", err)
		}
	}

	return nil
}

// MatchOperations attempts to match a slice of operations with a slice of
// OperationDescriptions (high-level descriptions of what operations are
// desired). If matching succeeds, a slice of indicies are returned mapping
// OperationDescriptions to operations.
func MatchOperations(
	descriptions *Descriptions,
	operations []*types.Operation,
) ([]int, error) {
	if len(operations) == 0 {
		return nil, errors.New("unable to match anything to 0 operations")
	}

	if len(descriptions.OperationDescriptions) == 0 {
		return nil, errors.New("unable to match 0 descriptions")
	}

	if descriptions.RejectExtraOperations && len(descriptions.OperationDescriptions) != len(operations) {
		return nil, fmt.Errorf("expected %d operations, got %d", len(descriptions.OperationDescriptions), len(operations))
	}

	operationDescriptions := descriptions.OperationDescriptions
	matches := make([]int, len(operationDescriptions))

	// Set all matches to -1 so we know if any are unmatched
	for i := 0; i < len(matches); i++ {
		matches[i] = -1
	}

	// Match a *types.Operation to each *OperationDescription
	for i, op := range operations {
		operationMatch(i, op, operationDescriptions, matches)
	}

	// Error if any *OperationDescription is not matched
	for i := 0; i < len(matches); i++ {
		if matches[i] == -1 {
			return nil, fmt.Errorf("could not find match for Description %d", i)
		}
	}

	// Once matches are found, assert high-level descriptions between
	// *types.Operations
	if err := comparisonMatch(matches, descriptions, operations); err != nil {
		return nil, fmt.Errorf("%w: group descriptions not met", err)
	}

	return matches, nil
}
