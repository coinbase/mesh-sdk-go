package parser

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/coinbase/rosetta-sdk-go/types"
)

type AmountSign int

const (
	AnyAmountSign = iota
	NegativeAmountSign
	PositiveAmountSign
)

func (s AmountSign) match(amount *types.Amount) bool {
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

type MetadataRequirement struct {
	Key       string
	ValueKind reflect.Kind // ex: reflect.String
}

type AccountRequirement struct {
	Exists                 bool
	SubAccountExists       bool
	SubAccountAddress      string
	SubAccountMetadataKeys []*MetadataRequirement
}

type AmountRequirement struct {
	Exists   bool
	Sign     AmountSign
	Currency *types.Currency
}

type OperationRequirement struct {
	Account  *AccountRequirement
	Amount   *AmountRequirement
	Metadata []*MetadataRequirement
}

type GroupRequirement struct {
	EqualAmounts          [][]int
	OppositeAmounts       [][]int
	OperationRequirements []*OperationRequirement
}

func metadataKeyMatch(reqs []*MetadataRequirement, metadata map[string]interface{}) error {
	if len(reqs) == 0 && metadata == nil {
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

func accountMatch(req *AccountRequirement, account *types.AccountIdentifier) error {
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

	// Optionally can require a certain subaccount address
	if len(req.SubAccountAddress) > 0 && account.SubAccount.Address != req.SubAccountAddress {
		return fmt.Errorf("SubAccountIdentifier.Address is %s not %s", account.SubAccount.Address, req.SubAccountAddress)
	}

	if err := metadataKeyMatch(req.SubAccountMetadataKeys, account.SubAccount.Metadata); err != nil {
		return fmt.Errorf("%w: account metadata keys mismatch", err)
	}

	return nil
}

func amountMatch(req *AmountRequirement, amount *types.Amount) error {
	if amount == nil {
		if req.Exists {
			return errors.New("amount is missing")
		}

		return nil
	}

	if !req.Sign.match(amount) {
		return fmt.Errorf("amount sign was not %s", req.Sign.String())
	}

	// no currency restriction
	if req.Currency == nil {
		return nil
	}

	if amount.Currency == nil || types.Hash(amount.Currency) != types.Hash(req.Currency) {
		return fmt.Errorf("currency %+v is not %+v", amount.Currency, req.Currency)
	}

	return nil
}

func operationMatch(groupIndex int, operation *types.Operation, requirements []*OperationRequirement, matches []int) {
	// Skip any requirements that already have matches
	for i, req := range requirements {
		if matches[i] != -1 { // already matched
			continue
		}

		if err := accountMatch(req.Account, operation.Account); err != nil {
			continue
		}

		if err := amountMatch(req.Amount, operation.Amount); err != nil {
			continue
		}

		if err := metadataKeyMatch(req.Metadata, operation.Metadata); err != nil {
			continue
		}

		// Assign match
		matches[i] = groupIndex
		return
	}
}

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

func assertGroupRequirements(matches []int, groupRequirement *GroupRequirement, operations []*types.Operation) error {
	for _, amountMatch := range groupRequirement.EqualAmounts {
		ops := make([]*types.Operation, len(amountMatch))
		for j, reqIndex := range amountMatch {
			ops[j] = operations[matches[reqIndex]]
		}

		if err := equalAmounts(ops); err != nil {
			return fmt.Errorf("%w: operations not equal", err)
		}
	}

	for _, amountMatch := range groupRequirement.OppositeAmounts {
		if len(amountMatch) != 2 { // cannot have opposites without exactly 2
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

func ApplyRequirement(
	operations []*types.Operation,
	groupRequirement *GroupRequirement,
) ([]int, error) {
	operationRequirements := groupRequirement.OperationRequirements
	matches := make([]int, len(operationRequirements))

	// Set all matches to -1 so we know if any are unmatched
	for i := 0; i < len(matches); i++ {
		matches[i] = -1
	}

	// Match op to an *OperationRequirement
	for i, op := range operations {
		operationMatch(i, op, operationRequirements, matches)
	}

	// Error if any operationRequirement is not matched (do not error if more ops
	// than requirements)
	for i := 0; i < len(matches); i++ {
		if matches[i] == -1 {
			return nil, fmt.Errorf("could not find match for requirement %d", i)
		}
	}

	// Check if group requirements met
	if err := assertGroupRequirements(matches, groupRequirement, operations); err != nil {
		return nil, fmt.Errorf("%w: group requirements not met", err)
	}

	return matches, nil
}
