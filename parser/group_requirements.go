package parser

import (
	"reflect"

	"github.com/coinbase/rosetta-sdk-go/types"
)

type AmountSign int

const (
	AnyAmountSign = iota
	NegativeAmountSign
	PositiveAmountSign
)

func (s AmountSign) match(value string) bool {
	if s == AnyAmountSign {
		return true
	}

	numeric, err := types.BigInt(value)
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

type MetadataRequirement struct {
	key       string
	valueType reflect.Kind // ex: reflect.String
}

type AccountRequirement struct {
	Account                bool
	SubAccountAddress      string
	SubAccountMetadataKeys []*MetadataRequirement
}

type AmountRequirement struct {
	Amount   bool
	Sign     AmountSign
	Currency *types.Currency
}

type OperationRequirement struct {
	Account  *AccountRequirement
	Amount   *AmountRequirement
	Metadata *MetadataRequirement
}

type GroupRequirement struct {
	EqualAmounts          [][]int
	OppositeAmounts       [][]int
	OperationRequirements []*OperationRequirement
}

func matchMetadataKeys(reqs []*MetadataRequirement, metadata map[string]interface{}) bool {
	for _, req := range reqs {
		val, ok := metadata[req.key]
		if !ok {
			return false
		}

		if reflect.TypeOf(val).Kind() != req.valueType {
			return false
		}
	}

	return true
}

func accountMatch(req *AccountRequirement, account *types.AccountIdentifier) bool {
	return false
}

func amountMatch(req *AmountRequirement, amount *types.Amount) bool {
	return false
}

func metadataMatch(req *MetadataRequirement, metadata map[string]interface{}) bool {
	return false
}

func operationMatch(groupIndex int, operation *types.Operation, requirements []*OperationRequirement, matches []int) {
	// Skip any requirements that already have matches
	for i, req := range requirements {
		if matches[i] != -1 { // already matched
			continue
		}

		if !accountMatch(req.Account, operation.Account) {
			continue
		}

		if !amountMatch(req.Amount, operation.Amount) {
			continue
		}

		if !metadataMatch(req.Metadata, operation.Metadata) {
			continue
		}

		// Assign match
		matches[i] = groupIndex
		return
	}
}

func equalAmounts(ops []*types.Operation) bool {
	return false
}

func oppositeAmounts(a *types.Operation, b *types.Operation) bool {
	return false
}

func assertGroupRequirements(matches []int, groupRequirement *GroupRequirement, operationGroup *OperationGroup) bool {
	for _, amountMatch := range groupRequirement.EqualAmounts {
		ops := make([]*types.Operation, len(amountMatch))
		for j, reqIndex := range amountMatch {
			ops[j] = operationGroup.Operations[matches[reqIndex]]
		}

		if !equalAmounts(ops) {
			return false
		}
	}

	for _, amountMatch := range groupRequirement.OppositeAmounts {
		if len(amountMatch) != 2 { // cannot have opposites without exactly 2
			return false
		}

		if !oppositeAmounts(
			operationGroup.Operations[amountMatch[0]],
			operationGroup.Operations[amountMatch[1]],
		) {
			return false
		}
	}

	return true
}

func ApplyRequirement(
	operationGroup *OperationGroup,
	groupRequirement *GroupRequirement,
) (bool, []int) {
	operationRequirements := groupRequirement.OperationRequirements
	matches := make([]int, len(operationRequirements))

	// Set all matches to -1 so we know if any are unmatched
	for i := 0; i < len(matches); i++ {
		matches[i] = -1
	}

	// Match op to an *OperationRequirement
	for i, op := range operationGroup.Operations {
		operationMatch(i, op, operationRequirements, matches)
	}

	// Error if any operationRequirement is not matched (do not error if more ops
	// than requirements)
	for i := 0; i < len(matches); i++ {
		if matches[i] == -1 {
			return false, nil
		}
	}

	// Check if group requirements met
	if !assertGroupRequirements(matches, groupRequirement, operationGroup) {
		return false, nil
	}

	return true, matches
}
