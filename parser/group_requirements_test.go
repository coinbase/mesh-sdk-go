package parser

import (
	"reflect"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func TestGroupRequirements(t *testing.T) {
	var tests = map[string]struct {
		operations []*types.Operation
		groupReq   *GroupRequirement

		matches []int
		err     bool
	}{
		"simple transfer (with extra op)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "-100",
					},
				},
			},
			groupReq: &GroupRequirement{
				OppositeAmounts: [][]int{[]int{0, 1}},
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
					},
				},
			},
			matches: []int{2, 0},
			err:     false,
		},
		"simple transfer (with unequal amounts)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "-100",
					},
				},
			},
			groupReq: &GroupRequirement{
				EqualAmounts: [][]int{[]int{0, 1}},
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
					},
				},
			},
			matches: nil,
			err:     true,
		},
		"simple transfer (with equal amounts)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
			},
			groupReq: &GroupRequirement{
				EqualAmounts: [][]int{[]int{0, 1}},
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
						},
					},
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
						},
					},
				},
			},
			matches: []int{0, 2},
			err:     false,
		},
		"simple transfer (with currency)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "-100",
						Currency: &types.Currency{
							Symbol:   "ETH",
							Decimals: 18,
						},
					},
				},
			},
			groupReq: &GroupRequirement{
				OppositeAmounts: [][]int{[]int{0, 1}},
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   NegativeAmountSign,
							Currency: &types.Currency{
								Symbol:   "ETH",
								Decimals: 18,
							},
						},
					},
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   PositiveAmountSign,
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
				},
			},
			matches: []int{2, 0},
			err:     false,
		},
		"simple transfer (with missing currency)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
						Currency: &types.Currency{
							Symbol:   "ETH",
							Decimals: 18,
						},
					},
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "-100",
						Currency: &types.Currency{
							Symbol:   "ETH",
							Decimals: 18,
						},
					},
				},
			},
			groupReq: &GroupRequirement{
				OppositeAmounts: [][]int{[]int{0, 1}},
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   NegativeAmountSign,
							Currency: &types.Currency{
								Symbol:   "ETH",
								Decimals: 18,
							},
						},
					},
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   PositiveAmountSign,
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
				},
			},
			matches: nil,
			err:     true,
		},
		"simple transfer (with sender metadata)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub",
							Metadata: map[string]interface{}{
								"validator": "10",
							},
						},
					},
					Amount: &types.Amount{
						Value: "-100",
					},
				},
			},
			groupReq: &GroupRequirement{
				OppositeAmounts: [][]int{[]int{0, 1}},
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub",
							SubAccountMetadataKeys: []*MetadataRequirement{
								{
									Key:       "validator",
									ValueKind: reflect.String,
								},
							},
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
					},
				},
			},
			matches: []int{2, 0},
			err:     false,
		},
		"simple transfer (with missing sender address metadata)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "-100",
					},
				},
			},
			groupReq: &GroupRequirement{
				OppositeAmounts: [][]int{[]int{0, 1}},
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub",
							SubAccountMetadataKeys: []*MetadataRequirement{
								{
									Key:       "validator",
									ValueKind: reflect.String,
								},
							},
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountRequirement{
							Exists: true,
						},
						Amount: &AmountRequirement{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
					},
				},
			},
			matches: nil,
			err:     true,
		},
		"nil amount ops": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 1",
						},
					},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 2",
						},
					},
					Amount: &types.Amount{}, // allowed because no amount requirement provided
				},
			},
			groupReq: &GroupRequirement{
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 2",
						},
					},
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 1",
						},
					},
				},
			},
			matches: []int{1, 0},
			err:     false,
		},
		"nil amount ops (force false amount)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 1",
						},
					},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 2",
						},
					},
					Amount: &types.Amount{},
				},
			},
			groupReq: &GroupRequirement{
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 2",
						},
						Amount: &AmountRequirement{
							Exists: false,
						},
					},
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 1",
						},
					},
				},
			},
			matches: nil,
			err:     true,
		},
		"nil amount ops (only require metadata keys)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 1",
							Metadata: map[string]interface{}{
								"validator": -1000,
							},
						},
					},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 2",
						},
					},
				},
			},
			groupReq: &GroupRequirement{
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 2",
						},
					},
					{
						Account: &AccountRequirement{
							Exists:           true,
							SubAccountExists: true,
							SubAccountMetadataKeys: []*MetadataRequirement{
								{
									Key:       "validator",
									ValueKind: reflect.Int,
								},
							},
						},
					},
				},
			},
			matches: []int{1, 0},
			err:     false,
		},
		"nil amount ops (sub account address mismatch)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 3",
						},
					},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 2",
						},
					},
				},
			},
			groupReq: &GroupRequirement{
				OperationRequirements: []*OperationRequirement{
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 2",
						},
					},
					{
						Account: &AccountRequirement{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 1",
						},
					},
				},
			},
			matches: nil,
			err:     true,
		},
		"nil requirements": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 3",
						},
					},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 2",
						},
					},
				},
			},
			groupReq: &GroupRequirement{},
			matches:  nil,
			err:      true,
		},
		"2 empty requirements": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 3",
						},
					},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 2",
						},
					},
				},
			},
			groupReq: &GroupRequirement{
				OperationRequirements: []*OperationRequirement{
					{},
					{},
				},
			},
			matches: []int{0, 1},
			err:     false,
		},
		"empty operations": {
			operations: []*types.Operation{},
			groupReq: &GroupRequirement{
				OperationRequirements: []*OperationRequirement{
					{},
					{},
				},
			},
			matches: nil,
			err:     true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			matches, err := ApplyRequirement(test.operations, test.groupReq)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, test.matches, matches)
		})
	}
}
