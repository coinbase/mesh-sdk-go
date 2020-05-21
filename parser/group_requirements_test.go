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
