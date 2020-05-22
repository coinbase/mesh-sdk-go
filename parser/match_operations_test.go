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
	"reflect"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func TestMatchOperations(t *testing.T) {
	var tests = map[string]struct {
		operations   []*types.Operation
		descriptions *Descriptions

		matches []*types.Operation
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
			descriptions: &Descriptions{
				OppositeAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
					},
				},
			},
			matches: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "-100",
					},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
			},
			err: false,
		},
		"simple transfer (reject extra op)": {
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
			descriptions: &Descriptions{
				RejectExtraOperations: true,
				OppositeAmounts:       [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
					},
				},
			},
			matches: nil,
			err:     true,
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
			descriptions: &Descriptions{
				EqualAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
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
			descriptions: &Descriptions{
				EqualAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
						},
					},
				},
			},
			matches: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
			},
			err: false,
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
			descriptions: &Descriptions{
				OppositeAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
							Currency: &types.Currency{
								Symbol:   "ETH",
								Decimals: 18,
							},
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
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
			matches: []*types.Operation{
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
			},
			err: false,
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
			descriptions: &Descriptions{
				OppositeAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
							Currency: &types.Currency{
								Symbol:   "ETH",
								Decimals: 18,
							},
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
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
			descriptions: &Descriptions{
				OppositeAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub",
							SubAccountMetadataKeys: []*MetadataDescription{
								{
									Key:       "validator",
									ValueKind: reflect.String,
								},
							},
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
					},
				},
			},
			matches: []*types.Operation{
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
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
			},
			err: false,
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
			descriptions: &Descriptions{
				OppositeAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub",
							SubAccountMetadataKeys: []*MetadataDescription{
								{
									Key:       "validator",
									ValueKind: reflect.String,
								},
							},
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
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
			descriptions: &Descriptions{
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 2",
						},
					},
					{
						Account: &AccountDescription{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 1",
						},
					},
				},
			},
			matches: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 2",
						},
					},
					Amount: &types.Amount{},
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 1",
						},
					},
				},
			},
			err: false,
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
			descriptions: &Descriptions{
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 2",
						},
						Amount: &AmountDescription{
							Exists: false,
						},
					},
					{
						Account: &AccountDescription{
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
			descriptions: &Descriptions{
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 2",
						},
					},
					{
						Account: &AccountDescription{
							Exists:           true,
							SubAccountExists: true,
							SubAccountMetadataKeys: []*MetadataDescription{
								{
									Key:       "validator",
									ValueKind: reflect.Int,
								},
							},
						},
					},
				},
			},
			matches: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
						SubAccount: &types.SubAccountIdentifier{
							Address: "sub 2",
						},
					},
				},
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
			},
			err: false,
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
			descriptions: &Descriptions{
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists:            true,
							SubAccountExists:  true,
							SubAccountAddress: "sub 2",
						},
					},
					{
						Account: &AccountDescription{
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
		"nil descriptions": {
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
			descriptions: &Descriptions{},
			matches:      nil,
			err:          true,
		},
		"2 empty descriptions": {
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
			descriptions: &Descriptions{
				OperationDescriptions: []*OperationDescription{
					{},
					{},
				},
			},
			matches: []*types.Operation{
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
			err: false,
		},
		"empty operations": {
			operations: []*types.Operation{},
			descriptions: &Descriptions{
				OperationDescriptions: []*OperationDescription{
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
			matches, err := MatchOperations(test.descriptions, test.operations)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, test.matches, matches)
		})
	}
}
