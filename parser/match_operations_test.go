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
	"math/big"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestMatchOperations(t *testing.T) {
	var tests = map[string]struct {
		operations   []*types.Operation
		descriptions *Descriptions

		matches []*Match
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
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
							},
							Amount: &types.Amount{
								Value: "-100",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(-100)},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
							},
							Amount: &types.Amount{
								Value: "100",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(100)},
				},
			},
			err: false,
		},
		"simple transfer (with OppositeOrZeroAmounts) and opposite value amounts": {
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
				OppositeOrZeroAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeOrZeroAmountSign,
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveOrZeroAmountSign,
						},
					},
				},
			},
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
							},
							Amount: &types.Amount{
								Value: "-100",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(-100)},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
							},
							Amount: &types.Amount{
								Value: "100",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(100)},
				},
			},
			err: false,
		},
		"simple transfer (with OppositeOrZeroAmounts) and 0-value amounts": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "0",
					},
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "0",
					},
				},
			},
			descriptions: &Descriptions{
				OppositeOrZeroAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   AnyAmountSign,
						},
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   AnyAmountSign,
						},
					},
				},
			},
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
							},
							Amount: &types.Amount{
								Value: "0",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(0)},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
							},
							Amount: &types.Amount{
								Value: "0",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(0)},
				},
			},
			err: false,
		},
		"simple transfer (with too many opposite amounts)": {
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
				OppositeAmounts: [][]int{{0, 1, 2}},
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
			err: true,
		},
		"simple transfer (with missing account error)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{
					Amount: &types.Amount{
						Value: "-100",
					},
				},
			},
			descriptions: &Descriptions{
				OppositeAmounts: [][]int{{0, 1}},
				EqualAddresses:  [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: false,
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
		"simple transfer (check type)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
					Type: "output",
				},
				{}, // extra op ignored
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "-100",
					},
					Type: "input",
				},
			},
			descriptions: &Descriptions{
				OppositeAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Type: "input",
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Type: "output",
					},
				},
			},
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
							},
							Amount: &types.Amount{
								Value: "-100",
							},
							Type: "input",
						},
					},
					Amounts: []*big.Int{big.NewInt(-100)},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
							},
							Amount: &types.Amount{
								Value: "100",
							},
							Type: "output",
						},
					},
					Amounts: []*big.Int{big.NewInt(100)},
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
				ErrUnmatched:    true,
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
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
							},
							Amount: &types.Amount{
								Value: "100",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(100)},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
							},
							Amount: &types.Amount{
								Value: "100",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(100)},
				},
			},
			err: false,
		},
		"simple transfer (with coin action)": {
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
					CoinChange: &types.CoinChange{
						CoinAction: types.CoinSpent,
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
						CoinAction: types.CoinSpent,
					},
				},
			},
			matches: []*Match{
				{
					Operations: []*types.Operation{
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
					Amounts: []*big.Int{big.NewInt(-100)},
				},
				{
					Operations: []*types.Operation{
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
							CoinChange: &types.CoinChange{
								CoinAction: types.CoinSpent,
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(100)},
				},
			},
			err: false,
		},
		"simple transfer (missing coin action)": {
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
						CoinAction: types.CoinSpent,
					},
				},
			},
			err: true,
		},
		"simple transfer (incorrect coin action)": {
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
					CoinChange: &types.CoinChange{
						CoinAction: types.CoinCreated,
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
						CoinAction: types.CoinSpent,
					},
				},
			},
			err: true,
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
			matches: []*Match{
				{
					Operations: []*types.Operation{
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
					Amounts: []*big.Int{big.NewInt(-100)},
				},
				{
					Operations: []*types.Operation{
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
					Amounts: []*big.Int{big.NewInt(100)},
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
		"simple transfer (with sender metadata) and non-equal addresses": {
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
				EqualAddresses:  [][]int{{0, 1}},
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
		"simple transfer (with sender metadata)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
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
				EqualAddresses:  [][]int{{0, 1}},
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
			matches: []*Match{
				{
					Operations: []*types.Operation{
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
					Amounts: []*big.Int{big.NewInt(-100)},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
							},
							Amount: &types.Amount{
								Value: "100",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(100)},
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
					Amount: &types.Amount{
						Value: "100",
					}, // allowed because no amount requirement provided
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
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
								SubAccount: &types.SubAccountIdentifier{
									Address: "sub 2",
								},
							},
							Amount: &types.Amount{
								Value: "100",
							},
						},
					},
					Amounts: []*big.Int{big.NewInt(100)},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
								SubAccount: &types.SubAccountIdentifier{
									Address: "sub 1",
								},
							},
						},
					},
					Amounts: []*big.Int{nil},
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
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
								SubAccount: &types.SubAccountIdentifier{
									Address: "sub 2",
								},
							},
						},
					},
					Amounts: []*big.Int{nil},
				},
				{
					Operations: []*types.Operation{
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
					Amounts: []*big.Int{nil},
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
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
								SubAccount: &types.SubAccountIdentifier{
									Address: "sub 3",
								},
							},
						},
					},
					Amounts: []*big.Int{nil},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
								SubAccount: &types.SubAccountIdentifier{
									Address: "sub 2",
								},
							},
						},
					},
					Amounts: []*big.Int{nil},
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
		"simple repeated op": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "200",
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
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
					},
				},
			},
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
							},
							Amount: &types.Amount{
								Value: "200",
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
					Amounts: []*big.Int{
						big.NewInt(200),
						big.NewInt(100),
					},
				},
			},
			err: false,
		},
		"simple repeated op (no extra ops allowed)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "200",
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
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
					},
				},
				ErrUnmatched: true,
			},
			matches: nil,
			err:     true,
		},
		"simple repeated op (with invalid comparison indexes)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "200",
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
				OppositeAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
					},
				},
			},
			matches: nil,
			err:     true,
		},
		"simple repeated op (with overlapping, repeated descriptions)": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "200",
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
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
					},
					{ // will never be possible to meet this description
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
					},
				},
			},
			matches: nil,
			err:     true,
		},
		"complex repeated op": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "200",
					},
					Type: "output",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr3",
					},
					Amount: &types.Amount{
						Value: "200",
					},
					Type: "output",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "-200",
					},
					Type: "input",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr4",
					},
					Amount: &types.Amount{
						Value: "-200",
					},
					Type: "input",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr5",
					},
					Amount: &types.Amount{
						Value: "-1000",
					},
					Type: "runoff",
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
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
						Type:         "output",
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
						AllowRepeats: true,
						Type:         "input",
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
						AllowRepeats: true,
					},
				},
			},
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
							},
							Amount: &types.Amount{
								Value: "200",
							},
							Type: "output",
						},
						{
							Account: &types.AccountIdentifier{
								Address: "addr3",
							},
							Amount: &types.Amount{
								Value: "200",
							},
							Type: "output",
						},
					},
					Amounts: []*big.Int{
						big.NewInt(200),
						big.NewInt(200),
					},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr1",
							},
							Amount: &types.Amount{
								Value: "-200",
							},
							Type: "input",
						},
						{
							Account: &types.AccountIdentifier{
								Address: "addr4",
							},
							Amount: &types.Amount{
								Value: "-200",
							},
							Type: "input",
						},
					},
					Amounts: []*big.Int{
						big.NewInt(-200),
						big.NewInt(-200),
					},
				},
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr5",
							},
							Amount: &types.Amount{
								Value: "-1000",
							},
							Type: "runoff",
						},
					},
					Amounts: []*big.Int{
						big.NewInt(-1000),
					},
				},
			},
			err: false,
		},
		"optional description not met": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "200",
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
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
						Optional: true,
					},
				},
			},
			matches: []*Match{
				{
					Operations: []*types.Operation{
						{
							Account: &types.AccountIdentifier{
								Address: "addr2",
							},
							Amount: &types.Amount{
								Value: "200",
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
					Amounts: []*big.Int{
						big.NewInt(200),
						big.NewInt(100),
					},
				},
				nil,
			},
			err: false,
		},
		"optional description equal amounts not found": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "200",
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
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
						Optional: true,
					},
				},
			},
			matches: nil,
			err:     true,
		},
		"optional description opposite amounts not found": {
			operations: []*types.Operation{
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "200",
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
				OppositeAmounts: [][]int{{0, 1}},
				OperationDescriptions: []*OperationDescription{
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   PositiveAmountSign,
						},
						AllowRepeats: true,
					},
					{
						Account: &AccountDescription{
							Exists: true,
						},
						Amount: &AmountDescription{
							Exists: true,
							Sign:   NegativeAmountSign,
						},
						Optional: true,
					},
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

func TestMatch(t *testing.T) {
	var tests = map[string]struct {
		m *Match

		op     *types.Operation
		amount *big.Int
	}{
		"nil match": {},
		"empty match": {
			m: &Match{},
		},
		"single op match": {
			m: &Match{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 1,
						},
					},
				},
				Amounts: []*big.Int{
					big.NewInt(100),
				},
			},
			op: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 1,
				},
			},
			amount: big.NewInt(100),
		},
		"multi-op match": {
			m: &Match{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 1,
						},
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 2,
						},
					},
				},
				Amounts: []*big.Int{
					big.NewInt(100),
					big.NewInt(200),
				},
			},
			op: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 1,
				},
			},
			amount: big.NewInt(100),
		},
		"single op match with nil amount": {
			m: &Match{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 1,
						},
					},
				},
				Amounts: []*big.Int{nil},
			},
			op: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 1,
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			op, amount := test.m.First()
			assert.Equal(t, test.op, op)
			assert.Equal(t, test.amount, amount)
		})
	}
}
