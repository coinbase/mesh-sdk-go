// Copyright 2024 Coinbase, Inc.
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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func stringPointer(s string) *string {
	return &s
}

func TestFindExemptions(t *testing.T) {
	tests := map[string]struct {
		balanceExemptions []*types.BalanceExemption
		account           *types.AccountIdentifier
		currency          *types.Currency
		expected          []*types.BalanceExemption
	}{
		"no exemptions": {
			account: &types.AccountIdentifier{
				Address: "test",
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			expected: []*types.BalanceExemption{},
		},
		"no matching exemption": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 7,
					},
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			expected: []*types.BalanceExemption{},
		},
		"no matching exemptions": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 7,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "blah",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			expected: []*types.BalanceExemption{},
		},
		"currency match": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "blah",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			expected: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
		},
		"subaccount match": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 7,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "hello",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			expected: []*types.BalanceExemption{
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
		},
		"multiple match": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "hello",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			expected: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			parser := New(nil, nil, test.balanceExemptions)
			assert.Equal(t, test.expected, parser.FindExemptions(test.account, test.currency))
		})
	}
}

func TestMatchBalanceExemption(t *testing.T) {
	tests := map[string]struct {
		balanceExemptions []*types.BalanceExemption
		account           *types.AccountIdentifier
		currency          *types.Currency
		difference        string
		expected          *types.BalanceExemption
	}{
		"no exemptions": {
			account: &types.AccountIdentifier{
				Address: "test",
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "100",
		},
		"no matching exemption": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 7,
					},
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "100",
		},
		"no matching exemptions": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 7,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "blah",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "100",
		},
		"currency match": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "blah",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "100",
			expected: &types.BalanceExemption{
				ExemptionType: types.BalanceDynamic,
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		},
		"currency match, wrong sign": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceLessOrEqual,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "blah",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "100",
		},
		"currency match, right sign": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceGreaterOrEqual,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "blah",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "100",
			expected: &types.BalanceExemption{
				ExemptionType: types.BalanceGreaterOrEqual,
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		},
		"currency match, zero": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceGreaterOrEqual,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "blah",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "0",
			expected: &types.BalanceExemption{
				ExemptionType: types.BalanceGreaterOrEqual,
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		},
		"subaccount match": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 7,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "hello",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "100",
			expected: &types.BalanceExemption{
				ExemptionType:     types.BalanceDynamic,
				SubAccountAddress: stringPointer("hello"),
			},
		},
		"multiple match": {
			balanceExemptions: []*types.BalanceExemption{
				{
					ExemptionType: types.BalanceDynamic,
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				{
					ExemptionType:     types.BalanceDynamic,
					SubAccountAddress: stringPointer("hello"),
				},
			},
			account: &types.AccountIdentifier{
				Address: "test",
				SubAccount: &types.SubAccountIdentifier{
					Address: "hello",
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			difference: "100",
			expected: &types.BalanceExemption{
				ExemptionType: types.BalanceDynamic,
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			parser := New(nil, nil, test.balanceExemptions)
			exemptions := parser.FindExemptions(test.account, test.currency)
			assert.Equal(t, test.expected, MatchBalanceExemption(exemptions, test.difference))
		})
	}
}
