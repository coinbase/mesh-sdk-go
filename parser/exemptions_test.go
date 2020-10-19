package parser

import (
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
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
			assert.Equal(t, test.expected, parser.findExemptions(test.account, test.currency))
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
			assert.Equal(t, test.expected, parser.MatchBalanceExemption(test.account, test.currency, test.difference))
		})
	}
}
