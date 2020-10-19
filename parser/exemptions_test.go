package parser

import (
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

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
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			parser := New(nil, nil, test.balanceExemptions)
			assert.Equal(t, test.expected, parser.FindExemptions(test.account, test.currency))
		})
	}
}
