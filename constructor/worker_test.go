package constructor

import (
	"testing"

	mocks "github.com/coinbase/rosetta-sdk-go/mocks/constructor"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestWaitMessage(t *testing.T) {
	tests := map[string]struct {
		input *FindBalanceInput

		message string
	}{
		"simple message": {
			input: &FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}}`,
		},
		"message with address": {
			input: &FindBalanceInput{
				Address: "hello",
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello"}`,
		},
		"message with address and subaccount": {
			input: &FindBalanceInput{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "sub hello",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello","sub_account":{"address":"sub hello"}}`,
		},
		"message with address and not address": {
			input: &FindBalanceInput{
				Address: "hello",
				NotAddress: []string{
					"good",
					"bye",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello"} != to addresses ["good","bye"]`,
		},
		"message with address and not coins": {
			input: &FindBalanceInput{
				Address: "hello",
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello"} != to coins [{"identifier":"coin1"}]`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.message, waitMessage(test.input))
		})
	}
}

func TestFindBalanceWorker(t *testing.T) {
	tests := map[string]struct {
		input *FindBalanceInput

		mockHelper *mocks.WorkerHelper

		output *FindBalanceOutput
		err    error
	}{
		"simple message": {
			input: &FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}}`,
		},
		"message with address": {
			input: &FindBalanceInput{
				Address: "hello",
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello"}`,
		},
		"message with address and subaccount": {
			input: &FindBalanceInput{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "sub hello",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello","sub_account":{"address":"sub hello"}}`,
		},
		"message with address and not address": {
			input: &FindBalanceInput{
				Address: "hello",
				NotAddress: []string{
					"good",
					"bye",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello"} != to addresses ["good","bye"]`,
		},
		"message with address and not coins": {
			input: &FindBalanceInput{
				Address: "hello",
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello"} != to coins [{"identifier":"coin1"}]`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.message, waitMessage(test.input))
		})
	}
}
