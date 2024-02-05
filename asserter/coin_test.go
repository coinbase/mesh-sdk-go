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

package asserter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestCoin(t *testing.T) {
	var tests = map[string]struct {
		coin *types.Coin
		err  error
	}{
		"valid coin": {
			coin: &types.Coin{
				CoinIdentifier: &types.CoinIdentifier{
					Identifier: "coin1",
				},
				Amount: validAmount,
			},
			err: nil,
		},
		"nil": {
			coin: nil,
			err:  ErrCoinIsNil,
		},
		"invalid identifier": {
			coin: &types.Coin{
				CoinIdentifier: &types.CoinIdentifier{
					Identifier: "",
				},
				Amount: validAmount,
			},
			err: ErrCoinIdentifierNotSet,
		},
		"invalid amount": {
			coin: &types.Coin{
				CoinIdentifier: &types.CoinIdentifier{
					Identifier: "coin1",
				},
				Amount: &types.Amount{
					Value: "100",
				},
			},
			err: ErrAmountCurrencyIsNil,
		},
		"nil amount": {
			coin: &types.Coin{
				CoinIdentifier: &types.CoinIdentifier{
					Identifier: "coin1",
				},
			},
			err: ErrAmountValueMissing,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Coin(test.coin)
			if test.err != nil {
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCoins(t *testing.T) {
	var tests = map[string]struct {
		coins []*types.Coin
		err   error
	}{
		"valid coins": {
			coins: []*types.Coin{
				{
					CoinIdentifier: &types.CoinIdentifier{
						Identifier: "coin1",
					},
					Amount: validAmount,
				},
				{
					CoinIdentifier: &types.CoinIdentifier{
						Identifier: "coin2",
					},
					Amount: validAmount,
				},
			},
			err: nil,
		},
		"nil": {
			coins: nil,
			err:   nil,
		},
		"duplicate coins": {
			coins: []*types.Coin{
				{
					CoinIdentifier: &types.CoinIdentifier{
						Identifier: "coin1",
					},
					Amount: validAmount,
				},
				{
					CoinIdentifier: &types.CoinIdentifier{
						Identifier: "coin1",
					},
					Amount: validAmount,
				},
			},
			err: ErrCoinDuplicate,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Coins(test.coins)
			if test.err != nil {
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCoinChange(t *testing.T) {
	var tests = map[string]struct {
		change *types.CoinChange
		err    error
	}{
		"valid change": {
			change: &types.CoinChange{
				CoinIdentifier: &types.CoinIdentifier{
					Identifier: "coin1",
				},
				CoinAction: types.CoinCreated,
			},
			err: nil,
		},
		"nil": {
			change: nil,
			err:    ErrCoinChangeIsNil,
		},
		"invalid identifier": {
			change: &types.CoinChange{
				CoinIdentifier: &types.CoinIdentifier{
					Identifier: "",
				},
				CoinAction: types.CoinCreated,
			},
			err: ErrCoinIdentifierNotSet,
		},
		"invalid coin action": {
			change: &types.CoinChange{
				CoinIdentifier: &types.CoinIdentifier{
					Identifier: "coin1",
				},
				CoinAction: "hello",
			},
			err: ErrCoinActionInvalid,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := CoinChange(test.change)
			if test.err != nil {
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
