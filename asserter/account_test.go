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
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestContainsCurrency(t *testing.T) {
	var tests = map[string]struct {
		currencies []*types.Currency
		currency   *types.Currency
		contains   bool
	}{
		"simple contains": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			contains: true,
		},
		"complex contains": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah": "hello"},
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{"blah": "hello"},
			},
			contains: true,
		},
		"more complex contains": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah2": "bye", "blah": "hello"},
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{"blah": "hello", "blah2": "bye"},
			},
			contains: true,
		},
		"empty": {
			currencies: []*types.Currency{},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			contains: false,
		},
		"symbol mismatch": {
			currencies: []*types.Currency{
				{
					Symbol:   "ERX",
					Decimals: 8,
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 6,
			},
			contains: false,
		},
		"decimal mismatch": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 6,
			},
			contains: false,
		},
		"metadata mismatch": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah": "hello"},
				},
			},
			currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{"blah": "bye"},
			},
			contains: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			exists := ContainsCurrency(test.currencies, test.currency)
			assert.Equal(t, test.contains, exists)
		})
	}
}

func TestContainsDuplicateCurrency(t *testing.T) {
	var tests = map[string]struct {
		currencies []*types.Currency
		duplicate  bool
	}{
		"simple contains": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		},
		"complex contains": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah": "hello"},
				},
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah": "hello"},
				},
			},
			duplicate: true,
		},
		"more complex contains": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah2": "bye", "blah": "hello"},
				},
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah": "hello", "blah2": "bye"},
				},
			},
			duplicate: true,
		},
		"empty": {
			currencies: []*types.Currency{},
		},
		"symbol mismatch": {
			currencies: []*types.Currency{
				{
					Symbol:   "ERX",
					Decimals: 8,
				},
				{
					Symbol:   "BTC",
					Decimals: 6,
				},
			},
		},
		"decimal mismatch": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
				},
				{
					Symbol:   "BTC",
					Decimals: 6,
				},
			},
		},
		"metadata mismatch": {
			currencies: []*types.Currency{
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah": "hello"},
				},
				{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{"blah": "bye"},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			exists := ContainsDuplicateCurrency(test.currencies)
			if test.duplicate {
				assert.NotNil(t, exists)
			} else {
				assert.Nil(t, exists)
			}
		})
	}
}

func TestAccountBalance(t *testing.T) {
	var (
		validBlock = &types.BlockIdentifier{
			Index: 1000,
			Hash:  "jsakdl",
		}

		invalidBlock = &types.BlockIdentifier{
			Index: 1,
			Hash:  "",
		}

		invalidIndex = int64(1001)
		invalidHash  = "ajsdk"

		validAmount = &types.Amount{
			Value: "100",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	var tests = map[string]struct {
		requestBlock  *types.PartialBlockIdentifier
		responseBlock *types.BlockIdentifier
		balances      []*types.Amount
		metadata      map[string]interface{}
		err           error
	}{
		"simple balance": {
			responseBlock: validBlock,
			balances: []*types.Amount{
				validAmount,
			},
			err: nil,
		},
		"invalid block": {
			responseBlock: invalidBlock,
			balances: []*types.Amount{
				validAmount,
			},
			err: errors.New("BlockIdentifier.Hash is missing"),
		},
		"duplicate currency": {
			responseBlock: validBlock,
			balances: []*types.Amount{
				validAmount,
				validAmount,
			},
			err: ErrCurrencyUsedMultipleTimes,
		},
		"valid historical request index": {
			requestBlock: &types.PartialBlockIdentifier{
				Index: &validBlock.Index,
			},
			responseBlock: validBlock,
			balances: []*types.Amount{
				validAmount,
			},
			err: nil,
		},
		"valid historical request hash": {
			requestBlock: &types.PartialBlockIdentifier{
				Hash: &validBlock.Hash,
			},
			responseBlock: validBlock,
			balances: []*types.Amount{
				validAmount,
			},
			err: nil,
		},
		"valid historical request": {
			requestBlock:  types.ConstructPartialBlockIdentifier(validBlock),
			responseBlock: validBlock,
			balances: []*types.Amount{
				validAmount,
			},
			metadata: map[string]interface{}{"sequence": 1},
			err:      nil,
		},
		"invalid historical request index": {
			requestBlock: &types.PartialBlockIdentifier{
				Hash:  &validBlock.Hash,
				Index: &invalidIndex,
			},
			responseBlock: validBlock,
			balances: []*types.Amount{
				validAmount,
			},
			err: fmt.Errorf(
				"requested block index %d, but got %d: %w",
				invalidIndex,
				validBlock.Index,
				ErrReturnedBlockIndexMismatch,
			),
		},
		"invalid historical request hash": {
			requestBlock: &types.PartialBlockIdentifier{
				Hash:  &invalidHash,
				Index: &validBlock.Index,
			},
			responseBlock: validBlock,
			balances: []*types.Amount{
				validAmount,
			},
			err: fmt.Errorf(
				"requested block hash %s, but got %s: %w",
				invalidHash,
				validBlock.Hash,
				ErrReturnedBlockHashMismatch,
			),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := AccountBalanceResponse(
				test.requestBlock,
				&types.AccountBalanceResponse{
					BlockIdentifier: test.responseBlock,
					Balances:        test.balances,
				},
			)
			if test.err != nil {
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
