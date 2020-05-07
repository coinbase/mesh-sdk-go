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

package asserter

import (
	"errors"
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
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
			exists := containsCurrency(test.currencies, test.currency)
			assert.Equal(t, test.contains, exists)
		})
	}
}

func TestAccoutBalance(t *testing.T) {
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
			err: fmt.Errorf("currency %+v used in balance multiple times", validAmount.Currency),
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
				"request block index %d does not match response block index %d",
				invalidIndex,
				validBlock.Index,
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
				"request block hash %s does not match response block hash %s",
				invalidHash,
				validBlock.Hash,
			),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := AccountBalanceResponse(
				test.requestBlock,
				test.responseBlock,
				test.balances,
			)
			assert.Equal(t, test.err, err)
		})
	}
}
