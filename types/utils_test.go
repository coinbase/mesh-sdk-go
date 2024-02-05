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

package types

import (
	"encoding/json"
	"errors"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstructPartialBlockIdentifier(t *testing.T) {
	blockIdentifier := &BlockIdentifier{
		Index: 1,
		Hash:  "block 1",
	}

	partialBlockIdentifier := &PartialBlockIdentifier{
		Index: &blockIdentifier.Index,
		Hash:  &blockIdentifier.Hash,
	}

	assert.Equal(
		t,
		partialBlockIdentifier,
		ConstructPartialBlockIdentifier(blockIdentifier),
	)
}

func TestHash(t *testing.T) {
	var tests = map[string][]interface{}{
		"simple": {
			1,
			1,
		},
		"complex": {
			map[string]interface{}{
				"a": "b",
				"b": "c",
				"c": "d",
				"blahz": json.RawMessage(
					`{"test":6, "wha":{"sweet":3, "nice":true}, "neat0":"hello"}`,
				),
				"d": map[string]interface{}{
					"t": "p",
					"e": 2,
					"k": "l",
					"blah": json.RawMessage(
						`{"test":2, "neat":"hello", "cool":{"sweet":3, "nice":true}}`,
					),
				},
			},
			map[string]interface{}{
				"b": "c",
				"blahz": json.RawMessage(
					`{"wha":{"sweet":3, "nice":true},"test":6, "neat0":"hello"}`,
				),
				"a": "b",
				"d": map[string]interface{}{
					"e": 2,
					"k": "l",
					"t": "p",
					"blah": json.RawMessage(
						`{"test":2, "neat":"hello", "cool":{"nice":true, "sweet":3}}`,
					),
				},
				"c": "d",
			},
			map[string]interface{}{
				"a": "b",
				"d": map[string]interface{}{
					"k": "l",
					"t": "p",
					"blah": json.RawMessage(
						`{"test":2, "cool":{"nice":true, "sweet":3}, "neat":"hello"}`,
					),
					"e": 2,
				},
				"c": "d",
				"blahz": json.RawMessage(
					`{"wha":{"nice":true, "sweet":3},"test":6, "neat0":"hello"}`,
				),
				"b": "c",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var val string
			for _, v := range test {
				if val == "" {
					val = Hash(v)
				} else {
					assert.Equal(t, val, Hash(v))
				}
			}
		})
	}
}

func TestAddValues(t *testing.T) {
	var tests = map[string]struct {
		a      string
		b      string
		result string
		err    error
	}{
		"simple": {
			a:      "1",
			b:      "1",
			result: "2",
			err:    nil,
		},
		"large": {
			a:      "1000000000000000000000000",
			b:      "100000000000000000000000000000000",
			result: "100000001000000000000000000000000",
			err:    nil,
		},
		"decimal": {
			a:      "10000000000000000000000.01",
			b:      "100000000000000000000000000000000",
			result: "",
			err:    errors.New("10000000000000000000000.01 is not an integer"),
		},
		"negative": {
			a:      "-13213",
			b:      "12332",
			result: "-881",
			err:    nil,
		},
		"invalid number": {
			a:      "-13213",
			b:      "hello",
			result: "",
			err:    errors.New("hello is not an integer"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := AddValues(test.a, test.b)
			assert.Equal(t, test.err, err)
			assert.Equal(t, test.result, result)
		})
	}
}

func TestSubtractValues(t *testing.T) {
	var tests = map[string]struct {
		a      string
		b      string
		result string
		err    error
	}{
		"simple": {
			a:      "1",
			b:      "1",
			result: "0",
			err:    nil,
		},
		"large": {
			a:      "1000000000000000000000000",
			b:      "100000000000000000000000000000000",
			result: "-99999999000000000000000000000000",
			err:    nil,
		},
		"decimal": {
			a:      "10000000000000000000000.01",
			b:      "100000000000000000000000000000000",
			result: "",
			err:    errors.New("10000000000000000000000.01 is not an integer"),
		},
		"negative": {
			a:      "-13213",
			b:      "12332",
			result: "-25545",
			err:    nil,
		},
		"invalid number": {
			a:      "-13213",
			b:      "hello",
			result: "",
			err:    errors.New("hello is not an integer"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := SubtractValues(test.a, test.b)
			assert.Equal(t, test.err, err)
			assert.Equal(t, test.result, result)
		})
	}
}

func TestNegateValue(t *testing.T) {
	var tests = map[string]struct {
		val    string
		result string
		err    error
	}{
		"positive number": {
			val:    "100",
			result: "-100",
			err:    nil,
		},
		"negative number": {
			val:    "-100",
			result: "100",
			err:    nil,
		},
		"decimal number": {
			val:    "-100.1",
			result: "",
			err:    errors.New("-100.1 is not an integer"),
		},
		"non-number": {
			val:    "hello",
			result: "",
			err:    errors.New("hello is not an integer"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := NegateValue(test.val)
			assert.Equal(t, test.result, result)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestGetAccountString(t *testing.T) {
	var tests = map[string]struct {
		account *AccountIdentifier
		err     bool
		key     string
	}{
		"simple account": {
			account: &AccountIdentifier{
				Address: "hello",
			},
			key: "hello",
		},
		"subaccount": {
			account: &AccountIdentifier{
				Address: "hello",
				SubAccount: &SubAccountIdentifier{
					Address: "stake",
				},
			},
			key: "hello:stake",
		},
		"subaccount with string metadata": {
			account: &AccountIdentifier{
				Address: "hello",
				SubAccount: &SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool": "neat",
					},
				},
			},
			key: "hello:stake:map[cool:neat]",
		},
		"subaccount with number metadata": {
			account: &AccountIdentifier{
				Address: "hello",
				SubAccount: &SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool": 1,
					},
				},
			},
			key: "hello:stake:map[cool:1]",
		},
		"subaccount with complex metadata": {
			account: &AccountIdentifier{
				Address: "hello",
				SubAccount: &SubAccountIdentifier{
					Address: "stake",
					Metadata: map[string]interface{}{
						"cool":    1,
						"awesome": "neat",
					},
				},
			},
			key: "hello:stake:map[awesome:neat cool:1]",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			accountString := AccountString(test.account)
			assert.Equal(t, test.key, accountString)
		})
	}
}

func TestCurrencyString(t *testing.T) {
	var tests = map[string]struct {
		currency *Currency
		key      string
	}{
		"simple currency": {
			currency: &Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
			key: "BTC:8",
		},
		"currency with string metadata": {
			currency: &Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": "satoshi",
				},
			},
			key: "BTC:8:map[issuer:satoshi]",
		},
		"currency with number metadata": {
			currency: &Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": 1,
				},
			},
			key: "BTC:8:map[issuer:1]",
		},
		"currency with complex metadata": {
			currency: &Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": "satoshi",
					"count":  10,
				},
			},
			key: "BTC:8:map[count:10 issuer:satoshi]",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			currencyString := CurrencyString(test.currency)
			assert.Equal(t, test.key, currencyString)
		})
	}
}

func TestMarshalMap(t *testing.T) {
	var tests = map[string]struct {
		input  interface{}
		result map[string]interface{}

		err bool
	}{
		"currency": {
			input: &Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": "test",
				},
			},
			result: map[string]interface{}{
				"symbol":   "BTC",
				"decimals": int32(8),
				"metadata": map[string]interface{}{
					"issuer": "test",
				},
			},
		},
		"block": {
			input: &Block{
				BlockIdentifier: &BlockIdentifier{
					Index: 100,
					Hash:  "block 100",
				},
				ParentBlockIdentifier: &BlockIdentifier{
					Index: 99,
					Hash:  "block 99",
				},
				Timestamp:    1000,
				Transactions: []*Transaction{},
			},
			result: map[string]interface{}{
				"block_identifier": &BlockIdentifier{
					Index: 100,
					Hash:  "block 100",
				},
				"parent_block_identifier": &BlockIdentifier{
					Index: 99,
					Hash:  "block 99",
				},
				"timestamp":    int64(1000),
				"transactions": []*Transaction{},
			},
		},
		"nil": {
			input:  nil,
			result: nil,
		},
		"non-map": {
			input:  []string{"hello", "hi"},
			result: nil,
			err:    true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := MarshalMap(test.input)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, test.result, result)
		})
	}
}

func TestUnmarshalMap(t *testing.T) {
	var tests = map[string]struct {
		input        map[string]interface{}
		outputStruct interface{}
		result       interface{}

		err error
	}{
		"simple": {
			input: map[string]interface{}{
				"symbol":   "BTC",
				"decimals": 8,
				"metadata": map[string]interface{}{
					"issuer": "test",
				},
			},
			outputStruct: &Currency{},
			result: &Currency{
				Symbol:   "BTC",
				Decimals: 8,
				Metadata: map[string]interface{}{
					"issuer": "test",
				},
			},
			err: nil,
		},
		"block": {
			input: map[string]interface{}{
				"block_identifier": &BlockIdentifier{
					Index: 100,
					Hash:  "block 100",
				},
				"parent_block_identifier": &BlockIdentifier{
					Index: 99,
					Hash:  "block 99",
				},
				"timestamp":    1000,
				"transactions": []*Transaction{},
			},
			outputStruct: &Block{},
			result: &Block{
				BlockIdentifier: &BlockIdentifier{
					Index: 100,
					Hash:  "block 100",
				},
				ParentBlockIdentifier: &BlockIdentifier{
					Index: 99,
					Hash:  "block 99",
				},
				Timestamp:    1000,
				Transactions: []*Transaction{},
			},
			err: nil,
		},
		"block raw": {
			input: map[string]interface{}{
				"block_identifier": map[string]interface{}{
					"index": 100,
					"hash":  "block 100",
				},
				"parent_block_identifier": map[string]interface{}{
					"index": 99,
					"hash":  "block 99",
				},
				"timestamp": 1000,
			},
			outputStruct: &Block{},
			result: &Block{
				BlockIdentifier: &BlockIdentifier{
					Index: 100,
					Hash:  "block 100",
				},
				ParentBlockIdentifier: &BlockIdentifier{
					Index: 99,
					Hash:  "block 99",
				},
				Timestamp: 1000,
			},
			err: nil,
		},
		"struct mismatch": {
			input: map[string]interface{}{
				"block_identifier": &BlockIdentifier{
					Index: 100,
					Hash:  "block 100",
				},
				"parent_block_identifier": &BlockIdentifier{
					Index: 99,
					Hash:  "block 99",
				},
				"timestamp":    1000,
				"transactions": []*Transaction{},
			},
			outputStruct: &Currency{},
			result:       &Currency{},
			err:          nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := UnmarshalMap(test.input, &test.outputStruct)
			assert.Equal(t, test.err, err)
			assert.Equal(t, test.result, test.outputStruct)
		})
	}
}

func TestAmountValue(t *testing.T) {
	var tests = map[string]struct {
		amount *Amount
		result *big.Int
		err    error
	}{
		"positive integer": {
			amount: &Amount{Value: "100"},
			result: big.NewInt(100),
		},
		"negative integer": {
			amount: &Amount{Value: "-100"},
			result: big.NewInt(-100),
		},
		"nil": {
			err: errors.New("amount value cannot be nil"),
		},
		"float": {
			amount: &Amount{Value: "100.1"},
			err:    errors.New("100.1 is not an integer"),
		},
		"not number": {
			amount: &Amount{Value: "hello"},
			err:    errors.New("hello is not an integer"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)

			result, err := AmountValue(test.amount)
			assert.Equal(test.result, result)
			assert.Equal(test.err, err)
		})
	}
}

func TestExtractAmount(t *testing.T) {
	var (
		currency1 = &Currency{
			Symbol:   "curr1",
			Decimals: 4,
		}

		currency2 = &Currency{
			Symbol:   "curr2",
			Decimals: 7,
		}

		amount1 = &Amount{
			Value:    "100",
			Currency: currency1,
		}

		amount2 = &Amount{
			Value:    "200",
			Currency: currency2,
		}

		balances = []*Amount{
			amount1,
			amount2,
		}

		badCurr = &Currency{
			Symbol:   "no curr",
			Decimals: 100,
		}
	)

	t.Run("Non-existent currency", func(t *testing.T) {
		result := ExtractAmount(balances, badCurr)
		assert.Equal(
			t,
			result.Value,
			"0",
		)
	})

	t.Run("Simple account", func(t *testing.T) {
		result := ExtractAmount(balances, currency1)
		assert.Equal(t, amount1, result)
	})

	t.Run("SubAccount", func(t *testing.T) {
		result := ExtractAmount(balances, currency2)
		assert.Equal(t, amount2, result)
	})
}
