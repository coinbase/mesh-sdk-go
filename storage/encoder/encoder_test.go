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

package encoder

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func runCompressions(e *Encoder, t *testing.T) {
	for i := int64(0); i < 500; i++ {
		b := &types.BlockIdentifier{
			Index: i,
			Hash:  fmt.Sprintf("block %d", i),
		}

		bEnc, err := e.Encode("", b)
		assert.NoError(t, err)

		tx := &types.Transaction{
			TransactionIdentifier: &types.TransactionIdentifier{Hash: fmt.Sprintf("tx %d", i)},
		}
		txEnc, err := e.Encode("", tx)
		assert.NoError(t, err)

		block := &types.Block{
			BlockIdentifier:       b,
			ParentBlockIdentifier: b,
			Transactions:          []*types.Transaction{tx},
		}
		blockEnc, err := e.Encode("", block)
		assert.NoError(t, err)

		var bDec types.BlockIdentifier
		assert.NoError(t, e.Decode("", bEnc, &bDec, true))
		assert.Equal(t, types.Hash(b), types.Hash(bDec))

		var txDec types.Transaction
		assert.NoError(t, e.Decode("", txEnc, &txDec, true))
		assert.Equal(t, types.Hash(tx), types.Hash(txDec))

		var blockDec types.Block
		assert.NoError(t, e.Decode("", blockEnc, &blockDec, true))
		assert.Equal(t, types.Hash(block), types.Hash(blockDec))
	}
}

func TestEncoder(t *testing.T) {
	e, err := NewEncoder(nil, NewBufferPool(), true)
	assert.NoError(t, err)

	g, _ := errgroup.WithContext(context.Background())

	for i := 0; i < 10; i++ {
		g.Go(func() error {
			runCompressions(e, t)

			return nil
		})
	}

	assert.NoError(t, g.Wait())
}

var (
	benchmarkCoin = &types.AccountCoin{
		Account: &types.AccountIdentifier{
			Address: "hello",
		},
		Coin: &types.Coin{
			CoinIdentifier: &types.CoinIdentifier{
				Identifier: "coin1",
			},
			Amount: &types.Amount{
				Value: "100",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		},
	}

	complexCoin = &types.AccountCoin{
		Account: &types.AccountIdentifier{
			Address: "hello",
			SubAccount: &types.SubAccountIdentifier{
				Address: "sub",
				Metadata: map[string]interface{}{
					"test": "stuff",
				},
			},
		},
		Coin: &types.Coin{
			CoinIdentifier: &types.CoinIdentifier{
				Identifier: "coin1",
			},
			Amount: &types.Amount{
				Value: "100",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{
						"issuer": "satoshi",
					},
				},
			},
		},
	}
)

func BenchmarkAccountCoinStandard(b *testing.B) {
	e, _ := NewEncoder(nil, NewBufferPool(), true)

	for i := 0; i < b.N; i++ {
		// encode
		compressedResult, _ := e.Encode("", benchmarkCoin)

		// decode
		var decoded types.AccountCoin
		_ = e.Decode("", compressedResult, &decoded, true)
	}
}

func BenchmarkComplexAccountCoinStandard(b *testing.B) {
	e, _ := NewEncoder(nil, NewBufferPool(), true)

	for i := 0; i < b.N; i++ {
		// encode
		compressedResult, _ := e.Encode("", complexCoin)

		// decode
		var decoded types.AccountCoin
		_ = e.Decode("", compressedResult, &decoded, true)
	}
}

func BenchmarkAccountCoinOptimized(b *testing.B) {
	e, _ := NewEncoder(nil, NewBufferPool(), true)

	for i := 0; i < b.N; i++ {
		// encode
		manualResult, _ := e.EncodeAccountCoin(benchmarkCoin)

		// decode
		var decoded types.AccountCoin
		_ = e.DecodeAccountCoin(manualResult, &decoded, true)
	}
}
func BenchmarkComplexAccountCoinOptimized(b *testing.B) {
	e, _ := NewEncoder(nil, NewBufferPool(), true)

	for i := 0; i < b.N; i++ {
		// encode
		manualResult, _ := e.EncodeAccountCoin(complexCoin)

		// decode
		var decoded types.AccountCoin
		_ = e.DecodeAccountCoin(manualResult, &decoded, true)
	}
}

func TestEncodeDecodeAccountCoin(t *testing.T) {
	tests := map[string]struct {
		accountCoin *types.AccountCoin
	}{
		"simple": {
			accountCoin: benchmarkCoin,
		},
		"sub account info": {
			accountCoin: &types.AccountCoin{
				Account: &types.AccountIdentifier{
					Address: "hello",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub",
						Metadata: map[string]interface{}{
							"test": "stuff",
						},
					},
				},
				Coin: &types.Coin{
					CoinIdentifier: &types.CoinIdentifier{
						Identifier: "coin1",
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
		},
		"currency metadata": {
			accountCoin: &types.AccountCoin{
				Account: &types.AccountIdentifier{
					Address: "hello",
				},
				Coin: &types.Coin{
					CoinIdentifier: &types.CoinIdentifier{
						Identifier: "coin1",
					},
					Amount: &types.Amount{
						Value: "100",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
							Metadata: map[string]interface{}{
								"issuer": "satoshi",
							},
						},
					},
				},
			},
		},
	}

	for name, test := range tests {
		e, err := NewEncoder(nil, NewBufferPool(), true)
		assert.NoError(t, err)

		t.Run(name, func(t *testing.T) {
			standardResult, err := e.Encode("", test.accountCoin)
			assert.NoError(t, err)
			optimizedResult, err := e.EncodeAccountCoin(test.accountCoin)
			assert.NoError(t, err)
			fmt.Printf(
				"Uncompressed: %d, Standard Compressed: %d, Optimized: %d\n",
				len(types.PrintStruct(test.accountCoin)),
				len(standardResult),
				len(optimizedResult),
			)

			var decoded types.AccountCoin
			assert.NoError(t, e.DecodeAccountCoin(optimizedResult, &decoded, true))

			assert.Equal(t, test.accountCoin, &decoded)
		})
	}
}

func TestEncodeDecodeAccountCurrency(t *testing.T) {
	tests := map[string]struct {
		accountCurrency *types.AccountCurrency
	}{
		"simple": {
			accountCurrency: &types.AccountCurrency{
				Account: &types.AccountIdentifier{
					Address: "hello",
				},
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		},
		"sub account info": {
			accountCurrency: &types.AccountCurrency{
				Account: &types.AccountIdentifier{
					Address: "hello",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub",
						Metadata: map[string]interface{}{
							"test": "stuff",
						},
					},
				},
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		},
		"currency metadata": {
			accountCurrency: &types.AccountCurrency{
				Account: &types.AccountIdentifier{
					Address: "hello",
				},
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
					Metadata: map[string]interface{}{
						"issuer": "satoshi",
					},
				},
			},
		},
	}

	for name, test := range tests {
		e, err := NewEncoder(nil, NewBufferPool(), true)
		assert.NoError(t, err)

		t.Run(name, func(t *testing.T) {
			standardResult, err := e.Encode("", test.accountCurrency)
			assert.NoError(t, err)
			optimizedResult, err := e.EncodeAccountCurrency(test.accountCurrency)
			assert.NoError(t, err)
			fmt.Printf(
				"Uncompressed: %d, Standard Compressed: %d, Optimized: %d\n",
				len(types.PrintStruct(test.accountCurrency)),
				len(standardResult),
				len(optimizedResult),
			)

			var decoded types.AccountCurrency
			assert.NoError(t, e.DecodeAccountCurrency(optimizedResult, &decoded, true))

			assert.Equal(t, test.accountCurrency, &decoded)
		})
	}
}
