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

package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func runCompressions(c *Compressor, t *testing.T) {
	for i := int64(0); i < 500; i++ {
		b := &types.BlockIdentifier{
			Index: i,
			Hash:  fmt.Sprintf("block %d", i),
		}

		bEnc, err := c.Encode("", b)
		assert.NoError(t, err)

		tx := &types.Transaction{
			TransactionIdentifier: &types.TransactionIdentifier{Hash: fmt.Sprintf("tx %d", i)},
		}
		txEnc, err := c.Encode("", tx)
		assert.NoError(t, err)

		block := &types.Block{
			BlockIdentifier:       b,
			ParentBlockIdentifier: b,
			Transactions:          []*types.Transaction{tx},
		}
		blockEnc, err := c.Encode("", block)
		assert.NoError(t, err)

		var bDec types.BlockIdentifier
		assert.NoError(t, c.Decode("", bEnc, &bDec, true))
		assert.Equal(t, types.Hash(b), types.Hash(bDec))

		var txDec types.Transaction
		assert.NoError(t, c.Decode("", txEnc, &txDec, true))
		assert.Equal(t, types.Hash(tx), types.Hash(txDec))

		var blockDec types.Block
		assert.NoError(t, c.Decode("", blockEnc, &blockDec, true))
		assert.Equal(t, types.Hash(block), types.Hash(blockDec))
	}
}

func TestCompressor(t *testing.T) {
	c, err := NewCompressor(nil, NewBufferPool())
	assert.NoError(t, err)

	g, _ := errgroup.WithContext(context.Background())

	for i := 0; i < 10; i++ {
		g.Go(func() error {
			runCompressions(c, t)

			return nil
		})
	}

	assert.NoError(t, g.Wait())
}

var (
	benchmarkCoin = &AccountCoin{
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

	complexCoin = &AccountCoin{
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
	c, _ := NewCompressor(nil, NewBufferPool())

	for i := 0; i < b.N; i++ {
		// encode
		compressedResult, _ := c.Encode("", benchmarkCoin)

		// decode
		var decoded AccountCoin
		_ = c.Decode("", compressedResult, &decoded, true)
	}
}

func BenchmarkComplexAccountCoinStandard(b *testing.B) {
	c, _ := NewCompressor(nil, NewBufferPool())

	for i := 0; i < b.N; i++ {
		// encode
		compressedResult, _ := c.Encode("", complexCoin)

		// decode
		var decoded AccountCoin
		_ = c.Decode("", compressedResult, &decoded, true)
	}
}

func BenchmarkAccountCoinOptimized(b *testing.B) {
	c, _ := NewCompressor(nil, NewBufferPool())

	for i := 0; i < b.N; i++ {
		// encode
		manualResult, _ := c.EncodeAccountCoin(benchmarkCoin)

		// decode
		var decoded AccountCoin
		_ = c.DecodeAccountCoin(manualResult, &decoded, true)
	}
}
func BenchmarkComplexAccountCoinOptimized(b *testing.B) {
	c, _ := NewCompressor(nil, NewBufferPool())

	for i := 0; i < b.N; i++ {
		// encode
		manualResult, _ := c.EncodeAccountCoin(complexCoin)

		// decode
		var decoded AccountCoin
		_ = c.DecodeAccountCoin(manualResult, &decoded, true)
	}
}

func TestEncodeDecodeAccountCoin(t *testing.T) {
	tests := map[string]struct {
		accountCoin *AccountCoin
	}{
		"simple": {
			accountCoin: benchmarkCoin,
		},
		"sub account info": {
			accountCoin: &AccountCoin{
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
			accountCoin: &AccountCoin{
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
		c, err := NewCompressor(nil, NewBufferPool())
		assert.NoError(t, err)

		t.Run(name, func(t *testing.T) {
			standardResult, err := c.Encode("", test.accountCoin)
			assert.NoError(t, err)
			optimizedResult, err := c.EncodeAccountCoin(test.accountCoin)
			assert.NoError(t, err)
			fmt.Printf(
				"Uncompressed: %d, Standard Compressed: %d, Optimized: %d\n",
				len(types.PrintStruct(test.accountCoin)),
				len(standardResult),
				len(optimizedResult),
			)

			var decoded AccountCoin
			assert.NoError(t, c.DecodeAccountCoin(optimizedResult, &decoded, true))

			assert.Equal(t, test.accountCoin, &decoded)
		})
	}
}
