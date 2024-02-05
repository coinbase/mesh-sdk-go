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

package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	basicNetwork = &types.NetworkIdentifier{
		Blockchain: "blockchain",
		Network:    "network",
	}

	basicAccount = &types.AccountIdentifier{
		Address: "address",
	}

	basicBlock = &types.BlockIdentifier{
		Index: 10,
		Hash:  "block 10",
	}

	basicAmounts = []*types.Amount{
		{
			Value: "1000",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		},
	}

	basicCoins = []*types.Coin{
		{
			Amount: &types.Amount{
				Value: "1000",
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
			CoinIdentifier: &types.CoinIdentifier{
				Identifier: "coin",
			},
		},
	}
)

func TestAccountBalanceRetry(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier
		account *types.AccountIdentifier

		errorsBeforeSuccess int
		expectedBlock       *types.BlockIdentifier
		expectedAmounts     []*types.Amount
		expectedError       error
		retriableError      bool
		non500Error         bool

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			network:           basicNetwork,
			account:           basicAccount,
			expectedBlock:     basicBlock,
			expectedAmounts:   basicAmounts,
			fetcherMaxRetries: 5,
		},
		"retry failures": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 2,
			expectedBlock:       basicBlock,
			expectedAmounts:     basicAmounts,
			fetcherMaxRetries:   5,
			retriableError:      true,
		},
		"non-500 retry failures": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 2,
			expectedBlock:       basicBlock,
			expectedAmounts:     basicAmounts,
			fetcherMaxRetries:   5,
			non500Error:         true,
		},
		"non-retriable error": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 2,
			fetcherMaxRetries:   5,
			expectedError:       ErrRequestFailed,
		},
		"exhausted retries": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 2,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
			retriableError:      true,
		},
		"cancel context": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 6,
			expectedError:       context.Canceled,
			fetcherMaxRetries:   5,
			shouldCancel:        true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				tries       = 0
				assert      = assert.New(t)
				ctx, cancel = context.WithCancel(context.Background())
				endpoint    = "/account/balance"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				expected := &types.AccountBalanceRequest{
					NetworkIdentifier: test.network,
					AccountIdentifier: test.account,
				}
				var accountRequest *types.AccountBalanceRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&accountRequest))
				assert.Equal(expected, accountRequest)

				if test.shouldCancel {
					cancel()
				}

				if tries < test.errorsBeforeSuccess {
					if test.non500Error {
						w.Header().Set("Content-Type", "html/text; charset=UTF-8")
						w.WriteHeader(http.StatusGatewayTimeout)
						fmt.Fprintln(w, "blah")
					} else {
						w.Header().Set("Content-Type", "application/json; charset=UTF-8")
						w.WriteHeader(http.StatusInternalServerError)
						fmt.Fprintln(w, types.PrettyPrintStruct(&types.Error{
							Retriable: test.retriableError,
						}))
					}
					tries++
					return
				}

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, types.PrettyPrintStruct(
					&types.AccountBalanceResponse{
						BlockIdentifier: test.expectedBlock,
						Balances:        test.expectedAmounts,
					},
				))
			}))

			defer ts.Close()

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
			)
			block, amounts, metadata, err := f.AccountBalanceRetry(
				ctx,
				test.network,
				test.account,
				nil,
				nil,
			)
			assert.Equal(test.expectedBlock, block)
			assert.Equal(test.expectedAmounts, amounts)
			assert.Nil(metadata)
			assert.True(checkError(err, test.expectedError))
		})
	}
}

func TestAccountCoinsRetry(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier
		account *types.AccountIdentifier

		errorsBeforeSuccess int
		expectedBlock       *types.BlockIdentifier
		expectedCoins       []*types.Coin
		expectedError       error
		retriableError      bool
		non500Error         bool

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			network:           basicNetwork,
			account:           basicAccount,
			expectedBlock:     basicBlock,
			expectedCoins:     basicCoins,
			fetcherMaxRetries: 5,
		},
		"retry failures": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 2,
			expectedBlock:       basicBlock,
			expectedCoins:       basicCoins,
			fetcherMaxRetries:   5,
			retriableError:      true,
		},
		"non-500 retry failures": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 2,
			expectedBlock:       basicBlock,
			expectedCoins:       basicCoins,
			fetcherMaxRetries:   5,
			non500Error:         true,
		},
		"non-retriable error": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 2,
			fetcherMaxRetries:   5,
			expectedError:       ErrRequestFailed,
		},
		"exhausted retries": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 2,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
			retriableError:      true,
		},
		"cancel context": {
			network:             basicNetwork,
			account:             basicAccount,
			errorsBeforeSuccess: 6,
			expectedError:       context.Canceled,
			fetcherMaxRetries:   5,
			shouldCancel:        true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				tries       = 0
				assert      = assert.New(t)
				ctx, cancel = context.WithCancel(context.Background())
				endpoint    = "/account/coins"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				expected := &types.AccountCoinsRequest{
					NetworkIdentifier: test.network,
					AccountIdentifier: test.account,
				}
				var accountRequest *types.AccountCoinsRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&accountRequest))
				assert.Equal(expected, accountRequest)

				if test.shouldCancel {
					cancel()
				}

				if tries < test.errorsBeforeSuccess {
					if test.non500Error {
						w.Header().Set("Content-Type", "html/text; charset=UTF-8")
						w.WriteHeader(http.StatusGatewayTimeout)
						fmt.Fprintln(w, "blah")
					} else {
						w.Header().Set("Content-Type", "application/json; charset=UTF-8")
						w.WriteHeader(http.StatusInternalServerError)
						fmt.Fprintln(w, types.PrettyPrintStruct(&types.Error{
							Retriable: test.retriableError,
						}))
					}
					tries++
					return
				}

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, types.PrettyPrintStruct(
					&types.AccountCoinsResponse{
						BlockIdentifier: test.expectedBlock,
						Coins:           test.expectedCoins,
					},
				))
			}))

			defer ts.Close()

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
			)
			block, coins, metadata, err := f.AccountCoinsRetry(
				ctx,
				test.network,
				test.account,
				false,
				nil,
			)
			assert.Equal(test.expectedBlock, block)
			assert.Equal(test.expectedCoins, coins)
			assert.Nil(metadata)
			assert.True(checkError(err, test.expectedError))
		})
	}
}
