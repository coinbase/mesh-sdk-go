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

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	basicFullBlock = &types.Block{
		BlockIdentifier: basicBlock,
		ParentBlockIdentifier: &types.BlockIdentifier{
			Index: 9,
			Hash:  "block 9",
		},
		Timestamp: 1582833600000,
	}

	basicTransaction = &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "tx 1",
		},
	}

	otherTransactions = []*types.TransactionIdentifier{
		basicTransaction.TransactionIdentifier,
	}

	basicBlockWithTransactions = &types.Block{
		BlockIdentifier: basicBlock,
		ParentBlockIdentifier: &types.BlockIdentifier{
			Index: 9,
			Hash:  "block 9",
		},
		Timestamp: 1582833600000,
		Transactions: []*types.Transaction{
			basicTransaction,
		},
	}
)

func TestBlockRetry(t *testing.T) {
	var tests = map[string]struct {
		network                   *types.NetworkIdentifier
		blockIdentifier           *types.BlockIdentifier
		containsOtherTransactions bool
		transaction               *types.Transaction
		blockResponse             *types.Block

		errorsBeforeSuccess            int
		transactionErrorsBeforeSuccess int
		expectedBlock                  *types.Block
		expectedError                  error
		retriableError                 bool
		retriableErrorTransaction      bool

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"omitted block": {
			network:           basicNetwork,
			blockIdentifier:   basicBlock,
			expectedBlock:     nil,
			fetcherMaxRetries: 0,
		},
		"no failures": {
			network:           basicNetwork,
			blockIdentifier:   basicBlock,
			blockResponse:     basicFullBlock,
			expectedBlock:     basicFullBlock,
			fetcherMaxRetries: 5,
		},
		"retry failures with other transactions": {
			network:                        basicNetwork,
			blockIdentifier:                basicBlock,
			blockResponse:                  basicFullBlock,
			transaction:                    basicTransaction,
			containsOtherTransactions:      true,
			transactionErrorsBeforeSuccess: 2,
			expectedBlock:                  basicBlockWithTransactions,
			fetcherMaxRetries:              5,
			retriableError:                 true,
			retriableErrorTransaction:      true,
		},
		"retry failures": {
			network:             basicNetwork,
			blockIdentifier:     basicBlock,
			blockResponse:       basicFullBlock,
			errorsBeforeSuccess: 2,
			expectedBlock:       basicFullBlock,
			fetcherMaxRetries:   5,
			retriableError:      true,
		},
		"non-retriable error": {
			network:             basicNetwork,
			blockIdentifier:     basicBlock,
			errorsBeforeSuccess: 2,
			fetcherMaxRetries:   5,
			expectedError:       ErrRequestFailed,
		},
		"non-retriable error with transactions": {
			network:                        basicNetwork,
			blockIdentifier:                basicBlock,
			blockResponse:                  basicFullBlock,
			transaction:                    basicTransaction,
			containsOtherTransactions:      true,
			transactionErrorsBeforeSuccess: 2,
			retriableError:                 true,
			expectedError:                  ErrRequestFailed,
			fetcherMaxRetries:              5,
		},
		"exhausted retries": {
			network:             basicNetwork,
			blockIdentifier:     basicBlock,
			errorsBeforeSuccess: 2,
			retriableError:      true,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
		},
		"cancel context": {
			network:             basicNetwork,
			blockIdentifier:     basicBlock,
			errorsBeforeSuccess: 6,
			retriableError:      true,
			expectedError:       context.Canceled,
			fetcherMaxRetries:   5,
			shouldCancel:        true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				blockTries       = 0
				transactionTries = 0
				assert           = assert.New(t)
				ctx, cancel      = context.WithCancel(context.Background())
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				if r.URL.RequestURI() == "/block" {
					expected := &types.BlockRequest{
						NetworkIdentifier: test.network,
						BlockIdentifier: types.ConstructPartialBlockIdentifier(
							test.blockIdentifier,
						),
					}
					var blockRequest *types.BlockRequest
					assert.NoError(json.NewDecoder(r.Body).Decode(&blockRequest))
					assert.Equal(expected, blockRequest)

					if test.shouldCancel {
						cancel()
					}

					if blockTries < test.errorsBeforeSuccess {
						w.Header().Set("Content-Type", "application/json; charset=UTF-8")
						w.WriteHeader(http.StatusInternalServerError)
						fmt.Fprintln(w, types.PrettyPrintStruct(&types.Error{
							Retriable: test.retriableError,
						}))
						blockTries++
						return
					}

					w.Header().Set("Content-Type", "application/json; charset=UTF-8")
					w.WriteHeader(http.StatusOK)
					resp := &types.BlockResponse{
						Block: test.blockResponse,
					}
					if test.containsOtherTransactions {
						resp.OtherTransactions = otherTransactions
					}

					fmt.Fprintln(w, types.PrettyPrintStruct(resp))
					return
				}

				assert.Equal("/block/transaction", r.URL.RequestURI())

				expected := &types.BlockTransactionRequest{
					NetworkIdentifier:     test.network,
					BlockIdentifier:       test.blockIdentifier,
					TransactionIdentifier: test.transaction.TransactionIdentifier,
				}

				var blockTransactionRequest types.BlockTransactionRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&blockTransactionRequest))
				assert.Equal(expected, &blockTransactionRequest)

				if transactionTries < test.transactionErrorsBeforeSuccess {
					w.Header().Set("Content-Type", "application/json; charset=UTF-8")
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintln(w, types.PrettyPrintStruct(&types.Error{
						Retriable: test.retriableErrorTransaction,
					}))
					transactionTries++
					return
				}

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, types.PrettyPrintStruct(&types.BlockTransactionResponse{
					Transaction: test.transaction,
				}))
			}))

			defer ts.Close()
			a, err := asserter.NewClientWithOptions(
				basicNetwork,
				&types.BlockIdentifier{
					Index: 0,
					Hash:  "block 0",
				},
				basicNetworkOptions.Allow.OperationTypes,
				basicNetworkOptions.Allow.OperationStatuses,
				nil,
				nil,
				&asserter.Validations{
					Enabled: false,
				},
			)
			assert.NoError(err)

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
				WithAsserter(a),
			)
			block, blockErr := f.BlockRetry(
				ctx,
				test.network,
				types.ConstructPartialBlockIdentifier(test.blockIdentifier),
			)
			assert.Equal(test.expectedBlock, block)
			assert.True(checkError(blockErr, test.expectedError))
		})
	}
}
