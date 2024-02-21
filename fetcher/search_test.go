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
	basicSearchTransactionsRequest = &types.SearchTransactionsRequest{
		NetworkIdentifier: basicNetwork,
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "tx",
		},
	}

	nextOffset             = types.Int64(1)
	basicBlockTransactions = []*types.BlockTransaction{
		{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "block",
				Index: 1,
			},
			Transaction: basicTransaction,
		},
	}
)

func TestSearchTransactionsRetry(t *testing.T) {
	var tests = map[string]struct {
		errorsBeforeSuccess       int
		expectedNextOffset        *int64
		expectedBlockTransactions []*types.BlockTransaction
		expectedError             error
		retriableError            bool
		non500Error               bool

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			expectedNextOffset:        nextOffset,
			expectedBlockTransactions: basicBlockTransactions,
			fetcherMaxRetries:         5,
		},
		"retry failures": {
			expectedNextOffset:        nextOffset,
			expectedBlockTransactions: basicBlockTransactions,
			errorsBeforeSuccess:       2,
			fetcherMaxRetries:         5,
			retriableError:            true,
		},
		"non-500 retry failures": {
			expectedNextOffset:        nextOffset,
			expectedBlockTransactions: basicBlockTransactions,
			errorsBeforeSuccess:       2,
			fetcherMaxRetries:         5,
			non500Error:               true,
		},
		"non-retriable error": {
			errorsBeforeSuccess: 2,
			fetcherMaxRetries:   5,
			expectedError:       ErrRequestFailed,
		},
		"exhausted retries": {
			errorsBeforeSuccess: 2,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
			retriableError:      true,
		},
		"cancel context": {
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
				endpoint    = "/search/transactions"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				var req *types.SearchTransactionsRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&req))
				assert.Equal(basicSearchTransactionsRequest, req)

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
					&types.SearchTransactionsResponse{
						NextOffset:   test.expectedNextOffset,
						Transactions: test.expectedBlockTransactions,
					},
				))
			}))

			defer ts.Close()

			a, aerr := asserter.NewClientWithOptions(
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
			assert.NoError(aerr)

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
				WithAsserter(a),
			)
			off, txs, err := f.SearchTransactionsRetry(
				ctx,
				basicSearchTransactionsRequest,
			)
			fmt.Println(err)
			assert.Equal(test.expectedNextOffset, off)
			assert.Equal(test.expectedBlockTransactions, txs)
			assert.True(checkError(err, test.expectedError))
		})
	}
}
