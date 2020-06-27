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

package fetcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
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
)

func TestBlockRetry(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier
		block   *types.BlockIdentifier

		errorsBeforeSuccess int
		expectedBlock       *types.Block
		expectedError       error

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			network:           basicNetwork,
			block:             basicBlock,
			expectedBlock:     basicFullBlock,
			fetcherMaxRetries: 5,
		},
		"retry failures": {
			network:             basicNetwork,
			block:               basicBlock,
			errorsBeforeSuccess: 2,
			expectedBlock:       basicFullBlock,
			fetcherMaxRetries:   5,
		},
		"exhausted retries": {
			network:             basicNetwork,
			block:               basicBlock,
			errorsBeforeSuccess: 2,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
		},
		"cancel context": {
			network:             basicNetwork,
			block:               basicBlock,
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
				endpoint    = "/block"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				expected := &types.BlockRequest{
					NetworkIdentifier: test.network,
					BlockIdentifier:   types.ConstructPartialBlockIdentifier(test.block),
				}
				var blockRequest *types.BlockRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&blockRequest))
				assert.Equal(expected, blockRequest)

				if test.shouldCancel {
					cancel()
				}

				if tries < test.errorsBeforeSuccess {
					w.Header().Set("Content-Type", "application/json; charset=UTF-8")
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintln(w, "{}")
					tries++
					return
				}

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, types.PrettyPrintStruct(
					&types.BlockResponse{
						Block: test.expectedBlock,
					},
				))
			}))

			defer ts.Close()
			a, err := asserter.NewWithOptions(
				basicNetwork,
				&types.BlockIdentifier{
					Index: 0,
					Hash:  "block 0",
				},
				basicNetworkOptions.Allow.OperationTypes,
				basicNetworkOptions.Allow.OperationStatuses,
				nil,
				false,
			)
			assert.NoError(err)

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
				WithAsserter(a),
			)
			block, err := f.BlockRetry(
				ctx,
				test.network,
				types.ConstructPartialBlockIdentifier(test.block),
			)
			assert.Equal(test.expectedBlock, block)
			assert.True(errors.Is(err, test.expectedError))
		})
	}
}
