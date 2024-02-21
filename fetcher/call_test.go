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

func TestCallRetry(t *testing.T) {
	var (
		basicMethod = "call"
		basicParams = map[string]interface{}{
			"hello": "cool",
		}
		basicResult = map[string]interface{}{
			"bye": "neat",
		}
	)

	var tests = map[string]struct {
		network    *types.NetworkIdentifier
		method     string
		parameters map[string]interface{}

		errorsBeforeSuccess int
		expectedResult      map[string]interface{}
		expectedIdempotency bool
		expectedError       error
		retriableError      bool

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			network:             basicNetwork,
			method:              basicMethod,
			parameters:          basicParams,
			expectedResult:      basicResult,
			expectedIdempotency: true,
			fetcherMaxRetries:   5,
		},
		"retry failures": {
			network:             basicNetwork,
			method:              basicMethod,
			parameters:          basicParams,
			errorsBeforeSuccess: 2,
			expectedResult:      basicResult,
			expectedIdempotency: true,
			fetcherMaxRetries:   5,
			retriableError:      true,
		},
		"non-retriable error": {
			network:             basicNetwork,
			method:              basicMethod,
			parameters:          basicParams,
			errorsBeforeSuccess: 2,
			fetcherMaxRetries:   5,
			expectedError:       ErrRequestFailed,
		},
		"exhausted retries": {
			network:             basicNetwork,
			method:              basicMethod,
			parameters:          basicParams,
			errorsBeforeSuccess: 2,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
			retriableError:      true,
		},
		"cancel context": {
			network:             basicNetwork,
			method:              basicMethod,
			parameters:          basicParams,
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
				endpoint    = "/call"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				expected := &types.CallRequest{
					NetworkIdentifier: test.network,
					Method:            test.method,
					Parameters:        test.parameters,
				}
				var callRequest *types.CallRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&callRequest))
				assert.Equal(expected, callRequest)

				if test.shouldCancel {
					cancel()
				}

				if tries < test.errorsBeforeSuccess {
					w.Header().Set("Content-Type", "application/json; charset=UTF-8")
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintln(w, types.PrettyPrintStruct(&types.Error{
						Retriable: test.retriableError,
					}))
					tries++
					return
				}

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, types.PrettyPrintStruct(
					&types.CallResponse{
						Result:     test.expectedResult,
						Idempotent: test.expectedIdempotency,
					},
				))
			}))

			defer ts.Close()

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
			)
			result, idempotency, err := f.CallRetry(
				ctx,
				test.network,
				test.method,
				test.parameters,
			)
			assert.Equal(test.expectedResult, result)
			assert.Equal(test.expectedIdempotency, idempotency)
			assert.True(checkError(err, test.expectedError))
		})
	}
}
