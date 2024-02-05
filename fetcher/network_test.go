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
	basicNetworkStatus = &types.NetworkStatusResponse{
		CurrentBlockIdentifier: basicBlock,
		CurrentBlockTimestamp:  1582833600000,
		GenesisBlockIdentifier: &types.BlockIdentifier{
			Index: 0,
			Hash:  "block 0",
		},
	}

	basicNetworkList = &types.NetworkListResponse{
		NetworkIdentifiers: []*types.NetworkIdentifier{
			basicNetwork,
		},
	}

	basicNetworkOptions = &types.NetworkOptionsResponse{
		Version: &types.Version{
			RosettaVersion: "1.4.0",
			NodeVersion:    "0.0.1",
		},
		Allow: &types.Allow{
			OperationStatuses: []*types.OperationStatus{
				{
					Status:     "SUCCESS",
					Successful: true,
				},
			},
			OperationTypes: []string{"transfer"},
		},
	}
)

func TestNetworkStatusRetry(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier

		errorsBeforeSuccess int
		expectedStatus      *types.NetworkStatusResponse
		expectedError       error
		retriableError      bool

		fetcherForceRetry bool
		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			network:           basicNetwork,
			expectedStatus:    basicNetworkStatus,
			fetcherMaxRetries: 5,
		},
		"retry failures": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedStatus:      basicNetworkStatus,
			fetcherMaxRetries:   5,
			retriableError:      true,
		},
		"non-retriable failure": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedError:       ErrRequestFailed,
			fetcherMaxRetries:   5,
		},
		"non-retriable failure (with force)": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedStatus:      basicNetworkStatus,
			fetcherMaxRetries:   5,
			fetcherForceRetry:   true,
		},
		"exhausted retries": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
			retriableError:      true,
		},
		"cancel context": {
			network:             basicNetwork,
			errorsBeforeSuccess: 6,
			expectedError:       context.Canceled,
			fetcherMaxRetries:   5,
			shouldCancel:        true,
			retriableError:      true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				tries       = 0
				assert      = assert.New(t)
				ctx, cancel = context.WithCancel(context.Background())
				endpoint    = "/network/status"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				expected := &types.NetworkRequest{
					NetworkIdentifier: test.network,
				}
				var networkRequest *types.NetworkRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&networkRequest))
				assert.Equal(expected, networkRequest)

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
				fmt.Fprintln(w, types.PrettyPrintStruct(test.expectedStatus))
			}))

			defer ts.Close()

			opts := []Option{
				WithRetryElapsedTime(5 * time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
			}
			if test.fetcherForceRetry {
				opts = append(opts, WithForceRetry())
			}

			f := New(
				ts.URL,
				opts...,
			)
			status, err := f.NetworkStatusRetry(
				ctx,
				test.network,
				nil,
			)
			assert.Equal(test.expectedStatus, status)
			assert.True(checkError(err, test.expectedError))
		})
	}
}

func TestNetworkListRetry(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier

		errorsBeforeSuccess int
		expectedList        *types.NetworkListResponse
		expectedError       error
		retriableError      bool

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			network:           basicNetwork,
			expectedList:      basicNetworkList,
			fetcherMaxRetries: 5,
		},
		"retry failures": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedList:        basicNetworkList,
			fetcherMaxRetries:   5,
			retriableError:      true,
		},
		"exhausted retries": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
			retriableError:      true,
		},
		"non-retriable error": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedError:       ErrRequestFailed,
			fetcherMaxRetries:   5,
		},
		"cancel context": {
			network:             basicNetwork,
			errorsBeforeSuccess: 6,
			expectedError:       context.Canceled,
			fetcherMaxRetries:   5,
			shouldCancel:        true,
			retriableError:      true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				tries       = 0
				assert      = assert.New(t)
				ctx, cancel = context.WithCancel(context.Background())
				endpoint    = "/network/list"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				expected := &types.MetadataRequest{}
				var metadataRequest *types.MetadataRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&metadataRequest))
				assert.Equal(expected, metadataRequest)

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
				fmt.Fprintln(w, types.PrettyPrintStruct(test.expectedList))
			}))

			defer ts.Close()

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
			)
			list, err := f.NetworkListRetry(
				ctx,
				nil,
			)
			assert.Equal(test.expectedList, list)
			assert.True(checkError(err, test.expectedError))
		})
	}
}

func TestNetworkOptionsRetry(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier

		errorsBeforeSuccess int
		expectedOptions     *types.NetworkOptionsResponse
		expectedError       error
		retriableError      bool

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			network:           basicNetwork,
			expectedOptions:   basicNetworkOptions,
			fetcherMaxRetries: 5,
		},
		"retry failures": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedOptions:     basicNetworkOptions,
			fetcherMaxRetries:   5,
			retriableError:      true,
		},
		"exhausted retries": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
			retriableError:      true,
		},
		"non-retriable error": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedError:       ErrRequestFailed,
			fetcherMaxRetries:   5,
		},
		"cancel context": {
			network:             basicNetwork,
			errorsBeforeSuccess: 6,
			expectedError:       context.Canceled,
			fetcherMaxRetries:   5,
			shouldCancel:        true,
			retriableError:      true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				tries       = 0
				assert      = assert.New(t)
				ctx, cancel = context.WithCancel(context.Background())
				endpoint    = "/network/options"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				expected := &types.NetworkRequest{
					NetworkIdentifier: test.network,
				}
				var networkRequest *types.NetworkRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&networkRequest))
				assert.Equal(expected, networkRequest)

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
				fmt.Fprintln(w, types.PrettyPrintStruct(test.expectedOptions))
			}))

			defer ts.Close()

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
			)
			options, err := f.NetworkOptionsRetry(
				ctx,
				test.network,
				nil,
			)
			assert.Equal(test.expectedOptions, options)
			assert.True(checkError(err, test.expectedError))
		})
	}
}
