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
	"github.com/coinbase/rosetta-sdk-go/client"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	otherNetwork = &types.NetworkIdentifier{
		Blockchain: "other",
		Network:    "other",
	}

	otherNetworkStatus = &types.NetworkStatusResponse{
		CurrentBlockIdentifier: basicBlock,
		CurrentBlockTimestamp:  1582834600000,
		GenesisBlockIdentifier: &types.BlockIdentifier{
			Index: 10,
			Hash:  "block 10",
		},
	}

	complexNetworkList = &types.NetworkListResponse{
		NetworkIdentifiers: []*types.NetworkIdentifier{
			basicNetwork,
			otherNetwork,
		},
	}

	otherNetworkOptions = &types.NetworkOptionsResponse{
		Version: &types.Version{
			RosettaVersion: "1.4.0",
			NodeVersion:    "0.0.1",
		},
		Allow: &types.Allow{
			OperationStatuses: []*types.OperationStatus{
				{
					Status:     "OTHER",
					Successful: true,
				},
			},
			OperationTypes: []string{"input"},
		},
	}
)

func TestInitializeAsserter(t *testing.T) {
	var tests = map[string]struct {
		network        *types.NetworkIdentifier
		networkRequest *types.NetworkRequest // used for both /network/options and /network/status
		networkList    *types.NetworkListResponse
		networkStatus  *types.NetworkStatusResponse
		networkOptions *types.NetworkOptionsResponse

		expectedNetwork *types.NetworkIdentifier
		expectedStatus  *types.NetworkStatusResponse
		expectedError   error
	}{
		"default network": {
			networkRequest: &types.NetworkRequest{
				NetworkIdentifier: basicNetwork,
			},
			networkList:     basicNetworkList,
			networkStatus:   basicNetworkStatus,
			networkOptions:  basicNetworkOptions,
			expectedNetwork: basicNetwork,
			expectedStatus:  basicNetworkStatus,
		},
		"specify network": {
			network: basicNetwork,
			networkRequest: &types.NetworkRequest{
				NetworkIdentifier: basicNetwork,
			},
			networkList:     basicNetworkList,
			networkStatus:   basicNetworkStatus,
			networkOptions:  basicNetworkOptions,
			expectedNetwork: basicNetwork,
			expectedStatus:  basicNetworkStatus,
		},
		"other network": {
			network: otherNetwork,
			networkRequest: &types.NetworkRequest{
				NetworkIdentifier: otherNetwork,
			},
			networkList:     complexNetworkList,
			networkStatus:   otherNetworkStatus,
			networkOptions:  otherNetworkOptions,
			expectedNetwork: otherNetwork,
			expectedStatus:  otherNetworkStatus,
		},
		"no networks": {
			network: otherNetwork,
			networkRequest: &types.NetworkRequest{
				NetworkIdentifier: otherNetwork,
			},
			networkList:   &types.NetworkListResponse{},
			expectedError: ErrNoNetworks,
		},
		"missing network": {
			network: otherNetwork,
			networkRequest: &types.NetworkRequest{
				NetworkIdentifier: otherNetwork,
			},
			networkList:    basicNetworkList,
			networkOptions: basicNetworkOptions,
			expectedError:  ErrNetworkMissing,
		},
		"invalid options": {
			networkRequest: &types.NetworkRequest{
				NetworkIdentifier: basicNetwork,
			},
			networkList:   basicNetworkList,
			networkStatus: basicNetworkStatus,
			networkOptions: &types.NetworkOptionsResponse{
				Allow: &types.Allow{
					OperationStatuses: []*types.OperationStatus{
						{
							Status:     "OTHER",
							Successful: false,
						},
						{
							Status:     "OTHER",
							Successful: true,
						},
					},
				},
			},
			expectedError: asserter.ErrVersionIsNil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				assert = assert.New(t)
				ctx    = context.Background()
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)

				switch r.URL.RequestURI() {
				case "/network/list":
					fmt.Fprintln(w, types.PrettyPrintStruct(test.networkList))
				case "/network/status":
					var networkRequest *types.NetworkRequest
					assert.NoError(json.NewDecoder(r.Body).Decode(&networkRequest))
					assert.Equal(test.networkRequest, networkRequest)
					fmt.Fprintln(w, types.PrettyPrintStruct(test.networkStatus))
				case "/network/options":
					var networkRequest *types.NetworkRequest
					assert.NoError(json.NewDecoder(r.Body).Decode(&networkRequest))
					assert.Equal(test.networkRequest, networkRequest)
					fmt.Fprintln(w, types.PrettyPrintStruct(test.networkOptions))
				}
			}))

			defer ts.Close()

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
			)

			networkIdentifier, networkStatus, err := f.InitializeAsserter(ctx, test.network, "")
			assert.Equal(test.expectedNetwork, networkIdentifier)
			assert.Equal(test.expectedStatus, networkStatus)
			assert.True(checkError(err, test.expectedError))
		})
	}
}

func TestNewWithHTTPCLient(t *testing.T) {
	// Callers can pass an http.Client to
	// the fetcher via WithClient.
	// Ensure that the fetcher does not
	// override it.
	httpClient := &http.Client{}
	apiClient := client.NewAPIClient(
		client.NewConfiguration(
			"https://serveraddress",
			DefaultUserAgent,
			httpClient,
		),
	)
	fetcher := New("https://serveraddress", WithClient(apiClient))
	var assert = assert.New(t)
	assert.Same(httpClient, fetcher.rosettaClient.GetConfig().HTTPClient)
}

func TestNewWithTimeout(t *testing.T) {
	var assert = assert.New(t)

	fetcher := New("https://serveraddress")
	assert.Equal(DefaultHTTPTimeout, fetcher.rosettaClient.GetConfig().HTTPClient.Timeout)

	var customTimeout = 6 * time.Minute
	fetcher2 := New("https://serveraddress", WithTimeout(customTimeout))
	assert.Equal(customTimeout, fetcher2.rosettaClient.GetConfig().HTTPClient.Timeout)

	// If we pass in a http timeout value when initializing the fetcher, and also pass in an
	// existing client,
	// we will simply respect the timeout on the existing client and not override it.
	var existingClientTimeout = 3 * time.Minute
	httpClient := &http.Client{
		Timeout: existingClientTimeout,
	}
	apiClient := client.NewAPIClient(
		client.NewConfiguration(
			"https://serveraddress",
			DefaultUserAgent,
			httpClient,
		),
	)
	fetcher3 := New("https://serveraddress", WithClient(apiClient), WithTimeout(6*time.Minute))
	assert.Equal(existingClientTimeout, fetcher3.rosettaClient.GetConfig().HTTPClient.Timeout)
}
