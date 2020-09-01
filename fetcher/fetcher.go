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
	"errors"
	"net/http"
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/client"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// DefaultElapsedTime is the default limit on time
	// spent retrying a fetch.
	DefaultElapsedTime = 1 * time.Minute

	// DefaultRetries is the default number of times to
	// attempt a retry on a failed request.
	DefaultRetries = 10

	// DefaultHTTPTimeout is the default timeout for
	// HTTP requests.
	DefaultHTTPTimeout = 10 * time.Second

	// DefaultTransactionConcurrency is the default
	// number of transactions a Fetcher will try to
	// get concurrently when populating a block (if
	// transactions are not included in the original
	// block fetch).
	DefaultTransactionConcurrency = 8

	// DefaultUserAgent is the default userAgent
	// to populate on requests to a Rosetta server.
	DefaultUserAgent = "rosetta-sdk-go"
)

var (
	// ErrNoNetworks is returned when there are no
	// networks available for syncing.
	ErrNoNetworks = errors.New("no networks available")

	// ErrNetworkMissing is returned during asserter initialization
	// when the provided *types.NetworkIdentifier is not in the
	// *types.NetworkListResponse.
	ErrNetworkMissing = errors.New("network missing")

	// ErrRequestFailed is returned when a request fails.
	ErrRequestFailed = errors.New("request failed")

	// ErrExhaustedRetries is returned when a request with retries
	// fails because it was attempted too many times.
	ErrExhaustedRetries = errors.New("retries exhausted")

	// ErrAssertionFailed is returned when a fetch succeeds
	// but fails assertion.
	ErrAssertionFailed = errors.New("assertion failed")
)

// Fetcher contains all logic to communicate with a Rosetta Server.
type Fetcher struct {
	// Asserter is a public variable because
	// it can be used to determine if a retrieved
	// types.Operation is successful and should
	// be applied.
	Asserter               *asserter.Asserter
	rosettaClient          *client.APIClient
	transactionConcurrency uint64
	maxRetries             uint64
	retryElapsedTime       time.Duration
}

// Error wraps the two possible types of error responses returned
// by the Rosetta Client
type Error struct {
	Err       error        `json:"err"`
	ClientErr *types.Error `json:"client_err"`
}

// New constructs a new Fetcher with provided options.
func New(
	serverAddress string,
	options ...Option,
) *Fetcher {
	// Create default fetcher
	clientCfg := client.NewConfiguration(
		serverAddress,
		DefaultUserAgent,
		&http.Client{
			Timeout: DefaultHTTPTimeout,
		})
	client := client.NewAPIClient(clientCfg)

	f := &Fetcher{
		rosettaClient:          client,
		transactionConcurrency: DefaultTransactionConcurrency,
		maxRetries:             DefaultRetries,
		retryElapsedTime:       DefaultElapsedTime,
	}

	// Override defaults with any provided options
	for _, opt := range options {
		opt(f)
	}

	return f
}

// InitializeAsserter creates an Asserter for
// validating responses. The Asserter is created
// by fetching the NetworkStatus and NetworkOptions
// from a Rosetta server.
//
// If a *types.NetworkIdentifier is provided,
// it will be used to fetch status and options. Not providing
// a *types.NetworkIdentifier will result in the first
// network returned by NetworkList to be used.
//
// This method should be called before making any
// validated client requests.
func (f *Fetcher) InitializeAsserter(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
) (
	*types.NetworkIdentifier,
	*types.NetworkStatusResponse,
	*Error,
) {
	if f.Asserter != nil {
		return nil, nil, &Error{Err: errors.New("asserter already initialized")}
	}

	// Attempt to fetch network list
	networkList, err := f.NetworkListRetry(ctx, nil)
	if err != nil {
		return nil, nil, err
	}

	if len(networkList.NetworkIdentifiers) == 0 {
		return nil, nil, &Error{Err: ErrNoNetworks}
	}

	primaryNetwork := networkList.NetworkIdentifiers[0]
	if networkIdentifier != nil {
		exists, _ := CheckNetworkListForNetwork(networkList, networkIdentifier)
		if !exists {
			return nil, nil, &Error{Err: ErrNetworkMissing}
		}

		primaryNetwork = networkIdentifier
	}

	// Attempt to fetch network status
	networkStatus, err := f.NetworkStatusRetry(
		ctx,
		primaryNetwork,
		nil,
	)
	if err != nil {
		return nil, nil, err
	}

	// Attempt to fetch network options
	networkOptions, err := f.NetworkOptionsRetry(
		ctx,
		primaryNetwork,
		nil,
	)
	if err != nil {
		return nil, nil, err
	}

	newAsserter, assertErr := asserter.NewClientWithResponses(
		primaryNetwork,
		networkStatus,
		networkOptions,
	)
	if assertErr != nil {
		return nil, nil, &Error{Err: assertErr}
	}
	f.Asserter = newAsserter

	return primaryNetwork, networkStatus, nil
}
