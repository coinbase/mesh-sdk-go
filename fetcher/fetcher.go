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
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/fatih/color"
	"golang.org/x/sync/semaphore"

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

	// DefaultUserAgent is the default userAgent
	// to populate on requests to a Rosetta server.
	DefaultUserAgent = "rosetta-sdk-go"

	// DefaultIdleConnTimeout is the maximum amount of time an idle
	// (keep-alive) connection will remain idle before closing
	// itself.
	DefaultIdleConnTimeout = 30 * time.Second

	// DefaultMaxConnections limits the number of concurrent
	// connections we will attempt to make. Most OS's have a
	// default connection limit of 128, so we set the default
	// below that.
	DefaultMaxConnections = 120

	// semaphoreRequestWeight is the weight of each request.
	semaphoreRequestWeight = int64(1)
)

// Fetcher contains all logic to communicate with a Rosetta Server.
type Fetcher struct {
	// Asserter is a public variable because
	// it can be used to determine if a retrieved
	// types.Operation is successful and should
	// be applied.
	Asserter         *asserter.Asserter
	rosettaClient    *client.APIClient
	maxConnections   int
	maxRetries       uint64
	retryElapsedTime time.Duration
	insecureTLS      bool
	forceRetry       bool
	httpTimeout      time.Duration
	metaData         string

	// connectionSemaphore is used to limit the
	// number of concurrent requests we make.
	connectionSemaphore *semaphore.Weighted
}

// New constructs a new Fetcher with provided options.
func New(
	serverAddress string,
	options ...Option,
) *Fetcher {
	f := &Fetcher{
		maxConnections:   DefaultMaxConnections,
		maxRetries:       DefaultRetries,
		retryElapsedTime: DefaultElapsedTime,
		httpTimeout:      DefaultHTTPTimeout,
	}

	// Override defaults with any provided options
	for _, opt := range options {
		opt(f)
	}

	if f.rosettaClient == nil {
		// Override transport idle connection settings
		//
		// See this conversation around why `.Clone()` is used here:
		// https://github.com/golang/go/issues/26013
		defaultTransport := http.DefaultTransport.(*http.Transport).Clone()
		defaultTransport.IdleConnTimeout = DefaultIdleConnTimeout
		defaultTransport.MaxIdleConns = f.maxConnections
		defaultTransport.MaxIdleConnsPerHost = DefaultMaxConnections
		defaultHTTPClient := &http.Client{
			Timeout:   f.httpTimeout,
			Transport: defaultTransport,
		}

		// Create default fetcher
		clientCfg := client.NewConfiguration(
			serverAddress,
			DefaultUserAgent,
			defaultHTTPClient,
		)
		f.rosettaClient = client.NewAPIClient(clientCfg)
	}

	if f.insecureTLS {
		if transport, ok := f.rosettaClient.GetConfig().HTTPClient.Transport.(*http.Transport); ok {
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402
		}
	}

	// Initialize the connection semaphore
	f.connectionSemaphore = semaphore.NewWeighted(int64(f.maxConnections))

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
	validationFilePath string,
) (
	*types.NetworkIdentifier,
	*types.NetworkStatusResponse,
	*Error,
) {
	if f.Asserter != nil {
		color.Red("asserter already initialized%s", f.metaData)
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
		exists, supportedNetworks := CheckNetworkListForNetwork(networkList, networkIdentifier)
		if !exists {
			return nil, nil, &Error{
				Err: fmt.Errorf(
					"%s not in %s: %w",
					types.PrintStruct(networkIdentifier),
					types.PrintStruct(supportedNetworks),
					ErrNetworkMissing,
				),
			}
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
		validationFilePath,
	)
	if assertErr != nil {
		return nil, nil, &Error{Err: assertErr}
	}
	f.Asserter = newAsserter

	return primaryNetwork, networkStatus, nil
}
