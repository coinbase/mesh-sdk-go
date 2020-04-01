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
	"net/http"
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"
)

const (
	// DefaultElapsedTime is the default limit on time
	// spent retrying a fetch.
	DefaultElapsedTime = 1 * time.Minute

	// DefaultRetries is the default number of times to
	// attempt a retry on a failed request.
	DefaultRetries = 10

	// DefaultBlockConcurrency is the default number of
	// blocks a Fetcher will try to get concurrently.
	DefaultBlockConcurrency = 8

	// DefaultTransactionConcurrency is the default
	// number of transactions a Fetcher will try to
	// get concurrently when populating a block (if
	// transactions are not included in the original
	// block fetch).
	DefaultTransactionConcurrency = 8
)

// Fetcher contains all logic to communicate with a Rosetta Server.
type Fetcher struct {
	// Asserter is a public variable because
	// it can be used to determine if a retrieved
	// rosetta.Operation is successful and should
	// be applied.
	Asserter               *asserter.Asserter
	rosettaClient          *rosetta.APIClient
	blockConcurrency       uint64
	transactionConcurrency uint64
}

// New constructs a new Fetcher.
func New(
	ctx context.Context,
	serverAddress string,
	userAgent string,
	httpClient *http.Client,
	blockConcurrency uint64,
	transactionConcurrency uint64,
) *Fetcher {
	clientCfg := rosetta.NewConfiguration(serverAddress, userAgent, httpClient)
	client := rosetta.NewAPIClient(clientCfg)

	return &Fetcher{
		rosettaClient:          client,
		blockConcurrency:       blockConcurrency,
		transactionConcurrency: transactionConcurrency,
	}
}

// InitializeAsserter creates an Asserter for
// validating responses. The Asserter is created
// from a rosetta.NetworkStatusResponse. This
// method should be called before making any
// validated client requests.
func (f *Fetcher) InitializeAsserter(
	ctx context.Context,
) (*rosetta.NetworkStatusResponse, error) {
	// Attempt to fetch network status
	networkResponse, err := f.NetworkStatusRetry(
		ctx,
		nil,
		DefaultElapsedTime,
		DefaultRetries,
	)
	if err != nil {
		return nil, err
	}

	f.Asserter = asserter.New(
		ctx,
		networkResponse,
	)

	return networkResponse, nil
}
