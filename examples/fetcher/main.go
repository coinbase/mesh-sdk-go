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

package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// serverURL is the URL of a Rosetta Server.
	serverURL = "http://localhost:8080"

	// agent is the user-agent on requests to the
	// Rosetta Server.
	agent = "rosetta-sdk-go"

	// defaultTimeout is the default timeout for
	// HTTP requests.
	defaultTimeout = 5 * time.Second

	// blockConcurrency is the number of blocks to
	// fetch concurrently in a call to fetcher.SyncBlockRange.
	blockConcurrency = 4

	// transactionConcurrency is the number of transactions to
	// fetch concurrently (if BlockResponse.OtherTransactions
	// is populated) in a call to fetcher.SyncBlockRange.
	transactionConcurrency = 4

	// maxElapsedTime is the maximum amount of time we will
	// spend retrying failed requests.
	maxElapsedTime = 1 * time.Minute

	// maxRetries is the maximum number of times we will
	// retry a failed request.
	maxRetries = 10
)

func main() {
	ctx := context.Background()

	// Step 1: Create a new fetcher
	newFetcher := fetcher.New(
		ctx,
		serverURL,
		agent,
		&http.Client{
			Timeout: defaultTimeout,
		},
		blockConcurrency,
		transactionConcurrency,
	)

	// Step 2: Initialize the fetcher's asserter
	//
	// Behind the scenes this makes a call to get the
	// network status and uses the response to inform
	// the asserter what are valid responses.
	networkStatusResponse, err := newFetcher.InitializeAsserter(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Step 3: Print the network status response
	prettyNetworkStatusResponse, err := json.MarshalIndent(
		networkStatusResponse,
		"",
		" ",
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Network Status Response: %s\n", string(prettyNetworkStatusResponse))

	// Step 4: Fetch the current block with retries (automatically
	// asserted for correctness)
	//
	// It is important to note that this assertion only ensures
	// required fields are populated and that operations
	// in the block only use types and statuses that were
	// provided in the networkStatusResponse. To run more
	// intensive validation, use the Rosetta Validator. It
	// can be found at: https://github.com/coinbase/rosetta-validator
	//
	// On another note, notice that fetcher.BlockRetry
	// automatically fetches all transactions that are
	// returned in BlockResponse.OtherTransactions. If you use
	// the client directly, you will need to implement a mechanism
	// to fully populate the block by fetching all these
	// transactions.
	primaryNetwork := networkStatusResponse.NetworkStatus[0]
	block, err := newFetcher.BlockRetry(
		ctx,
		primaryNetwork.NetworkIdentifier,
		types.ConstructPartialBlockIdentifier(
			primaryNetwork.NetworkInformation.CurrentBlockIdentifier,
		),
		maxElapsedTime,
		maxRetries,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Step 5: Print the block
	prettyBlock, err := json.MarshalIndent(
		block,
		"",
		" ",
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Current Block: %s\n", string(prettyBlock))
}
