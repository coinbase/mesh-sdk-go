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

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/client"
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
	defaultTimeout = 10 * time.Second
)

func main() {
	ctx := context.Background()

	// Step 1: Create a client
	clientCfg := client.NewConfiguration(
		serverURL,
		agent,
		&http.Client{
			Timeout: defaultTimeout,
		},
	)

	client := client.NewAPIClient(clientCfg)

	// Step 2: Fetch the network status
	networkStatusResponse, rosettaErr, err := client.NetworkAPI.NetworkStatus(
		ctx,
		&types.NetworkStatusRequest{},
	)
	if rosettaErr != nil {
		log.Printf("Rosetta Error: %+v\n", rosettaErr)
	}
	if err != nil {
		log.Fatal(err)
	}

	// Step 3: Print the response
	prettyNetworkStatusResponse, err := json.MarshalIndent(networkStatusResponse, "", " ")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Network Status Response: %s\n", string(prettyNetworkStatusResponse))

	// Step 4: Assert the response is valid
	err = asserter.NetworkStatusResponse(networkStatusResponse)
	if err != nil {
		log.Fatalf("Assertion Error: %s\n", err.Error())
	}

	// Step 5: Create an asserter using the NetworkStatusResponse
	//
	// This will be used later to assert that a fetched block is
	// valid.
	asserter, err := asserter.New(
		ctx,
		networkStatusResponse,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Step 6: Fetch the current block
	primaryNetwork := networkStatusResponse.NetworkStatus[0]
	block, rosettaErr, err := client.BlockAPI.Block(
		ctx,
		&types.BlockRequest{
			NetworkIdentifier: primaryNetwork.NetworkIdentifier,
			BlockIdentifier: &types.PartialBlockIdentifier{
				Index: &primaryNetwork.NetworkInformation.CurrentBlockIdentifier.Index,
			},
		},
	)
	if rosettaErr != nil {
		log.Printf("Rosetta Error: %+v\n", rosettaErr)
	}
	if err != nil {
		log.Fatal(err)
	}

	// Step 7: Print the block
	prettyBlock, err := json.MarshalIndent(block.Block, "", " ")
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Current Block: %s\n", string(prettyBlock))

	// Step 8: Assert the block response is valid
	//
	// It is important to note that this only ensures
	// required fields are populated and that operations
	// in the block only use types and statuses that were
	// provided in the networkStatusResponse. To run more
	// intensive validation, use the Rosetta Validator. It
	// can be found at: https://github.com/coinbase/rosetta-validator
	err = asserter.Block(block.Block)
	if err != nil {
		log.Fatalf("Assertion Error: %s\n", err.Error())
	}

	// Step 9: Print remaining transactions to fetch
	//
	// If you want the client to automatically fetch these, consider
	// using the fetcher package.
	for _, txn := range block.OtherTransactions {
		log.Printf("Other Transaction: %+v\n", txn)
	}
}
