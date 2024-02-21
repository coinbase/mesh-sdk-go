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

package main

import (
	"context"
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

	// Step 2: Get all available networks
	networkList, rosettaErr, err := client.NetworkAPI.NetworkList(
		ctx,
		&types.MetadataRequest{},
	)
	if rosettaErr != nil {
		log.Printf("Rosetta Error: %+v\n", rosettaErr)
	}
	if err != nil {
		log.Fatal(err)
	}

	if len(networkList.NetworkIdentifiers) == 0 {
		log.Fatal("no available networks")
	}

	primaryNetwork := networkList.NetworkIdentifiers[0]

	// Step 3: Print the primary network
	log.Printf("Primary Network: %s\n", types.PrettyPrintStruct(primaryNetwork))

	// Step 4: Fetch the network status
	networkStatus, rosettaErr, err := client.NetworkAPI.NetworkStatus(
		ctx,
		&types.NetworkRequest{
			NetworkIdentifier: primaryNetwork,
		},
	)
	if rosettaErr != nil {
		log.Printf("Rosetta Error: %+v\n", rosettaErr)
	}
	if err != nil {
		log.Fatal(err)
	}

	// Step 5: Print the response
	log.Printf("Network Status: %s\n", types.PrettyPrintStruct(networkStatus))

	// Step 6: Assert the response is valid
	err = asserter.NetworkStatusResponse(networkStatus)
	if err != nil {
		log.Fatalf("Assertion Error: %s\n", err.Error())
	}

	// Step 7: Fetch the network options
	networkOptions, rosettaErr, err := client.NetworkAPI.NetworkOptions(
		ctx,
		&types.NetworkRequest{
			NetworkIdentifier: primaryNetwork,
		},
	)
	if rosettaErr != nil {
		log.Printf("Rosetta Error: %+v\n", rosettaErr)
	}
	if err != nil {
		log.Fatal(err)
	}

	// Step 8: Print the response
	log.Printf("Network Options: %s\n", types.PrettyPrintStruct(networkOptions))

	// Step 9: Assert the response is valid
	err = asserter.NetworkOptionsResponse(networkOptions)
	if err != nil {
		log.Fatalf("Assertion Error: %s\n", err.Error())
	}

	// Step 10: Create an asserter using the retrieved NetworkStatus and
	// NetworkOptions.
	//
	// This will be used later to assert that a fetched block is
	// valid.
	asserter, err := asserter.NewClientWithResponses(
		primaryNetwork,
		networkStatus,
		networkOptions,
		"",
	)
	if err != nil {
		log.Fatal(err)
	}

	// Step 11: Fetch the current block
	block, rosettaErr, err := client.BlockAPI.Block(
		ctx,
		&types.BlockRequest{
			NetworkIdentifier: primaryNetwork,
			BlockIdentifier: types.ConstructPartialBlockIdentifier(
				networkStatus.CurrentBlockIdentifier,
			),
		},
	)
	if rosettaErr != nil {
		log.Printf("Rosetta Error: %+v\n", rosettaErr)
	}
	if err != nil {
		log.Fatal(err)
	}

	// Step 12: Print the block
	log.Printf("Current Block: %s\n", types.PrettyPrintStruct(block.Block))

	// Step 13: Assert the block response is valid
	//
	// It is important to note that this only ensures
	// required fields are populated and that operations
	// in the block only use types and statuses that were
	// provided in the networkStatusResponse. To run more
	// intensive validation, use the Rosetta CLI. It
	// can be found at: https://github.com/coinbase/rosetta-cli
	err = asserter.Block(block.Block)
	if err != nil {
		log.Fatalf("Assertion Error: %s\n", err.Error())
	}

	// Step 14: Print remaining transactions to fetch
	//
	// If you want the client to automatically fetch these, consider
	// using the fetcher package.
	for _, txn := range block.OtherTransactions {
		log.Printf("Other Transaction: %+v\n", txn)
	}
}
