// Copyright 2025 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package examples

import (
	"context"
	"fmt"
	"log"

	"github.com/coinbase/rosetta-sdk-go/client"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// ExampleConstructionPreprocessOperations demonstrates how to use the new
// ConstructionPreprocessOperations endpoint to parse high-level transaction
// construction operations into Rosetta operations.
func ExampleConstructionPreprocessOperations() {
	ctx := context.Background()

	// Create a Rosetta client
	config := &client.Configuration{
		BasePath: "http://localhost:8080", // Replace with your Rosetta node URL
	}
	rosettaClient := client.NewAPIClient(config)

	// Create a fetcher (recommended for production use)
	// This example shows both direct client usage and fetcher usage

	// Example 1: Using the client directly
	fmt.Println("Example 1: Using client directly")
	request := &types.ConstructionPreprocessOperationsRequest{
		NetworkIdentifier: &types.NetworkIdentifier{
			Blockchain: "bitcoin",
			Network:    "mainnet",
		},
		FromAddress: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
		ConstructOp: "transfer", // This would be the high-level operation type
		Options: map[string]interface{}{
			"to_address": "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
			"amount":     "100000000", // 1 BTC in satoshis
			"asset":      "btc",
		},
	}

	response, clientErr, err := rosettaClient.ConstructionAPI.ConstructionPreprocessOperations(
		ctx,
		request,
	)

	if err != nil {
		log.Printf("API call failed: %v", err)
		if clientErr != nil {
			log.Printf("Client error: %v", clientErr)
		}
		return
	}

	fmt.Printf("Operations returned: %d\n", len(response.Operations))
	for i, operation := range response.Operations {
		fmt.Printf("  Operation %d: Type=%s, Account=%v, Amount=%v\n",
			i, operation.Type,
			operation.Account,
			operation.Amount,
		)
	}

	if response.MaxFee != nil {
		fmt.Printf("Max Fee: %s %s\n", response.MaxFee.Value, response.MaxFee.Currency.Symbol)
	}

	if len(response.Metadata) > 0 {
		fmt.Printf("Metadata length: %d bytes\n", len(response.Metadata))
	}

	// Example 2: Using the fetcher (recommended approach)
	fmt.Println("\nExample 2: Using fetcher (recommended)")

	// You would normally create the fetcher with proper configuration
	// This is just for demonstration
	fetcher := &fetcher.Fetcher{
		// Configure with your asserter, client, etc.
		// See fetcher documentation for proper setup
	}

	operations, maxFee, metadata, fetchErr := fetcher.ConstructionPreprocessOperations(
		ctx,
		&types.NetworkIdentifier{
			Blockchain: "bitcoin",
			Network:    "mainnet",
		},
		"1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", // fromAddress
		"transfer",                            // constructOp
		map[string]interface{}{                // options
			"to_address": "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
			"amount":     "100000000",
			"asset":      "btc",
		},
	)

	if fetchErr != nil {
		log.Printf("Fetcher call failed: %v", fetchErr.Err)
		return
	}

	fmt.Printf("Operations: %d\n", len(operations))
	if maxFee != nil {
		fmt.Printf("Max Fee: %s %s\n", maxFee.Value, maxFee.Currency.Symbol)
	}
	if len(metadata) > 0 {
		fmt.Printf("Metadata length: %d bytes\n", len(metadata))
	}
}

// ExampleFallbackPattern demonstrates the fallback pattern where you first try
// the local OperationSelector and then fall back to the Rosetta API.
func ExampleFallbackPattern() {
	ctx := context.Background()

	// This simulates the pattern you described for chainstdio
	fmt.Println("Fallback Pattern Example:")

	// Simulate the chainstdio input structure
	type TransactionConstructOpInput struct {
		FromAddress string
		ConstructOp string
		Options     []byte // JSON encoded options
	}

	input := &TransactionConstructOpInput{
		FromAddress: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
		ConstructOp: "stake",
		Options:     []byte(`{"amount": "1000000", "validator": "validator123"}`),
	}

	// Step 1: Try local OperationSelector
	fmt.Println("1. Trying local OperationSelector...")
	// operations, maxFee, metadata, err := c.OperationSelector.ParseTransactionConstructOp(ctx, input)
	// Simulate that it returns "construct op not supported" error
	err := fmt.Errorf("bad request - construct op not supported: %s", input.ConstructOp)

	if err != nil && isConstructOpNotSupportedError(err) {
		fmt.Printf("   Local parsing failed: %v\n", err)

		// Step 2: Fall back to Rosetta API
		fmt.Println("2. Falling back to Rosetta API...")

		// Convert input to Rosetta format
		options := make(map[string]interface{})
		// Parse input.Options JSON into options map
		// json.Unmarshal(input.Options, &options)

		// Create client and call Rosetta API
		config := &client.Configuration{
			BasePath: "http://localhost:8080",
		}
		rosettaClient := client.NewAPIClient(config)

		request := &types.ConstructionPreprocessOperationsRequest{
			NetworkIdentifier: &types.NetworkIdentifier{
				Blockchain: "bitcoin", // or your blockchain
				Network:    "mainnet",
			},
			FromAddress: input.FromAddress,
			ConstructOp: input.ConstructOp,
			Options:     options,
		}

		response, clientErr, err := rosettaClient.ConstructionAPI.ConstructionPreprocessOperations(
			ctx,
			request,
		)

		if err != nil {
			log.Printf("Rosetta API also failed: %v", err)
			if clientErr != nil {
				log.Printf("Client error: %v", clientErr)
			}
			return
		}

		fmt.Printf("   Rosetta API succeeded! Got %d operations\n", len(response.Operations))

		// Now you can use the operations for further transaction construction
		// operations := response.Operations
		// maxFee := response.MaxFee
		// metadata := response.Metadata

		return
	}

	fmt.Println("   Local parsing succeeded!")
}

// Helper function to check if error indicates unsupported construct op
func isConstructOpNotSupportedError(err error) bool {
	return err != nil &&
		(fmt.Sprintf("%v", err) == "bad request - construct op not supported" ||
		 fmt.Sprintf("%v", err) == "construct op not supported")
}