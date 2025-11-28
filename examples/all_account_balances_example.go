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

package main

import (
	"context"
	"fmt"
	"log"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// ExampleAllAccountBalances demonstrates how to use the AllAccountBalances API
// to fetch all balances (main account and all sub-accounts) in a single request.
func ExampleAllAccountBalances() {
	ctx := context.Background()

	// Setup the fetcher with the server address
	f := fetcher.New("https://rosetta-api.example.com")

	// Define network and account identifiers
	networkIdentifier := &types.NetworkIdentifier{
		Blockchain: "bitcoin",
		Network:    "mainnet",
	}

	accountIdentifier := &types.AccountIdentifier{
		Address: "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
	}

	// Optional: specify block for historical balance lookup
	// For current balance, leave as nil
	var blockIdentifier *types.PartialBlockIdentifier = nil

	// Optional: specify currencies to filter results
	// For all currencies, leave as nil
	var currencies []*types.Currency = nil

	// Fetch all account balances (main + all sub-accounts) in a single call
	blockID, accountBalances, metadata, err := f.AllAccountBalancesRetry(
		ctx,
		networkIdentifier,
		accountIdentifier,
		blockIdentifier,
		currencies,
	)
	if err != nil {
		log.Fatalf("Failed to fetch account all balances: %v", err)
	}

	// Display results
	fmt.Printf("Block ID: %s\n", types.PrintStruct(blockID))
	fmt.Printf("Metadata: %s\n", types.PrintStruct(metadata))
	fmt.Printf("Total account balances found: %d\n", len(accountBalances))

	// Iterate through all balances
	for i, balance := range accountBalances {
		fmt.Printf("\n--- Balance %d ---\n", i+1)

		// Check if this is a sub-account or main account
		if balance.SubAccountIdentifier != nil {
			fmt.Printf("Sub-Account: %s\n", types.PrintStruct(balance.SubAccountIdentifier))
		} else {
			fmt.Printf("Main Account Balance\n")
		}

		// Display balance type if available
		if balance.BalanceType != "" {
			fmt.Printf("Balance Type: %s\n", balance.BalanceType)
		}

		// Display all currency balances for this account/sub-account
		fmt.Printf("Currencies:\n")
		for _, amount := range balance.Balances {
			fmt.Printf("  - %s %s\n", amount.Value, amount.Currency.Symbol)
		}

		// Display sub-account specific metadata if available
		if balance.Metadata != nil {
			fmt.Printf("Sub-Account Metadata: %s\n", types.PrintStruct(balance.Metadata))
		}
	}
}

// ExampleAllAccountBalancesVsIndividual demonstrates the difference between
// using AllAccountBalances (single request) vs multiple AccountBalance calls.
func ExampleAllAccountBalancesVsIndividual() {
	ctx := context.Background()
	f := fetcher.New("https://rosetta-api.example.com")

	networkIdentifier := &types.NetworkIdentifier{
		Blockchain: "cosmos",
		Network:    "mainnet",
	}

	accountIdentifier := &types.AccountIdentifier{
		Address: "cosmos1abc123def456ghi789...",
	}

	fmt.Println("=== Method 1: Single AllAccountBalances Call ===")

	// Single call to get all balances
	blockID, allBalances, _, err := f.AllAccountBalancesRetry(
		ctx, networkIdentifier, accountIdentifier, nil, nil,
	)
	if err != nil {
		log.Printf("AllAccountBalances error: %v", err)
	} else {
		fmt.Printf("✅ Single call returned %d balances at block %s\n",
			len(allBalances), blockID.Hash)
	}

	fmt.Println("\n=== Method 2: Multiple Individual AccountBalance Calls ===")

	// Multiple calls for each sub-account type (traditional approach)
	subAccountTypes := []string{"delegated", "unbonding", "rewards"}

	// Main account balance
	blockID, mainBalances, _, err := f.AccountBalanceRetry(
		ctx, networkIdentifier, accountIdentifier, nil, nil,
	)
	if err != nil {
		log.Printf("Main balance error: %v", err)
	} else {
		fmt.Printf("✅ Main account: %d balances\n", len(mainBalances))
	}

	// Sub-account balances (requires separate calls)
	for _, subType := range subAccountTypes {
		subAccountIdentifier := &types.AccountIdentifier{
			Address: accountIdentifier.Address,
			SubAccount: &types.SubAccountIdentifier{
				Address:  subType,
				Metadata: map[string]interface{}{"type": subType},
			},
		}

		_, subBalances, _, err := f.AccountBalanceRetry(
			ctx, networkIdentifier, subAccountIdentifier, nil, nil,
		)
		if err != nil {
			log.Printf("❌ Sub-account %s error: %v", subType, err)
		} else {
			fmt.Printf("✅ Sub-account %s: %d balances\n", subType, len(subBalances))
		}
	}

	fmt.Println("\n💡 AllAccountBalances reduces network calls and provides consistent block context!")
}

func main() {
	fmt.Println("AllAccountBalances API Example")
	fmt.Println("==============================")

	// Comment out the examples you don't want to run
	ExampleAllAccountBalances()
	// ExampleAllAccountBalancesVsIndividual()
}