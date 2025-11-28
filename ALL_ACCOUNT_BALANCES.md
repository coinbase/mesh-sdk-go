# AllAccountBalances API

## Overview

The `AllAccountBalances` API endpoint provides a more efficient way to retrieve balances for all sub-accounts of a given account in a single request, rather than making multiple individual `/account/balance` calls.

## Problem Solved

Previously, to get all balances for an account with multiple sub-accounts (e.g., staking accounts with delegated, unbonding, rewards balances), clients had to:

1. Make a call to `/account/balance` for the main account
2. Make separate calls to `/account/balance` for each known sub-account type
3. Aggregate the results manually
4. Handle potential inconsistencies due to different block contexts

This resulted in multiple network requests and potential race conditions when the blockchain state changes between requests.

## Solution

The new `/account/all_balances` endpoint:

- Returns all account balances (main + sub-accounts) in a single request
- Ensures all balances are from the same block context
- Reduces network overhead and latency
- Simplifies client implementation

## API Specification

### Endpoint
- **Method**: `POST`
- **Path**: `/account/all_balances`
- **Content-Type**: `application/json`

### Request Format

```json
{
  "network_identifier": {
    "blockchain": "cosmos",
    "network": "mainnet"
  },
  "account_identifier": {
    "address": "cosmos1abc123def456ghi789..."
  },
  "block_identifier": {
    "index": 12345,
    "hash": "0xabc..."
  },
  "currencies": [
    {
      "symbol": "ATOM",
      "decimals": 6
    }
  ]
}
```

### Response Format

```json
{
  "block_identifier": {
    "index": 12345,
    "hash": "0xabc...",
    "parent_hash": "0xdef..."
  },
  "account_balances": [
    {
      "sub_account_identifier": null,
      "balances": [
        {
          "value": "1000000",
          "currency": {
            "symbol": "ATOM",
            "decimals": 6
          }
        }
      ],
      "balance_type": "spendable",
      "metadata": {}
    },
    {
      "sub_account_identifier": {
        "address": "delegated",
        "metadata": {
          "validator": "cosmosvaloper1abc..."
        }
      },
      "balances": [
        {
          "value": "5000000",
          "currency": {
            "symbol": "ATOM",
            "decimals": 6
          }
        }
      ],
      "balance_type": "delegated",
      "metadata": {
        "validator_address": "cosmosvaloper1abc...",
        "delegation_time": "2024-01-01T00:00:00Z"
      }
    }
  ],
  "metadata": {
    "sequence_number": "42"
  }
}
```

## Field Descriptions

### Request Fields

- `network_identifier`: Network to query (required)
- `account_identifier`: Account to get balances for (required)
- `block_identifier`: Optional block for historical queries
- `currencies`: Optional currency filter

### Response Fields

- `block_identifier`: Block at which balances were queried
- `account_balances`: Array of account balances including:
  - `sub_account_identifier`: null for main account, populated for sub-accounts
  - `balances`: Array of currency amounts for this account/sub-account
  - `balance_type`: Type of balance (e.g., "spendable", "delegated", "unbonding")
  - `metadata`: Account/sub-account specific metadata
- `metadata`: General account metadata (e.g., sequence numbers)

## Usage Examples

### Go SDK Usage

```go
package main

import (
    "context"
    "github.com/coinbase/rosetta-sdk-go/client"
    "github.com/coinbase/rosetta-sdk-go/fetcher"
    "github.com/coinbase/rosetta-sdk-go/types"
)

func main() {
    cfg := client.NewConfiguration("https://rosetta-api.example.com", "MyApp/1.0", nil)
    rosettaClient := client.NewAPIClient(cfg)
    f := fetcher.New(rosettaClient)

    networkID := &types.NetworkIdentifier{
        Blockchain: "cosmos",
        Network:    "mainnet",
    }

    accountID := &types.AccountIdentifier{
        Address: "cosmos1abc123...",
    }

    // Get all balances in a single call
    blockID, balances, metadata, err := f.AllAccountBalancesRetry(
        context.Background(),
        networkID,
        accountID,
        nil, // current block
        nil, // all currencies
    )

    if err != nil {
        panic(err)
    }

    for _, balance := range balances {
        if balance.SubAccountIdentifier == nil {
            // Main account balance
            fmt.Printf("Main account: %v\n", balance.Balances)
        } else {
            // Sub-account balance
            fmt.Printf("Sub-account %s: %v\n",
                balance.SubAccountIdentifier.Address,
                balance.Balances)
        }
    }
}
```

### Direct HTTP Usage

```bash
curl -X POST https://rosetta-api.example.com/account/all_balances \
  -H "Content-Type: application/json" \
  -d '{
    "network_identifier": {
      "blockchain": "cosmos",
      "network": "mainnet"
    },
    "account_identifier": {
      "address": "cosmos1abc123def456ghi789..."
    }
  }'
```

## Implementation Guidelines

### For Rosetta Implementers

When implementing this endpoint:

1. **Consistent Block Context**: Ensure all balances are retrieved from the same block
2. **Sub-Account Discovery**: Include all available sub-account types for the given account
3. **Balance Types**: Use standard balance types where possible:
   - `"spendable"` - Liquid/available balance
   - `"delegated"` - Staked/bonded tokens
   - `"unbonding"` - Tokens in unbonding period
   - `"rewards"` - Pending staking rewards
   - `"total"` - Sum of all balances

4. **Error Handling**: Return appropriate errors for invalid accounts or unsupported historical queries

5. **Performance**: Optimize to avoid multiple internal queries where possible

### For Client Applications

Benefits of using `AllAccountBalances`:

- **Reduced Latency**: Single network request vs multiple requests
- **Consistency**: All balances from the same block height
- **Completeness**: Discover all sub-account types without prior knowledge
- **Simplified Code**: Less error handling and aggregation logic

Migration from individual calls:

```go
// Old approach - multiple calls
mainBalance, _ := fetcher.AccountBalance(ctx, network, account, nil, nil)
delegatedBalance, _ := fetcher.AccountBalance(ctx, network, delegatedAccount, nil, nil)
unbondingBalance, _ := fetcher.AccountBalance(ctx, network, unbondingAccount, nil, nil)

// New approach - single call
allBalances, _ := fetcher.AllAccountBalances(ctx, network, account, nil, nil)
```

## Compatibility

- **Backwards Compatible**: Existing `/account/balance` endpoint remains unchanged
- **Optional Implementation**: Rosetta implementations can choose whether to support this endpoint
- **Graceful Degradation**: Clients can fallback to individual calls if endpoint returns 404

## Testing

The SDK includes comprehensive tests and examples:

- `examples/account_all_balances_example.go` - Usage examples
- `asserter/account_test.go` - Response validation tests
- `fetcher/account_test.go` - Fetcher integration tests
- `server/api_account_test.go` - Server handler tests

## Related Endpoints

- `/account/balance` - Get balance for a specific account/sub-account
- `/account/coins` - Get unspent coins for UTXO-based chains
- `/block` - Get block information for historical queries

## Specification Updates

This feature adds the following to the Rosetta API specification:

- New request type: `AllAccountBalancesRequest`
- New response type: `AllAccountBalancesResponse`
- New composite type: `AccountBalanceWithSubAccount`
- New endpoint: `POST /account/all_balances`