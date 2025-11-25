# ConstructionPreprocessOperations API

## Overview

The `ConstructionPreprocessOperations` endpoint has been added to mesh-sdk-go to provide a fallback mechanism for parsing high-level transaction construction operations into Rosetta operations. This API is designed to mirror the signature and behavior of `ParseTransactionConstructOp` method used in chainstdio.

## API Specification

### Endpoint
- **POST** `/construction/preprocess_operations`

### Request (`ConstructionPreprocessOperationsRequest`)
```json
{
    "network_identifier": {
        "blockchain": "bitcoin",
        "network": "mainnet"
    },
    "from_address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
    "construct_op": "transfer",
    "options": {
        "to_address": "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2",
        "amount": "100000000",
        "asset": "btc"
    }
}
```

### Response (`ConstructionPreprocessOperationsResponse`)
```json
{
    "operations": [
        {
            "operation_identifier": {"index": 0},
            "type": "input",
            "account": {"address": "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"},
            "amount": {
                "value": "-100000000",
                "currency": {"symbol": "BTC", "decimals": 8}
            }
        },
        {
            "operation_identifier": {"index": 1},
            "type": "output",
            "account": {"address": "1BvBMSEYstWetqTFn5Au4m4GFg7xJaNVN2"},
            "amount": {
                "value": "100000000",
                "currency": {"symbol": "BTC", "decimals": 8}
            }
        }
    ],
    "max_fee": {
        "value": "10000",
        "currency": {"symbol": "BTC", "decimals": 8}
    },
    "metadata": "eyJmZWUiOiAiMTAwMDAifQ=="  // Base64 encoded JSON bytes
}
```

## Type Definitions

### Go Types

```go
type ConstructionPreprocessOperationsRequest struct {
    NetworkIdentifier *NetworkIdentifier     `json:"network_identifier"`
    FromAddress       string                 `json:"from_address,omitempty"`
    ConstructOp       string                 `json:"construct_op"`
    Options           map[string]interface{} `json:"options,omitempty"`
}

type ConstructionPreprocessOperationsResponse struct {
    Operations []*Operation `json:"operations"`
    MaxFee     *Amount      `json:"max_fee,omitempty"`
    Metadata   []byte       `json:"metadata,omitempty"`
}
```

### Mapping to chainstdio Types

The API is designed to match the `ParseTransactionConstructOp` signature:

**chainstdio Input (`TransactionConstructOpInput`)**:
- `from_address` string → `from_address` string
- `construct_op` string → `construct_op` string
- `options` []byte → `options` map[string]interface{} (JSON parsed)

**chainstdio Output**:
- `operations []*api.RosettaTransactionOperation` → `operations []*Operation`
- `maxFee *api.RosettaAmount` → `max_fee *Amount`
- `metadata []byte` → `metadata []byte`
- `err error` → HTTP error response

## Usage in chainstdio

### Fallback Pattern Implementation

```go
// In GetPreprocessOperations function
func (c *rosettaClient) GetPreprocessOperations(
    ctx context.Context,
    transactionConstructOp *api.TransactionConstructOpInput,
) ([]*api.RosettaTransactionOperation, *api.RosettaAmount, []byte, error) {

    // Step 1: Try local OperationSelector
    operations, maxFee, metadata, err := c.OperationSelector.ParseTransactionConstructOp(
        ctx,
        transactionConstructOp,
    )

    if err != nil && isConstructOpNotSupportedError(err) {
        // Step 2: Fallback to Rosetta API

        // Convert options from []byte to map[string]interface{}
        options := make(map[string]interface{})
        if len(transactionConstructOp.Options) > 0 {
            if err := json.Unmarshal(transactionConstructOp.Options, &options); err != nil {
                return nil, nil, nil, fmt.Errorf("failed to parse options: %w", err)
            }
        }

        // Call Rosetta API
        request := &types.ConstructionPreprocessOperationsRequest{
            NetworkIdentifier: c.networkIdentifier,
            FromAddress:       transactionConstructOp.FromAddress,
            ConstructOp:       transactionConstructOp.ConstructOp,
            Options:           options,
        }

        response, clientErr, err := c.rosettaClient.ConstructionAPI.ConstructionPreprocessOperations(
            ctx,
            request,
        )

        if err != nil {
            return nil, nil, nil, fmt.Errorf("rosetta API failed: %w", err)
        }

        // Convert response back to chainstdio types
        operations = convertToRosettaTransactionOperations(response.Operations)
        maxFee = convertToRosettaAmount(response.MaxFee)
        metadata = response.Metadata
    }

    return operations, maxFee, metadata, err
}

func isConstructOpNotSupportedError(err error) bool {
    // Check if error indicates unsupported construct_op
    return strings.Contains(err.Error(), "construct op not supported") ||
           strings.Contains(err.Error(), "bad request - construct op not supported")
}
```

## Implementation Components

### 1. Types Package
- `types/construction_preprocess_operations_request.go`
- `types/construction_preprocess_operations_response.go`

### 2. Client Package
- `client/api_construction.go` - Added `ConstructionPreprocessOperations` method

### 3. Server Package
- `server/api.go` - Updated interfaces and router
- `server/api_construction.go` - Added HTTP handler and route

### 4. Asserter Package
- `asserter/construction.go` - Response validation
- `asserter/server.go` - Request validation
- `asserter/errors.go` - Error constants

### 5. Fetcher Package
- `fetcher/construction.go` - High-level wrapper with connection management

## Error Handling

The API follows standard Rosetta error handling:

- **400 Bad Request**: Invalid request format or missing required fields
- **500 Internal Server Error**: Server-side processing errors
- **Network errors**: Handled by client with retry logic

### Validation Errors
- `ErrConstructionPreprocessOperationsRequestIsNil`
- `ErrConstructionPreprocessOperationsRequestConstructOpEmpty`
- `ErrConstructionPreprocessOperationsResponseIsNil`

## Example Usage

See `examples/construction_preprocess_operations_example.go` for complete usage examples including:

1. Direct client API usage
2. Fetcher wrapper usage (recommended)
3. Fallback pattern implementation

## Benefits

1. **Seamless Integration**: Matches existing `ParseTransactionConstructOp` signature
2. **Fallback Mechanism**: Graceful degradation when local parsing fails
3. **Type Safety**: Full type validation and error handling
4. **Performance**: Connection pooling and retry logic via Fetcher
5. **Extensibility**: Easy to add new `construct_op` types via Rosetta implementations

## Testing

The implementation passes all build tests and includes:
- Type validation
- JSON serialization/deserialization
- HTTP client/server communication
- Error handling scenarios

To test the build:
```bash
go build ./...
```