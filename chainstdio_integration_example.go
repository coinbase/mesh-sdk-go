// Example: How to integrate ConstructionPreprocessOperations in chainstdio
// without modifying mesh-sdk-go types package

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/client"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Define the types directly in chainstdio
type ConstructionPreprocessOperationsRequest struct {
	NetworkIdentifier *types.NetworkIdentifier `json:"network_identifier"`
	FromAddress       string                   `json:"from_address,omitempty"`
	ConstructOp       string                   `json:"construct_op"`
	Options           map[string]interface{}   `json:"options,omitempty"`
}

type ConstructionPreprocessOperationsResponse struct {
	Operations []*types.Operation `json:"operations"`
	MaxFee     *types.Amount      `json:"max_fee,omitempty"`
	Metadata   []byte            `json:"metadata,omitempty"`
}

// Implement the API call directly in chainstdio
func callConstructionPreprocessOperations(
	ctx context.Context,
	rosettaClient *client.APIClient,
	request *ConstructionPreprocessOperationsRequest,
) (*ConstructionPreprocessOperationsResponse, error) {

	// Prepare HTTP request
	url := rosettaClient.GetConfig().BasePath + "/construction/preprocess_operations"

	// Marshal request to JSON
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(string(requestBytes)))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	// Execute request
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer httpResp.Body.Close()

	// Handle non-200 responses
	if httpResp.StatusCode != http.StatusOK {
		var errorResp types.Error
		if err := json.NewDecoder(httpResp.Body).Decode(&errorResp); err == nil {
			return nil, fmt.Errorf("API error: %s", errorResp.Message)
		}
		return nil, fmt.Errorf("HTTP error: %d", httpResp.StatusCode)
	}

	// Decode response
	var response ConstructionPreprocessOperationsResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &response, nil
}

// Example usage in chainstdio GetPreprocessOperations
func GetPreprocessOperationsWithFallback(
	ctx context.Context,
	operationSelector interface{}, // Your OperationSelector interface
	rosettaClient *client.APIClient,
	networkIdentifier *types.NetworkIdentifier,
	transactionConstructOp interface{}, // Your TransactionConstructOpInput type
) (operations []*types.Operation, maxFee *types.Amount, metadata []byte, err error) {

	// Simulate the original input structure
	type TransactionConstructOpInput struct {
		FromAddress string `json:"from_address,omitempty"`
		ConstructOp string `json:"construct_op"`
		Options     []byte `json:"options,omitempty"`
	}

	// This would be your actual input
	input := transactionConstructOp.(*TransactionConstructOpInput)

	// Step 1: Try local OperationSelector (this is your existing code)
	// operations, maxFee, metadata, err := operationSelector.ParseTransactionConstructOp(ctx, input)

	// Simulate that local parsing failed
	err = fmt.Errorf("bad request - construct op not supported: %s", input.ConstructOp)

	if err != nil && isConstructOpNotSupportedError(err) {
		fmt.Printf("Local parsing failed: %v\n", err)
		fmt.Println("Falling back to Rosetta API...")

		// Step 2: Fallback to Rosetta API

		// Convert Options from []byte to map[string]interface{}
		options := make(map[string]interface{})
		if len(input.Options) > 0 {
			if unmarshalErr := json.Unmarshal(input.Options, &options); unmarshalErr != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse options: %w", unmarshalErr)
			}
		}

		// Create request
		request := &ConstructionPreprocessOperationsRequest{
			NetworkIdentifier: networkIdentifier,
			FromAddress:       input.FromAddress,
			ConstructOp:       input.ConstructOp,
			Options:           options,
		}

		// Call Rosetta API
		response, err := callConstructionPreprocessOperations(ctx, rosettaClient, request)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("Rosetta API failed: %w", err)
		}

		fmt.Printf("Rosetta API succeeded! Got %d operations\n", len(response.Operations))

		// Return the parsed results
		return response.Operations, response.MaxFee, response.Metadata, nil
	}

	// If local parsing succeeded, return those results
	// return operations, maxFee, metadata, err
	return nil, nil, nil, err // placeholder
}

func isConstructOpNotSupportedError(err error) bool {
	if err == nil {
		return false
	}
	errorMsg := err.Error()
	return strings.Contains(errorMsg, "construct op not supported") ||
		strings.Contains(errorMsg, "bad request - construct op not supported")
}

func main() {
	// Example usage
	ctx := context.Background()

	// Create Rosetta client
	config := &client.Configuration{
		BasePath: "http://localhost:8080", // Your Rosetta node URL
	}
	rosettaClient := client.NewAPIClient(config)

	// Example network identifier
	networkIdentifier := &types.NetworkIdentifier{
		Blockchain: "bitcoin",
		Network:    "mainnet",
	}

	// Example input that would come from chainstdio
	input := &struct {
		FromAddress string `json:"from_address,omitempty"`
		ConstructOp string `json:"construct_op"`
		Options     []byte `json:"options,omitempty"`
	}{
		FromAddress: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
		ConstructOp: "stake",
		Options:     []byte(`{"amount": "1000000", "validator": "validator123"}`),
	}

	// Call with fallback
	operations, maxFee, metadata, err := GetPreprocessOperationsWithFallback(
		ctx,
		nil, // operationSelector placeholder
		rosettaClient,
		networkIdentifier,
		input,
	)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Successfully got %d operations\n", len(operations))
	if maxFee != nil {
		fmt.Printf("Max fee: %s %s\n", maxFee.Value, maxFee.Currency.Symbol)
	}
	if len(metadata) > 0 {
		fmt.Printf("Metadata: %d bytes\n", len(metadata))
	}
}