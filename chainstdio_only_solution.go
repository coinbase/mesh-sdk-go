// Complete solution: Implement ConstructionPreprocessOperations directly in chainstdio
// This approach requires NO changes to mesh-sdk-go

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/client"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Define types directly in chainstdio (no mesh-sdk-go changes needed)
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

// RosettaClient wrapper that includes the fallback functionality
type RosettaClientWithFallback struct {
	client            *client.APIClient
	networkIdentifier *types.NetworkIdentifier
	operationSelector OperationSelector // Your existing interface
}

// Your existing OperationSelector interface (simplified for example)
type OperationSelector interface {
	ParseTransactionConstructOp(
		ctx context.Context,
		input *TransactionConstructOpInput,
	) (
		operations []*types.Operation, // Simplified - normally would be your RosettaTransactionOperation type
		maxFee *types.Amount,         // Simplified - normally would be your RosettaAmount type
		metadata []byte,
		err error,
	)
}

// Your existing input type
type TransactionConstructOpInput struct {
	FromAddress string `json:"from_address,omitempty"`
	ConstructOp string `json:"construct_op"`
	Options     []byte `json:"options,omitempty"`
}

// Constructor
func NewRosettaClientWithFallback(
	basePath string,
	networkIdentifier *types.NetworkIdentifier,
	operationSelector OperationSelector,
) *RosettaClientWithFallback {
	config := &client.Configuration{BasePath: basePath}
	return &RosettaClientWithFallback{
		client:            client.NewAPIClient(config),
		networkIdentifier: networkIdentifier,
		operationSelector: operationSelector,
	}
}

// Main method: GetPreprocessOperations with fallback
func (r *RosettaClientWithFallback) GetPreprocessOperations(
	ctx context.Context,
	input *TransactionConstructOpInput,
) ([]*types.Operation, *types.Amount, []byte, error) {

	// Step 1: Try local OperationSelector first
	operations, maxFee, metadata, err := r.operationSelector.ParseTransactionConstructOp(ctx, input)

	// Step 2: Check if we need fallback
	if err != nil && isConstructOpNotSupportedError(err) {
		fmt.Printf("Local parsing failed: %v\n", err)
		fmt.Println("Falling back to Rosetta API...")

		// Call Rosetta API as fallback
		return r.callConstructionPreprocessOperations(ctx, input)
	}

	// Return results from local OperationSelector
	return operations, maxFee, metadata, err
}

// HTTP call to ConstructionPreprocessOperations endpoint
func (r *RosettaClientWithFallback) callConstructionPreprocessOperations(
	ctx context.Context,
	input *TransactionConstructOpInput,
) ([]*types.Operation, *types.Amount, []byte, error) {

	// Convert input format
	options := make(map[string]interface{})
	if len(input.Options) > 0 {
		if err := json.Unmarshal(input.Options, &options); err != nil {
			return nil, nil, nil, fmt.Errorf("failed to parse options: %w", err)
		}
	}

	// Create request
	request := &ConstructionPreprocessOperationsRequest{
		NetworkIdentifier: r.networkIdentifier,
		FromAddress:       input.FromAddress,
		ConstructOp:       input.ConstructOp,
		Options:           options,
	}

	// Marshal request
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// Create HTTP request
	url := r.client.GetConfig().BasePath + "/construction/preprocess_operations"
	httpReq, err := http.NewRequestWithContext(
		ctx,
		"POST",
		url,
		bytes.NewReader(requestBytes),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	// Set headers
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	// Execute request
	httpResp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer httpResp.Body.Close()

	// Read response body
	body, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Handle errors
	if httpResp.StatusCode != http.StatusOK {
		var errorResp types.Error
		if json.Unmarshal(body, &errorResp) == nil {
			return nil, nil, nil, fmt.Errorf("API error (%d): %s", httpResp.StatusCode, errorResp.Message)
		}
		return nil, nil, nil, fmt.Errorf("HTTP error %d: %s", httpResp.StatusCode, string(body))
	}

	// Parse successful response
	var response ConstructionPreprocessOperationsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to decode response: %w", err)
	}

	fmt.Printf("Rosetta API succeeded! Got %d operations\n", len(response.Operations))
	return response.Operations, response.MaxFee, response.Metadata, nil
}

// Helper function to detect unsupported construct op errors
func isConstructOpNotSupportedError(err error) bool {
	if err == nil {
		return false
	}
	errorMsg := strings.ToLower(err.Error())
	return strings.Contains(errorMsg, "construct op not supported") ||
		strings.Contains(errorMsg, "bad request - construct op not supported") ||
		strings.Contains(errorMsg, "operation not supported")
}

// Example usage in chainstdio
func ExampleUsageInChainstdio() {
	ctx := context.Background()

	// Create network identifier
	networkIdentifier := &types.NetworkIdentifier{
		Blockchain: "bitcoin",
		Network:    "mainnet",
	}

	// Your existing OperationSelector implementation
	var operationSelector OperationSelector // = your actual implementation

	// Create client with fallback
	rosettaClient := NewRosettaClientWithFallback(
		"http://localhost:8080", // Your Rosetta node URL
		networkIdentifier,
		operationSelector,
	)

	// Example input that would normally fail local parsing
	input := &TransactionConstructOpInput{
		FromAddress: "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa",
		ConstructOp: "stake", // This construct_op is not supported locally
		Options:     []byte(`{"amount": "1000000", "validator": "validator123"}`),
	}

	// Call with automatic fallback
	operations, maxFee, metadata, err := rosettaClient.GetPreprocessOperations(ctx, input)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Use the results
	fmt.Printf("Successfully got %d operations\n", len(operations))
	if maxFee != nil {
		fmt.Printf("Max fee: %s %s\n", maxFee.Value, maxFee.Currency.Symbol)
	}
	if len(metadata) > 0 {
		fmt.Printf("Metadata: %d bytes\n", len(metadata))
	}

	// Now you can proceed with these operations in your existing workflow:
	// - Pass operations to ConstructionMetadata
	// - Continue with ConstructionPayloads, etc.
}

func main() {
	fmt.Println("ConstructionPreprocessOperations fallback implementation")
	fmt.Println("This solution requires NO changes to mesh-sdk-go!")

	// Run example
	ExampleUsageInChainstdio()
}