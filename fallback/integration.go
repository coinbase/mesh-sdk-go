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

package fallback

import (
	"context"
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// OperationSelectorWithFallback wraps an existing operation selector and adds
// fallback functionality using the Rosetta API when local parsing fails
type OperationSelectorWithFallback struct {
	OriginalSelector interface{} // The original operation selector
	FallbackClient   *FallbackClient
	NetworkID        *types.NetworkIdentifier
}

// NewOperationSelectorWithFallback creates a wrapper that adds fallback functionality
func NewOperationSelectorWithFallback(
	originalSelector interface{},
	fallbackClient *FallbackClient,
	networkID *types.NetworkIdentifier,
) *OperationSelectorWithFallback {
	return &OperationSelectorWithFallback{
		OriginalSelector: originalSelector,
		FallbackClient:   fallbackClient,
		NetworkID:        networkID,
	}
}

// IsConstructOpNotSupportedError checks if an error indicates that a construct operation
// is not supported by the local operation selector
func IsConstructOpNotSupportedError(err error) bool {
	if err == nil {
		return false
	}

	// Check for common error messages that indicate unsupported operations
	errMsg := err.Error()
	return errMsg == "construct op not supported" ||
		   errMsg == "bad request - construct op not supported" ||
		   errMsg == "construct operation not supported"
}

// FallbackParseTransactionConstructOp provides fallback parsing when the original
// operation selector doesn't support a construct operation
func (w *OperationSelectorWithFallback) FallbackParseTransactionConstructOp(
	ctx context.Context,
	fromAddress string,
	constructOp string,
	options map[string]interface{},
	originalErr error,
) (operations []*types.Operation, maxFee *types.Amount, metadata []byte, err error) {
	// Only attempt fallback if the original error indicates unsupported operation
	if !IsConstructOpNotSupportedError(originalErr) {
		return nil, nil, nil, originalErr
	}

	if w.FallbackClient == nil {
		return nil, nil, nil, fmt.Errorf("fallback client not configured: %w", originalErr)
	}

	// Try the fallback API
	operations, maxFee, metadata, fallbackErr := w.FallbackClient.ConstructionPreprocessOperations(
		ctx,
		w.NetworkID,
		fromAddress,
		constructOp,
		options,
	)

	if fallbackErr != nil {
		// If fallback also fails, return the original error with context
		return nil, nil, nil, fmt.Errorf("both local and fallback parsing failed - local: %w, fallback: %v", originalErr, fallbackErr)
	}

	return operations, maxFee, metadata, nil
}

// Example usage pattern for integration:
//
// In chainstdio's rosetta_client.go, you would modify the preprocessOperationsResponse function:
//
// func (c *Constructor) preprocessOperationsResponse(
//     operations []*api.RosettaTransactionOperation,
//     maxFee *api.RosettaAmount,
//     metadata []byte,
//     err error,
// ) (*api.GetPreprocessOperationsResponse, error) {
//     if err != nil {
//         // Check if this is a construct op not supported error and we have fallback configured
//         if fallback.IsConstructOpNotSupportedError(err) && c.FallbackClient != nil {
//             // Try fallback - this would require adding the original request context
//             // and construct op details to this function signature
//             fallbackOps, fallbackMaxFee, fallbackMetadata, fallbackErr := c.FallbackClient.FallbackParseTransactionConstructOp(
//                 ctx, fromAddress, constructOp, options, err,
//             )
//             if fallbackErr == nil {
//                 // Convert to API format and return success
//                 // ... conversion logic ...
//             }
//         }
//
//         // Original error handling
//         return &api.GetPreprocessOperationsResponse{
//             Status: &api.RpcStatus{
//                 Code:    api.RpcStatus_INVALID_ARGUMENT,
//                 Message: err.Error(),
//             },
//         }, err
//     }
//     // ... rest of original function
// }