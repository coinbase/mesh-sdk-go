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

	"github.com/coinbase/rosetta-sdk-go/server"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// ExampleConstructionService shows how to implement ConstructionAPIServicer
// with optional support for ConstructionPreprocessOperations
type ExampleConstructionService struct{}

// Ensure the service implements the interface
var _ server.ConstructionAPIServicer = &ExampleConstructionService{}

// All the required methods must be implemented...
func (s *ExampleConstructionService) ConstructionCombine(
	ctx context.Context,
	request *types.ConstructionCombineRequest,
) (*types.ConstructionCombineResponse, *types.Error) {
	// Your implementation here
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionCombine not implemented",
	}
}

func (s *ExampleConstructionService) ConstructionDerive(
	ctx context.Context,
	request *types.ConstructionDeriveRequest,
) (*types.ConstructionDeriveResponse, *types.Error) {
	// Your implementation here
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionDerive not implemented",
	}
}

func (s *ExampleConstructionService) ConstructionHash(
	ctx context.Context,
	request *types.ConstructionHashRequest,
) (*types.TransactionIdentifierResponse, *types.Error) {
	// Your implementation here
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionHash not implemented",
	}
}

func (s *ExampleConstructionService) ConstructionMetadata(
	ctx context.Context,
	request *types.ConstructionMetadataRequest,
) (*types.ConstructionMetadataResponse, *types.Error) {
	// Your implementation here
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionMetadata not implemented",
	}
}

func (s *ExampleConstructionService) ConstructionParse(
	ctx context.Context,
	request *types.ConstructionParseRequest,
) (*types.ConstructionParseResponse, *types.Error) {
	// Your implementation here
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionParse not implemented",
	}
}

func (s *ExampleConstructionService) ConstructionPayloads(
	ctx context.Context,
	request *types.ConstructionPayloadsRequest,
) (*types.ConstructionPayloadsResponse, *types.Error) {
	// Your implementation here
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionPayloads not implemented",
	}
}

func (s *ExampleConstructionService) ConstructionPreprocess(
	ctx context.Context,
	request *types.ConstructionPreprocessRequest,
) (*types.ConstructionPreprocessResponse, *types.Error) {
	// Your implementation here
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionPreprocess not implemented",
	}
}

// ConstructionPreprocessOperations - OPTIONAL API Implementation
// This method is optional and can simply return "Not Implemented" if not supported
func (s *ExampleConstructionService) ConstructionPreprocessOperations(
	ctx context.Context,
	request *types.ConstructionPreprocessOperationsRequest,
) (*types.ConstructionPreprocessOperationsResponse, *types.Error) {
	// This is an OPTIONAL API - you can simply return "Not Implemented"
	// if your blockchain doesn't need fallback operation parsing

	// Simple "not implemented" response (recommended for most implementations)
	return nil, &types.Error{
		Code:    501, // HTTP 501 Not Implemented
		Message: "ConstructionPreprocessOperations not supported by this implementation",
		Details: map[string]interface{}{
			"construct_op": request.ConstructOp,
			"suggestion":   "This Rosetta implementation doesn't support fallback operation parsing",
		},
	}
}

// ExampleFullImplementation shows how to implement ConstructionPreprocessOperations
// if you want to provide actual operation parsing functionality
func ExampleFullImplementation(
	ctx context.Context,
	request *types.ConstructionPreprocessOperationsRequest,
) (*types.ConstructionPreprocessOperationsResponse, *types.Error) {
	// Parse the construct_op and convert to Rosetta operations
	switch request.ConstructOp {
	case "transfer":
		// Parse transfer operation from request.Options
		// Create Rosetta operations for the transfer
		return &types.ConstructionPreprocessOperationsResponse{
			Operations: []*types.Operation{
				// ... transfer operations would be built here
			},
			MaxFee:   nil, // calculateMaxFee(request),
			Metadata: nil, // buildMetadata(request),
		}, nil

	case "stake":
		// Handle staking operations
		return &types.ConstructionPreprocessOperationsResponse{
			Operations: []*types.Operation{
				// ... staking operations would be built here
			},
		}, nil

	default:
		return nil, &types.Error{
			Code:    400,
			Message: "Unsupported construct_op: " + request.ConstructOp,
		}
	}
}

func (s *ExampleConstructionService) ConstructionSubmit(
	ctx context.Context,
	request *types.ConstructionSubmitRequest,
) (*types.TransactionIdentifierResponse, *types.Error) {
	// Your implementation here
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionSubmit not implemented",
	}
}

// BaseConstructionService provides default implementations for optional APIs
// Other services can embed this to get sensible defaults
type BaseConstructionService struct{}

func (b *BaseConstructionService) ConstructionPreprocessOperations(
	ctx context.Context,
	request *types.ConstructionPreprocessOperationsRequest,
) (*types.ConstructionPreprocessOperationsResponse, *types.Error) {
	return nil, &types.Error{
		Code:    501,
		Message: "ConstructionPreprocessOperations not implemented",
		Details: map[string]interface{}{
			"note": "This is an optional API. Override this method to provide custom implementation.",
		},
	}
}

// MyCustomService can embed BaseConstructionService to get default implementations
type MyCustomService struct {
	BaseConstructionService // Embedded struct provides default for optional APIs
}

// Now MyCustomService only needs to implement the methods it actually wants to support
// The optional ConstructionPreprocessOperations will automatically return "Not Implemented"

var _ server.ConstructionAPIServicer = &MyCustomService{}

// You still need to implement all required methods, but you get sensible defaults for optional ones
func (s *MyCustomService) ConstructionCombine(
	ctx context.Context,
	request *types.ConstructionCombineRequest,
) (*types.ConstructionCombineResponse, *types.Error) {
	// Your actual implementation
	return nil, nil
}

// ... implement other required methods ...

func (s *MyCustomService) ConstructionDerive(context.Context, *types.ConstructionDeriveRequest) (*types.ConstructionDeriveResponse, *types.Error) {
	return nil, nil
}
func (s *MyCustomService) ConstructionHash(context.Context, *types.ConstructionHashRequest) (*types.TransactionIdentifierResponse, *types.Error) {
	return nil, nil
}
func (s *MyCustomService) ConstructionMetadata(context.Context, *types.ConstructionMetadataRequest) (*types.ConstructionMetadataResponse, *types.Error) {
	return nil, nil
}
func (s *MyCustomService) ConstructionParse(context.Context, *types.ConstructionParseRequest) (*types.ConstructionParseResponse, *types.Error) {
	return nil, nil
}
func (s *MyCustomService) ConstructionPayloads(context.Context, *types.ConstructionPayloadsRequest) (*types.ConstructionPayloadsResponse, *types.Error) {
	return nil, nil
}
func (s *MyCustomService) ConstructionPreprocess(context.Context, *types.ConstructionPreprocessRequest) (*types.ConstructionPreprocessResponse, *types.Error) {
	return nil, nil
}
func (s *MyCustomService) ConstructionSubmit(context.Context, *types.ConstructionSubmitRequest) (*types.TransactionIdentifierResponse, *types.Error) {
	return nil, nil
}

// Note: ConstructionPreprocessOperations is automatically handled by BaseConstructionService
// If you want to override it with your own implementation, just define the method:
//
// func (s *MyCustomService) ConstructionPreprocessOperations(
//     ctx context.Context,
//     request *types.ConstructionPreprocessOperationsRequest,
// ) (*types.ConstructionPreprocessOperationsResponse, *types.Error) {
//     // Your custom implementation
//     return customImplementation(request)
// }
