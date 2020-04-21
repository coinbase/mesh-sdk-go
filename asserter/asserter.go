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

package asserter

import (
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	// ErrAsserterNotInitialized is returned when some call in the asserter
	// package requires the asserter to be initialized first.
	ErrAsserterNotInitialized = errors.New("asserter not initialized")
)

// Asserter contains all logic to perform static
// validation on Rosetta Server responses.
type Asserter struct {
	network            *types.NetworkIdentifier
	operationTypes     []string
	operationStatusMap map[string]bool
	errorTypeMap       map[int32]*types.Error
	genesisBlock       *types.BlockIdentifier
}

// NewWithResponses constructs a new Asserter
// from a NetworkStatusResponse and
// NetworkOptionsResponse.
func NewWithResponses(
	network *types.NetworkIdentifier,
	networkStatus *types.NetworkStatusResponse,
	networkOptions *types.NetworkOptionsResponse,
) (*Asserter, error) {
	if err := NetworkIdentifier(network); err != nil {
		return nil, err
	}

	if err := NetworkStatusResponse(networkStatus); err != nil {
		return nil, err
	}

	if err := NetworkOptionsResponse(networkOptions); err != nil {
		return nil, err
	}

	return NewWithOptions(
		network,
		networkStatus.GenesisBlockIdentifier,
		networkOptions.Allow.OperationTypes,
		networkOptions.Allow.OperationStatuses,
		networkOptions.Allow.Errors,
	)
}

// NewWithFile constructs a new Asserter using a specification
// file instead of responses. This can be useful for running reliable
// systems that error when updates to the server (more error types,
// more operations, etc.) significantly change how to parse the chain.
func NewWithFile(
	filePath string,
) (*Asserter, error) {
	// load file

	// parse items

	// run NewWithOptions

	return nil, errors.New("not implemented")
}

// NewWithOptions constructs a new Asserter using the provided
// arguments instead of using a NetworkStatusResponse and a
// NetworkOptionsResponse. NewWithOptions does not check the
// correctness of inputs.
func NewWithOptions(
	network *types.NetworkIdentifier,
	genesisBlockIdentifier *types.BlockIdentifier,
	operationTypes []string,
	operationStatuses []*types.OperationStatus,
	errors []*types.Error,
) (*Asserter, error) {
	if err := NetworkIdentifier(network); err != nil {
		return nil, err
	}

	if err := BlockIdentifier(genesisBlockIdentifier); err != nil {
		return nil, err
	}

	asserter := &Asserter{
		network:        network,
		operationTypes: operationTypes,
		genesisBlock:   genesisBlockIdentifier,
	}

	asserter.operationStatusMap = map[string]bool{}
	for _, status := range operationStatuses {
		asserter.operationStatusMap[status.Status] = status.Successful
	}

	asserter.errorTypeMap = map[int32]*types.Error{}
	for _, err := range errors {
		asserter.errorTypeMap[err.Code] = err
	}

	return asserter, nil
}

// Configuration returns all variables currently set in an Asserter.
// This function will error if it is called on an uninitialized asserter.
func (a *Asserter) Configuration() (*types.NetworkIdentifier, *types.BlockIdentifier, []string, []*types.OperationStatus, []*types.Error, error) {
	if a == nil {
		return nil, nil, nil, nil, nil, ErrAsserterNotInitialized
	}

	operationStatuses := []*types.OperationStatus{}
	for k, v := range a.operationStatusMap {
		operationStatuses = append(operationStatuses, &types.OperationStatus{
			Status:     k,
			Successful: v,
		})
	}

	errors := []*types.Error{}
	for _, v := range a.errorTypeMap {
		errors = append(errors, v)
	}

	return a.network, a.genesisBlock, a.operationTypes, operationStatuses, errors, nil
}

// OperationSuccessful returns a boolean indicating if a types.Operation is
// successful and should be applied in a transaction. This should only be called
// AFTER an operation has been validated.
func (a *Asserter) OperationSuccessful(operation *types.Operation) (bool, error) {
	if a == nil {
		return false, ErrAsserterNotInitialized
	}

	val, ok := a.operationStatusMap[operation.Status]
	if !ok {
		return false, fmt.Errorf("%s not found", operation.Status)
	}

	return val, nil
}
