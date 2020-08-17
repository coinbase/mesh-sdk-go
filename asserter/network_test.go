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
	"testing"

	"github.com/coinbase/rosetta-sdk-go/asserter/errs"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestNetworkIdentifier(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier
		err     error
	}{
		"valid network": {
			network: &types.NetworkIdentifier{
				Blockchain: "bitcoin",
				Network:    "mainnet",
			},
			err: nil,
		},
		"nil network": {
			network: nil,
			err:     errs.ErrNetworkIdentifierIsNil,
		},
		"invalid blockchain": {
			network: &types.NetworkIdentifier{
				Blockchain: "",
				Network:    "mainnet",
			},
			err: errs.ErrNetworkIdentifierBlockchainMissing,
		},
		"invalid network": {
			network: &types.NetworkIdentifier{
				Blockchain: "bitcoin",
				Network:    "",
			},
			err: errs.ErrNetworkIdentifierNetworkMissing,
		},
		"valid sub_network": {
			network: &types.NetworkIdentifier{
				Blockchain: "bitcoin",
				Network:    "mainnet",
				SubNetworkIdentifier: &types.SubNetworkIdentifier{
					Network: "shard 1",
				},
			},
			err: nil,
		},
		"invalid sub_network": {
			network: &types.NetworkIdentifier{
				Blockchain:           "bitcoin",
				Network:              "mainnet",
				SubNetworkIdentifier: &types.SubNetworkIdentifier{},
			},
			err: errs.ErrSubNetworkIdentifierInvalid,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := NetworkIdentifier(test.network)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestVersion(t *testing.T) {
	var (
		middlewareVersion        = "1.2"
		invalidMiddlewareVersion = ""
		validRosettaVersion      = "1.4.0"
	)

	var tests = map[string]struct {
		version *types.Version
		err     error
	}{
		"valid version": {
			version: &types.Version{
				RosettaVersion: validRosettaVersion,
				NodeVersion:    "1.0",
			},
			err: nil,
		},
		"valid version with middleware": {
			version: &types.Version{
				RosettaVersion:    validRosettaVersion,
				NodeVersion:       "1.0",
				MiddlewareVersion: &middlewareVersion,
			},
			err: nil,
		},
		"old RosettaVersion": {
			version: &types.Version{
				RosettaVersion: "1.2.0",
				NodeVersion:    "1.0",
			},
			err: nil,
		},
		"nil version": {
			version: nil,
			err:     errors.New("version is nil"),
		},
		"invalid NodeVersion": {
			version: &types.Version{
				RosettaVersion: validRosettaVersion,
			},
			err: errs.ErrVersionNodeVersionMissing,
		},
		"invalid MiddlewareVersion": {
			version: &types.Version{
				RosettaVersion:    validRosettaVersion,
				NodeVersion:       "1.0",
				MiddlewareVersion: &invalidMiddlewareVersion,
			},
			err: errs.ErrVersionMiddlewareVersionMissing,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Version(test.version)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestAllow(t *testing.T) {
	var (
		operationStatuses = []*types.OperationStatus{
			{
				Status:     "SUCCESS",
				Successful: true,
			},
			{
				Status:     "FAILURE",
				Successful: false,
			},
		}

		operationTypes = []string{
			"PAYMENT",
		}
	)

	var tests = map[string]struct {
		allow *types.Allow
		err   error
	}{
		"valid Allow": {
			allow: &types.Allow{
				OperationStatuses: operationStatuses,
				OperationTypes:    operationTypes,
			},
		},
		"nil Allow": {
			allow: nil,
			err:   errs.ErrAllowIsNil,
		},
		"no OperationStatuses": {
			allow: &types.Allow{
				OperationTypes: operationTypes,
			},
			err: errs.ErrNoAllowedOperationStatuses,
		},
		"no successful OperationStatuses": {
			allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					operationStatuses[1],
				},
				OperationTypes: operationTypes,
			},
			err: errs.ErrNoSuccessfulAllowedOperationStatuses,
		},
		"no OperationTypes": {
			allow: &types.Allow{
				OperationStatuses: operationStatuses,
			},
			err: errors.New("no Allow.OperationTypes found"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.err, Allow(test.allow))
		})
	}
}

func TestError(t *testing.T) {
	var tests = map[string]struct {
		rosettaError *types.Error
		err          error
	}{
		"valid error": {
			rosettaError: &types.Error{
				Code:    12,
				Message: "signature invalid",
			},
			err: nil,
		},
		"nil error": {
			rosettaError: nil,
			err:          errs.ErrErrorIsNil,
		},
		"negative code": {
			rosettaError: &types.Error{
				Code:    -1,
				Message: "signature invalid",
			},
			err: errs.ErrErrorCodeIsNeg,
		},
		"empty message": {
			rosettaError: &types.Error{
				Code: 0,
			},
			err: errs.ErrErrorMessageMissing,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.err, Error(test.rosettaError))
		})
	}
}

func TestErrors(t *testing.T) {
	var tests = map[string]struct {
		rosettaErrors []*types.Error
		err           error
	}{
		"valid errs": {
			rosettaErrors: []*types.Error{
				{
					Code:    0,
					Message: "error 1",
				},
				{
					Code:    1,
					Message: "error 2",
				},
			},
			err: nil,
		},
		"duplicate error codes": {
			rosettaErrors: []*types.Error{
				{
					Code:    0,
					Message: "error 1",
				},
				{
					Code:    0,
					Message: "error 2",
				},
			},
			err: errs.ErrErrorCodeUsedMultipleTimes,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.err, Errors(test.rosettaErrors))
		})
	}
}

func TestNetworkListResponse(t *testing.T) {
	var (
		network1 = &types.NetworkIdentifier{
			Blockchain: "blockchain 1",
			Network:    "network 1",
		}
		network1Sub = &types.NetworkIdentifier{
			Blockchain: "blockchain 1",
			Network:    "network 1",
			SubNetworkIdentifier: &types.SubNetworkIdentifier{
				Network: "subnetwork",
			},
		}
		network2 = &types.NetworkIdentifier{
			Blockchain: "blockchain 2",
			Network:    "network 2",
		}
		network3 = &types.NetworkIdentifier{
			Network: "network 2",
		}
	)

	var tests = map[string]struct {
		networkListResponse *types.NetworkListResponse
		err                 error
	}{
		"valid network list": {
			networkListResponse: &types.NetworkListResponse{
				NetworkIdentifiers: []*types.NetworkIdentifier{
					network1,
					network1Sub,
					network2,
				},
			},
			err: nil,
		},
		"nil network list": {
			networkListResponse: nil,
			err:                 errs.ErrNetworkListResponseIsNil,
		},
		"network list duplicate": {
			networkListResponse: &types.NetworkListResponse{
				NetworkIdentifiers: []*types.NetworkIdentifier{
					network1Sub,
					network1Sub,
				},
			},
			err: errs.ErrNetworkListResponseNetworksContinsDuplicates,
		},
		"invalid network": {
			networkListResponse: &types.NetworkListResponse{
				NetworkIdentifiers: []*types.NetworkIdentifier{
					network3,
				},
			},
			err: errs.ErrNetworkIdentifierBlockchainMissing,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.err, NetworkListResponse(test.networkListResponse))
		})
	}
}
