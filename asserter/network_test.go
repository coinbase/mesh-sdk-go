// Copyright 2024 Coinbase, Inc.
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
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
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
			err:     ErrNetworkIdentifierIsNil,
		},
		"invalid blockchain": {
			network: &types.NetworkIdentifier{
				Blockchain: "",
				Network:    "mainnet",
			},
			err: ErrNetworkIdentifierBlockchainMissing,
		},
		"invalid network": {
			network: &types.NetworkIdentifier{
				Blockchain: "bitcoin",
				Network:    "",
			},
			err: ErrNetworkIdentifierNetworkMissing,
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
			err: ErrSubNetworkIdentifierInvalid,
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
			err:     ErrVersionIsNil,
		},
		"invalid NodeVersion": {
			version: &types.Version{
				RosettaVersion: validRosettaVersion,
			},
			err: ErrVersionNodeVersionMissing,
		},
		"invalid MiddlewareVersion": {
			version: &types.Version{
				RosettaVersion:    validRosettaVersion,
				NodeVersion:       "1.0",
				MiddlewareVersion: &invalidMiddlewareVersion,
			},
			err: ErrVersionMiddlewareVersionMissing,
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

		callMethods = []string{
			"call",
		}

		balanceExemptions = []*types.BalanceExemption{
			{
				ExemptionType: types.BalanceDynamic,
				Currency: &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				},
			},
		}

		negativeIndex = int64(-1)
		positiveIndex = int64(100)
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
		"valid Allow with call methods and exemptions": {
			allow: &types.Allow{
				OperationStatuses:       operationStatuses,
				OperationTypes:          operationTypes,
				CallMethods:             callMethods,
				BalanceExemptions:       balanceExemptions,
				HistoricalBalanceLookup: true,
				TimestampStartIndex:     &positiveIndex,
			},
		},
		"invalid Allow with exemptions and no historical": {
			allow: &types.Allow{
				OperationStatuses: operationStatuses,
				OperationTypes:    operationTypes,
				CallMethods:       callMethods,
				BalanceExemptions: balanceExemptions,
			},
			err: ErrBalanceExemptionNoHistoricalLookup,
		},
		"invalid timestamp start index": {
			allow: &types.Allow{
				OperationStatuses:   operationStatuses,
				OperationTypes:      operationTypes,
				TimestampStartIndex: &negativeIndex,
			},
			err: ErrTimestampStartIndexInvalid,
		},
		"nil Allow": {
			allow: nil,
			err:   ErrAllowIsNil,
		},
		"no OperationStatuses": {
			allow: &types.Allow{
				OperationTypes: operationTypes,
			},
			err: ErrNoAllowedOperationStatuses,
		},
		"no successful OperationStatuses": {
			allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					operationStatuses[1],
				},
				OperationTypes: operationTypes,
			},
			err: ErrNoSuccessfulAllowedOperationStatuses,
		},
		"no OperationTypes": {
			allow: &types.Allow{
				OperationStatuses: operationStatuses,
			},
			err: ErrStringArrayEmpty,
		},
		"duplicate call methods": {
			allow: &types.Allow{
				OperationStatuses: operationStatuses,
				OperationTypes:    operationTypes,
				CallMethods:       []string{"call", "call"},
				BalanceExemptions: balanceExemptions,
			},
			err: errors.New("Allow.CallMethods contains a duplicate call"),
		},
		"empty exemption": {
			allow: &types.Allow{
				OperationStatuses: operationStatuses,
				OperationTypes:    operationTypes,
				CallMethods:       []string{"call"},
				BalanceExemptions: []*types.BalanceExemption{
					{
						ExemptionType: types.BalanceDynamic,
					},
				},
			},
			err: ErrBalanceExemptionMissingSubject,
		},
		"invalid exemption type": {
			allow: &types.Allow{
				OperationStatuses: operationStatuses,
				OperationTypes:    operationTypes,
				CallMethods:       []string{"call"},
				BalanceExemptions: []*types.BalanceExemption{
					{
						ExemptionType: "test",
					},
				},
			},
			err: ErrBalanceExemptionTypeInvalid,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Allow(test.allow)
			assert.True(
				t,
				errors.Is(err, test.err) || strings.Contains(err.Error(), test.err.Error()),
			)
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
			err:          ErrErrorIsNil,
		},
		"negative code": {
			rosettaError: &types.Error{
				Code:    -1,
				Message: "signature invalid",
			},
			err: ErrErrorCodeIsNeg,
		},
		"empty message": {
			rosettaError: &types.Error{
				Code: 0,
			},
			err: ErrErrorMessageMissing,
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
		"valid errors": {
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
		"details populated": {
			rosettaErrors: []*types.Error{
				{
					Code:    0,
					Message: "error 1",
					Details: map[string]interface{}{
						"hello": "goodbye",
					},
				},
				{
					Code:    1,
					Message: "error 2",
				},
			},
			err: ErrErrorDetailsPopulated,
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
			err: ErrErrorCodeUsedMultipleTimes,
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
			err:                 ErrNetworkListResponseIsNil,
		},
		"network list duplicate": {
			networkListResponse: &types.NetworkListResponse{
				NetworkIdentifiers: []*types.NetworkIdentifier{
					network1Sub,
					network1Sub,
				},
			},
			err: ErrNetworkListResponseNetworksContainsDuplicates,
		},
		"invalid network": {
			networkListResponse: &types.NetworkListResponse{
				NetworkIdentifiers: []*types.NetworkIdentifier{
					network3,
				},
			},
			err: fmt.Errorf(
				"network identifier %s is invalid: %w",
				types.PrintStruct(network3),
				ErrNetworkIdentifierBlockchainMissing,
			),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.err, NetworkListResponse(test.networkListResponse))
		})
	}
}
