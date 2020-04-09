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

	"github.com/coinbase/rosetta-sdk-go/models"

	"github.com/stretchr/testify/assert"
)

func TestNetworkIdentifier(t *testing.T) {
	var tests = map[string]struct {
		network *models.NetworkIdentifier
		err     error
	}{
		"valid network": {
			network: &models.NetworkIdentifier{
				Blockchain: "bitcoin",
				Network:    "mainnet",
			},
			err: nil,
		},
		"nil network": {
			network: nil,
			err:     errors.New("NetworkIdentifier.Blockchain is missing"),
		},
		"invalid blockchain": {
			network: &models.NetworkIdentifier{
				Blockchain: "",
				Network:    "mainnet",
			},
			err: errors.New("NetworkIdentifier.Blockchain is missing"),
		},
		"invalid network": {
			network: &models.NetworkIdentifier{
				Blockchain: "bitcoin",
				Network:    "",
			},
			err: errors.New("NetworkIdentifier.Network is missing"),
		},
		"valid sub_network": {
			network: &models.NetworkIdentifier{
				Blockchain: "bitcoin",
				Network:    "mainnet",
				SubNetworkIdentifier: &models.SubNetworkIdentifier{
					Network: "shard 1",
				},
			},
			err: nil,
		},
		"invalid sub_network": {
			network: &models.NetworkIdentifier{
				Blockchain:           "bitcoin",
				Network:              "mainnet",
				SubNetworkIdentifier: &models.SubNetworkIdentifier{},
			},
			err: errors.New("NetworkIdentifier.SubNetworkIdentifier.Network is missing"),
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
		validRosettaVersion      = "1.2.4"
	)

	var tests = map[string]struct {
		version *models.Version
		err     error
	}{
		"valid version": {
			version: &models.Version{
				RosettaVersion: validRosettaVersion,
				NodeVersion:    "1.0",
			},
			err: nil,
		},
		"valid version with middleware": {
			version: &models.Version{
				RosettaVersion:    validRosettaVersion,
				NodeVersion:       "1.0",
				MiddlewareVersion: &middlewareVersion,
			},
			err: nil,
		},
		"old RosettaVersion": {
			version: &models.Version{
				RosettaVersion: "1.2.2",
				NodeVersion:    "1.0",
			},
			err: nil,
		},
		"nil version": {
			version: nil,
			err:     errors.New("version is nil"),
		},
		"invalid NodeVersion": {
			version: &models.Version{
				RosettaVersion: validRosettaVersion,
			},
			err: errors.New("Version.NodeVersion is missing"),
		},
		"invalid MiddlewareVersion": {
			version: &models.Version{
				RosettaVersion:    validRosettaVersion,
				NodeVersion:       "1.0",
				MiddlewareVersion: &invalidMiddlewareVersion,
			},
			err: errors.New("Version.MiddlewareVersion is missing"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := Version(test.version)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestNetworkOptions(t *testing.T) {
	var (
		operationStatuses = []*models.OperationStatus{
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
		networkOptions *models.Options
		err            error
	}{
		"valid options": {
			networkOptions: &models.Options{
				OperationStatuses: operationStatuses,
				OperationTypes:    operationTypes,
			},
		},
		"nil options": {
			networkOptions: nil,
			err:            errors.New("options is nil"),
		},
		"no OperationStatuses": {
			networkOptions: &models.Options{
				OperationTypes: operationTypes,
			},
			err: errors.New("no Options.OperationStatuses found"),
		},
		"no successful OperationStatuses": {
			networkOptions: &models.Options{
				OperationStatuses: []*models.OperationStatus{
					operationStatuses[1],
				},
				OperationTypes: operationTypes,
			},
			err: errors.New("no successful Options.OperationStatuses found"),
		},
		"no OperationTypes": {
			networkOptions: &models.Options{
				OperationStatuses: operationStatuses,
			},
			err: errors.New("no Options.OperationTypes found"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.err, NetworkOptions(test.networkOptions))
		})
	}
}

func TestError(t *testing.T) {
	var tests = map[string]struct {
		rosettaError *models.Error
		err          error
	}{
		"valid error": {
			rosettaError: &models.Error{
				Code:    12,
				Message: "signature invalid",
			},
			err: nil,
		},
		"nil error": {
			rosettaError: nil,
			err:          errors.New("Error is nil"),
		},
		"negative code": {
			rosettaError: &models.Error{
				Code:    -1,
				Message: "signature invalid",
			},
			err: errors.New("Error.Code is negative"),
		},
		"empty message": {
			rosettaError: &models.Error{
				Code: 0,
			},
			err: errors.New("Error.Message is missing"),
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
		rosettaErrors []*models.Error
		err           error
	}{
		"valid errors": {
			rosettaErrors: []*models.Error{
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
			rosettaErrors: []*models.Error{
				{
					Code:    0,
					Message: "error 1",
				},
				{
					Code:    0,
					Message: "error 2",
				},
			},
			err: errors.New("error code used multiple times"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.err, Errors(test.rosettaErrors))
		})
	}
}
