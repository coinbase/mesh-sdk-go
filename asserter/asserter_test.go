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
	"context"
	"errors"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestNewWithResponses(t *testing.T) {
	var (
		validNetworkStatus = &types.NetworkStatusResponse{
			GenesisBlockIdentifier: &types.BlockIdentifier{
				Index: 0,
				Hash:  "block 0",
			},
			CurrentBlockIdentifier: &types.BlockIdentifier{
				Index: 100,
				Hash:  "block 100",
			},
			CurrentBlockTimestamp: 100,
			Peers: []*types.Peer{
				{
					PeerID: "peer 1",
				},
			},
		}

		invalidNetworkStatus = &types.NetworkStatusResponse{
			GenesisBlockIdentifier: &types.BlockIdentifier{
				Index: 0,
				Hash:  "block 0",
			},
			CurrentBlockTimestamp: 100,
			Peers: []*types.Peer{
				{
					PeerID: "peer 1",
				},
			},
		}

		validNetworkOptions = &types.NetworkOptionsResponse{
			Version: &types.Version{
				RosettaVersion: "1.2.3",
				NodeVersion:    "1.0",
			},
			Allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					{
						Status:     "Success",
						Successful: true,
					},
				},
				OperationTypes: []string{
					"Transfer",
				},
				Errors: []*types.Error{
					{
						Code:      1,
						Message:   "error",
						Retriable: true,
					},
				},
			},
		}

		invalidNetworkOptions = &types.NetworkOptionsResponse{
			Allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					{
						Status:     "Success",
						Successful: true,
					},
				},
				OperationTypes: []string{
					"Transfer",
				},
				Errors: []*types.Error{
					{
						Code:      1,
						Message:   "error",
						Retriable: true,
					},
				},
			},
		}
	)

	var tests = map[string]struct {
		networkStatus  *types.NetworkStatusResponse
		networkOptions *types.NetworkOptionsResponse

		err error
	}{
		"valid responses": {
			networkStatus:  validNetworkStatus,
			networkOptions: validNetworkOptions,

			err: nil,
		},
		"invalid network status": {
			networkStatus:  invalidNetworkStatus,
			networkOptions: validNetworkOptions,

			err: errors.New("BlockIdentifier is nil"),
		},
		"invalid network options": {
			networkStatus:  validNetworkStatus,
			networkOptions: invalidNetworkOptions,

			err: errors.New("version is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			asserter, err := NewWithResponses(
				context.Background(),
				test.networkStatus,
				test.networkOptions,
			)

			if err == nil {
				assert.NotNil(t, asserter)
			}

			assert.Equal(t, test.err, err)
		})
	}
}
