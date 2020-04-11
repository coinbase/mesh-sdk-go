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

package services

import (
	"github.com/coinbase/rosetta-sdk-go/server"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// NetworkAPIService implements the server.NetworkAPIServicer interface.
type NetworkAPIService struct {
	network *types.NetworkIdentifier
}

// NewNetworkAPIService creates a new instance of a NetworkAPIService.
func NewNetworkAPIService(network *types.NetworkIdentifier) server.NetworkAPIServicer {
	return &NetworkAPIService{
		network: network,
	}
}

// NetworkStatus implements the /network/status endpoint.
func (s *NetworkAPIService) NetworkStatus(
	*types.NetworkStatusRequest,
) (*types.NetworkStatusResponse, *types.Error) {
	return &types.NetworkStatusResponse{
		NetworkStatus: []*types.NetworkStatus{
			{
				NetworkIdentifier: s.network,
				NetworkInformation: &types.NetworkInformation{
					CurrentBlockIdentifier: &types.BlockIdentifier{
						Index: 1000,
						Hash:  "block 1000",
					},
					CurrentBlockTimestamp: int64(1586483189000),
					GenesisBlockIdentifier: &types.BlockIdentifier{
						Index: 0,
						Hash:  "block 0",
					},
					Peers: []*types.Peer{
						{
							PeerID: "peer 1",
						},
					},
				},
			},
		},
		Version: &types.Version{
			RosettaVersion: "1.3.0",
			NodeVersion:    "0.0.1",
		},
		Options: &types.Options{
			OperationStatuses: []*types.OperationStatus{
				{
					Status:     "Success",
					Successful: true,
				},
				{
					Status:     "Reverted",
					Successful: false,
				},
			},
			OperationTypes: []string{
				"Transfer",
				"Reward",
			},
			Errors: []*types.Error{
				{
					Code:    1,
					Message: "not implemented",
				},
			},
		},
	}, nil
}
