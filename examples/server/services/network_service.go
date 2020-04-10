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
	"github.com/coinbase/rosetta-sdk-go/models"
	"github.com/coinbase/rosetta-sdk-go/server"
)

// NetworkAPIService implements the server.NetworkAPIServicer interface.
type NetworkAPIService struct {
	network *models.NetworkIdentifier
}

// NewNetworkAPIService creates a new instance of a NetworkAPIService.
func NewNetworkAPIService(network *models.NetworkIdentifier) server.NetworkAPIServicer {
	return &NetworkAPIService{
		network: network,
	}
}

// NetworkStatus implements the /network/status endpoint.
func (s *NetworkAPIService) NetworkStatus(
	*models.NetworkStatusRequest,
) (*models.NetworkStatusResponse, *models.Error) {
	return &models.NetworkStatusResponse{
		NetworkStatus: []*models.NetworkStatus{
			{
				NetworkIdentifier: s.network,
				NetworkInformation: &models.NetworkInformation{
					CurrentBlockIdentifier: &models.BlockIdentifier{
						Index: 1000,
						Hash:  "block 1000",
					},
					CurrentBlockTimestamp: int64(1586483189000),
					GenesisBlockIdentifier: &models.BlockIdentifier{
						Index: 0,
						Hash:  "block 0",
					},
					Peers: []*models.Peer{
						{
							PeerID: "peer 1",
						},
					},
				},
			},
		},
		Version: &models.Version{
			RosettaVersion: "1.3.0",
			NodeVersion:    "0.0.1",
		},
		Options: &models.Options{
			OperationStatuses: []*models.OperationStatus{
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
			Errors: []*models.Error{
				{
					Code:    1,
					Message: "not implemented",
				},
			},
		},
	}, nil
}
