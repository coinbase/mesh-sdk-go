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

// BlockAPIService implements the server.BlockAPIServicer interface.
type BlockAPIService struct {
	network *models.NetworkIdentifier
}

// NewBlockAPIService creates a new instance of a BlockAPIService.
func NewBlockAPIService(network *models.NetworkIdentifier) server.BlockAPIServicer {
	return &BlockAPIService{
		network: network,
	}
}

// Block implements the /block endpoint.
func (s *BlockAPIService) Block(
	request *models.BlockRequest,
) (*models.BlockResponse, *models.Error) {
	return &models.BlockResponse{
		Block: &models.Block{
			BlockIdentifier: &models.BlockIdentifier{
				Index: 1000,
				Hash:  "block 1000",
			},
			ParentBlockIdentifier: &models.BlockIdentifier{
				Index: 999,
				Hash:  "block 999",
			},
			Timestamp: 1586483189000,
			Transactions: []*models.Transaction{
				{
					TransactionIdentifier: &models.TransactionIdentifier{
						Hash: "transaction 0",
					},
					Operations: []*models.Operation{
						{
							OperationIdentifier: &models.OperationIdentifier{
								Index: 0,
							},
							Type:   "Transfer",
							Status: "Success",
							Account: &models.AccountIdentifier{
								Address: "account 0",
							},
							Amount: &models.Amount{
								Value: "-1000",
								Currency: &models.Currency{
									Symbol:   "ROS",
									Decimals: 2,
								},
							},
						},
						{
							OperationIdentifier: &models.OperationIdentifier{
								Index: 1,
							},
							RelatedOperations: []*models.OperationIdentifier{
								{
									Index: 0,
								},
							},
							Type:   "Transfer",
							Status: "Reverted",
							Account: &models.AccountIdentifier{
								Address: "account 1",
							},
							Amount: &models.Amount{
								Value: "1000",
								Currency: &models.Currency{
									Symbol:   "ROS",
									Decimals: 2,
								},
							},
						},
					},
				},
			},
		},
		OtherTransactions: []*models.TransactionIdentifier{
			{
				Hash: "transaction 1",
			},
		},
	}, nil
}

// BlockTransaction implements the /block/transaction endpoint.
func (s *BlockAPIService) BlockTransaction(
	request *models.BlockTransactionRequest,
) (*models.BlockTransactionResponse, *models.Error) {
	return &models.BlockTransactionResponse{
		Transaction: &models.Transaction{
			TransactionIdentifier: &models.TransactionIdentifier{
				Hash: "transaction 1",
			},
			Operations: []*models.Operation{
				{
					OperationIdentifier: &models.OperationIdentifier{
						Index: 0,
					},
					Type:   "Reward",
					Status: "Success",
					Account: &models.AccountIdentifier{
						Address: "account 2",
					},
					Amount: &models.Amount{
						Value: "1000",
						Currency: &models.Currency{
							Symbol:   "ROS",
							Decimals: 2,
						},
					},
				},
			},
		},
	}, nil
}
