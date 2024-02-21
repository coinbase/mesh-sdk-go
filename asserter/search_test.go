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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestSearchTransactionsResponse(t *testing.T) {
	validTransaction := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "blah",
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(0),
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: int64(1),
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: int64(0),
					},
				},
				Type:    "PAYMENT",
				Status:  types.String("SUCCESS"),
				Account: validAccount,
				Amount:  validAmount,
			},
		},
	}
	tests := map[string]struct {
		response *types.SearchTransactionsResponse
		err      error
	}{
		"no transactions": {
			response: &types.SearchTransactionsResponse{},
		},
		"valid next": {
			response: &types.SearchTransactionsResponse{
				NextOffset: types.Int64(1),
			},
		},
		"invalid next": {
			response: &types.SearchTransactionsResponse{
				NextOffset: types.Int64(-1),
			},
			err: ErrNextOffsetInvalid,
		},
		"valid count": {
			response: &types.SearchTransactionsResponse{
				TotalCount: 0,
			},
		},
		"invalid count": {
			response: &types.SearchTransactionsResponse{
				TotalCount: -1,
			},
			err: ErrTotalCountInvalid,
		},
		"valid next + transaction": {
			response: &types.SearchTransactionsResponse{
				NextOffset: types.Int64(1),
				Transactions: []*types.BlockTransaction{
					{
						BlockIdentifier: validBlockIdentifier,
						Transaction:     validTransaction,
					},
				},
			},
		},
		"valid next + invalid blockIdentifier": {
			response: &types.SearchTransactionsResponse{
				NextOffset: types.Int64(1),
				Transactions: []*types.BlockTransaction{
					{
						BlockIdentifier: &types.BlockIdentifier{},
						Transaction:     validTransaction,
					},
				},
			},
			err: ErrBlockIdentifierHashMissing,
		},
		"valid next + invalid transaction": {
			err: ErrTxIdentifierIsNil,
			response: &types.SearchTransactionsResponse{
				NextOffset: types.Int64(1),
				Transactions: []*types.BlockTransaction{
					{
						BlockIdentifier: validBlockIdentifier,
						Transaction:     &types.Transaction{},
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			asserter, err := NewClientWithResponses(
				&types.NetworkIdentifier{
					Blockchain: "hello",
					Network:    "world",
				},
				&types.NetworkStatusResponse{
					GenesisBlockIdentifier: &types.BlockIdentifier{
						Index: 0,
						Hash:  "0",
					},
					CurrentBlockIdentifier: &types.BlockIdentifier{
						Index: 100,
						Hash:  "block 100",
					},
					CurrentBlockTimestamp: MinUnixEpoch + 1,
					Peers: []*types.Peer{
						{
							PeerID: "peer 1",
						},
					},
				},
				&types.NetworkOptionsResponse{
					Version: &types.Version{
						RosettaVersion: "1.4.0",
						NodeVersion:    "1.0",
					},
					Allow: &types.Allow{
						OperationStatuses: []*types.OperationStatus{
							{
								Status:     "SUCCESS",
								Successful: true,
							},
							{
								Status:     "FAILURE",
								Successful: false,
							},
						},
						OperationTypes: []string{
							"PAYMENT",
						},
					},
				},
				"",
			)
			assert.NotNil(t, asserter)
			assert.NoError(t, err)

			err = asserter.SearchTransactionsResponse(
				test.response,
			)
			if test.err != nil {
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
