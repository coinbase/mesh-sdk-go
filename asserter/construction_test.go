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

func TestTransactionConstruction(t *testing.T) {
	validAmount := &types.Amount{
		Value: "1",
		Currency: &types.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
	}

	invalidAmount := &types.Amount{
		Value: "",
		Currency: &types.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
	}

	var tests = map[string]struct {
		response *types.TransactionConstructionResponse
		err      error
	}{
		"valid response": {
			response: &types.TransactionConstructionResponse{
				NetworkFee: validAmount,
			},
			err: nil,
		},
		"valid response with metadata": {
			response: &types.TransactionConstructionResponse{
				NetworkFee: validAmount,
				Metadata: &map[string]interface{}{
					"blah": "hello",
				},
			},
			err: nil,
		},
		"invalid amount": {
			response: &types.TransactionConstructionResponse{
				NetworkFee: invalidAmount,
			},
			err: errors.New("Amount.Value is missing"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := TransactionConstruction(test.response)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestTransactionSubmit(t *testing.T) {
	var tests = map[string]struct {
		response *types.TransactionSubmitResponse
		err      error
	}{
		"valid response": {
			response: &types.TransactionSubmitResponse{
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "tx1",
				},
			},
			err: nil,
		},
		"invalid transaction identifier": {
			response: &types.TransactionSubmitResponse{},
			err:      errors.New("TransactionIdentifier is nil"),
		},
	}

	for name, test := range tests {
		asserter, err := New(
			context.Background(),
			&types.NetworkStatusResponse{
				NetworkStatus: []*types.NetworkStatus{
					{
						NetworkInformation: &types.NetworkInformation{
							GenesisBlockIdentifier: &types.BlockIdentifier{
								Index: 0,
							},
						},
					},
				},
				Options: &types.Options{
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
		)
		assert.NoError(t, err)
		t.Run(name, func(t *testing.T) {
			err := asserter.TransactionSubmit(test.response)
			assert.Equal(t, test.err, err)
		})
	}
}
