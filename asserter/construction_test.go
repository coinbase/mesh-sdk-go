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

	"github.com/coinbase/rosetta-sdk-go/models"

	"github.com/stretchr/testify/assert"
)

func TestTransactionConstruction(t *testing.T) {
	validAmount := &models.Amount{
		Value: "1",
		Currency: &models.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
	}

	invalidAmount := &models.Amount{
		Value: "",
		Currency: &models.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
	}

	var tests = map[string]struct {
		response *models.TransactionConstructionResponse
		err      error
	}{
		"valid response": {
			response: &models.TransactionConstructionResponse{
				NetworkFee: validAmount,
			},
			err: nil,
		},
		"valid response with metadata": {
			response: &models.TransactionConstructionResponse{
				NetworkFee: validAmount,
				Metadata: &map[string]interface{}{
					"blah": "hello",
				},
			},
			err: nil,
		},
		"invalid amount": {
			response: &models.TransactionConstructionResponse{
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
		response *models.TransactionSubmitResponse
		err      error
	}{
		"valid response": {
			response: &models.TransactionSubmitResponse{
				TransactionIdentifier: &models.TransactionIdentifier{
					Hash: "tx1",
				},
			},
			err: nil,
		},
		"invalid transaction identifier": {
			response: &models.TransactionSubmitResponse{},
			err:      errors.New("TransactionIdentifier is nil"),
		},
	}

	for name, test := range tests {
		asserter, err := New(
			context.Background(),
			&models.NetworkStatusResponse{
				NetworkStatus: []*models.NetworkStatus{
					{
						NetworkInformation: &models.NetworkInformation{
							GenesisBlockIdentifier: &models.BlockIdentifier{
								Index: 0,
							},
						},
					},
				},
				Options: &models.Options{
					OperationStatuses: []*models.OperationStatus{
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
