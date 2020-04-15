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

func TestConstructionMetadata(t *testing.T) {
	var tests = map[string]struct {
		response *types.ConstructionMetadataResponse
		err      error
	}{
		"valid response": {
			response: &types.ConstructionMetadataResponse{
				Metadata: &map[string]interface{}{},
			},
			err: nil,
		},
		"invalid metadata": {
			response: &types.ConstructionMetadataResponse{},
			err:      errors.New("Metadata is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ConstructionMetadata(test.response)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestConstructionSubmit(t *testing.T) {
	var tests = map[string]struct {
		response *types.ConstructionSubmitResponse
		err      error
	}{
		"valid response": {
			response: &types.ConstructionSubmitResponse{
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "tx1",
				},
			},
			err: nil,
		},
		"invalid transaction identifier": {
			response: &types.ConstructionSubmitResponse{},
			err:      errors.New("TransactionIdentifier is nil"),
		},
	}

	for name, test := range tests {
		asserter, err := NewWithResponses(
			context.Background(),
			&types.NetworkStatusResponse{
				GenesisBlockIdentifier: &types.BlockIdentifier{
					Index: 0,
				},
			},
			&types.NetworkOptionsResponse{
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
		)
		assert.NoError(t, err)
		t.Run(name, func(t *testing.T) {
			err := asserter.ConstructionSubmit(test.response)
			assert.Equal(t, test.err, err)
		})
	}
}
