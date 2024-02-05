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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestErrorMap(t *testing.T) {
	emptyString := ""
	var tests = map[string]struct {
		err         *types.Error
		expectedErr error
	}{
		"matching error": {
			err: &types.Error{
				Code:      10,
				Message:   "error 10",
				Retriable: true,
				Details: map[string]interface{}{
					"hello": "goodbye",
				},
			},
		},
		"empty description": {
			err: &types.Error{
				Code:        10,
				Message:     "error 10",
				Description: &emptyString,
				Retriable:   true,
				Details: map[string]interface{}{
					"hello": "goodbye",
				},
			},
			expectedErr: ErrErrorDescriptionEmpty,
		},
		"negative error": {
			err: &types.Error{
				Code:      -1,
				Message:   "error 10",
				Retriable: true,
				Details: map[string]interface{}{
					"hello": "goodbye",
				},
			},
			expectedErr: ErrErrorCodeIsNeg,
		},
		"retriable error": {
			err: &types.Error{
				Code:    10,
				Message: "error 10",
				Details: map[string]interface{}{
					"hello": "goodbye",
				},
			},
			expectedErr: ErrErrorRetriableMismatch,
		},
		"code mismatch": {
			err: &types.Error{
				Code:    20,
				Message: "error 20",
				Details: map[string]interface{}{
					"hello": "goodbye",
				},
			},
			expectedErr: ErrErrorUnexpectedCode,
		},
		"message mismatch": {
			err: &types.Error{
				Code:      10,
				Retriable: true,
				Message:   "error 11",
				Details: map[string]interface{}{
					"hello": "goodbye",
				},
			},
			expectedErr: ErrErrorMessageMismatch,
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
						Hash:  "block 0",
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
						Errors: []*types.Error{
							{
								Code:      10,
								Message:   "error 10",
								Retriable: true,
							},
							{
								Code:    1,
								Message: "error 1",
							},
						},
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

			err = asserter.Error(test.err)
			if test.expectedErr != nil {
				assert.True(t, errors.Is(err, test.expectedErr))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
