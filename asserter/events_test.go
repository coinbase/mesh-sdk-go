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

func TestEventsBlocksResponse(t *testing.T) {
	tests := map[string]struct {
		response *types.EventsBlocksResponse
		err      error
	}{
		"no events": {
			response: &types.EventsBlocksResponse{},
		},
		"invalid max": {
			response: &types.EventsBlocksResponse{
				MaxSequence: -1,
			},
			err: ErrMaxSequenceInvalid,
		},
		"valid event": {
			response: &types.EventsBlocksResponse{
				MaxSequence: 100,
				Events: []*types.BlockEvent{
					{
						Sequence: 0,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.ADDED,
					},
					{
						Sequence: 1,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.REMOVED,
					},
				},
			},
		},
		"invalid identifier": {
			err: ErrBlockIdentifierHashMissing,
			response: &types.EventsBlocksResponse{
				MaxSequence: 100,
				Events: []*types.BlockEvent{
					{
						Sequence: 0,
						BlockIdentifier: &types.BlockIdentifier{
							Index: 0,
						},
						Type: types.ADDED,
					},
					{
						Sequence: 1,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.REMOVED,
					},
				},
			},
		},
		"invalid event type": {
			err: ErrBlockEventTypeInvalid,
			response: &types.EventsBlocksResponse{
				MaxSequence: 100,
				Events: []*types.BlockEvent{
					{
						Sequence: 0,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: "revert",
					},
					{
						Sequence: 1,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.REMOVED,
					},
				},
			},
		},
		"gap events": {
			err: ErrSequenceOutOfOrder,
			response: &types.EventsBlocksResponse{
				MaxSequence: 100,
				Events: []*types.BlockEvent{
					{
						Sequence: 0,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.ADDED,
					},
					{
						Sequence: 2,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.REMOVED,
					},
				},
			},
		},
		"out of order events": {
			err: ErrSequenceOutOfOrder,
			response: &types.EventsBlocksResponse{
				MaxSequence: 100,
				Events: []*types.BlockEvent{
					{
						Sequence: 1,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.ADDED,
					},
					{
						Sequence: 0,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.REMOVED,
					},
				},
			},
		},
		"negative sequence": {
			response: &types.EventsBlocksResponse{
				MaxSequence: 100,
				Events: []*types.BlockEvent{
					{
						Sequence: -1,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.ADDED,
					},
					{
						Sequence: 0,
						BlockIdentifier: &types.BlockIdentifier{
							Hash:  "0",
							Index: 0,
						},
						Type: types.REMOVED,
					},
				},
			},
			err: ErrSequenceInvalid,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := EventsBlocksResponse(
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
