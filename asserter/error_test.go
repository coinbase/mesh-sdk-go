package asserter

import (
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestErrorMap(t *testing.T) {
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
			)
			assert.NotNil(t, asserter)
			assert.NoError(t, err)

			err = asserter.Error(test.err)
			if test.expectedErr != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.expectedErr.Error())
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
