package parser

import (
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func TestExpectedOperation(t *testing.T) {
	var tests = map[string]struct {
		intent   *types.Operation
		observed *types.Operation

		err bool
	}{
		"simple match": {
			intent: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 1,
				},
				Type: "transfer",
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Amount: &types.Amount{
					Value: "100",
				},
			},
			observed: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 3,
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: 2,
					},
				},
				Status: "success",
				Type:   "transfer",
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Amount: &types.Amount{
					Value: "100",
				},
			},
		},
		"account mismatch": {
			intent: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 1,
				},
				Type: "transfer",
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Amount: &types.Amount{
					Value: "100",
				},
			},
			observed: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 3,
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: 2,
					},
				},
				Status: "success",
				Type:   "transfer",
				Account: &types.AccountIdentifier{
					Address: "addr2",
				},
				Amount: &types.Amount{
					Value: "100",
				},
			},
			err: true,
		},
		"amount mismatch": {
			intent: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 1,
				},
				Type: "transfer",
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Amount: &types.Amount{
					Value: "100",
				},
			},
			observed: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 3,
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: 2,
					},
				},
				Status: "success",
				Type:   "transfer",
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Amount: &types.Amount{
					Value: "150",
				},
			},
			err: true,
		},
		"type mismatch": {
			intent: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 1,
				},
				Type: "transfer",
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Amount: &types.Amount{
					Value: "100",
				},
			},
			observed: &types.Operation{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 3,
				},
				RelatedOperations: []*types.OperationIdentifier{
					{
						Index: 2,
					},
				},
				Status: "success",
				Type:   "reward",
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Amount: &types.Amount{
					Value: "100",
				},
			},
			err: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ExpectedOperation(test.intent, test.observed)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestExpectedOperations(t *testing.T) {
	var tests = map[string]struct {
		intent   []*types.Operation
		observed []*types.Operation
		errExtra bool

		err bool
	}{
		"simple match": {
			intent: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					Type: "transfer",
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 5,
					},
					Type: "fee",
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "50",
					},
				},
			},
			observed: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 2,
					},
					Status: "success",
					Type:   "fee",
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "50",
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 3,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 2,
						},
					},
					Status: "success",
					Type:   "transfer",
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
			},
		},
		"err extra": {
			intent: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					Type: "transfer",
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
			},
			observed: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 2,
					},
					Status: "success",
					Type:   "fee",
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 3,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 2,
						},
					},
					Status: "success",
					Type:   "transfer",
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
			},
			errExtra: true,
			err:      true,
		},
		"missing match": {
			intent: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					Type: "transfer",
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 5,
					},
					Type: "fee",
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Amount: &types.Amount{
						Value: "50",
					},
				},
			},
			observed: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 3,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 2,
						},
					},
					Status: "success",
					Type:   "transfer",
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Amount: &types.Amount{
						Value: "100",
					},
				},
			},
			err: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ExpectedOperations(test.intent, test.observed, test.errExtra)
			if test.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
