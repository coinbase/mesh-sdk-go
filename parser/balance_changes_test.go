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

package parser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestBalanceChanges(t *testing.T) {
	var (
		currency = &types.Currency{
			Symbol:   "Blah",
			Decimals: 2,
		}

		recipient = &types.AccountIdentifier{
			Address: "acct1",
		}

		recipientAmount = &types.Amount{
			Value:    "100",
			Currency: currency,
		}

		emptyAccountAndAmount = &types.Operation{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type:   "Transfer",
			Status: types.String("Success"),
		}

		emptyAmount = &types.Operation{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type:    "Transfer",
			Status:  types.String("Success"),
			Account: recipient,
		}

		recipientOperation = &types.Operation{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type:    "Transfer",
			Status:  types.String("Success"),
			Account: recipient,
			Amount:  recipientAmount,
		}

		recipientFailureOperation = &types.Operation{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Type:    "Transfer",
			Status:  types.String("Failure"),
			Account: recipient,
			Amount:  recipientAmount,
		}

		recipientTransaction = &types.Transaction{
			TransactionIdentifier: &types.TransactionIdentifier{
				Hash: "tx1",
			},
			Operations: []*types.Operation{
				emptyAccountAndAmount,
				emptyAmount,
				recipientOperation,
				recipientFailureOperation,
			},
		}

		defaultStatus = []*types.OperationStatus{
			{
				Status:     "Success",
				Successful: true,
			},
			{
				Status:     "Failure",
				Successful: false,
			},
		}
	)

	var tests = map[string]struct {
		block         *types.Block
		orphan        bool
		changes       []*BalanceChange
		allowedStatus []*types.OperationStatus
		exemptFunc    ExemptOperation
		err           error
	}{
		"simple block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					recipientTransaction,
				},
				Timestamp: asserter.MinUnixEpoch + 1,
			},
			orphan: false,
			changes: []*BalanceChange{
				{
					Account:  recipient,
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "100",
				},
			},
			allowedStatus: defaultStatus,
			err:           nil,
		},
		"simple block account exempt": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					recipientTransaction,
				},
				Timestamp: asserter.MinUnixEpoch + 1,
			},
			orphan:        false,
			changes:       []*BalanceChange{},
			allowedStatus: defaultStatus,
			exemptFunc: func(op *types.Operation) bool {
				return types.Hash(op.Account) ==
					types.Hash(recipientOperation.Account)
			},
			err: nil,
		},
		"single account sum block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					simpleTransactionFactory("tx1", "addr1", "100", currency),
					simpleTransactionFactory("tx2", "addr1", "150", currency),
					simpleTransactionFactory("tx3", "addr2", "150", currency),
				},
				Timestamp: asserter.MinUnixEpoch + 1,
			},
			orphan: false,
			changes: []*BalanceChange{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "250",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "150",
				},
			},
			allowedStatus: defaultStatus,
			err:           nil,
		},
		"single account sum orphan block": {
			block: &types.Block{
				BlockIdentifier: &types.BlockIdentifier{
					Hash:  "1",
					Index: 1,
				},
				ParentBlockIdentifier: &types.BlockIdentifier{
					Hash:  "0",
					Index: 0,
				},
				Transactions: []*types.Transaction{
					simpleTransactionFactory("tx1", "addr1", "100", currency),
					simpleTransactionFactory("tx2", "addr1", "150", currency),
					simpleTransactionFactory("tx3", "addr2", "150", currency),
				},
				Timestamp: asserter.MinUnixEpoch + 1,
			},
			orphan: true,
			changes: []*BalanceChange{
				{
					Account: &types.AccountIdentifier{
						Address: "addr1",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "-250",
				},
				{
					Account: &types.AccountIdentifier{
						Address: "addr2",
					},
					Currency: currency,
					Block: &types.BlockIdentifier{
						Hash:  "1",
						Index: 1,
					},
					Difference: "-150",
				},
			},
			allowedStatus: defaultStatus,
			err:           nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			asserter, err := simpleAsserterConfiguration(test.allowedStatus)
			assert.NoError(t, err)
			assert.NotNil(t, asserter)

			parser := New(
				asserter,
				test.exemptFunc,
				nil,
			)

			changes, err := parser.BalanceChanges(
				context.Background(),
				test.block,
				test.orphan,
			)

			assert.ElementsMatch(t, test.changes, changes)
			assert.Equal(t, test.err, err)
		})
	}
}

func simpleTransactionFactory(
	hash string,
	address string,
	value string,
	currency *types.Currency,
) *types.Transaction {
	return &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: hash,
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 0,
				},
				Type:   "Transfer",
				Status: types.String("Success"),
				Account: &types.AccountIdentifier{
					Address: address,
				},
				Amount: &types.Amount{
					Value:    value,
					Currency: currency,
				},
			},
		},
	}
}

func simpleAsserterConfiguration(
	allowedStatus []*types.OperationStatus,
) (*asserter.Asserter, error) {
	return asserter.NewClientWithOptions(
		&types.NetworkIdentifier{
			Blockchain: "bitcoin",
			Network:    "mainnet",
		},
		&types.BlockIdentifier{
			Hash:  "block 0",
			Index: 0,
		},
		[]string{"Transfer"},
		allowedStatus,
		[]*types.Error{},
		nil,
		&asserter.Validations{
			Enabled: false,
		},
	)
}
