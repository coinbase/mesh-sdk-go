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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestSortOperationGroups(t *testing.T) {
	m := map[int]*OperationGroup{
		2: {
			Operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 2,
					},
				},
			},
		},
		4: {
			Operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 4,
					},
				},
			},
		},
		0: {
			Operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 0,
						},
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 3,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 1,
						},
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 0,
					},
				},
			},
		},
		5: {
			Operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 5,
					},
				},
			},
		},
	}

	sortedGroups := sortOperationGroups(6, m)
	assert.Equal(t, []*OperationGroup{
		{
			Operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 0,
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 1,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 0,
						},
					},
				},
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 3,
					},
					RelatedOperations: []*types.OperationIdentifier{
						{
							Index: 1,
						},
					},
				},
			},
		},
		{
			Operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 2,
					},
				},
			},
		},
		{
			Operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 4,
					},
				},
			},
		},
		{
			Operations: []*types.Operation{
				{
					OperationIdentifier: &types.OperationIdentifier{
						Index: 5,
					},
				},
			},
		},
	}, sortedGroups)
}

func TestGroupOperations(t *testing.T) {
	var tests = map[string]struct {
		transaction *types.Transaction
		groups      []*OperationGroup
	}{
		"no ops": {
			transaction: &types.Transaction{},
			groups:      []*OperationGroup{},
		},
		"unrelated ops": {
			transaction: &types.Transaction{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 0,
						},
						Type: "op 0",
						Amount: &types.Amount{
							Currency: &types.Currency{
								Symbol: "BTC",
							},
						},
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 1,
						},
						Type: "op 1",
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 2,
						},
						Type: "op 2",
					},
				},
			},
			groups: []*OperationGroup{
				{
					Type: "op 0",
					Operations: []*types.Operation{
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 0,
							},
							Type: "op 0",
							Amount: &types.Amount{
								Currency: &types.Currency{
									Symbol: "BTC",
								},
							},
						},
					},
					Currencies: []*types.Currency{
						{
							Symbol: "BTC",
						},
					},
				},
				{
					Type:             "op 1",
					NilAmountPresent: true,
					Operations: []*types.Operation{
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 1,
							},
							Type: "op 1",
						},
					},
					Currencies: []*types.Currency{},
				},
				{
					Type:             "op 2",
					NilAmountPresent: true,
					Operations: []*types.Operation{
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 2,
							},
							Type: "op 2",
						},
					},
					Currencies: []*types.Currency{},
				},
			},
		},
		"related ops": {
			transaction: &types.Transaction{
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 0,
						},
						Type: "type 0",
						Amount: &types.Amount{
							Currency: &types.Currency{
								Symbol: "BTC",
							},
						},
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 1,
						},
						Type: "type 1",
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 2,
						},
						Type: "type 2",
						Amount: &types.Amount{
							Currency: &types.Currency{
								Symbol: "BTC",
							},
						},
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 3,
						},
						RelatedOperations: []*types.OperationIdentifier{
							{Index: 2},
						},
						Type: "type 2",
						Amount: &types.Amount{
							Currency: &types.Currency{
								Symbol: "ETH",
							},
						},
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 4,
						},
						RelatedOperations: []*types.OperationIdentifier{
							{Index: 2},
						},
						Type: "type 4",
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 5,
						},
						RelatedOperations: []*types.OperationIdentifier{
							{Index: 0},
						},
						Type: "type 0",
						Amount: &types.Amount{
							Currency: &types.Currency{
								Symbol: "BTC",
							},
						},
					},
				},
			},
			groups: []*OperationGroup{
				{
					Type: "type 0",
					Operations: []*types.Operation{
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 0,
							},
							Type: "type 0",
							Amount: &types.Amount{
								Currency: &types.Currency{
									Symbol: "BTC",
								},
							},
						},
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 5,
							},
							RelatedOperations: []*types.OperationIdentifier{
								{Index: 0},
							},
							Type: "type 0",
							Amount: &types.Amount{
								Currency: &types.Currency{
									Symbol: "BTC",
								},
							},
						},
					},
					Currencies: []*types.Currency{
						{
							Symbol: "BTC",
						},
					},
				},
				{
					Type:             "type 1",
					NilAmountPresent: true,
					Operations: []*types.Operation{
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 1,
							},
							Type: "type 1",
						},
					},
					Currencies: []*types.Currency{},
				},
				{
					Type:             "",
					NilAmountPresent: true,
					Currencies: []*types.Currency{
						{
							Symbol: "BTC",
						},
						{
							Symbol: "ETH",
						},
					},
					Operations: []*types.Operation{
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 2,
							},
							Type: "type 2",
							Amount: &types.Amount{
								Currency: &types.Currency{
									Symbol: "BTC",
								},
							},
						},
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 3,
							},
							RelatedOperations: []*types.OperationIdentifier{
								{Index: 2},
							},
							Type: "type 2",
							Amount: &types.Amount{
								Currency: &types.Currency{
									Symbol: "ETH",
								},
							},
						},
						{
							OperationIdentifier: &types.OperationIdentifier{
								Index: 4,
							},
							RelatedOperations: []*types.OperationIdentifier{
								{Index: 2},
							},
							Type: "type 4",
						},
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.ElementsMatch(t, test.groups, GroupOperations(test.transaction))
		})
	}
}
