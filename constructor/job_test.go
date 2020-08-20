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

package constructor

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"

	mocks "github.com/coinbase/rosetta-sdk-go/mocks/constructor"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tidwall/gjson"
)

func assertVariableEquality(t *testing.T, state string, variable string, expected interface{}) {
	var saved interface{}
	err := json.Unmarshal([]byte(gjson.Get(state, variable).Raw), &saved)
	assert.NoError(t, err)
	assert.Equal(t, types.Hash(expected), types.Hash(saved))
}

func TestJob_ComplicatedTransfer(t *testing.T) {
	ctx := context.Background()
	s := &Scenario{
		Name: "create_address",
		Actions: []*Action{
			{
				Type:       SetVariable,
				Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
				OutputPath: "network",
			},
			{
				Type:       GenerateKey,
				Input:      `{"curve_type": "secp256k1"}`,
				OutputPath: "key",
			},
			{
				Type:       Derive,
				Input:      `{"network_identifier": {{network}}, "public_key": {{key.public_key}}}`,
				OutputPath: "address",
			},
			{
				Type:  SaveAddress,
				Input: `{"address": {{address.address}}, "keypair": {{key.public_key}}}`,
			},
			{
				Type:  PrintMessage,
				Input: `{{address.address}}`,
			},
		},
	}

	s2 := &Scenario{
		Name: "create_send",
		Actions: []*Action{
			{
				Type:       RandomString,
				Input:      `{"regex": "[a-z]+", "limit":10}`,
				OutputPath: "random_address",
			},
			{
				Type:       SetVariable,
				Input:      `[{"operation_identifier":{"index":0},"type":"","status":"","account":{"address":{{address.address}}},"amount":{"value":"-90","currency":{"symbol":"BTC","decimals":8}}},{"operation_identifier":{"index":1},"type":"","status":"","account":{"address":{{random_address}}},"amount":{"value":"100","currency":{"symbol":"BTC","decimals":8}}}]`, // nolint
				OutputPath: "create_send.operations",
			},
			{
				Type:       SetVariable,
				Input:      `{{network}}`,
				OutputPath: "create_send.network",
			},
			{
				Type:       SetVariable,
				Input:      `"10"`,
				OutputPath: "valA",
			},
			{
				Type:       SetVariable,
				Input:      `"16"`,
				OutputPath: "valB",
			},
			{
				Type:       Math,
				Input:      `{"operation":"addition", "left_value":{{valA}}, "right_value":{{valB}}}`,
				OutputPath: "create_send.confirmation_depth",
			},
			{ // Attempt to overwrite confirmation depth
				Type:       Math,
				Input:      `{"operation":"subtraction", "left_value":"100", "right_value":{{create_send.confirmation_depth}}}`,
				OutputPath: "create_send.confirmation_depth",
			},
		},
	}

	workflow := &Workflow{
		Name:      string(CreateAccount),
		Scenarios: []*Scenario{s, s2},
	}
	j := NewJob(workflow)

	mockHelper := &mocks.WorkerHelper{}

	network := &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Testnet3",
	}
	address := "test"
	mockHelper.On(
		"Derive",
		ctx,
		network,
		mock.Anything,
		(map[string]interface{})(nil),
	).Return(
		address,
		nil,
		nil,
	).Once()
	mockHelper.On(
		"StoreKey",
		ctx,
		address,
		mock.Anything,
	).Return(
		nil,
	).Once()
	worker := NewWorker(mockHelper)

	assert.False(t, j.checkComplete())

	b, err := j.Process(ctx, worker)
	assert.Nil(t, b)
	assert.NoError(t, err)

	assert.False(t, j.checkComplete())
	assert.Equal(t, 1, j.Index)

	assertVariableEquality(t, j.State, "network", network)
	assertVariableEquality(t, j.State, "key.public_key.curve_type", types.Secp256k1)
	assertVariableEquality(t, j.State, "address.address", address)

	b, err = j.Process(ctx, worker)
	assert.NoError(t, err)

	randomAddress := gjson.Get(j.State, "random_address")
	assert.True(t, randomAddress.Exists())
	matched, err := regexp.Match("[a-z]+", []byte(randomAddress.String()))
	assert.True(t, matched)
	assert.NoError(t, err)

	assert.Equal(t, &Broadcast{
		Network: network,
		Intent: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 0,
				},
				Account: &types.AccountIdentifier{
					Address: "test",
				},
				Amount: &types.Amount{
					Value: "-90",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 1,
				},
				Account: &types.AccountIdentifier{
					Address: randomAddress.String(),
				},
				Amount: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
		},
		ConfirmationDepth: 74,
	}, b)

	assert.True(t, j.checkComplete())
	assert.Equal(t, 2, j.Index)

	assertVariableEquality(t, j.State, "network", network)
	assertVariableEquality(t, j.State, "key.public_key.curve_type", types.Secp256k1)
	assertVariableEquality(t, j.State, "address.address", address)

	mockHelper.AssertExpectations(t)
}
