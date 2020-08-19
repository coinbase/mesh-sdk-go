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

func TestJob_CreateAccount(t *testing.T) {
	ctx := context.Background()
	s := &Scenario{
		Name: "create address",
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
		Name: "random address",
		Actions: []*Action{
			{
				Type:       RandomString,
				Input:      `{"regex": "[0-9]+", "limit":10}`,
				OutputPath: "random_address",
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
	assert.Nil(t, b)
	assert.NoError(t, err)

	assert.True(t, j.checkComplete())
	assert.Equal(t, 2, j.Index)

	assertVariableEquality(t, j.State, "network", network)
	assertVariableEquality(t, j.State, "key.public_key.curve_type", types.Secp256k1)
	assertVariableEquality(t, j.State, "address.address", address)

	randomAddress := gjson.Get(j.State, "random_address")
	assert.True(t, randomAddress.Exists())
	matched, err := regexp.Match("[0-9]+", []byte(randomAddress.String()))
	assert.True(t, matched)
	assert.NoError(t, err)

	mockHelper.AssertExpectations(t)
}
