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

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	mocks "github.com/coinbase/rosetta-sdk-go/mocks/constructor/worker"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tidwall/gjson"
)

func TestWaitMessage(t *testing.T) {
	tests := map[string]struct {
		input *job.FindBalanceInput

		message string
	}{
		"simple message": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}}`,
		},
		"message with address": {
			input: &job.FindBalanceInput{
				Address: "hello",
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on address hello`, // nolint
		},
		"message with address and subaccount": {
			input: &job.FindBalanceInput{
				Address: "hello",
				SubAccount: &types.SubAccountIdentifier{
					Address: "sub hello",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on address hello with sub_account {"address":"sub hello"}`, // nolint
		},
		"message with only subaccount": {
			input: &job.FindBalanceInput{
				SubAccount: &types.SubAccountIdentifier{
					Address: "sub hello",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} with sub_account {"address":"sub hello"}`, // nolint
		},
		"message with address and not address": {
			input: &job.FindBalanceInput{
				Address: "hello",
				NotAddress: []string{
					"good",
					"bye",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on address hello != to addresses ["good","bye"]`, // nolint
		},
		"message with address and not coins": {
			input: &job.FindBalanceInput{
				Address: "hello",
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `Waiting for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on address hello != to coins [{"identifier":"coin1"}]`, // nolint
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.message, waitMessage(test.input))
		})
	}
}

func TestFindBalanceWorker(t *testing.T) {
	ctx := context.Background()

	tests := map[string]struct {
		input *job.FindBalanceInput

		mockHelper *mocks.Helper

		output *job.FindBalanceOutput
		err    error
	}{
		"simple find balance with wait": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				Wait:       true,
				NotAddress: []string{"addr4"},
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Twice()
				helper.On(
					"LockedAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2"},
					nil,
				).Twice()
				helper.On(
					"Balance",
					ctx,
					mock.Anything,
					&types.AccountIdentifier{
						Address:    "addr1",
						SubAccount: (*types.SubAccountIdentifier)(nil),
					},
					&types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				).Return(&types.Amount{
					Value: "99",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				}, nil).Once()
				helper.On("Balance", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return(&types.Amount{
					Value: "0",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				}, nil).Once()
				helper.On("Balance", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return(&types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				}, nil).Once()

				return helper
			}(),
			output: &job.FindBalanceOutput{
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Balance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
		},
		"simple find balance with subaccount with wait": {
			input: &job.FindBalanceInput{
				SubAccount: &types.SubAccountIdentifier{
					Address: "sub1",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				Wait:       true,
				NotAddress: []string{"addr4"},
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Twice()
				helper.On(
					"LockedAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2"},
					nil,
				).Twice()
				helper.On("Balance", ctx, mock.Anything, &types.AccountIdentifier{
					Address: "addr1",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub1",
					},
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return(&types.Amount{
					Value: "99",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				}, nil).Once()
				helper.On("Balance", ctx, mock.Anything, &types.AccountIdentifier{
					Address: "addr3",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub1",
					},
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return(&types.Amount{
					Value: "0",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				}, nil).Once()
				helper.On("Balance", ctx, mock.Anything, &types.AccountIdentifier{
					Address: "addr1",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub1",
					},
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return(&types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				}, nil).Once()

				return helper
			}(),
			output: &job.FindBalanceOutput{
				Account: &types.AccountIdentifier{
					Address: "addr1",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub1",
					},
				},
				Balance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
		},
		"simple find coin with wait": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				Wait:        true,
				NotAddress:  []string{"addr4"},
				RequireCoin: true,
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Twice()
				helper.On(
					"LockedAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2"},
					nil,
				).Twice()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin1",
						},
						Amount: &types.Amount{
							Value: "20000",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin2",
						},
						Amount: &types.Amount{
							Value: "99",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
				}, nil).Once()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{}, nil).Once()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin2",
						},
						Amount: &types.Amount{
							Value: "99",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin3",
						},
						Amount: &types.Amount{
							Value: "100",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
				}, nil).Once()
				return helper
			}(),
			output: &job.FindBalanceOutput{
				Account: &types.AccountIdentifier{
					Address: "addr1",
				},
				Balance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				Coin: &types.CoinIdentifier{
					Identifier: "coin3",
				},
			},
		},
		"could not find coin (no create)": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAddress:  []string{"addr4"},
				RequireCoin: true,
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
				Create: -1,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Once()
				helper.On(
					"LockedAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2"},
					nil,
				).Once()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin1",
						},
						Amount: &types.Amount{
							Value: "20000",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin2",
						},
						Amount: &types.Amount{
							Value: "99",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
				}, nil).Once()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{}, nil).Once()

				return helper
			}(),
			err: ErrUnsatisfiable,
		},
		"could not find coin (unsatisfiable)": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAddress:  []string{"addr4"},
				RequireCoin: true,
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
				Create: 10,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Once()
				helper.On(
					"LockedAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2"},
					nil,
				).Once()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin1",
						},
						Amount: &types.Amount{
							Value: "20000",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin2",
						},
						Amount: &types.Amount{
							Value: "99",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
				}, nil).Once()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{}, nil).Once()

				return helper
			}(),
			err: ErrUnsatisfiable,
		},
		"could not find coin (too many accounts)": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAddress:  []string{"addr4"},
				RequireCoin: true,
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
				Create: 2,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Once()
				helper.On(
					"LockedAddresses",
					ctx,
					mock.Anything,
				).Return(
					[]string{"addr2"},
					nil,
				).Once()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin1",
						},
						Amount: &types.Amount{
							Value: "20000",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin2",
						},
						Amount: &types.Amount{
							Value: "99",
							Currency: &types.Currency{
								Symbol:   "BTC",
								Decimals: 8,
							},
						},
					},
				}, nil).Once()
				helper.On("Coins", ctx, mock.Anything, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}, &types.Currency{
					Symbol:   "BTC",
					Decimals: 8,
				}).Return([]*types.Coin{}, nil).Once()

				return helper
			}(),
			err: ErrUnsatisfiable,
		},
		"invalid amount": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAddress:  []string{"addr4"},
				RequireCoin: true,
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
				Create: 2,
			},
			mockHelper: &mocks.Helper{},
			err:        ErrInvalidInput,
		},
		"invalid currency": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "",
						Decimals: 8,
					},
				},
				NotAddress:  []string{"addr4"},
				RequireCoin: true,
				NotCoins: []*types.CoinIdentifier{
					{
						Identifier: "coin1",
					},
				},
				Create: 2,
			},
			mockHelper: &mocks.Helper{},
			err:        ErrInvalidInput,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Setup DB
			dir, err := utils.CreateTempDir()
			assert.NoError(t, err)
			defer utils.RemoveTempDir(dir)

			db, err := storage.NewBadgerStorage(ctx, dir)
			assert.NoError(t, err)
			assert.NotNil(t, db)
			defer db.Close(ctx)

			dbTx := db.NewDatabaseTransaction(ctx, true)
			defer dbTx.Discard(ctx)

			worker := New(test.mockHelper)
			output, err := worker.FindBalanceWorker(ctx, dbTx, types.PrintStruct(test.input))
			if test.err != nil {
				assert.Equal(t, "", output)
				assert.True(t, errors.Is(err, test.err))
			} else {
				assert.NoError(t, err)
				assert.Equal(t, types.PrintStruct(test.output), output)
			}

			test.mockHelper.AssertExpectations(t)
		})
	}
}

func assertVariableEquality(t *testing.T, state string, variable string, expected interface{}) {
	var saved interface{}
	err := json.Unmarshal([]byte(gjson.Get(state, variable).Raw), &saved)
	assert.NoError(t, err)
	assert.Equal(t, types.Hash(expected), types.Hash(saved))
}

func TestJob_ComplicatedTransfer(t *testing.T) {
	ctx := context.Background()
	s := &job.Scenario{
		Name: "create_address",
		Actions: []*job.Action{
			{
				Type:       job.SetVariable,
				Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
				OutputPath: "network",
			},
			{
				Type:       job.GenerateKey,
				Input:      `{"curve_type": "secp256k1"}`,
				OutputPath: "key",
			},
			{
				Type:       job.Derive,
				Input:      `{"network_identifier": {{network}}, "public_key": {{key.public_key}}}`,
				OutputPath: "address",
			},
			{
				Type:  job.SaveAddress,
				Input: `{"address": {{address.address}}, "keypair": {{key.public_key}}}`,
			},
			{
				Type:  job.PrintMessage,
				Input: `{{address.address}}`,
			},
		},
	}

	s2 := &job.Scenario{
		Name: "create_send",
		Actions: []*job.Action{
			{
				Type:       job.RandomString,
				Input:      `{"regex": "[a-z]+", "limit":10}`,
				OutputPath: "random_address",
			},
			{
				Type:       job.SetVariable,
				Input:      `[{"operation_identifier":{"index":0},"type":"","status":"","account":{"address":{{address.address}}},"amount":{"value":"-90","currency":{"symbol":"BTC","decimals":8}}},{"operation_identifier":{"index":1},"type":"","status":"","account":{"address":{{random_address}}},"amount":{"value":"100","currency":{"symbol":"BTC","decimals":8}}}]`, // nolint
				OutputPath: "create_send.operations",
			},
			{
				Type:       job.SetVariable,
				Input:      `{{network}}`,
				OutputPath: "create_send.network",
			},
			{
				Type:       job.SetVariable,
				Input:      `"10"`,
				OutputPath: "valA",
			},
			{
				Type:       job.SetVariable,
				Input:      `"16"`,
				OutputPath: "valB",
			},
			{
				Type:       job.Math,
				Input:      `{"operation":"addition", "left_value":{{valA}}, "right_value":{{valB}}}`,
				OutputPath: "create_send.confirmation_depth",
			},
			{ // Attempt to overwrite confirmation depth
				Type:       job.Math,
				Input:      `{"operation":"subtraction", "left_value":"100", "right_value":{{create_send.confirmation_depth}}}`,
				OutputPath: "create_send.confirmation_depth",
			},
		},
	}

	workflow := &job.Workflow{
		Name:      string(job.CreateAccount),
		Scenarios: []*job.Scenario{s, s2},
	}
	j := job.New(workflow)

	mockHelper := &mocks.Helper{}

	// Setup DB
	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(dir)

	db, err := storage.NewBadgerStorage(ctx, dir)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close(ctx)

	dbTx := db.NewDatabaseTransaction(ctx, true)

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
		dbTx,
		address,
		mock.Anything,
	).Return(
		nil,
	).Once()
	worker := New(mockHelper)

	assert.False(t, j.CheckComplete())

	b, err := worker.Process(ctx, dbTx, j)
	assert.Nil(t, b)
	assert.NoError(t, err)

	assert.False(t, j.CheckComplete())
	assert.Equal(t, 1, j.Index)

	assertVariableEquality(t, j.State, "network", network)
	assertVariableEquality(t, j.State, "key.public_key.curve_type", types.Secp256k1)
	assertVariableEquality(t, j.State, "address.address", address)

	b, err = worker.Process(ctx, dbTx, j)
	assert.NoError(t, err)

	randomAddress := gjson.Get(j.State, "random_address")
	assert.True(t, randomAddress.Exists())
	matched, err := regexp.Match("[a-z]+", []byte(randomAddress.String()))
	assert.True(t, matched)
	assert.NoError(t, err)

	assert.Equal(t, &job.Broadcast{
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

	assert.True(t, j.CheckComplete())
	assert.Equal(t, 2, j.Index)

	assertVariableEquality(t, j.State, "network", network)
	assertVariableEquality(t, j.State, "key.public_key.curve_type", types.Secp256k1)
	assertVariableEquality(t, j.State, "address.address", address)

	mockHelper.AssertExpectations(t)
}

func TestJob_Failures(t *testing.T) {
	tests := map[string]struct {
		scenario    *job.Scenario
		helper      *mocks.Helper
		newIndex    int
		complete    bool
		expectedErr error
	}{
		"invalid action": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:       "stuff",
						Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
						OutputPath: "network",
					},
				},
			},
			expectedErr: ErrInvalidActionType,
			helper:      &mocks.Helper{},
		},
		"invalid json": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:       job.SetVariable,
						Input:      `"network":"Testnet3", "blockchain":"Bitcoin"}`,
						OutputPath: "network",
					},
				},
			},
			expectedErr: ErrInvalidJSON,
			helper:      &mocks.Helper{},
		},
		"missing variable": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:       job.SetVariable,
						Input:      `{"network":{{var}}, "blockchain":"Bitcoin"}`,
						OutputPath: "network",
					},
				},
			},
			expectedErr: ErrVariableNotFound,
			helper:      &mocks.Helper{},
		},
		"invalid input: generate key": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:       job.GenerateKey,
						Input:      `{"curve_typ": "secp256k1"}`,
						OutputPath: "key",
					},
				},
			},
			expectedErr: ErrInvalidInput,
			helper:      &mocks.Helper{},
		},
		"invalid input: derive": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:       job.Derive,
						Input:      `{"public_key": {}}`,
						OutputPath: "address",
					},
				},
			},
			expectedErr: ErrInvalidInput,
			helper:      &mocks.Helper{},
		},
		"invalid input: save address input": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  job.SaveAddress,
						Input: `{}`,
					},
				},
			},
			expectedErr: ErrInvalidInput,
			helper:      &mocks.Helper{},
		},
		"invalid action: job.Math": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  job.Math,
						Input: `{"operation":"addition", "left_value":"1", "right_value":"B"}`,
					},
				},
			},
			expectedErr: ErrActionFailed,
			helper:      &mocks.Helper{},
		},
		"invalid broadcast: invalid operations": {
			scenario: &job.Scenario{
				Name: "create_send",
				Actions: []*job.Action{
					{
						Type:       job.SetVariable,
						Input:      `[{"operation_identifier":{"index":0},"type":"","statsbf":""}]`, // nolint
						OutputPath: "create_send.operations",
					},
				},
			},
			newIndex:    1,
			complete:    true,
			expectedErr: job.ErrOperationFormat,
			helper:      &mocks.Helper{},
		},
		"invalid broadcast: missing confirmation depth": {
			scenario: &job.Scenario{
				Name: "create_send",
				Actions: []*job.Action{
					{
						Type:       job.SetVariable,
						Input:      `[{"operation_identifier":{"index":0},"type":"","status":""}]`, // nolint
						OutputPath: "create_send.operations",
					},
				},
			},
			newIndex:    1,
			complete:    true,
			expectedErr: job.ErrConfirmationDepthInvalid,
			helper:      &mocks.Helper{},
		},
		"invalid broadcast: missing network identifier": {
			scenario: &job.Scenario{
				Name: "create_send",
				Actions: []*job.Action{
					{
						Type:       job.SetVariable,
						Input:      `[{"operation_identifier":{"index":0},"type":"","status":""}]`, // nolint
						OutputPath: "create_send.operations",
					},
					{
						Type:       job.SetVariable,
						Input:      `"10"`,
						OutputPath: "create_send.confirmation_depth",
					},
				},
			},
			newIndex:    1,
			complete:    true,
			expectedErr: job.ErrNetworkInvalid,
			helper:      &mocks.Helper{},
		},
		"invalid broadcast: metadata incorrect": {
			scenario: &job.Scenario{
				Name: "create_send",
				Actions: []*job.Action{
					{
						Type:       job.SetVariable,
						Input:      `[{"operation_identifier":{"index":0},"type":"","status":""}]`, // nolint
						OutputPath: "create_send.operations",
					},
					{
						Type:       job.SetVariable,
						Input:      `"10"`,
						OutputPath: "create_send.confirmation_depth",
					},
					{
						Type:       job.SetVariable,
						Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
						OutputPath: "create_send.network",
					},
					{
						Type:       job.SetVariable,
						Input:      `"hello"`,
						OutputPath: "create_send.preprocess_metadata",
					},
				},
			},
			newIndex:    1,
			complete:    true,
			expectedErr: job.ErrMetadataInvalid,
			helper:      &mocks.Helper{},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			workflow := &job.Workflow{
				Name:      "random",
				Scenarios: []*job.Scenario{test.scenario},
			}
			j := job.New(workflow)
			worker := New(test.helper)

			// Setup DB
			dir, err := utils.CreateTempDir()
			assert.NoError(t, err)
			defer utils.RemoveTempDir(dir)

			db, err := storage.NewBadgerStorage(ctx, dir)
			assert.NoError(t, err)
			assert.NotNil(t, db)
			defer db.Close(ctx)

			dbTx := db.NewDatabaseTransaction(ctx, true)

			assert.False(t, j.CheckComplete())

			b, err := worker.Process(ctx, dbTx, j)
			assert.Nil(t, b)
			assert.True(t, errors.Is(err, test.expectedErr))

			assert.Equal(t, test.complete, j.CheckComplete())
			assert.Equal(t, test.newIndex, j.Index)

			test.helper.AssertExpectations(t)
		})
	}
}
