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

package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/tidwall/gjson"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	mocks "github.com/coinbase/rosetta-sdk-go/mocks/constructor/worker"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

func TestBalanceMessage(t *testing.T) {
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
			message: `looking for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}}`,
		},
		"message with account": {
			input: &job.FindBalanceInput{
				AccountIdentifier: &types.AccountIdentifier{Address: "hello"},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `looking for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello"}`, // nolint
		},
		"message with only subaccount": {
			input: &job.FindBalanceInput{
				SubAccountIdentifier: &types.SubAccountIdentifier{
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
			message: `looking for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} with sub_account {"address":"sub hello"}`, // nolint
		},
		"message with not address": {
			input: &job.FindBalanceInput{
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
			message: `looking for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} != to addresses ["good","bye"]`, // nolint
		},
		"message with not account": {
			input: &job.FindBalanceInput{
				NotAccountIdentifier: []*types.AccountIdentifier{
					{Address: "good"},
					{Address: "bye"},
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
			},
			message: `looking for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} != to accounts [{"address":"good"},{"address":"bye"}]`, // nolint
		},
		"message with account and not coins": {
			input: &job.FindBalanceInput{
				AccountIdentifier: &types.AccountIdentifier{Address: "hello"},
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
			message: `looking for balance {"value":"100","currency":{"symbol":"BTC","decimals":8}} on account {"address":"hello"} != to coins [{"identifier":"coin1"}]`, // nolint
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.message, balanceMessage(test.input))
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
		"simple find balance (satisfiable)": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAccountIdentifier: []*types.AccountIdentifier{
					{Address: "addr4"},
				},
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
					nil,
				).Once()
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
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				}, nil).Once()

				return helper
			}(),
			output: &job.FindBalanceOutput{
				AccountIdentifier: &types.AccountIdentifier{
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
		"simple find balance (random create)": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "0",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAccountIdentifier: []*types.AccountIdentifier{
					{Address: "addr4"},
				},
				CreateLimit:       100,
				CreateProbability: 100,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
					nil,
				).Once()

				return helper
			}(),
			err: ErrCreateAccount,
		},
		"simple find balance (can't random create)": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAccountIdentifier: []*types.AccountIdentifier{
					{Address: "addr4"},
				},
				CreateLimit:       100,
				CreateProbability: 100,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
					nil,
				).Once()
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
					Value: "10",
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
					Value: "15",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				}, nil).Once()

				return helper
			}(),
			err: ErrUnsatisfiable,
		},
		"simple find balance (no create and unsatisfiable)": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAccountIdentifier: []*types.AccountIdentifier{
					{Address: "addr4"},
				},
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
					nil,
				).Once()
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

				return helper
			}(),
			err: ErrUnsatisfiable,
		},
		"simple find balance and create": {
			input: &job.FindBalanceInput{
				MinimumBalance: &types.Amount{
					Value: "0",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAccountIdentifier: []*types.AccountIdentifier{
					{Address: "addr4"},
				},
				CreateLimit: 100,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr1"},
						{Address: "addr2"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()

				return helper
			}(),
			err: ErrCreateAccount,
		},
		"simple find balance with subaccount": {
			input: &job.FindBalanceInput{
				SubAccountIdentifier: &types.SubAccountIdentifier{
					Address: "sub1",
				},
				MinimumBalance: &types.Amount{
					Value: "100",
					Currency: &types.Currency{
						Symbol:   "BTC",
						Decimals: 8,
					},
				},
				NotAddress: []string{"addr4"},
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{
							Address: "addr1",
							SubAccount: &types.SubAccountIdentifier{
								Address: "sub1",
							},
						},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
					nil,
				).Once()
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
				AccountIdentifier: &types.AccountIdentifier{
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
		"simple find coin": {
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
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
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
				AccountIdentifier: &types.AccountIdentifier{
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
					Identifier: "coin2",
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
				CreateLimit: -1,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
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
				CreateLimit: 10,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
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
				CreateLimit: 2,
			},
			mockHelper: func() *mocks.Helper {
				helper := &mocks.Helper{}
				helper.On(
					"AllAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
						{Address: "addr1"},
						{Address: "addr3"},
						{Address: "addr4"},
					},
					nil,
				).Once()
				helper.On(
					"LockedAccounts",
					ctx,
					mock.Anything,
				).Return(
					[]*types.AccountIdentifier{
						{Address: "addr2"},
					},
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
				CreateLimit: 2,
			},
			mockHelper: &mocks.Helper{},
			err:        asserter.ErrAmountValueMissing,
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
				CreateLimit: 2,
			},
			mockHelper: &mocks.Helper{},
			err:        asserter.ErrAmountCurrencySymbolEmpty,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Setup DB
			dir, err := utils.CreateTempDir()
			assert.NoError(t, err)
			defer utils.RemoveTempDir(dir)

			db, err := database.NewBadgerDatabase(
				ctx,
				dir,
				database.WithIndexCacheSize(database.TinyIndexCacheSize),
			)
			assert.NoError(t, err)
			assert.NotNil(t, db)
			defer db.Close(ctx)

			dbTx := db.Transaction(ctx)
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
				OutputPath: "account",
			},
			{
				Type:  job.SaveAccount,
				Input: `{"account_identifier": {{account.account_identifier}}, "keypair": {{key.public_key}}}`,
			},
			{
				Type:  job.PrintMessage,
				Input: `{{account.account_identifier}}`,
			},
		},
	}

	s2 := &job.Scenario{
		Name: "create_send",
		Actions: []*job.Action{
			{
				Type:       job.SetVariable,
				Input:      `{"symbol":"BTC","decimals":8}`,
				OutputPath: "default_curr",
			},
			{
				Type:       job.RandomString,
				Input:      `{"regex": "[a-z]+", "limit":10}`,
				OutputPath: "random_address",
			},
			{
				Type:       job.SetVariable,
				Input:      `[{"operation_identifier":{"index":0},"type":"","account":{{account.account_identifier}},"amount":{"value":"-90","currency":{{default_curr}}}},{"operation_identifier":{"index":1},"type":"","account":{"address":{{random_address}}},"amount":{"value":"100","currency":{{default_curr}}}}]`, // nolint
				OutputPath: "create_send.operations",
			},
			{
				Type:       job.SetVariable,
				Input:      `{{network}}`,
				OutputPath: "create_send.network",
			},
			{
				Type:       job.RandomNumber,
				Input:      `{"minimum":"10", "maximum":"100"}`,
				OutputPath: "rand_number",
			},
			{
				Type:  job.Assert,
				Input: `{{rand_number}}`,
			},
			{
				Type:       job.SetVariable,
				Input:      `{"symbol":"ETH","decimals":18}`,
				OutputPath: "eth_curr",
			},
			{
				Type:       job.SetVariable,
				Input:      `[{"value":"100", "currency":{{default_curr}}},{"value":"200", "currency":{{eth_curr}}}]`,
				OutputPath: "mock_suggested_fee_resp",
			},
			{
				Type:       job.FindCurrencyAmount,
				Input:      `{"currency":{{eth_curr}}, "amounts":{{mock_suggested_fee_resp}}}`,
				OutputPath: "eth_amount",
			},
			{
				Type:       job.Math,
				Input:      `{"operation":"subtraction", "left_value":{{eth_amount.value}}, "right_value":"200"}`,
				OutputPath: "eth_check",
			},
			{
				Type:  job.Assert,
				Input: `{{eth_check}}`,
			},
			{
				Type:  job.PrintMessage,
				Input: `{"random_number": {{rand_number}}}`,
			},
			{
				Type:       job.LoadEnv,
				Input:      `"valA"`,
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
			{ // Test multiplication / division
				Type:       job.Math,
				Input:      `{"operation":"multiplication", "left_value":"2", "right_value":{{create_send.confirmation_depth}}}`,
				OutputPath: "create_send.confirmation_depth",
			},
			{
				Type:       job.Math,
				Input:      `{"operation":"division", "left_value":"296", "right_value":{{create_send.confirmation_depth}}}`,
				OutputPath: "create_send.confirmation_depth",
			},
		},
	}

	os.Setenv("valA", `"10"`)
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

	db, err := database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close(ctx)

	dbTx := db.Transaction(ctx)

	network := &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Testnet3",
	}
	account := &types.AccountIdentifier{Address: "test"}
	mockHelper.On(
		"Derive",
		ctx,
		network,
		mock.Anything,
		(map[string]interface{})(nil),
	).Return(
		account,
		nil,
		nil,
	).Once()
	mockHelper.On(
		"StoreKey",
		ctx,
		dbTx,
		account,
		mock.Anything,
	).Return(
		nil,
	).Once()
	worker := New(mockHelper)

	assert.False(t, j.CheckComplete())

	b, executionErr := worker.Process(ctx, dbTx, j)
	assert.Nil(t, b)
	assert.Nil(t, executionErr)

	assert.False(t, j.CheckComplete())
	assert.Equal(t, 1, j.Index)

	assertVariableEquality(t, j.State, "network", network)
	assertVariableEquality(t, j.State, "key.public_key.curve_type", types.Secp256k1)
	assertVariableEquality(t, j.State, "account.account_identifier", account)

	b, executionErr = worker.Process(ctx, dbTx, j)
	assert.Nil(t, executionErr)

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
				Account: account,
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
		ConfirmationDepth: 2,
	}, b)

	assert.True(t, j.CheckComplete())
	assert.Equal(t, 2, j.Index)

	assertVariableEquality(t, j.State, "network", network)
	assertVariableEquality(t, j.State, "key.public_key.curve_type", types.Secp256k1)
	assertVariableEquality(t, j.State, "account.account_identifier", account)

	mockHelper.AssertExpectations(t)
}

func TestJob_Failures(t *testing.T) {
	tests := map[string]struct {
		scenario *job.Scenario
		helper   *mocks.Helper
		newIndex int
		complete bool

		executionErr *Error
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
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:       "stuff",
					Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
					OutputPath: "network",
				},
				ProcessedInput: `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
				Err:            ErrInvalidActionType,
			},
			helper: &mocks.Helper{},
		},
		"assertion invalid input": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  job.Assert,
						Input: `"hello"`,
					},
				},
			},
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:  job.Assert,
					Input: `"hello"`,
				},
				ProcessedInput: `"hello"`,
				Err:            fmt.Errorf("hello is not an integer"),
			},
			helper: &mocks.Helper{},
		},
		"failed assertion": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  job.Assert,
						Input: `"-1"`,
					},
				},
			},
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:  job.Assert,
					Input: `"-1"`,
				},
				ProcessedInput: `"-1"`,
				Err:            ErrActionFailed,
			},
			helper: &mocks.Helper{},
		},
		"invalid currency": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  job.FindCurrencyAmount,
						Input: `{"currency":{"decimals":8}}`,
					},
				},
			},
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:  job.FindCurrencyAmount,
					Input: `{"currency":{"decimals":8}}`,
				},
				ProcessedInput: `{"currency":{"decimals":8}}`,
				Err:            asserter.ErrAmountCurrencySymbolEmpty,
			},
			helper: &mocks.Helper{},
		},
		"repeat currency": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  job.FindCurrencyAmount,
						Input: `{"currency":{"symbol":"BTC", "decimals":8},"amounts":[{"value":"100","currency":{"symbol":"BTC", "decimals":8}},{"value":"100","currency":{"symbol":"BTC", "decimals":8}}]}`, // nolint
					},
				},
			},
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:  job.FindCurrencyAmount,
					Input: `{"currency":{"symbol":"BTC", "decimals":8},"amounts":[{"value":"100","currency":{"symbol":"BTC", "decimals":8}},{"value":"100","currency":{"symbol":"BTC", "decimals":8}}]}`, // nolint
				},
				ProcessedInput: `{"currency":{"symbol":"BTC", "decimals":8},"amounts":[{"value":"100","currency":{"symbol":"BTC", "decimals":8}},{"value":"100","currency":{"symbol":"BTC", "decimals":8}}]}`, // nolint
				Err:            asserter.ErrCurrencyUsedMultipleTimes,
			},
			helper: &mocks.Helper{},
		},
		"can't find currency": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  job.Assert,
						Input: `"1"`,
					},
					{
						Type:       job.SetVariable,
						Input:      `"BTC"`,
						OutputPath: "symbol",
					},
					{
						Type:  job.FindCurrencyAmount,
						Input: `{"currency":{"symbol":{{symbol}}, "decimals":8}}`,
					},
				},
			},
			executionErr: &Error{
				Workflow:    "random",
				Scenario:    "create_address",
				ActionIndex: 2,
				Action: &job.Action{
					Type:  job.FindCurrencyAmount,
					Input: `{"currency":{"symbol":{{symbol}}, "decimals":8}}`,
				},
				ProcessedInput: `{"currency":{"symbol":"BTC", "decimals":8}}`,
				State:          "{\"symbol\":\"BTC\"}",
				Err:            ErrActionFailed,
			},
			helper: &mocks.Helper{},
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
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:       job.SetVariable,
					Input:      `"network":"Testnet3", "blockchain":"Bitcoin"}`,
					OutputPath: "network",
				},
				Err: ErrInvalidJSON,
			},
			helper: &mocks.Helper{},
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
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:       job.SetVariable,
					Input:      `{"network":{{var}}, "blockchain":"Bitcoin"}`,
					OutputPath: "network",
				},
				Err: ErrVariableNotFound,
			},
			helper: &mocks.Helper{},
		},
		"invalid input: negative difference in random amount": {
			scenario: &job.Scenario{
				Name: "random_number",
				Actions: []*job.Action{
					{
						Type:  job.RandomNumber,
						Input: `{"minimum":"-100", "maximum":"-200"}`,
					},
				},
			},
			executionErr: &Error{
				Workflow: "random",
				Scenario: "random_number",
				Action: &job.Action{
					Type:  job.RandomNumber,
					Input: `{"minimum":"-100", "maximum":"-200"}`,
				},
				ProcessedInput: `{"minimum":"-100", "maximum":"-200"}`,
				Err:            fmt.Errorf("maximum value -200 < minimum value -100"),
			},
			helper: &mocks.Helper{},
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
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:       job.GenerateKey,
					Input:      `{"curve_typ": "secp256k1"}`,
					OutputPath: "key",
				},
				ProcessedInput: `{"curve_typ": "secp256k1"}`,
				Err:            fmt.Errorf("unknown field \"curve_typ\""),
			},
			helper: &mocks.Helper{},
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
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:       job.Derive,
					Input:      `{"public_key": {}}`,
					OutputPath: "address",
				},
				ProcessedInput: `{"public_key": {}}`,
				Err:            asserter.ErrPublicKeyBytesEmpty,
			},
			helper: &mocks.Helper{},
		},
		"invalid input: save address input": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  job.SaveAccount,
						Input: `{}`,
					},
				},
			},
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:  job.SaveAccount,
					Input: `{}`,
				},
				ProcessedInput: `{}`,
				Err:            asserter.ErrAccountIsNil,
			},
			helper: &mocks.Helper{},
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
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:  job.Math,
					Input: `{"operation":"addition", "left_value":"1", "right_value":"B"}`,
				},
				ProcessedInput: `{"operation":"addition", "left_value":"1", "right_value":"B"}`,
				Err:            fmt.Errorf("B is not an integer"),
			},
			helper: &mocks.Helper{},
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
			newIndex: 1,
			complete: true,
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_send",
				State:    "{\"create_send\":{\"operations\":[{\"operation_identifier\":{\"index\":0},\"type\":\"\",\"statsbf\":\"\"}]}}", // nolint
				Err:      fmt.Errorf("failed to unmarshal operations of scenario create_send"),
			},
			helper: &mocks.Helper{},
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
			newIndex: 1,
			complete: true,
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_send",
				State:    "{\"create_send\":{\"operations\":[{\"operation_identifier\":{\"index\":0},\"type\":\"\",\"status\":\"\"}]}}", // nolint
				Err: fmt.Errorf(
					"failed to unmarshal confirmation depth of scenario create_send",
				),
			},
			helper: &mocks.Helper{},
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
			newIndex: 1,
			complete: true,
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_send",
				State:    "{\"create_send\":{\"operations\":[{\"operation_identifier\":{\"index\":0},\"type\":\"\",\"status\":\"\"}],\"confirmation_depth\":\"10\"}}", // nolint
				Err:      fmt.Errorf("failed to unmarshal network of scenario create_send"),
			},
			helper: &mocks.Helper{},
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
			newIndex: 1,
			complete: true,
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_send",
				State:    "{\"create_send\":{\"operations\":[{\"operation_identifier\":{\"index\":0},\"type\":\"\",\"status\":\"\"}],\"confirmation_depth\":\"10\",\"network\":{\"network\":\"Testnet3\", \"blockchain\":\"Bitcoin\"},\"preprocess_metadata\":\"hello\"}}", // nolint
				Err: fmt.Errorf(
					"failed to unmarshal preprocess metadata of scenario create_send",
				),
			},
			helper: &mocks.Helper{},
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

			db, err := database.NewBadgerDatabase(
				ctx,
				dir,
				database.WithIndexCacheSize(database.TinyIndexCacheSize),
			)
			assert.NoError(t, err)
			assert.NotNil(t, db)
			defer db.Close(ctx)

			dbTx := db.Transaction(ctx)

			assert.False(t, j.CheckComplete())

			b, executionErr := worker.Process(ctx, dbTx, j)
			assert.Nil(t, b)
			assert.Contains(t, executionErr.Err.Error(), test.executionErr.Err.Error())
			executionErr.Err = test.executionErr.Err // makes equality check easier
			assert.Equal(t, test.executionErr, executionErr)

			assert.Equal(t, test.complete, j.CheckComplete())
			assert.Equal(t, test.newIndex, j.Index)

			test.helper.AssertExpectations(t)
		})
	}
}

func TestHTTPRequestWorker(t *testing.T) {
	var tests = map[string]struct {
		input          *job.HTTPRequestInput
		dontPrependURL bool

		expectedPath    string
		expectedLatency int
		expectedMethod  string
		expectedBody    string

		response    string
		contentType string
		statusCode  int

		output string
		err    error
	}{
		"simple get": {
			input: &job.HTTPRequestInput{
				Method:  job.MethodGet,
				URL:     "/faucet?test=123",
				Timeout: 100,
			},
			expectedPath:    "/faucet?test=123",
			expectedLatency: 1,
			expectedMethod:  http.MethodGet,
			expectedBody:    "",
			contentType:     "application/json; charset=UTF-8",
			response:        `{"money":100}`,
			statusCode:      http.StatusOK,
			output:          `{"money":100}`,
		},
		"simple post": {
			input: &job.HTTPRequestInput{
				Method:  job.MethodPost,
				URL:     "/faucet",
				Timeout: 100,
				Body:    `{"address":"123"}`,
			},
			expectedPath:    "/faucet",
			expectedLatency: 1,
			expectedMethod:  http.MethodPost,
			expectedBody:    `{"address":"123"}`,
			contentType:     "application/json; charset=UTF-8",
			response:        `{"money":100}`,
			statusCode:      http.StatusOK,
			output:          `{"money":100}`,
		},
		"invalid method": {
			input: &job.HTTPRequestInput{
				Method:  "hello",
				URL:     "/faucet",
				Timeout: 100,
				Body:    `{"address":"123"}`,
			},
			err: ErrInvalidInput,
		},
		"invalid timeout": {
			input: &job.HTTPRequestInput{
				Method:  job.MethodPost,
				URL:     "/faucet",
				Timeout: -1,
				Body:    `{"address":"123"}`,
			},
			err: ErrInvalidInput,
		},
		"no url": {
			input: &job.HTTPRequestInput{
				Method:  job.MethodPost,
				URL:     "",
				Timeout: 100,
				Body:    `{"address":"123"}`,
			},
			dontPrependURL: true,
			err:            fmt.Errorf("empty url"),
		},
		"invalid url": {
			input: &job.HTTPRequestInput{
				Method:  job.MethodPost,
				URL:     "blah",
				Timeout: 100,
				Body:    `{"address":"123"}`,
			},
			dontPrependURL: true,
			err:            fmt.Errorf("invalid URI for request"),
		},
		"timeout": {
			input: &job.HTTPRequestInput{
				Method:  job.MethodGet,
				URL:     "/faucet?test=123",
				Timeout: 1,
			},
			expectedPath:    "/faucet?test=123",
			expectedLatency: 1200,
			expectedMethod:  http.MethodGet,
			expectedBody:    "",
			contentType:     "application/json; charset=UTF-8",
			response:        `{"money":100}`,
			statusCode:      http.StatusOK,
			err: fmt.Errorf(
				"context deadline exceeded (Client.Timeout exceeded while awaiting headers)",
			),
		},
		"error": {
			input: &job.HTTPRequestInput{
				Method:  job.MethodGet,
				URL:     "/faucet?test=123",
				Timeout: 10,
			},
			expectedPath:    "/faucet?test=123",
			expectedLatency: 1,
			expectedMethod:  http.MethodGet,
			expectedBody:    "",
			contentType:     "application/json; charset=UTF-8",
			response:        `{"money":100}`,
			statusCode:      http.StatusInternalServerError,
			err:             ErrActionFailed,
		},
		"invalid content type": { // we don't throw an error
			input: &job.HTTPRequestInput{
				Method:  job.MethodGet,
				URL:     "/faucet?test=123",
				Timeout: 10,
			},
			expectedPath:    "/faucet?test=123",
			expectedLatency: 1,
			expectedMethod:  http.MethodGet,
			expectedBody:    "",
			contentType:     "text/plain",
			response:        `{"money":100}`,
			statusCode:      http.StatusOK,
			output:          `{"money":100}`,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, test.expectedMethod, r.Method)
				assert.Equal(t, test.expectedPath, r.URL.RequestURI())
				defer r.Body.Close()

				body, err := ioutil.ReadAll(r.Body)
				assert.NoError(t, err)
				assert.Equal(t, test.expectedBody, string(body))

				time.Sleep(time.Duration(test.expectedLatency) * time.Millisecond)

				w.Header().Set("Content-Type", test.contentType)
				w.WriteHeader(test.statusCode)
				fmt.Fprintf(w, test.response)
			}))

			defer ts.Close()

			if !test.dontPrependURL {
				test.input.URL = ts.URL + test.input.URL
			}

			output, err := HTTPRequestWorker(types.PrintStruct(test.input))
			if test.err != nil {
				assert.Equal(t, "", output)
				assert.Contains(t, err.Error(), test.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, test.output, output)
			}
		})
	}
}

func TestBlobWorkers(t *testing.T) {
	tests := map[string]struct {
		scenario *job.Scenario
		helper   *mocks.Helper

		assertState  map[string]string
		executionErr *Error
	}{
		"simple save and get": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  "set_blob",
						Input: `{"key":"Testnet3", "value":"Bitcoin"}`,
					},
					{
						Type:       "get_blob",
						Input:      `{"key":"Testnet3"}`,
						OutputPath: "k",
					},
				},
			},
			assertState: map[string]string{
				"k": "Bitcoin",
			},
			helper: func() *mocks.Helper {
				h := &mocks.Helper{}
				h.On(
					"SetBlob",
					mock.Anything,
					mock.Anything,
					types.Hash("Testnet3"),
					[]byte(`"Bitcoin"`),
				).Return(nil).Once()

				h.On(
					"GetBlob",
					mock.Anything,
					mock.Anything,
					types.Hash("Testnet3"),
				).Return(true, []byte(`"Bitcoin"`), nil).Once()

				return h
			}(),
		},
		"get missing": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:       "get_blob",
						Input:      `{"key":"Testnet3"}`,
						OutputPath: "k",
					},
				},
			},
			assertState: map[string]string{},
			helper: func() *mocks.Helper {
				h := &mocks.Helper{}
				h.On(
					"GetBlob",
					mock.Anything,
					mock.Anything,
					types.Hash("Testnet3"),
				).Return(false, []byte{}, nil).Once()

				return h
			}(),
			executionErr: &Error{
				Workflow: "random",
				Scenario: "create_address",
				Action: &job.Action{
					Type:       "get_blob",
					Input:      `{"key":"Testnet3"}`,
					OutputPath: "k",
				},
				ProcessedInput: `{"key":"Testnet3"}`,
				Err:            ErrActionFailed,
			},
		},
		"complex save and get": {
			scenario: &job.Scenario{
				Name: "create_address",
				Actions: []*job.Action{
					{
						Type:  "set_blob",
						Input: `{"key":{"address":"hello", "sub_account":{"address":"neat"}}, "value":{"stuff":"neat"}}`,
					},
					{
						Type:  "set_blob",
						Input: `{"key":{"address":"hello", "sub_account":{"address":"neat2"}}, "value":"addr2"}`,
					},
					{
						Type:       "get_blob",
						Input:      `{"key":{"sub_account":{"address":"neat"}, "address":"hello"}}`, // switch order
						OutputPath: "k",
					},
				},
			},
			assertState: map[string]string{
				"k": `{"stuff":"neat"}`,
			},
			helper: func() *mocks.Helper {
				h := &mocks.Helper{}
				h.On(
					"SetBlob",
					mock.Anything,
					mock.Anything,
					types.Hash(&types.AccountIdentifier{
						Address: "hello",
						SubAccount: &types.SubAccountIdentifier{
							Address: "neat",
						},
					}),
					[]byte(`{"stuff":"neat"}`),
				).Return(nil).Once()
				h.On(
					"SetBlob",
					mock.Anything,
					mock.Anything,
					types.Hash(&types.AccountIdentifier{
						Address: "hello",
						SubAccount: &types.SubAccountIdentifier{
							Address: "neat2",
						},
					}),
					[]byte(`"addr2"`),
				).Return(nil).Once()

				h.On(
					"GetBlob",
					mock.Anything,
					mock.Anything,
					types.Hash(&types.AccountIdentifier{
						Address: "hello",
						SubAccount: &types.SubAccountIdentifier{
							Address: "neat",
						},
					}),
				).Return(
					true,
					[]byte(`{"stuff":"neat"}`),
					nil,
				).Once()

				return h
			}(),
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

			db, err := database.NewBadgerDatabase(
				ctx,
				dir,
				database.WithIndexCacheSize(database.TinyIndexCacheSize),
			)
			assert.NoError(t, err)
			assert.NotNil(t, db)
			defer db.Close(ctx)

			dbTx := db.Transaction(ctx)

			assert.False(t, j.CheckComplete())

			b, executionErr := worker.Process(ctx, dbTx, j)
			assert.Nil(t, b)
			if test.executionErr != nil {
				assert.True(t, errors.Is(executionErr.Err, test.executionErr.Err))
				executionErr.Err = test.executionErr.Err // makes equality check easier
				assert.Equal(t, test.executionErr, executionErr)
			} else {
				assert.Nil(t, executionErr)

				for k, v := range test.assertState {
					value := gjson.Get(j.State, k)
					assert.True(t, value.Exists())
					assert.Equal(t, v, value.String())
				}
			}

			test.helper.AssertExpectations(t)
		})
	}
}
