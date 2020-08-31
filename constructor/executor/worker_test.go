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

package executor

import (
	"context"
	"errors"
	"testing"

	mocks "github.com/coinbase/rosetta-sdk-go/mocks/constructor/executor"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/stretchr/testify/assert"
)

func TestWaitMessage(t *testing.T) {
	tests := map[string]struct {
		input *FindBalanceInput

		message string
	}{
		"simple message": {
			input: &FindBalanceInput{
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
			input: &FindBalanceInput{
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
			input: &FindBalanceInput{
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
			input: &FindBalanceInput{
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
			input: &FindBalanceInput{
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
			input: &FindBalanceInput{
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

	// Setup DB
	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(dir)

	db, err := storage.NewBadgerStorage(ctx, dir)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	defer db.Close(ctx)

	dbTx := db.NewDatabaseTransaction(ctx, true)

	tests := map[string]struct {
		input *FindBalanceInput

		mockHelper *mocks.Helper

		output *FindBalanceOutput
		err    error
	}{
		"simple find balance with wait": {
			input: &FindBalanceInput{
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
					dbTx,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Twice()
				helper.On("LockedAddresses", ctx, dbTx).Return([]string{"addr2"}, nil).Twice()
				helper.On("Balance", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}).Return([]*types.Amount{
					{
						Value: "99",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				}, nil).Once()
				helper.On("Balance", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}).Return([]*types.Amount{
					{
						Value: "101",
						Currency: &types.Currency{
							Symbol:   "ETH",
							Decimals: 18,
						},
					},
				}, nil).Once()
				helper.On("Balance", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}).Return([]*types.Amount{
					{
						Value: "100",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				}, nil).Once()

				return helper
			}(),
			output: &FindBalanceOutput{
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
			input: &FindBalanceInput{
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
					dbTx,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Twice()
				helper.On("LockedAddresses", ctx, dbTx).Return([]string{"addr2"}, nil).Twice()
				helper.On("Balance", ctx, dbTx, &types.AccountIdentifier{
					Address: "addr1",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub1",
					},
				}).Return([]*types.Amount{
					{
						Value: "99",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				}, nil).Once()
				helper.On("Balance", ctx, dbTx, &types.AccountIdentifier{
					Address: "addr3",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub1",
					},
				}).Return([]*types.Amount{
					{
						Value: "101",
						Currency: &types.Currency{
							Symbol:   "ETH",
							Decimals: 18,
						},
					},
				}, nil).Once()
				helper.On("Balance", ctx, dbTx, &types.AccountIdentifier{
					Address: "addr1",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub1",
					},
				}).Return([]*types.Amount{
					{
						Value: "100",
						Currency: &types.Currency{
							Symbol:   "BTC",
							Decimals: 8,
						},
					},
				}, nil).Once()

				return helper
			}(),
			output: &FindBalanceOutput{
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
			input: &FindBalanceInput{
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
					dbTx,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Twice()
				helper.On("LockedAddresses", ctx, dbTx).Return([]string{"addr2"}, nil).Twice()
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
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
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin4",
						},
						Amount: &types.Amount{
							Value: "101",
							Currency: &types.Currency{
								Symbol:   "ETH",
								Decimals: 18,
							},
						},
					},
				}, nil).Once()
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
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
			output: &FindBalanceOutput{
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
			input: &FindBalanceInput{
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
					dbTx,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Once()
				helper.On("LockedAddresses", ctx, dbTx).Return([]string{"addr2"}, nil).Once()
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
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
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin4",
						},
						Amount: &types.Amount{
							Value: "101",
							Currency: &types.Currency{
								Symbol:   "ETH",
								Decimals: 18,
							},
						},
					},
				}, nil).Once()

				return helper
			}(),
			err: ErrUnsatisfiable,
		},
		"could not find coin (unsatisfiable)": {
			input: &FindBalanceInput{
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
					dbTx,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Once()
				helper.On("LockedAddresses", ctx, dbTx).Return([]string{"addr2"}, nil).Once()
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
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
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin4",
						},
						Amount: &types.Amount{
							Value: "101",
							Currency: &types.Currency{
								Symbol:   "ETH",
								Decimals: 18,
							},
						},
					},
				}, nil).Once()

				return helper
			}(),
			err: ErrUnsatisfiable,
		},
		"could not find coin (too many accounts)": {
			input: &FindBalanceInput{
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
					dbTx,
				).Return(
					[]string{"addr2", "addr1", "addr3", "addr4"},
					nil,
				).Once()
				helper.On("LockedAddresses", ctx, dbTx).Return([]string{"addr2"}, nil).Once()
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr1",
					SubAccount: (*types.SubAccountIdentifier)(nil),
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
				helper.On("Coins", ctx, dbTx, &types.AccountIdentifier{
					Address:    "addr3",
					SubAccount: (*types.SubAccountIdentifier)(nil),
				}).Return([]*types.Coin{
					{
						CoinIdentifier: &types.CoinIdentifier{
							Identifier: "coin4",
						},
						Amount: &types.Amount{
							Value: "101",
							Currency: &types.Currency{
								Symbol:   "ETH",
								Decimals: 18,
							},
						},
					},
				}, nil).Once()

				return helper
			}(),
			err: ErrUnsatisfiable,
		},
		"invalid amount": {
			input: &FindBalanceInput{
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
			input: &FindBalanceInput{
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
			worker := NewWorker(test.mockHelper)
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
