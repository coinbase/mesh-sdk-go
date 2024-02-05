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

package modules

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/neilotoole/errgroup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	mocks "github.com/coinbase/rosetta-sdk-go/mocks/storage/modules"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

var (
	blockIdentifier = &types.BlockIdentifier{
		Hash:  "block",
		Index: 1,
	}
	account = &types.AccountIdentifier{
		Address: "blah",
	}

	account2 = &types.AccountIdentifier{
		Address: "blah2",
	}

	account3 = &types.AccountIdentifier{
		Address: "blah",
		SubAccount: &types.SubAccountIdentifier{
			Address: "extra account",
		},
	}

	account4 = &types.AccountIdentifier{
		Address: "blah4",
	}

	account5 = &types.AccountIdentifier{
		Address: "blah5",
	}

	accountCoins = []*types.Coin{
		{
			CoinIdentifier: &types.CoinIdentifier{Identifier: "coin1"},
			Amount:         coinBlock.Transactions[0].Operations[0].Amount,
		},
	}

	account2Coins = []*types.Coin{
		{
			CoinIdentifier: &types.CoinIdentifier{Identifier: "coin2"},
			Amount:         coinBlock.Transactions[0].Operations[1].Amount,
		},
	}

	account3Coins = []*types.Coin{
		{
			CoinIdentifier: &types.CoinIdentifier{Identifier: "coin3"},
			Amount:         coinBlock3.Transactions[0].Operations[0].Amount,
		},
		{
			CoinIdentifier: &types.CoinIdentifier{Identifier: "coin4"},
			Amount:         coinBlock3.Transactions[1].Operations[0].Amount,
		},
	}

	coins1 = &types.Coin{
		CoinIdentifier: &types.CoinIdentifier{Identifier: "bulkCoin1"},
		Amount: &types.Amount{
			Value:    "10",
			Currency: currency,
		},
	}

	coins2 = &types.Coin{
		CoinIdentifier: &types.CoinIdentifier{Identifier: "bulkCoin2"},
		Amount: &types.Amount{
			Value:    "20",
			Currency: currency,
		},
	}

	coins3 = &types.Coin{
		CoinIdentifier: &types.CoinIdentifier{Identifier: "bulkCoin3"},
		Amount: &types.Amount{
			Value:    "30",
			Currency: currency,
		},
	}

	coins4 = &types.Coin{
		CoinIdentifier: &types.CoinIdentifier{Identifier: "bulkCoin4"},
		Amount: &types.Amount{
			Value:    "40",
			Currency: currency,
		},
	}

	coins5 = &types.Coin{
		CoinIdentifier: &types.CoinIdentifier{Identifier: "bulkCoin5"},
		Amount: &types.Amount{
			Value:    "50",
			Currency: currency,
		},
	}

	successStatus = types.String("success")
	failureStatus = types.String("failure")

	currency = &types.Currency{
		Symbol:   "sym",
		Decimals: 12,
	}

	currency2 = &types.Currency{
		Symbol:   "sym2",
		Decimals: 12,
	}

	coinBlock = &types.Block{
		Transactions: []*types.Transaction{
			{
				Operations: []*types.Operation{
					{
						Account: account,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "10",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin1",
							},
						},
					},
					{
						Account: account2,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "15",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinSpent,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin2",
							},
						},
					},
					{
						Account: account2,
						Status:  failureStatus,
						Amount: &types.Amount{
							Value:    "20",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinSpent,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin2",
							},
						},
					},
				},
			},
		},
	}

	coinBlock2 = &types.Block{
		Transactions: []*types.Transaction{
			{
				Operations: []*types.Operation{
					{
						Account: account,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "-10",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinSpent,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin1",
							},
						},
					},
				},
			},
		},
	}

	coinBlock3 = &types.Block{
		Transactions: []*types.Transaction{
			{
				Operations: []*types.Operation{
					{
						Account: account3,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "4",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin3",
							},
						},
					},
				},
			},
			{
				Operations: []*types.Operation{
					{
						Account: account3,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "6",
							Currency: currency2,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin4",
							},
						},
					},
				},
			},
			{
				Operations: []*types.Operation{
					{
						Account: account3,
						Status:  failureStatus,
						Amount: &types.Amount{
							Value:    "12",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin5",
							},
						},
					},
				},
			},
			{
				Operations: []*types.Operation{
					{
						Account: account3,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "12",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin6",
							},
						},
					},
				},
			},
			{
				Operations: []*types.Operation{
					{
						Account: account3,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "12",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinSpent,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin6",
							},
						},
					},
				},
			},
		},
	}

	coinBlockRepeat = &types.Block{
		Transactions: []*types.Transaction{
			{
				Operations: []*types.Operation{
					{
						Account: account,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "10",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin_repeat",
							},
						},
					},
					{
						Account: account,
						Status:  successStatus,
						Amount: &types.Amount{
							Value:    "20",
							Currency: currency,
						},
						CoinChange: &types.CoinChange{
							CoinAction: types.CoinCreated,
							CoinIdentifier: &types.CoinIdentifier{
								Identifier: "coin_repeat",
							},
						},
					},
				},
			},
		},
	}

	accBalance1 = &utils.AccountBalance{
		Account: &types.AccountIdentifier{
			Address: "acc1",
		},
		Coins: []*types.Coin{
			{
				CoinIdentifier: &types.CoinIdentifier{Identifier: "accCoin1"},
				Amount: &types.Amount{
					Value:    "30",
					Currency: currency,
				},
			},
			{
				CoinIdentifier: &types.CoinIdentifier{Identifier: "accCoin2"},
				Amount: &types.Amount{
					Value:    "40",
					Currency: currency,
				},
			},
		},
	}

	accBalance2 = &utils.AccountBalance{
		Account: &types.AccountIdentifier{
			Address: "acc2",
		},
		Coins: []*types.Coin{
			{
				CoinIdentifier: &types.CoinIdentifier{Identifier: "accCoin3"},
				Amount: &types.Amount{
					Value:    "10",
					Currency: currency,
				},
			},
			{
				CoinIdentifier: &types.CoinIdentifier{Identifier: "accCoin4"},
				Amount: &types.Amount{
					Value:    "20",
					Currency: currency,
				},
			},
		},
	}

	acctCoins = []*utils.AccountCoinsResponse{
		{
			Coins: accBalance1.Coins,
		},
		{
			Coins: accBalance2.Coins,
		},
	}
)

func TestCoinStorage(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	a, err := asserter.NewClientWithOptions(
		&types.NetworkIdentifier{
			Blockchain: "bitcoin",
			Network:    "mainnet",
		},
		&types.BlockIdentifier{
			Hash:  "block 0",
			Index: 0,
		},
		[]string{"Transfer"},
		[]*types.OperationStatus{
			{
				Status:     *successStatus,
				Successful: true,
			},
			{
				Status:     *failureStatus,
				Successful: false,
			},
		},
		[]*types.Error{},
		nil,
		&asserter.Validations{
			Enabled: false,
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, a)

	mockHelper := &mocks.CoinStorageHelper{}

	c := NewCoinStorage(database, mockHelper, a)

	t.Run("AddCoins before blocks", func(t *testing.T) {
		// Ensure correct status is thrown when can't get coin
		coin, owner, err := c.GetCoin(ctx, coins5.CoinIdentifier)
		assert.True(t, errors.Is(err, storageErrs.ErrCoinNotFound))
		assert.Nil(t, coin)
		assert.Nil(t, owner)

		accountCoins := []*types.AccountCoin{
			{
				Account: account4,
				Coin:    coins4,
			},
			{
				Account: account5,
				Coin:    coins5,
			},
		}

		err = c.AddCoins(ctx, accountCoins)
		assert.NoError(t, err)

		// Assert error in getcoins when no CurrentBlockIdentifier
		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			nil,
			errors.New("blah"),
		).Once()
		coinsGot, block, err := c.GetCoins(ctx, account5)
		assert.Error(t, err)
		assert.Nil(t, coinsGot)
		assert.Nil(t, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			nil,
			errors.New("blah"),
		).Once()
		coinsGot, block, err = c.GetCoins(ctx, account4)
		assert.Error(t, err)
		assert.Nil(t, coinsGot)
		assert.Nil(t, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coinsGot, block, err = c.GetCoins(ctx, account5)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)
		assert.ElementsMatch(t, coinsGot, []*types.Coin{coins5})

		coin, owner, err = c.GetCoin(ctx, coins5.CoinIdentifier)
		assert.NoError(t, err)
		assert.Equal(t, coins5, coin)
		assert.Equal(t, account5, owner)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coinsGot, block, err = c.GetCoins(ctx, account4)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)
		assert.ElementsMatch(t, coinsGot, []*types.Coin{coins4})

		coin, owner, err = c.GetCoin(ctx, coins4.CoinIdentifier)
		assert.NoError(t, err)
		assert.Equal(t, coins4, coin)
		assert.Equal(t, account4, owner)
	})

	t.Run("get coins of unset account", func(t *testing.T) {
		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*types.Coin{}, coins)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		bal, coinIdentifier, block, err := c.GetLargestCoin(ctx, account, currency)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(0), bal)
		assert.Nil(t, coinIdentifier)
		assert.Equal(t, blockIdentifier, block)
	})

	t.Run("add block", func(t *testing.T) {
		tx := c.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitFunc, err := c.AddingBlock(gctx, g, coinBlock, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, tx.Commit(ctx))

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, accountCoins, coins)
		assert.Equal(t, blockIdentifier, block)
	})

	t.Run("add duplicate coin", func(t *testing.T) {
		tx := c.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitFunc, err := c.AddingBlock(gctx, g, coinBlock, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.Error(t, g.Wait())
		tx.Discard(ctx)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, accountCoins, coins)
		assert.Equal(t, blockIdentifier, block)
	})

	t.Run("add duplicate coin in same block", func(t *testing.T) {
		tx := c.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitFunc, err := c.AddingBlock(gctx, g, coinBlockRepeat, tx)
		assert.Nil(t, commitFunc)
		assert.Error(t, err)
		assert.NoError(t, g.Wait())
		tx.Discard(ctx)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, accountCoins, coins)
		assert.Equal(t, blockIdentifier, block)
	})

	t.Run("remove block", func(t *testing.T) {
		tx := c.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitFunc, err := c.RemovingBlock(gctx, g, coinBlock, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, tx.Commit(ctx))

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*types.Coin{}, coins)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err = c.GetCoins(ctx, account2)
		assert.NoError(t, err)
		assert.Equal(t, account2Coins, coins)
		assert.Equal(t, blockIdentifier, block)
	})

	t.Run("spend coin", func(t *testing.T) {
		tx := c.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitFunc, err := c.AddingBlock(gctx, g, coinBlock, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, tx.Commit(ctx))

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, accountCoins, coins)
		assert.Equal(t, blockIdentifier, block)

		tx = c.db.Transaction(ctx)
		g, gctx = errgroup.WithContext(ctx)
		commitFunc, err = c.AddingBlock(gctx, g, coinBlock2, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, tx.Commit(ctx))

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err = c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*types.Coin{}, coins)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err = c.GetCoins(ctx, account2)
		assert.NoError(t, err)
		assert.Equal(t, []*types.Coin{}, coins)
		assert.Equal(t, blockIdentifier, block)
	})

	t.Run("add block with multiple outputs for 1 account", func(t *testing.T) {
		tx := c.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitFunc, err := c.AddingBlock(gctx, g, coinBlock3, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, tx.Commit(ctx))

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*types.Coin{}, coins)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err = c.GetCoins(ctx, account3)
		assert.NoError(t, err)
		assert.ElementsMatch(t, account3Coins, coins)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		bal, coinIdentifier, block, err := c.GetLargestCoin(ctx, account3, currency)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(4), bal)
		assert.Equal(t, &types.CoinIdentifier{Identifier: "coin3"}, coinIdentifier)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		bal, coinIdentifier, block, err = c.GetLargestCoin(ctx, account3, currency2)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(6), bal)
		assert.Equal(t, &types.CoinIdentifier{Identifier: "coin4"}, coinIdentifier)
		assert.Equal(t, blockIdentifier, block)
	})

	t.Run("remove block that creates and spends single coin", func(t *testing.T) {
		tx := c.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitFunc, err := c.RemovingBlock(gctx, g, coinBlock3, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, tx.Commit(ctx))

		tx = c.db.Transaction(ctx)
		g, gctx = errgroup.WithContext(ctx)
		commitFunc, err = c.AddingBlock(gctx, g, coinBlock3, tx)
		assert.Nil(t, commitFunc)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, tx.Commit(ctx))

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, []*types.Coin{}, coins)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err = c.GetCoins(ctx, account3)
		assert.NoError(t, err)
		assert.ElementsMatch(t, account3Coins, coins)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		bal, coinIdentifier, block, err := c.GetLargestCoin(ctx, account3, currency)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(4), bal)
		assert.Equal(t, &types.CoinIdentifier{Identifier: "coin3"}, coinIdentifier)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		bal, coinIdentifier, block, err = c.GetLargestCoin(ctx, account3, currency2)
		assert.NoError(t, err)
		assert.Equal(t, big.NewInt(6), bal)
		assert.Equal(t, &types.CoinIdentifier{Identifier: "coin4"}, coinIdentifier)
		assert.Equal(t, blockIdentifier, block)
	})

	t.Run("AddCoins after block", func(t *testing.T) {
		accountCoins := []*types.AccountCoin{
			{
				Account: account,
				Coin:    coins1,
			},
			{
				Account: account2,
				Coin:    coins2,
			},
			{
				Account: account3,
				Coin:    coins3,
			},
			{
				Account: account4,
				Coin:    coins4,
			},
		}

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		account1coins, block, err := c.GetCoins(ctx, account)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		account2coins, block, err := c.GetCoins(ctx, account2)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		account3coins, block, err := c.GetCoins(ctx, account3)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)

		err = c.AddCoins(ctx, accountCoins)
		assert.NoError(t, err)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coinsGot, block, err := c.GetCoins(ctx, account)
		account1coins = append(account1coins, coins1)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)
		assert.ElementsMatch(t, coinsGot, account1coins)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coinsGot, block, err = c.GetCoins(ctx, account2)
		account2coins = append(account2coins, coins2)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)
		assert.ElementsMatch(t, coinsGot, account2coins)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coinsGot, block, err = c.GetCoins(ctx, account3)
		account3coins = append(account3coins, coins3)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)
		assert.ElementsMatch(t, coinsGot, account3coins)

		// Does not add duplicate
		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coinsGot, block, err = c.GetCoins(ctx, account4)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)
		assert.ElementsMatch(t, coinsGot, []*types.Coin{coins4})
	})

	t.Run("SetCoinsImported", func(t *testing.T) {
		accts := []*types.AccountIdentifier{accBalance1.Account, accBalance2.Account}
		err := c.SetCoinsImported(ctx, accts, acctCoins)
		assert.NoError(t, err)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err := c.GetCoins(ctx, accBalance1.Account)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)
		assert.ElementsMatch(t, accBalance1.Coins, coins)

		mockHelper.On(
			"CurrentBlockIdentifier",
			ctx,
			mock.Anything,
		).Return(
			blockIdentifier,
			nil,
		).Once()
		coins, block, err = c.GetCoins(ctx, accBalance2.Account)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, block)
		assert.ElementsMatch(t, accBalance2.Coins, coins)
	})

	mockHelper.AssertExpectations(t)
}
