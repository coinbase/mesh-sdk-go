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
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/big"
	"path"
	"testing"

	"github.com/neilotoole/errgroup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	mocks "github.com/coinbase/rosetta-sdk-go/mocks/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/parser"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

func baseAsserter() *asserter.Asserter {
	a, _ := asserter.NewClientWithOptions(
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
				Status:     "Success",
				Successful: true,
			},
		},
		[]*types.Error{},
		nil,
		&asserter.Validations{
			Enabled: false,
		},
	)
	return a
}

func exemptFunc() parser.ExemptOperation {
	return func(op *types.Operation) bool {
		return false
	}
}

func TestBalance(t *testing.T) {
	var (
		genesisAccount = &types.AccountIdentifier{
			Address: "genesis",
		}
		account = &types.AccountIdentifier{
			Address: "blah",
		}
		account2 = &types.AccountIdentifier{
			Address: "blah2",
		}
		account3 = &types.AccountIdentifier{
			Address: "blah3",
		}
		subAccount = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
			},
		}
		subAccountNewPointer = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
			},
		}
		subAccountMetadata = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": "hello",
				},
			},
		}
		subAccountMetadataNewPointer = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": "hello",
				},
			},
		}
		subAccountMetadata2 = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": float64(10),
				},
			},
		}
		subAccountMetadata2NewPointer = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": float64(10),
				},
			},
		}
		exemptionAccount = &types.AccountIdentifier{
			Address: "exemption",
		}
		exemptionCurrency = &types.Currency{
			Symbol:   "exempt",
			Decimals: 3,
		}
		exemptions = []*types.BalanceExemption{
			{
				ExemptionType: types.BalanceGreaterOrEqual,
				Currency:      exemptionCurrency,
			},
		}
		currency = &types.Currency{
			Symbol:   "BLAH",
			Decimals: 2,
		}
		amount = &types.Amount{
			Value:    "100",
			Currency: currency,
		}
		amountWithPrevious = &types.Amount{
			Value:    "110",
			Currency: currency,
		}
		amountNilCurrency = &types.Amount{
			Value: "100",
		}
		genesisBlock = &types.BlockIdentifier{
			Hash:  "0",
			Index: 0,
		}
		newBlock = &types.BlockIdentifier{
			Hash:  "kdasdj",
			Index: 123890,
		}
		newBlock2 = &types.BlockIdentifier{
			Hash:  "pkdasdj",
			Index: 123891,
		}
		result = &types.Amount{
			Value:    "200",
			Currency: currency,
		}
		newBlock3 = &types.BlockIdentifier{
			Hash:  "pkdgdj",
			Index: 123892,
		}
		newBlock4 = &types.BlockIdentifier{
			Hash:  "asdjkajsdk",
			Index: 123893,
		}
		largeDeduction = &types.Amount{
			Value:    "-1000",
			Currency: currency,
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	mockHelper := &mocks.BalanceStorageHelper{}
	mockHelper.On("Asserter").Return(baseAsserter())
	mockHelper.On("ExemptFunc").Return(exemptFunc())
	mockHelper.On("BalanceExemptions").Return(exemptions)
	mockHandler := &mocks.BalanceStorageHandler{}
	storage.Initialize(mockHelper, mockHandler)

	t.Run("Get balance at nil block", func(t *testing.T) {
		amount, err := storage.GetOrSetBalance(ctx, account, currency, nil)
		assert.True(t, errors.Is(err, storageErrs.ErrBlockNil))
		assert.Nil(t, amount)
	})

	t.Run("Get unset balance", func(t *testing.T) {
		amount, err := storage.GetBalance(ctx, account, currency, newBlock.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
	})

	t.Run("Get unset balance and set", func(t *testing.T) {
		mockHelper.On(
			"AccountBalance",
			ctx,
			account,
			currency,
			newBlock,
		).Return(
			&types.Amount{Value: "10", Currency: currency},
			nil,
		).Once()
		mockHandler.On("AccountsSeen", ctx, mock.Anything, 1).Return(nil).Once()
		amount, err := storage.GetOrSetBalance(ctx, account, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "10",
			Currency: currency,
		}, amount)

		amount, err = storage.GetOrSetBalance(ctx, account, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "10",
			Currency: currency,
		}, amount)
	})

	t.Run("Set and get genesis balance", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		mockHandler.On("AccountsSeen", ctx, txn, 1).Return(nil).Once()
		err := storage.SetBalance(
			ctx,
			txn,
			genesisAccount,
			&types.Amount{
				Value:    amount.Value,
				Currency: currency,
			},
			genesisBlock,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		amount, err := storage.GetOrSetBalance(ctx, genesisAccount, currency, genesisBlock)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "100",
			Currency: currency,
		}, amount)
	})

	t.Run("Set and get balance", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)

		// When deleting records, we decrement the account seen count.
		mockHandler.On("AccountsSeen", ctx, txn, -1).Return(nil).Once()

		// When adding the account, we increment the account seen count.
		mockHandler.On("AccountsSeen", ctx, txn, 1).Return(nil).Once()

		err := storage.SetBalance(
			ctx,
			txn,
			account,
			amount,
			newBlock,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetOrSetBalance(ctx, account, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)

		retrievedAmount, err = storage.GetOrSetBalance(ctx, account, currency, newBlock2)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("Set and get balance with storage helper", func(t *testing.T) {
		mockHelper.On(
			"AccountBalance",
			ctx,
			account3,
			currency,
			(*types.BlockIdentifier)(nil),
		).Return(
			&types.Amount{Value: "10", Currency: currency},
			nil,
		).Once()
		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account3,
				Currency:   currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.True(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetOrSetBalance(ctx, account3, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, amountWithPrevious, retrievedAmount)
	})

	t.Run("Set balance with nil currency", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account,
				Currency:   nil,
				Block:      newBlock,
				Difference: amountNilCurrency.Value,
			},
			nil,
		)
		assert.False(t, newAccount)
		assert.EqualError(t, err, "invalid currency")
		txn.Discard(ctx)

		retrievedAmount, err := storage.GetOrSetBalance(ctx, account, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("Modify existing balance", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account,
				Currency:   currency,
				Block:      newBlock2,
				Difference: amount.Value,
			},
			nil,
		)
		assert.False(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetOrSetBalance(ctx, account, currency, newBlock2)
		assert.NoError(t, err)
		assert.Equal(t, result, retrievedAmount)
	})

	t.Run("Discard transaction", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account,
				Currency:   currency,
				Block:      newBlock3,
				Difference: amount.Value,
			},
			nil,
		)
		assert.False(t, newAccount)
		assert.NoError(t, err)

		// Get balance during transaction
		readTx := storage.db.ReadTransaction(ctx)
		defer readTx.Discard(ctx)
		retrievedAmount, err := storage.GetBalanceTransactional(
			ctx,
			readTx,
			account,
			currency,
			newBlock2.Index,
		)
		assert.NoError(t, err)
		assert.Equal(t, result, retrievedAmount)

		txn.Discard(ctx)
	})

	t.Run("Attempt modification to push balance negative on existing account", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account,
				Currency:   largeDeduction.Currency,
				Block:      newBlock3,
				Difference: largeDeduction.Value,
			},
			nil,
		)
		assert.True(t, errors.Is(err, storageErrs.ErrNegativeBalance))
		assert.False(t, newAccount)
		txn.Discard(ctx)
	})

	t.Run("Attempt modification to push balance negative on new acct", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		mockHelper.On(
			"AccountBalance",
			ctx,
			account2,
			largeDeduction.Currency,
			(*types.BlockIdentifier)(nil),
		).Return(
			&types.Amount{Value: "0", Currency: largeDeduction.Currency},
			nil,
		).Once()
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account2,
				Currency:   largeDeduction.Currency,
				Block:      newBlock2,
				Difference: largeDeduction.Value,
			},
			nil,
		)
		assert.False(t, newAccount)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, storageErrs.ErrNegativeBalance))
		txn.Discard(ctx)
	})

	t.Run("sub account set and get balance", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		mockHelper.On(
			"AccountBalance",
			ctx,
			subAccount,
			amount.Currency,
			(*types.BlockIdentifier)(nil),
		).Return(
			&types.Amount{Value: "0", Currency: amount.Currency},
			nil,
		).Once()
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    subAccount,
				Currency:   amount.Currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.True(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetOrSetBalance(
			ctx,
			subAccountNewPointer,
			amount.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("sub account metadata set and get balance", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		mockHelper.On(
			"AccountBalance",
			ctx,
			subAccountMetadata,
			amount.Currency,
			(*types.BlockIdentifier)(nil),
		).Return(
			&types.Amount{Value: "0", Currency: amount.Currency},
			nil,
		).Once()
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    subAccountMetadata,
				Currency:   amount.Currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.True(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetOrSetBalance(
			ctx,
			subAccountMetadataNewPointer,
			amount.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("sub account unique metadata set and get balance", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		mockHelper.On(
			"AccountBalance",
			ctx,
			subAccountMetadata2,
			amount.Currency,
			(*types.BlockIdentifier)(nil),
		).Return(
			&types.Amount{Value: "0", Currency: amount.Currency},
			nil,
		).Once()
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    subAccountMetadata2,
				Currency:   amount.Currency,
				Block:      newBlock,
				Difference: amount.Value,
			},
			nil,
		)
		assert.True(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetOrSetBalance(
			ctx,
			subAccountMetadata2NewPointer,
			amount.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("balance exemption update", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		mockHandler.On("AccountsSeen", ctx, txn, 1).Return(nil).Once()
		err := storage.SetBalance(
			ctx,
			txn,
			exemptionAccount,
			&types.Amount{
				Value:    "0",
				Currency: exemptionCurrency,
			},
			genesisBlock,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		// Successful (balance > computed and negative intermediate value)
		mockHelper.On(
			"AccountBalance",
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock,
		).Return(
			&types.Amount{Value: "150", Currency: exemptionCurrency},
			nil,
		).Once()
		txn = storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    exemptionAccount,
				Currency:   exemptionCurrency,
				Block:      newBlock,
				Difference: "-10",
			},
			nil,
		)
		assert.False(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetOrSetBalance(
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, "150", retrievedAmount.Value)

		// Successful (balance == computed)
		mockHelper.On(
			"AccountBalance",
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock3,
		).Return(
			&types.Amount{Value: "200", Currency: exemptionCurrency},
			nil,
		).Once()
		txn = storage.db.Transaction(ctx)
		newAccount, err = storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    exemptionAccount,
				Currency:   exemptionCurrency,
				Block:      newBlock3,
				Difference: "50",
			},
			nil,
		)
		assert.False(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock3,
		)
		assert.NoError(t, err)
		assert.Equal(t, "200", retrievedAmount.Value)

		// Unsuccessful (balance < computed)
		mockHelper.On(
			"AccountBalance",
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock4,
		).Return(
			&types.Amount{Value: "10", Currency: exemptionCurrency},
			nil,
		).Once()
		txn = storage.db.Transaction(ctx)
		newAccount, err = storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    exemptionAccount,
				Currency:   exemptionCurrency,
				Block:      newBlock4,
				Difference: "50",
			},
			nil,
		)
		assert.False(t, newAccount)
		assert.Error(t, err)
		txn.Discard(ctx)

		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock4,
		)
		assert.NoError(t, err)
		assert.Equal(t, "200", retrievedAmount.Value)
	})

	t.Run("get all set AccountCurrency", func(t *testing.T) {
		accounts, err := storage.GetAllAccountCurrency(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*types.AccountCurrency{
			{
				Account:  genesisAccount,
				Currency: currency,
			},
			{
				Account:  account,
				Currency: currency,
			},
			{
				Account:  account3,
				Currency: currency,
			},
			{
				Account:  subAccount,
				Currency: currency,
			},
			{
				Account:  subAccountMetadata,
				Currency: currency,
			},
			{
				Account:  subAccountMetadata2,
				Currency: currency,
			},
			{
				Account:  exemptionAccount,
				Currency: exemptionCurrency,
			},
		}, accounts)
	})

	t.Run("prune negative index (no-op)", func(t *testing.T) {
		err := storage.PruneBalances(
			ctx,
			account,
			largeDeduction.Currency,
			-1238900,
		)
		assert.NoError(t, err)

		retrievedAmount, err := storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock3,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "200",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock2,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "200",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "100",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
	})

	t.Run("update existing balance", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		orphanValue, _ := new(big.Int).SetString(largeDeduction.Value, 10)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account,
				Currency:   largeDeduction.Currency,
				Block:      newBlock2,
				Difference: new(big.Int).Neg(orphanValue).String(),
			},
			nil,
		)
		assert.False(t, newAccount)
		assert.NoError(t, err)
		retrievedAmount, err := storage.GetBalanceTransactional(
			ctx,
			txn,
			account,
			largeDeduction.Currency,
			newBlock3.Index,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "1200",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		txn.Discard(ctx)

		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock3,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "200",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
	})

	t.Run("orphan balance correctly", func(t *testing.T) {
		retrievedAmount, err := storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "100",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)

		txn := storage.db.Transaction(ctx)
		shouldRemove, err := storage.OrphanBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:  account,
				Currency: largeDeduction.Currency,
				Block:    newBlock,
			},
		)
		assert.True(t, shouldRemove)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
	})

	t.Run("prune index", func(t *testing.T) {
		err := storage.PruneBalances(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock.Index,
		)
		assert.NoError(t, err)

		retrievedAmount, err := storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock3,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock2,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			account,
			largeDeduction.Currency,
			newBlock,
		)
		assert.True(t, errors.Is(err, storageErrs.ErrBalancePruned))
		assert.Nil(t, retrievedAmount)
	})

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestSetBalanceImported(t *testing.T) {
	var (
		blockIdentifier = &types.BlockIdentifier{
			Hash:  "block",
			Index: 1,
		}

		accountCoin = &types.AccountIdentifier{
			Address: "test",
		}

		currency = &types.Currency{
			Symbol:   "BLAH",
			Decimals: 2,
		}

		amountCoins = &types.Amount{
			Value:    "60",
			Currency: currency,
		}

		accountCoins = []*types.Coin{
			{
				CoinIdentifier: &types.CoinIdentifier{Identifier: "coin1"},
				Amount: &types.Amount{
					Value:    "30",
					Currency: currency,
				},
			},
			{
				CoinIdentifier: &types.CoinIdentifier{Identifier: "coin2"},
				Amount: &types.Amount{
					Value:    "30",
					Currency: currency,
				},
			},
		}

		accountBalance = &types.AccountIdentifier{
			Address: "test2",
		}

		amountBalance = &types.Amount{
			Value:    "100",
			Currency: currency,
		}

		accBalance1 = &utils.AccountBalance{
			Account: accountCoin,
			Amount:  amountCoins,
			Coins:   accountCoins,
			Block:   blockIdentifier,
		}

		accBalance2 = &utils.AccountBalance{
			Account: accountBalance,
			Amount:  amountBalance,
			Block:   blockIdentifier,
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	mockHelper := &mocks.BalanceStorageHelper{}
	mockHandler := &mocks.BalanceStorageHandler{}
	mockHelper.On("Asserter").Return(baseAsserter())
	mockHelper.On("ExemptFunc").Return(exemptFunc())
	mockHelper.On("BalanceExemptions").Return([]*types.BalanceExemption{})
	storage.Initialize(mockHelper, mockHandler)

	t.Run("Set balance successfully", func(t *testing.T) {
		mockHandler.On("AccountsSeen", ctx, mock.Anything, 1).Return(nil).Twice()
		err = storage.SetBalanceImported(
			ctx,
			nil,
			[]*utils.AccountBalance{accBalance1, accBalance2},
		)
		assert.NoError(t, err)

		amount1, err := storage.GetOrSetBalance(
			ctx,
			accountCoin,
			currency,
			blockIdentifier,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount1.Value, amountCoins.Value)

		amount2, err := storage.GetOrSetBalance(
			ctx,
			accountBalance,
			currency,
			blockIdentifier,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount2.Value, amountBalance.Value)
	})

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestBootstrapBalances(t *testing.T) {
	var (
		genesisBlockIdentifier = &types.BlockIdentifier{
			Index: 0,
			Hash:  "0",
		}

		newBlock = &types.BlockIdentifier{
			Index: 1,
			Hash:  "1",
		}

		account = &types.AccountIdentifier{
			Address: "hello",
		}

		account2 = &types.AccountIdentifier{
			Address: "hello world",
		}

		account3 = &types.AccountIdentifier{
			Address: "hello world new",
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	mockHelper := &mocks.BalanceStorageHelper{}
	mockHandler := &mocks.BalanceStorageHandler{}
	mockHelper.On("Asserter").Return(baseAsserter())
	mockHelper.On("ExemptFunc").Return(exemptFunc())
	mockHelper.On("BalanceExemptions").Return([]*types.BalanceExemption{})
	bootstrapBalancesFile := path.Join(newDir, "balances.csv")

	t.Run("File doesn't exist", func(t *testing.T) {
		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.Contains(t, err.Error(), "no such file or directory")
	})

	// Initialize file
	amount := &types.Amount{
		Value: "10",
		Currency: &types.Currency{
			Symbol:   "BTC",
			Decimals: 8,
		},
	}

	file, err := json.MarshalIndent([]*BootstrapBalance{
		{
			Account:  account,
			Value:    amount.Value,
			Currency: amount.Currency,
		},
		{
			Account:  account2,
			Value:    amount.Value,
			Currency: amount.Currency,
		},
		{
			Account:  account3,
			Value:    amount.Value,
			Currency: amount.Currency,
		},
	}, "", " ")
	assert.NoError(t, err)

	assert.NoError(
		t,
		ioutil.WriteFile(bootstrapBalancesFile, file, utils.DefaultFilePermissions),
	)

	t.Run("run before initializing helper/handler", func(t *testing.T) {
		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.True(t, errors.Is(err, storageErrs.ErrHelperHandlerMissing))
	})

	storage.Initialize(mockHelper, mockHandler)
	t.Run("Set balance successfully", func(t *testing.T) {
		mockHandler.On("AccountsSeen", ctx, mock.Anything, 1).Return(nil).Times(3)
		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.NoError(t, err)

		retrievedAmount, err := storage.GetOrSetBalance(
			ctx,
			account,
			amount.Currency,
			genesisBlockIdentifier,
		)

		assert.Equal(t, amount, retrievedAmount)
		assert.NoError(t, err)

		retrievedAmount2, err := storage.GetOrSetBalance(
			ctx,
			account2,
			amount.Currency,
			genesisBlockIdentifier,
		)

		assert.Equal(t, amount, retrievedAmount2)
		assert.NoError(t, err)

		retrievedAmount3, err := storage.GetOrSetBalance(
			ctx,
			account3,
			amount.Currency,
			genesisBlockIdentifier,
		)

		assert.Equal(t, amount, retrievedAmount3)
		assert.NoError(t, err)

		// Attempt to update balance
		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account,
				Currency:   amount.Currency,
				Block:      newBlock,
				Difference: "100",
			},
			newBlock,
		)
		assert.False(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err = storage.GetOrSetBalance(
			ctx,
			account,
			amount.Currency,
			newBlock,
		)

		assert.Equal(t, "110", retrievedAmount.Value)
		assert.NoError(t, err)
	})

	t.Run("Invalid file contents", func(t *testing.T) {
		assert.NoError(
			t,
			ioutil.WriteFile(
				bootstrapBalancesFile,
				[]byte("bad file"),
				utils.DefaultFilePermissions,
			),
		)

		err := storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.Error(t, err)
	})

	t.Run("Invalid account balance", func(t *testing.T) {
		amount := &types.Amount{
			Value: "-10",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		file, err := json.MarshalIndent([]*BootstrapBalance{
			{
				Account:  account,
				Value:    amount.Value,
				Currency: amount.Currency,
			},
		}, "", " ")
		assert.NoError(t, err)

		assert.NoError(
			t,
			ioutil.WriteFile(bootstrapBalancesFile, file, utils.DefaultFilePermissions),
		)

		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, "cannot bootstrap zero or negative balance -10")
	})

	t.Run("Invalid account value", func(t *testing.T) {
		amount := &types.Amount{
			Value: "goodbye",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}

		file, err := json.MarshalIndent([]*BootstrapBalance{
			{
				Account:  account,
				Value:    amount.Value,
				Currency: amount.Currency,
			},
		}, "", " ")
		assert.NoError(t, err)

		assert.NoError(
			t,
			ioutil.WriteFile(bootstrapBalancesFile, file, utils.DefaultFilePermissions),
		)

		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.EqualError(t, err, "goodbye is not an integer")
	})

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestBalanceReconciliation(t *testing.T) {
	var (
		account = &types.AccountIdentifier{
			Address: "blah",
		}
		subAccountMetadata2 = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "stake",
				Metadata: map[string]interface{}{
					"cool": float64(10),
				},
			},
		}
		currency = &types.Currency{
			Symbol:   "BLAH",
			Decimals: 2,
		}
		currency2 = &types.Currency{
			Symbol:   "BLAH2",
			Decimals: 4,
		}
		genesisBlock = &types.BlockIdentifier{
			Hash:  "0",
			Index: 0,
		}
		newBlock = &types.BlockIdentifier{
			Hash:  "kdasdj",
			Index: 123890,
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	mockHelper := &mocks.BalanceStorageHelper{}
	mockHandler := &mocks.BalanceStorageHandler{}
	mockHelper.On("Asserter").Return(baseAsserter())
	mockHelper.On("ExemptFunc").Return(exemptFunc())
	mockHelper.On("BalanceExemptions").Return([]*types.BalanceExemption{})

	t.Run("test estimated before helper/handler", func(t *testing.T) {
		coverage, err := storage.EstimatedReconciliationCoverage(ctx)
		assert.Equal(t, float64(-1), coverage)
		assert.True(t, errors.Is(err, storageErrs.ErrHelperHandlerMissing))
	})

	storage.Initialize(mockHelper, mockHandler)
	t.Run("attempt to store reconciliation for non-existent account", func(t *testing.T) {
		err := storage.Reconciled(ctx, account, currency, genesisBlock)
		assert.NoError(t, err)

		coverage, err := storage.ReconciliationCoverage(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, coverage)
	})

	t.Run("set balance", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account,
				Currency:   currency,
				Block:      genesisBlock,
				Difference: "100",
			},
			genesisBlock,
		)
		assert.True(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		coverage, err := storage.ReconciliationCoverage(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, coverage)
	})

	t.Run("store reconciliation", func(t *testing.T) {
		err := storage.Reconciled(ctx, account, currency, genesisBlock)
		assert.NoError(t, err)

		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    account,
				Currency:   currency2,
				Block:      genesisBlock,
				Difference: "200",
			},
			genesisBlock,
		)
		assert.True(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		coverage, err := storage.ReconciliationCoverage(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, coverage)

		coverage, err = storage.ReconciliationCoverage(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, coverage)
	})

	t.Run("update reconciliation", func(t *testing.T) {
		err := storage.Reconciled(ctx, account, currency, newBlock)
		assert.NoError(t, err)

		coverage, err := storage.ReconciliationCoverage(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, coverage)

		coverage, err = storage.ReconciliationCoverage(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, coverage)
	})

	t.Run("update reconciliation to old block", func(t *testing.T) {
		err := storage.Reconciled(ctx, account, currency, genesisBlock)
		assert.NoError(t, err)

		coverage, err := storage.ReconciliationCoverage(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, coverage)

		// We should skip update so this stays 0.5
		coverage, err = storage.ReconciliationCoverage(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, 0.5, coverage)
	})

	t.Run("add unreconciled", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		newAccount, err := storage.UpdateBalance(
			ctx,
			txn,
			&parser.BalanceChange{
				Account:    subAccountMetadata2,
				Currency:   currency2,
				Block:      newBlock,
				Difference: "200",
			},
			newBlock,
		)
		assert.True(t, newAccount)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		coverage, err := storage.ReconciliationCoverage(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, float64(1)/float64(3), coverage)
	})

	t.Run("test estimated no reconciliations", func(t *testing.T) {
		mockHelper.On("AccountsReconciled", ctx, mock.Anything).Return(big.NewInt(0), nil).Once()
		mockHelper.On("AccountsSeen", ctx, mock.Anything).Return(big.NewInt(0), nil).Once()
		coverage, err := storage.EstimatedReconciliationCoverage(ctx)
		assert.Equal(t, float64(0), coverage)
		assert.NoError(t, err)
	})

	t.Run("test estimated some reconciliations", func(t *testing.T) {
		mockHelper.On("AccountsReconciled", ctx, mock.Anything).Return(big.NewInt(1), nil).Once()
		mockHelper.On("AccountsSeen", ctx, mock.Anything).Return(big.NewInt(2), nil).Once()
		coverage, err := storage.EstimatedReconciliationCoverage(ctx)
		assert.Equal(t, float64(0.5), coverage)
		assert.NoError(t, err)
	})

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestBlockSyncing(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	mockHelper := &mocks.BalanceStorageHelper{}
	mockHandler := &mocks.BalanceStorageHandler{}
	mockHelper.On("Asserter").Return(baseAsserter())
	mockHelper.On("ExemptFunc").Return(exemptFunc())
	mockHelper.On("BalanceExemptions").Return([]*types.BalanceExemption{})
	storage.Initialize(mockHelper, mockHandler)

	// Genesis block with no transactions
	b0 := &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Index: 0,
			Hash:  "0",
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Index: 0,
			Hash:  "0",
		},
	}

	addr1 := &types.AccountIdentifier{
		Address: "addr1",
	}
	addr2 := &types.AccountIdentifier{
		Address: "addr2",
	}
	curr := &types.Currency{
		Symbol:   "ETH",
		Decimals: 18,
	}

	// Block 1 with transaction
	b1 := &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Index: 1,
			Hash:  "1",
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Index: 0,
			Hash:  "0",
		},
		Transactions: []*types.Transaction{
			{
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "1_0",
				},
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 0,
						},
						Account: addr1,
						Status:  types.String("Success"),
						Type:    "Transfer",
						Amount: &types.Amount{
							Value:    "100",
							Currency: curr,
						},
					},
				},
			},
		},
	}

	// Another Transaction for some acocunt in Block 1
	b2 := &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Index: 2,
			Hash:  "2",
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Index: 1,
			Hash:  "1",
		},
		Transactions: []*types.Transaction{
			{
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "2_0",
				},
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 0,
						},
						Account: addr1,
						Status:  types.String("Success"),
						Type:    "Transfer",
						Amount: &types.Amount{
							Value:    "-50",
							Currency: curr,
						},
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 1,
						},
						Account: addr2,
						Status:  types.String("Success"),
						Type:    "Transfer",
						Amount: &types.Amount{
							Value:    "50",
							Currency: curr,
						},
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 2,
						},
						Account: addr1,
						Status:  types.String("Success"),
						Type:    "Transfer",
						Amount: &types.Amount{
							Value:    "-1",
							Currency: curr,
						},
					},
				},
			},
		},
	}

	// Orphaned block with slightly different tx
	b2a := &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Index: 2,
			Hash:  "2a",
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Index: 1,
			Hash:  "1",
		},
		Transactions: []*types.Transaction{
			{
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "2_0",
				},
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 0,
						},
						Account: addr1,
						Status:  types.String("Success"),
						Type:    "Transfer",
						Amount: &types.Amount{
							Value:    "-100",
							Currency: curr,
						},
					},
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 1,
						},
						Account: addr2,
						Status:  types.String("Success"),
						Type:    "Transfer",
						Amount: &types.Amount{
							Value:    "100",
							Currency: curr,
						},
					},
				},
			},
		},
	}

	t.Run("add genesis block", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		_, err = storage.AddingBlock(gctx, g, b0, dbTx)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, dbTx.Commit(ctx))

		amount, err := storage.GetBalance(ctx, addr1, curr, b0.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b0.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
	})

	t.Run("add block 1", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"AccountBalance",
			gctx,
			addr1,
			curr,
			b0.BlockIdentifier,
		).Return(
			&types.Amount{Value: "1", Currency: curr},
			nil,
		).Once()
		mockHandler.On("AccountsSeen", gctx, dbTx, 1).Return(nil).Once()
		_, err = storage.AddingBlock(gctx, g, b1, dbTx)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, dbTx.Commit(ctx))

		amount, err := storage.GetBalance(ctx, addr1, curr, b0.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b1.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "101",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b1.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
	})

	t.Run("add block 2", func(t *testing.T) {
		// Reconcile a previously seen account
		err := storage.Reconciled(ctx, addr1, curr, b1.BlockIdentifier)
		assert.NoError(t, err)

		dbTx := database.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"AccountBalance",
			gctx,
			addr2,
			curr,
			b1.BlockIdentifier,
		).Return(
			&types.Amount{Value: "0", Currency: curr},
			nil,
		).Once()
		mockHandler.On("AccountsSeen", gctx, dbTx, 1).Return(nil).Once()
		mockHandler.On("AccountsReconciled", gctx, dbTx, 1).Return(nil).Once()
		_, err = storage.AddingBlock(gctx, g, b2, dbTx)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, dbTx.Commit(ctx))

		amount, err := storage.GetBalance(ctx, addr1, curr, b0.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b1.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "101",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b1.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b2.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "50",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b2.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "50",
			Currency: curr,
		}, amount)
	})

	t.Run("orphan block 2", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitWorker, err := storage.RemovingBlock(gctx, g, b2, dbTx)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, dbTx.Commit(ctx))
		mockHandler.On("BlockRemoved", ctx, b2, mock.Anything).Return(nil).Once()
		mockHandler.On("AccountsSeen", ctx, mock.Anything, -1).Return(nil).Once()
		assert.NoError(t, commitWorker(ctx))

		amount, err := storage.GetBalance(ctx, addr1, curr, b0.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b1.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "101",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b1.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b2.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "101",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b2.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
	})

	t.Run("orphan block 1", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitWorker, err := storage.RemovingBlock(gctx, g, b1, dbTx)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, dbTx.Commit(ctx))
		mockHandler.On("BlockRemoved", ctx, b1, mock.Anything).Return(nil).Once()
		mockHandler.On("AccountsSeen", ctx, mock.Anything, -1).Return(nil).Once()
		mockHandler.On("AccountsReconciled", ctx, mock.Anything, -1).Return(nil).Once()
		assert.NoError(t, commitWorker(ctx))

		amount, err := storage.GetBalance(ctx, addr1, curr, b0.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b0.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b1.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b1.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b2.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b2.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
	})

	t.Run("add block 1", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"AccountBalance",
			gctx,
			addr1,
			curr,
			b0.BlockIdentifier,
		).Return(
			&types.Amount{Value: "1", Currency: curr},
			nil,
		).Once()
		mockHandler.On("AccountsSeen", gctx, dbTx, 1).Return(nil).Once()
		_, err = storage.AddingBlock(gctx, g, b1, dbTx)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, dbTx.Commit(ctx))

		amount, err := storage.GetBalance(ctx, addr1, curr, b0.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b1.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "101",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b1.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b2.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "101",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b2.BlockIdentifier.Index)
		assert.True(t, errors.Is(err, storageErrs.ErrAccountMissing))
		assert.Nil(t, amount)
	})

	t.Run("add block 2a", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"AccountBalance",
			gctx,
			addr2,
			curr,
			b1.BlockIdentifier,
		).Return(
			&types.Amount{Value: "0", Currency: curr},
			nil,
		).Once()
		mockHandler.On("AccountsSeen", gctx, dbTx, 1).Return(nil).Once()
		_, err = storage.AddingBlock(gctx, g, b2a, dbTx)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		assert.NoError(t, dbTx.Commit(ctx))

		amount, err := storage.GetBalance(ctx, addr1, curr, b0.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b1.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "101",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b1.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr1, curr, b2.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "1",
			Currency: curr,
		}, amount)
		amount, err = storage.GetBalance(ctx, addr2, curr, b2.BlockIdentifier.Index)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "100",
			Currency: curr,
		}, amount)
	})

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}
