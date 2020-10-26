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

package storage

import (
	"context"
	"encoding/json"
	"errors"
	"io/ioutil"
	"math/big"
	"path"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/stretchr/testify/assert"
)

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
		mockHelper = &MockBalanceStorageHelper{
			AccountBalances: map[string]string{
				"genesis": "100",
			},
			BalExemptions: exemptions,
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	storage.Initialize(mockHelper, nil)

	t.Run("Get balance at nil block", func(t *testing.T) {
		amount, err := storage.GetBalance(ctx, account, currency, nil)
		assert.True(t, errors.Is(err, ErrBlockNil))
		assert.Nil(t, amount)
	})

	t.Run("Get unset balance", func(t *testing.T) {
		amount, err := storage.GetBalance(ctx, account, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: currency,
		}, amount)
	})

	t.Run("Set and get genesis balance", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
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

		amount, err := storage.GetBalance(ctx, genesisAccount, currency, genesisBlock)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "100",
			Currency: currency,
		}, amount)
	})

	t.Run("Set and get balance", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.SetBalance(
			ctx,
			txn,
			account,
			amount,
			newBlock,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetBalance(ctx, account, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)

		retrievedAmount, err = storage.GetBalance(ctx, account, currency, newBlock2)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("Set and get balance with storage helper", func(t *testing.T) {
		mockHelper.AccountBalanceAmount = "10"
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetBalance(ctx, account3, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, amountWithPrevious, retrievedAmount)

		mockHelper.AccountBalanceAmount = ""
	})

	t.Run("Set balance with nil currency", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.EqualError(t, err, "invalid currency")
		txn.Discard(ctx)

		retrievedAmount, err := storage.GetBalance(ctx, account, currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("Modify existing balance", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err = storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetBalance(ctx, account, currency, newBlock2)
		assert.NoError(t, err)
		assert.Equal(t, result, retrievedAmount)
	})

	t.Run("Discard transaction", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.NoError(t, err)

		// Get balance during transaction
		readTx := storage.db.NewDatabaseTransaction(ctx, false)
		defer readTx.Discard(ctx)
		retrievedAmount, err := storage.GetBalanceTransactional(
			ctx,
			readTx,
			account,
			currency,
			newBlock2,
		)
		assert.NoError(t, err)
		assert.Equal(t, result, retrievedAmount)

		txn.Discard(ctx)
	})

	t.Run("Attempt modification to push balance negative on existing account", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.True(t, errors.Is(err, ErrNegativeBalance))
		txn.Discard(ctx)
	})

	t.Run("Attempt modification to push balance negative on new acct", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrNegativeBalance))
		txn.Discard(ctx)
	})

	t.Run("sub account set and get balance", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetBalance(
			ctx,
			subAccountNewPointer,
			amount.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("sub account metadata set and get balance", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetBalance(
			ctx,
			subAccountMetadataNewPointer,
			amount.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("sub account unique metadata set and get balance", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetBalance(
			ctx,
			subAccountMetadata2NewPointer,
			amount.Currency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount, retrievedAmount)
	})

	t.Run("balance exemption update", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
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
		mockHelper.AccountBalanceAmount = "150"
		txn = storage.db.NewDatabaseTransaction(ctx, true)
		err = storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err := storage.GetBalance(
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock,
		)
		assert.NoError(t, err)
		assert.Equal(t, "150", retrievedAmount.Value)

		// Successful (balance == computed)
		mockHelper.AccountBalanceAmount = "200"
		txn = storage.db.NewDatabaseTransaction(ctx, true)
		err = storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err = storage.GetBalance(
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock3,
		)
		assert.NoError(t, err)
		assert.Equal(t, "200", retrievedAmount.Value)

		// Unsuccessful (balance < computed)
		mockHelper.AccountBalanceAmount = "10"
		txn = storage.db.NewDatabaseTransaction(ctx, true)
		err = storage.UpdateBalance(
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
		assert.Error(t, err)
		txn.Discard(ctx)

		retrievedAmount, err = storage.GetBalance(
			ctx,
			exemptionAccount,
			exemptionCurrency,
			newBlock4,
		)
		assert.NoError(t, err)
		assert.Equal(t, "200", retrievedAmount.Value)
		mockHelper.AccountBalanceAmount = ""
	})

	t.Run("get all set AccountCurrency", func(t *testing.T) {
		accounts, err := storage.GetAllAccountCurrency(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*reconciler.AccountCurrency{
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
			-1,
		)
		assert.NoError(t, err)

		retrievedAmount, err := storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock3)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "200",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		retrievedAmount, err = storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock2)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "200",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		retrievedAmount, err = storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "100",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
	})

	t.Run("update existing balance", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		orphanValue, _ := new(big.Int).SetString(largeDeduction.Value, 10)
		err := storage.UpdateBalance(
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
		assert.NoError(t, err)
		retrievedAmount, err := storage.GetBalanceTransactional(
			ctx,
			txn,
			account,
			largeDeduction.Currency,
			newBlock3,
		)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "1200",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		txn.Discard(ctx)

		retrievedAmount, err = storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock3)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "200",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
	})

	t.Run("orphan balance correctly", func(t *testing.T) {
		retrievedAmount, err := storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "100",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)

		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err = storage.OrphanBalance(
			ctx,
			txn,
			account,
			largeDeduction.Currency,
			newBlock2,
		)
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err = storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock3)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "100",
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

		retrievedAmount, err := storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock3)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		retrievedAmount, err = storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock2)
		assert.NoError(t, err)
		assert.Equal(t, &types.Amount{
			Value:    "0",
			Currency: largeDeduction.Currency,
		}, retrievedAmount)
		retrievedAmount, err = storage.GetBalance(ctx, account, largeDeduction.Currency, newBlock)
		assert.True(t, errors.Is(err, ErrBalancePruned))
		assert.Nil(t, retrievedAmount)
	})
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
		mockHelper = &MockBalanceStorageHelper{
			AccountBalances: map[string]string{
				"genesis": "100",
			},
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	storage.Initialize(mockHelper, nil)

	t.Run("Set balance successfully", func(t *testing.T) {
		err = storage.SetBalanceImported(
			ctx,
			nil,
			[]*utils.AccountBalance{accBalance1, accBalance2},
		)
		assert.NoError(t, err)

		amount1, err := storage.GetBalance(
			ctx,
			accountCoin,
			currency,
			blockIdentifier,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount1.Value, amountCoins.Value)

		amount2, err := storage.GetBalance(
			ctx,
			accountBalance,
			currency,
			blockIdentifier,
		)
		assert.NoError(t, err)
		assert.Equal(t, amount2.Value, amountBalance.Value)
	})
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
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	mockHelper := &MockBalanceStorageHelper{}
	storage.Initialize(mockHelper, nil)
	bootstrapBalancesFile := path.Join(newDir, "balances.csv")

	t.Run("File doesn't exist", func(t *testing.T) {
		err = storage.BootstrapBalances(
			ctx,
			bootstrapBalancesFile,
			genesisBlockIdentifier,
		)
		assert.Contains(t, err.Error(), "no such file or directory")
	})

	t.Run("Set balance successfully", func(t *testing.T) {
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
		assert.NoError(t, err)

		retrievedAmount, err := storage.GetBalance(
			ctx,
			account,
			amount.Currency,
			genesisBlockIdentifier,
		)

		assert.Equal(t, amount, retrievedAmount)
		assert.NoError(t, err)

		// Attempt to update balance
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err = storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		retrievedAmount, err = storage.GetBalance(
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
		mockHelper = &MockBalanceStorageHelper{
			AccountBalances: map[string]string{
				"genesis": "100",
			},
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBalanceStorage(database)
	storage.Initialize(mockHelper, nil)

	t.Run("attempt to store reconciliation for non-existent account", func(t *testing.T) {
		err := storage.Reconciled(ctx, account, currency, genesisBlock)
		assert.Error(t, err)

		coverage, err := storage.ReconciliationCoverage(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, coverage)
	})

	t.Run("set balance", func(t *testing.T) {
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err := storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		coverage, err := storage.ReconciliationCoverage(ctx, 0)
		assert.NoError(t, err)
		assert.Equal(t, 0.0, coverage)
	})

	t.Run("store reconciliation", func(t *testing.T) {
		err := storage.Reconciled(ctx, account, currency, genesisBlock)
		assert.NoError(t, err)

		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err = storage.UpdateBalance(
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
		txn := storage.db.NewDatabaseTransaction(ctx, true)
		err = storage.UpdateBalance(
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
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(ctx))

		coverage, err := storage.ReconciliationCoverage(ctx, 1)
		assert.NoError(t, err)
		assert.Equal(t, float64(1)/float64(3), coverage)
	})
}

var _ BalanceStorageHelper = (*MockBalanceStorageHelper)(nil)

type MockBalanceStorageHelper struct {
	AccountBalanceAmount string
	AccountBalances      map[string]string
	ExemptAccounts       []*reconciler.AccountCurrency
	BalExemptions        []*types.BalanceExemption
}

func (h *MockBalanceStorageHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) {
	if balance, ok := h.AccountBalances[account.Address]; ok {
		return &types.Amount{
			Value:    balance,
			Currency: currency,
		}, nil
	}

	value := "0"
	if len(h.AccountBalanceAmount) > 0 {
		value = h.AccountBalanceAmount
	}

	return &types.Amount{
		Value:    value,
		Currency: currency,
	}, nil
}

func (h *MockBalanceStorageHelper) Asserter() *asserter.Asserter {
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
	)
	return a
}

func (h *MockBalanceStorageHelper) ExemptFunc() parser.ExemptOperation {
	return func(op *types.Operation) bool {
		thisAcct := &reconciler.AccountCurrency{
			Account:  op.Account,
			Currency: op.Amount.Currency,
		}

		for _, acct := range h.ExemptAccounts {
			if types.Hash(acct) == types.Hash(thisAcct) {
				return true
			}
		}

		return false
	}
}

func (h *MockBalanceStorageHelper) BalanceExemptions() []*types.BalanceExemption {
	return h.BalExemptions
}
