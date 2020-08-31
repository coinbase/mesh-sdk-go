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
	"fmt"
	"math/big"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

const (
	coinNamespace        = "coin"
	coinAccountNamespace = "coin-account"
)

var _ BlockWorker = (*CoinStorage)(nil)

// CoinStorage implements storage methods for storing
// UTXOs.
type CoinStorage struct {
	db Database

	helper   CoinStorageHelper
	asserter *asserter.Asserter
}

// CoinStorageHelper is used by CoinStorage to determine
// at which block a Coin set is valid.
type CoinStorageHelper interface {
	// CurrentBlockIdentifier is called while fetching coins in a single
	// database transaction to return the *types.BlockIdentifier where
	// the Coin set is valid.
	CurrentBlockIdentifier(
		context.Context,
		DatabaseTransaction,
	) (*types.BlockIdentifier, error)
}

// NewCoinStorage returns a new CoinStorage.
func NewCoinStorage(
	db Database,
	helper CoinStorageHelper,
	asserter *asserter.Asserter,
) *CoinStorage {
	return &CoinStorage{
		db:       db,
		helper:   helper,
		asserter: asserter,
	}
}

func getCoinKey(identifier *types.CoinIdentifier) (string, []byte) {
	return coinNamespace, []byte(fmt.Sprintf("%s/%s", coinNamespace, identifier.Identifier))
}

func getCoinAccountPrefix(accountIdentifier *types.AccountIdentifier) []byte {
	return []byte(fmt.Sprintf("%s/%s", coinAccountNamespace, types.Hash(accountIdentifier)))
}

func getCoinAccountCoin(
	accountIdentifier *types.AccountIdentifier,
	coinIdentifier *types.CoinIdentifier,
) []byte {
	return []byte(
		fmt.Sprintf("%s/%s", getCoinAccountPrefix(accountIdentifier), coinIdentifier.Identifier),
	)
}

func (c *CoinStorage) getAndDecodeCoin(
	ctx context.Context,
	transaction DatabaseTransaction,
	coinIdentifier *types.CoinIdentifier,
) (bool, *types.Coin, error) {
	namespace, key := getCoinKey(coinIdentifier)
	exists, val, err := transaction.Get(ctx, key)
	if err != nil {
		return false, nil, fmt.Errorf("%w: unable to query for coin", err)
	}

	if !exists { // this could occur if coin was created before we started syncing
		return false, nil, nil
	}

	var coin types.Coin
	if err := c.db.Compressor().Decode(namespace, val, &coin); err != nil {
		return false, nil, fmt.Errorf("%w: unable to decode coin", err)
	}

	return true, &coin, nil
}

// AccountCoin contains an AccountIdentifier and a Coin that it owns
type AccountCoin struct {
	Account *types.AccountIdentifier
	Coin    *types.Coin
}

// AddCoins takes an array of AccountCoins and saves them to the database.
// It returns an error if the transaction fails.
func (c *CoinStorage) AddCoins(
	ctx context.Context,
	accountCoins []*AccountCoin,
) error {
	dbTransaction := c.db.NewDatabaseTransaction(ctx, true)
	defer dbTransaction.Discard(ctx)

	for _, accountCoin := range accountCoins {
		exists, _, err := c.getAndDecodeCoin(ctx, dbTransaction, accountCoin.Coin.CoinIdentifier)
		if err != nil {
			return fmt.Errorf("%w: unable to get coin", err)
		}

		if exists {
			continue
		}

		err = c.addCoin(ctx, accountCoin.Account, accountCoin.Coin, dbTransaction)
		if err != nil {
			return fmt.Errorf("%w: unable to add coin", err)
		}
	}

	if err := dbTransaction.Commit(ctx); err != nil {
		return fmt.Errorf("%w: unable to commit last reconciliation update", err)
	}

	return nil
}

func (c *CoinStorage) addCoin(
	ctx context.Context,
	account *types.AccountIdentifier,
	coin *types.Coin,
	transaction DatabaseTransaction,
) error {
	namespace, key := getCoinKey(coin.CoinIdentifier)
	encodedResult, err := c.db.Compressor().Encode(namespace, coin)
	if err != nil {
		return fmt.Errorf("%w: unable to encode coin data", err)
	}

	if err := storeUniqueKey(ctx, transaction, key, encodedResult); err != nil {
		return fmt.Errorf("%w: unable to store coin", err)
	}

	if err := storeUniqueKey(ctx, transaction, getCoinAccountCoin(account, coin.CoinIdentifier), []byte("")); err != nil {
		return fmt.Errorf("%w: unable to store account coin", err)
	}

	return nil
}

func getAndDecodeCoins(
	ctx context.Context,
	transaction DatabaseTransaction,
	accountIdentifier *types.AccountIdentifier,
) (map[string]struct{}, error) {
	items, err := transaction.Scan(ctx, getCoinAccountPrefix(accountIdentifier))
	if err != nil {
		return nil, fmt.Errorf("%w: unable to query coins for account", err)
	}

	coins := map[string]struct{}{}
	for _, item := range items {
		vals := strings.Split(string(item.Key), "/")
		coinIdentifier := vals[len(vals)-1]
		coins[coinIdentifier] = struct{}{}
	}

	return coins, nil
}

func (c *CoinStorage) removeCoin(
	ctx context.Context,
	account *types.AccountIdentifier,
	coinIdentifier *types.CoinIdentifier,
	transaction DatabaseTransaction,
) error {
	_, key := getCoinKey(coinIdentifier)
	exists, _, err := transaction.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("%w: unable to query for coin", err)
	}

	if !exists { // this could occur if coin was created before we started syncing
		fmt.Printf("%s does not exist\n", coinIdentifier)
		return nil
	}

	if err := transaction.Delete(ctx, key); err != nil {
		return fmt.Errorf("%w: unable to delete coin", err)
	}

	if err := transaction.Delete(ctx, getCoinAccountCoin(account, coinIdentifier)); err != nil {
		return fmt.Errorf("%w: unable to delete coin", err)
	}

	return nil
}

func (c *CoinStorage) skipOperation(
	operation *types.Operation,
) (bool, error) {
	if operation.CoinChange == nil {
		return true, nil
	}

	if operation.Amount == nil {
		return true, nil
	}

	success, err := c.asserter.OperationSuccessful(operation)
	if err != nil {
		return false, fmt.Errorf("%w: unable to parse operation success", err)
	}

	if !success {
		return true, nil
	}

	return false, nil
}

// updateCoins iterates through the transactions
// in a block to determine which coins to add
// and remove from storage.
//
// If a coin is created and spent in the same block,
// it is skipped (i.e. never added/removed from storage).
//
// Alternatively, we could add all coins to the database
// (regardless of whether they are spent in the same block),
// however, this would put a larger strain on the db.
func (c *CoinStorage) updateCoins(
	ctx context.Context,
	block *types.Block,
	addCoinCreated bool,
	dbTx DatabaseTransaction,
) error {
	addCoins := map[string]*types.Operation{}
	removeCoins := map[string]*types.Operation{}

	for _, txn := range block.Transactions {
		for _, operation := range txn.Operations {
			skip, err := c.skipOperation(operation)
			if err != nil {
				return fmt.Errorf("%w: unable to to determine if should skip operation", err)
			}
			if skip {
				continue
			}

			coinChange := operation.CoinChange
			identifier := coinChange.CoinIdentifier.Identifier
			addAction := types.CoinCreated
			if !addCoinCreated {
				addAction = types.CoinSpent
			}

			if coinChange.CoinAction == addAction {
				addCoins[identifier] = operation
				continue
			}

			removeCoins[identifier] = operation
		}
	}

	for identifier, op := range addCoins {
		if _, ok := removeCoins[identifier]; ok {
			continue
		}

		if err := c.addCoin(
			ctx,
			op.Account,
			&types.Coin{
				CoinIdentifier: op.CoinChange.CoinIdentifier,
				Amount:         op.Amount,
			},
			dbTx,
		); err != nil {
			return fmt.Errorf("%w: unable to add coin", err)
		}
	}

	for identifier, op := range removeCoins {
		if _, ok := addCoins[identifier]; ok {
			continue
		}

		if err := c.removeCoin(
			ctx,
			op.Account,
			op.CoinChange.CoinIdentifier,
			dbTx,
		); err != nil {
			return fmt.Errorf("%w: unable to remove coin", err)
		}
	}

	return nil
}

// AddingBlock is called by BlockStorage when adding a block.
func (c *CoinStorage) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	return nil, c.updateCoins(ctx, block, true, transaction)
}

// RemovingBlock is called by BlockStorage when removing a block.
func (c *CoinStorage) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	return nil, c.updateCoins(ctx, block, false, transaction)
}

// GetCoins returns all unspent coins for a provided *types.AccountIdentifier.
func (c *CoinStorage) GetCoins(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
) ([]*types.Coin, *types.BlockIdentifier, error) {
	transaction := c.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	coins, err := getAndDecodeCoins(ctx, transaction, accountIdentifier)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to query account identifier", err)
	}

	headBlockIdentifier, err := c.helper.CurrentBlockIdentifier(ctx, transaction)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to get current block identifier", err)
	}

	coinArr := []*types.Coin{}
	for coinIdentifier := range coins {
		exists, coin, err := c.getAndDecodeCoin(
			ctx,
			transaction,
			&types.CoinIdentifier{Identifier: coinIdentifier},
		)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to query coin", err)
		}

		if !exists {
			return nil, nil, fmt.Errorf("%w: unable to get coin %s", err, coinIdentifier)
		}

		coinArr = append(coinArr, coin)
	}

	return coinArr, headBlockIdentifier, nil
}

// GetLargestCoin returns the largest Coin for a
// *types.AccountIdentifier and *types.Currency.
// If no Coins are available, a 0 balance is returned.
func (c *CoinStorage) GetLargestCoin(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
	currency *types.Currency,
) (*big.Int, *types.CoinIdentifier, *types.BlockIdentifier, error) {
	coins, blockIdentifier, err := c.GetCoins(ctx, accountIdentifier)
	if err != nil {
		return nil, nil, nil, fmt.Errorf(
			"%w: unable to get utxo balance for %s",
			err,
			accountIdentifier.Address,
		)
	}

	bal := big.NewInt(0)
	var coinIdentifier *types.CoinIdentifier
	for _, coin := range coins {
		if types.Hash(
			coin.Amount.Currency,
		) != types.Hash(
			currency,
		) {
			continue
		}

		val, ok := new(big.Int).SetString(coin.Amount.Value, 10)
		if !ok {
			return nil, nil, nil, fmt.Errorf(
				"could not parse amount for coin %s",
				coin.CoinIdentifier.Identifier,
			)
		}

		if bal.Cmp(val) == -1 {
			bal = val
			coinIdentifier = coin.CoinIdentifier
		}
	}

	return bal, coinIdentifier, blockIdentifier, nil
}

// SetCoinsImported sets coins of a set of addresses by
// getting their coins from the tip block, and populating the database.
// This is used when importing prefunded addresses.
func (c *CoinStorage) SetCoinsImported(
	ctx context.Context,
	accountBalances []*utils.AccountBalance,
) error {
	var accountCoins []*AccountCoin
	for _, accountBalance := range accountBalances {
		for _, coin := range accountBalance.Coins {
			accountCoin := &AccountCoin{
				Account: accountBalance.Account,
				Coin:    coin,
			}

			accountCoins = append(accountCoins, accountCoin)
		}
	}

	if err := c.AddCoins(ctx, accountCoins); err != nil {
		return fmt.Errorf("%w: unable to import coins", err)
	}

	return nil
}
