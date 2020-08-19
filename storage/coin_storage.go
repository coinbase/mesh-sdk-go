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
	"errors"
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	coinNamespace        = "coinNamespace"
	coinAccountNamespace = "coinAccountNamespace"
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

func getCoinKey(identifier *types.CoinIdentifier) []byte {
	return []byte(fmt.Sprintf("%s/%s", coinNamespace, identifier.Identifier))
}

func getCoinAccountKey(accountIdentifier *types.AccountIdentifier) []byte {
	return []byte(fmt.Sprintf("%s/%s", coinAccountNamespace, types.Hash(accountIdentifier)))
}

func getAndDecodeCoin(
	ctx context.Context,
	transaction DatabaseTransaction,
	coinIdentifier *types.CoinIdentifier,
) (bool, *types.Coin, error) {
	exists, val, err := transaction.Get(ctx, getCoinKey(coinIdentifier))
	if err != nil {
		return false, nil, fmt.Errorf("%w: unable to query for coin", err)
	}

	if !exists { // this could occur if coin was created before we started syncing
		return false, nil, nil
	}

	var coin types.Coin
	if err := decode(val, &coin); err != nil {
		return false, nil, fmt.Errorf("%w: unable to decode coin", err)
	}

	return true, &coin, nil
}

type wallets map[string]map[string]struct{}

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

	cache := wallets{}
	for _, accountCoin := range accountCoins {
		exists, _, err := getAndDecodeCoin(ctx, dbTransaction, accountCoin.Coin.CoinIdentifier)
		if err != nil {
			return fmt.Errorf("%w: unable to get coin", err)
		}

		if exists {
			continue
		}

		err = addCoin(ctx, accountCoin.Account, accountCoin.Coin, dbTransaction, cache)
		if err != nil {
			return fmt.Errorf("%w: unable to add coin", err)
		}
	}

	if err := writeCache(ctx, dbTransaction, cache); err != nil {
		return fmt.Errorf("%w: unable to update wallets", err)
	}

	if err := dbTransaction.Commit(ctx); err != nil {
		return fmt.Errorf("%w: unable to commit last reconciliation update", err)
	}

	return nil
}

func addCoin(
	ctx context.Context,
	account *types.AccountIdentifier,
	coin *types.Coin,
	transaction DatabaseTransaction,
	cache wallets,
) error {
	encodedResult, err := encode(coin)
	if err != nil {
		return fmt.Errorf("%w: unable to encode coin data", err)
	}

	if err := transaction.Set(ctx, getCoinKey(coin.CoinIdentifier), encodedResult); err != nil {
		return fmt.Errorf("%w: unable to store coin", err)
	}

	addressKey := string(getCoinAccountKey(account))
	coins, ok := cache[addressKey]
	if !ok {
		accountExists, accountCoins, err := getAndDecodeCoins(ctx, transaction, account)
		if err != nil {
			return fmt.Errorf("%w: unable to query coin account", err)
		}

		if !accountExists {
			accountCoins = map[string]struct{}{}
		}

		coins = accountCoins
	}
	coinIdentifier := coin.CoinIdentifier.Identifier
	if _, exists := coins[coinIdentifier]; exists {
		return fmt.Errorf(
			"coin %s already exists in account %s",
			coinIdentifier,
			types.PrintStruct(account),
		)
	}

	coins[coinIdentifier] = struct{}{}
	cache[addressKey] = coins

	return nil
}

func (c *CoinStorage) tryAddingCoin(
	ctx context.Context,
	transaction DatabaseTransaction,
	operation *types.Operation,
	action types.CoinAction,
	cache wallets,
) error {
	if operation.CoinChange == nil {
		return errors.New("coin change cannot be nil")
	}

	if operation.CoinChange.CoinAction != action {
		return nil
	}

	coinIdentifier := operation.CoinChange.CoinIdentifier

	newCoin := &types.Coin{
		CoinIdentifier: coinIdentifier,
		Amount:         operation.Amount,
	}

	return addCoin(ctx, operation.Account, newCoin, transaction, cache)
}

func encodeAndSetCoins(
	ctx context.Context,
	transaction DatabaseTransaction,
	accountKey []byte,
	coins map[string]struct{},
) error {
	encodedResult, err := encode(coins)
	if err != nil {
		return fmt.Errorf("%w: unable to encode coins", err)
	}

	if err := transaction.Set(ctx, accountKey, encodedResult); err != nil {
		return fmt.Errorf("%w: unable to set coin account", err)
	}

	return nil
}

func getAndDecodeCoins(
	ctx context.Context,
	transaction DatabaseTransaction,
	accountIdentifier *types.AccountIdentifier,
) (bool, map[string]struct{}, error) {
	accountExists, val, err := transaction.Get(ctx, getCoinAccountKey(accountIdentifier))
	if err != nil {
		return false, nil, fmt.Errorf("%w: unable to query coin account", err)
	}

	if !accountExists {
		return false, nil, nil
	}

	var coins map[string]struct{}
	if err := decode(val, &coins); err != nil {
		return false, nil, fmt.Errorf("%w: unable to decode coin account", err)
	}

	return true, coins, nil
}

func (c *CoinStorage) tryRemovingCoin(
	ctx context.Context,
	transaction DatabaseTransaction,
	operation *types.Operation,
	action types.CoinAction,
	cache wallets,
) error {
	if operation.CoinChange == nil {
		return errors.New("coin change cannot be nil")
	}

	if operation.CoinChange.CoinAction != action {
		return nil
	}

	coinIdentifier := operation.CoinChange.CoinIdentifier

	exists, _, err := transaction.Get(ctx, getCoinKey(coinIdentifier))
	if err != nil {
		return fmt.Errorf("%w: unable to query for coin", err)
	}

	if !exists { // this could occur if coin was created before we started syncing
		return nil
	}

	if err := transaction.Delete(ctx, getCoinKey(coinIdentifier)); err != nil {
		return fmt.Errorf("%w: unable to delete coin", err)
	}

	addressKey := string(getCoinAccountKey(operation.Account))
	coins, ok := cache[addressKey]
	if !ok {
		accountExists, accountCoins, err := getAndDecodeCoins(ctx, transaction, operation.Account)
		if err != nil {
			return fmt.Errorf("%w: unable to query coin account", err)
		}

		if !accountExists {
			return fmt.Errorf("%w: unable to find owner of coin", err)
		}

		coins = accountCoins
	}

	if _, exists := coins[coinIdentifier.Identifier]; !exists {
		return fmt.Errorf(
			"unable to find coin %s in account %s",
			coinIdentifier,
			types.PrettyPrintStruct(operation.Account),
		)
	}

	delete(coins, coinIdentifier.Identifier)
	cache[addressKey] = coins

	return nil
}

// writeCache commits all changes stored in the cache to the database. This ensures
// that any block only ever contains 1 write per account identifier to update
// their persisted wallet.
func writeCache(ctx context.Context, transaction DatabaseTransaction, cache wallets) error {
	for account, coins := range cache {
		if err := encodeAndSetCoins(ctx, transaction, []byte(account), coins); err != nil {
			return fmt.Errorf("%w: unable to set coins", err)
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
	cache := wallets{}
	for _, txn := range block.Transactions {
		for _, operation := range txn.Operations {
			if operation.CoinChange == nil {
				continue
			}

			if operation.Amount == nil {
				continue
			}

			success, err := c.asserter.OperationSuccessful(operation)
			if err != nil {
				return nil, fmt.Errorf("%w: unable to parse operation success", err)
			}

			if !success {
				continue
			}

			if err := c.tryAddingCoin(ctx, transaction, operation, types.CoinCreated, cache); err != nil {
				return nil, fmt.Errorf("%w: unable to add coin", err)
			}

			if err := c.tryRemovingCoin(ctx, transaction, operation, types.CoinSpent, cache); err != nil {
				return nil, fmt.Errorf("%w: unable to remove coin", err)
			}
		}
	}

	if err := writeCache(ctx, transaction, cache); err != nil {
		return nil, fmt.Errorf("%w: unable to update wallets", err)
	}

	return nil, nil
}

// RemovingBlock is called by BlockStorage when removing a block.
func (c *CoinStorage) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	cache := wallets{}
	for _, txn := range block.Transactions {
		for _, operation := range txn.Operations {
			if operation.CoinChange == nil {
				continue
			}

			if operation.Amount == nil {
				continue
			}

			success, err := c.asserter.OperationSuccessful(operation)
			if err != nil {
				return nil, fmt.Errorf("%w: unable to parse operation success", err)
			}

			if !success {
				continue
			}

			// We add spent coins and remove created coins during a re-org (opposite of
			// AddingBlock).
			if err := c.tryAddingCoin(ctx, transaction, operation, types.CoinSpent, cache); err != nil {
				return nil, fmt.Errorf("%w: unable to add coin", err)
			}

			if err := c.tryRemovingCoin(ctx, transaction, operation, types.CoinCreated, cache); err != nil {
				return nil, fmt.Errorf("%w: unable to remove coin", err)
			}
		}
	}

	if err := writeCache(ctx, transaction, cache); err != nil {
		return nil, fmt.Errorf("%w: unable to update wallets", err)
	}

	return nil, nil
}

// GetCoins returns all unspent coins for a provided *types.AccountIdentifier.
func (c *CoinStorage) GetCoins(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
) ([]*types.Coin, *types.BlockIdentifier, error) {
	transaction := c.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	accountExists, coins, err := getAndDecodeCoins(ctx, transaction, accountIdentifier)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to query account identifier", err)
	}

	headBlockIdentifier, err := c.helper.CurrentBlockIdentifier(ctx, transaction)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to get current block identifier", err)
	}

	if !accountExists {
		return []*types.Coin{}, headBlockIdentifier, nil
	}

	coinArr := []*types.Coin{}
	for coinIdentifier := range coins {
		exists, coin, err := getAndDecodeCoin(
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
