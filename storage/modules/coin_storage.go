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
	"fmt"
	"math/big"
	"runtime"
	"strings"

	"github.com/neilotoole/errgroup"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/storage/errors"
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
	db     database.Database
	numCPU int

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
		database.Transaction,
	) (*types.BlockIdentifier, error)
}

// NewCoinStorage returns a new CoinStorage.
func NewCoinStorage(
	db database.Database,
	helper CoinStorageHelper,
	asserter *asserter.Asserter,
) *CoinStorage {
	return &CoinStorage{
		db:       db,
		numCPU:   runtime.NumCPU(),
		helper:   helper,
		asserter: asserter,
	}
}

func getCoinKey(identifier *types.CoinIdentifier) []byte {
	return []byte(fmt.Sprintf("%s/%s", coinNamespace, identifier.Identifier))
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
	transaction database.Transaction,
	coinIdentifier *types.CoinIdentifier,
) (bool, *types.Coin, *types.AccountIdentifier, error) {
	key := getCoinKey(coinIdentifier)
	exists, val, err := transaction.Get(ctx, key)
	if err != nil {
		return false, nil, nil, fmt.Errorf("unable to get coin: %w", err)
	}

	if !exists { // this could occur if coin was created before we started syncing
		return false, nil, nil, nil
	}

	var accountCoin types.AccountCoin
	if err := c.db.Encoder().DecodeAccountCoin(val, &accountCoin, true); err != nil {
		return false, nil, nil, fmt.Errorf("unable to decode coin: %w", err)
	}

	return true, accountCoin.Coin, accountCoin.Account, nil
}

// AddCoins takes an array of AccountCoins and saves them to the database.
// It returns an error if the transaction fails.
func (c *CoinStorage) AddCoins(
	ctx context.Context,
	accountCoins []*types.AccountCoin,
) error {
	dbTransaction := c.db.Transaction(ctx)
	defer dbTransaction.Discard(ctx)

	for _, accountCoin := range accountCoins {
		exists, _, _, err := c.getAndDecodeCoin(ctx, dbTransaction, accountCoin.Coin.CoinIdentifier)
		if err != nil {
			return fmt.Errorf("unable to get and decode coin: %w", err)
		}

		if exists {
			continue
		}

		err = c.addCoin(ctx, accountCoin.Account, accountCoin.Coin, dbTransaction)
		if err != nil {
			return fmt.Errorf(
				"unable to add coin for account %s: %w",
				types.PrintStruct(accountCoin.Account),
				err,
			)
		}
	}

	if err := dbTransaction.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit last reconciliation update: %w", err)
	}

	return nil
}

func (c *CoinStorage) addCoin(
	ctx context.Context,
	account *types.AccountIdentifier,
	coin *types.Coin,
	transaction database.Transaction,
) error {
	key := getCoinKey(coin.CoinIdentifier)
	encodedResult, err := c.db.Encoder().EncodeAccountCoin(&types.AccountCoin{
		Account: account,
		Coin:    coin,
	})
	if err != nil {
		return fmt.Errorf("unable to encode coin: %w", err)
	}

	if err := storeUniqueKey(ctx, transaction, key, encodedResult, true); err != nil {
		return fmt.Errorf("unable to store coin: %w", err)
	}

	if err := storeUniqueKey(
		ctx,
		transaction,
		getCoinAccountCoin(account, coin.CoinIdentifier),
		[]byte(""),
		false,
	); err != nil {
		return fmt.Errorf("unable to store account coin: %w", err)
	}

	return nil
}

func getAndDecodeCoins(
	ctx context.Context,
	transaction database.Transaction,
	accountIdentifier *types.AccountIdentifier,
) (map[string]struct{}, error) {
	coins := map[string]struct{}{}
	_, err := transaction.Scan(
		ctx,
		getCoinAccountPrefix(accountIdentifier),
		getCoinAccountPrefix(accountIdentifier),
		func(k []byte, v []byte) error {
			vals := strings.Split(string(k), "/")
			coinIdentifier := vals[len(vals)-1]
			coins[coinIdentifier] = struct{}{}
			return nil
		},
		false,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query coins for account: %w", err)
	}

	return coins, nil
}

func (c *CoinStorage) removeCoin(
	ctx context.Context,
	account *types.AccountIdentifier,
	coinIdentifier *types.CoinIdentifier,
	transaction database.Transaction,
) error {
	key := getCoinKey(coinIdentifier)
	exists, _, err := transaction.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("unable to get coin: %w", err)
	}

	if !exists { // this could occur if coin was created before we started syncing
		return nil
	}

	if err := transaction.Delete(ctx, key); err != nil {
		return fmt.Errorf("unable to delete coin: %w", err)
	}

	if err := transaction.Delete(ctx, getCoinAccountCoin(account, coinIdentifier)); err != nil {
		return fmt.Errorf("unable to delete account coin: %w", err)
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
		return false, fmt.Errorf(
			"unable to successfully parse operation %s: %w",
			types.PrintStruct(operation),
			err,
		)
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
func (c *CoinStorage) updateCoins( // nolint:gocognit
	ctx context.Context,
	g *errgroup.Group,
	block *types.Block,
	addCoinCreated bool,
	dbTx database.Transaction,
) error {
	addCoins := map[string]*types.Operation{}
	removeCoins := map[string]*types.Operation{}

	for _, txn := range block.Transactions {
		for _, operation := range txn.Operations {
			skip, err := c.skipOperation(operation)
			if err != nil {
				return fmt.Errorf(
					"unable to skip operation %s: %w",
					types.PrintStruct(operation),
					err,
				)
			}
			if skip {
				continue
			}

			coinChange := operation.CoinChange
			identifier := coinChange.CoinIdentifier.Identifier
			coinDict := removeCoins
			if addCoinCreated && coinChange.CoinAction == types.CoinCreated ||
				!addCoinCreated && coinChange.CoinAction == types.CoinSpent {
				coinDict = addCoins
			}

			if _, ok := coinDict[identifier]; ok {
				return fmt.Errorf(
					"coin identifier %s is invalid: %w",
					identifier,
					errors.ErrDuplicateCoinFound,
				)
			}

			coinDict[identifier] = operation
		}
	}

	for identifier, val := range addCoins {
		if _, ok := removeCoins[identifier]; ok {
			continue
		}

		// We need to set variable before calling goroutine
		// to avoid getting an updated pointer as loop iteration
		// continues.
		op := val
		g.Go(func() error {
			if err := c.addCoin(
				ctx,
				op.Account,
				&types.Coin{
					CoinIdentifier: op.CoinChange.CoinIdentifier,
					Amount:         op.Amount,
				},
				dbTx,
			); err != nil {
				return fmt.Errorf(
					"unable to add coin for account %s: %w",
					types.PrintStruct(op.Account),
					err,
				)
			}

			return nil
		})
	}

	for identifier, val := range removeCoins {
		if _, ok := addCoins[identifier]; ok {
			continue
		}

		// We need to set variable before calling goroutine
		// to avoid getting an updated pointer as loop iteration
		// continues.
		op := val
		g.Go(func() error {
			if err := c.removeCoin(
				ctx,
				op.Account,
				op.CoinChange.CoinIdentifier,
				dbTx,
			); err != nil {
				return fmt.Errorf(
					"unable to remove coin for account %s: %w",
					types.PrintStruct(op.Account),
					err,
				)
			}

			return nil
		})
	}

	return nil
}

// AddingBlock is called by BlockStorage when adding a block.
func (c *CoinStorage) AddingBlock(
	ctx context.Context,
	g *errgroup.Group,
	block *types.Block,
	transaction database.Transaction,
) (database.CommitWorker, error) {
	return nil, c.updateCoins(ctx, g, block, true, transaction)
}

// RemovingBlock is called by BlockStorage when removing a block.
func (c *CoinStorage) RemovingBlock(
	ctx context.Context,
	g *errgroup.Group,
	block *types.Block,
	transaction database.Transaction,
) (database.CommitWorker, error) {
	return nil, c.updateCoins(ctx, g, block, false, transaction)
}

// GetCoinsTransactional returns all unspent coins for a provided *types.AccountIdentifier.
func (c *CoinStorage) GetCoinsTransactional(
	ctx context.Context,
	dbTx database.Transaction,
	accountIdentifier *types.AccountIdentifier,
) ([]*types.Coin, *types.BlockIdentifier, error) {
	coins, err := getAndDecodeCoins(ctx, dbTx, accountIdentifier)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"unable to get and decode coins for account %s: %w",
			types.PrintStruct(accountIdentifier),
			err,
		)
	}

	headBlockIdentifier, err := c.helper.CurrentBlockIdentifier(ctx, dbTx)
	if err != nil {
		return nil, nil, fmt.Errorf("unable to get current block: %w", err)
	}

	coinArr := []*types.Coin{}
	for coinIdentifier := range coins {
		exists, coin, _, err := c.getAndDecodeCoin(
			ctx,
			dbTx,
			&types.CoinIdentifier{Identifier: coinIdentifier},
		)
		if err != nil {
			return nil, nil, fmt.Errorf(
				"unable to get and decode coin %s: %w",
				types.PrintStruct(coinIdentifier),
				err,
			)
		}

		if !exists {
			return nil, nil, errors.ErrCoinNotFound
		}

		coinArr = append(coinArr, coin)
	}

	return coinArr, headBlockIdentifier, nil
}

// GetCoins returns all unspent coins for a provided *types.AccountIdentifier.
func (c *CoinStorage) GetCoins(
	ctx context.Context,
	accountIdentifier *types.AccountIdentifier,
) ([]*types.Coin, *types.BlockIdentifier, error) {
	dbTx := c.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return c.GetCoinsTransactional(ctx, dbTx, accountIdentifier)
}

// GetCoinTransactional returns a *types.Coin by its identifier in a database
// transaction.
func (c *CoinStorage) GetCoinTransactional(
	ctx context.Context,
	dbTx database.Transaction,
	coinIdentifier *types.CoinIdentifier,
) (*types.Coin, *types.AccountIdentifier, error) {
	exists, coin, owner, err := c.getAndDecodeCoin(ctx, dbTx, coinIdentifier)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"unable to get and decode coin %s: %w",
			types.PrintStruct(coinIdentifier),
			err,
		)
	}

	if !exists {
		return nil, nil, errors.ErrCoinNotFound
	}

	return coin, owner, nil
}

// GetCoin returns a *types.Coin by its identifier.
func (c *CoinStorage) GetCoin(
	ctx context.Context,
	coinIdentifier *types.CoinIdentifier,
) (*types.Coin, *types.AccountIdentifier, error) {
	dbTx := c.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return c.GetCoinTransactional(ctx, dbTx, coinIdentifier)
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
			"unable to get utxo balance for account %s: %w",

			accountIdentifier.Address,
			err,
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

		val, ok := new(big.Int).SetString(coin.Amount.Value, 10) // nolint
		if !ok {
			return nil, nil, nil, fmt.Errorf(
				"coin %s is invalid: %w",
				coin.CoinIdentifier.Identifier,
				errors.ErrCoinParseFailed,
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
	accts []*types.AccountIdentifier,
	acctCoinsResp []*utils.AccountCoinsResponse,
) error {
	// Request array length should always equal response array length.
	// But we still check it for sure.
	if len(accts) != len(acctCoinsResp) {
		return fmt.Errorf(
			"the length of coin request %d and coin response %d are not equal",
			len(accts),
			len(acctCoinsResp),
		)
	}

	var acctCoins []*types.AccountCoin
	for i, resp := range acctCoinsResp {
		for _, coin := range resp.Coins {
			acctCoin := &types.AccountCoin{
				Account: accts[i],
				Coin:    coin,
			}

			acctCoins = append(acctCoins, acctCoin)
		}
	}

	if err := c.AddCoins(ctx, acctCoins); err != nil {
		return fmt.Errorf("unable to add coins: %w", err)
	}

	return nil
}
