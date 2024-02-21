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
	"fmt"
	"log"
	"math/big"
	"runtime"
	"sync"

	"github.com/neilotoole/errgroup"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

var _ BlockWorker = (*BalanceStorage)(nil)

const (
	// accountNamespace is prepended to any stored account.
	accountNamespace = "acc"

	// balanceNamespace is prepended to any stored balance.
	balanceNamespace = "bal"

	// historicalBalanceNamespace is prepended to any stored
	// historical balance.
	historicalBalanceNamespace = "hbal"

	// reconciliationNamespace is prepended to any stored
	// reconciliation.
	reconciliationNamepace = "recacc"

	// pruneNamespace is prepended to any stored pruning
	// record.
	pruneNamespace = "pruneacc"

	// maxBalancePruneSize is the maximum number of balances
	// we should consider pruning at one time.
	maxBalancePruneSize = 5000
)

var (
	errAccountFound = errors.New("account found")
	errTooManyKeys  = errors.New("too many keys")
)

/*
  Key Construction
*/

// GetAccountKey returns a deterministic hash of a types.Account + types.Currency.
func GetAccountKey(
	namespace string,
	account *types.AccountIdentifier,
	currency *types.Currency,
) []byte {
	return []byte(
		fmt.Sprintf("%s/%s/%s", namespace, types.Hash(account), types.Hash(currency)),
	)
}

// GetHistoricalBalanceKey returns a deterministic hash of a types.Account + types.Currency + block
// index.
func GetHistoricalBalanceKey(
	account *types.AccountIdentifier,
	currency *types.Currency,
	blockIndex int64,
) []byte {
	return []byte(
		fmt.Sprintf(
			"%s/%s/%s/%020d",
			historicalBalanceNamespace,
			types.Hash(account),
			types.Hash(currency),
			blockIndex,
		),
	)
}

// GetHistoricalBalancePrefix returns a deterministic hash of a types.Account + types.Currency to
// limit scan results.
func GetHistoricalBalancePrefix(account *types.AccountIdentifier, currency *types.Currency) []byte {
	return []byte(
		fmt.Sprintf(
			"%s/%s/%s/",
			historicalBalanceNamespace,
			types.Hash(account),
			types.Hash(currency),
		),
	)
}

// BalanceStorageHandler is invoked after balance changes are committed to the database.
type BalanceStorageHandler interface {
	BlockAdded(ctx context.Context, block *types.Block, changes []*parser.BalanceChange) error
	BlockRemoved(ctx context.Context, block *types.Block, changes []*parser.BalanceChange) error

	AccountsReconciled(ctx context.Context, dbTx database.Transaction, count int) error
	AccountsSeen(ctx context.Context, dbTx database.Transaction, count int) error
}

// BalanceStorageHelper functions are used by BalanceStorage to process balances. Defining an
// interface allows the client to determine if they wish to query the node for
// certain information or use another datastore.
type BalanceStorageHelper interface {
	AccountBalance(
		ctx context.Context,
		account *types.AccountIdentifier,
		currency *types.Currency,
		block *types.BlockIdentifier,
	) (*types.Amount, error)

	ExemptFunc() parser.ExemptOperation
	BalanceExemptions() []*types.BalanceExemption
	Asserter() *asserter.Asserter

	AccountsReconciled(ctx context.Context, dbTx database.Transaction) (*big.Int, error)
	AccountsSeen(ctx context.Context, dbTx database.Transaction) (*big.Int, error)
}

// BalanceStorage implements block specific storage methods
// on top of a database.Database and database.Transaction interface.
type BalanceStorage struct {
	db      database.Database
	helper  BalanceStorageHelper
	handler BalanceStorageHandler
	numCPU  int

	// To scale up write concurrency on reconciliation,
	// we don't update the global reconciliation counter
	// in the account-scoped reconciliatiion transaction.
	pendingReconciliations     int
	pendingReconciliationMutex *utils.PriorityMutex

	parser *parser.Parser
}

// NewBalanceStorage returns a new BalanceStorage.
func NewBalanceStorage(
	db database.Database,
) *BalanceStorage {
	return &BalanceStorage{
		db:                         db,
		numCPU:                     runtime.NumCPU(),
		pendingReconciliationMutex: new(utils.PriorityMutex),
	}
}

// Initialize adds a BalanceStorageHelper and BalanceStorageHandler to BalanceStorage.
// This must be called prior to syncing!
func (b *BalanceStorage) Initialize(
	helper BalanceStorageHelper,
	handler BalanceStorageHandler,
) {
	b.helper = helper
	b.handler = handler
	b.parser = parser.New(
		helper.Asserter(),
		helper.ExemptFunc(),
		helper.BalanceExemptions(),
	)
}

// AddingBlock is called by BlockStorage when adding a block to storage.
func (b *BalanceStorage) AddingBlock(
	ctx context.Context,
	g *errgroup.Group,
	block *types.Block,
	transaction database.Transaction,
) (database.CommitWorker, error) {
	if b.handler == nil {
		return nil, storageErrs.ErrHelperHandlerMissing
	}

	changes, err := b.parser.BalanceChanges(ctx, block, false)
	if err != nil {
		return nil, fmt.Errorf("unable to calculate balance changes: %w", err)
	}

	// Keep track of how many new accounts have been seen so that the counter
	// can be updated in a single op.
	for i := range changes {
		// We need to set variable before calling goroutine
		// to avoid getting an updated pointer as loop iteration
		// continues.
		change := changes[i]
		g.Go(func() error {
			newAccount, err := b.UpdateBalance(
				ctx,
				transaction,
				change,
				block.ParentBlockIdentifier,
			)
			if err != nil {
				return fmt.Errorf("unable to update balance: %w", err)
			}

			if !newAccount {
				return nil
			}

			return b.handler.AccountsSeen(ctx, transaction, 1)
		})
	}

	// Update accounts reconciled
	var pending int
	b.pendingReconciliationMutex.Lock(true)
	pending = b.pendingReconciliations
	b.pendingReconciliations = 0
	b.pendingReconciliationMutex.Unlock()

	if pending > 0 {
		if err := b.handler.AccountsReconciled(ctx, transaction, pending); err != nil {
			return nil, fmt.Errorf(
				"unable to update the total accounts reconciled by count: %w",
				err,
			)
		}
	}

	return func(ctx context.Context) error {
		return b.handler.BlockAdded(ctx, block, changes)
	}, nil
}

// RemovingBlock is called by BlockStorage when removing a block from storage.
func (b *BalanceStorage) RemovingBlock(
	ctx context.Context,
	g *errgroup.Group,
	block *types.Block,
	transaction database.Transaction,
) (database.CommitWorker, error) {
	if b.handler == nil {
		return nil, storageErrs.ErrHelperHandlerMissing
	}

	changes, err := b.parser.BalanceChanges(ctx, block, true)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to calculate balance changes for block %s: %w",
			types.PrintStruct(block),
			err,
		)
	}

	// staleAccounts should be removed because the orphaned
	// balance was the last stored balance.
	staleAccounts := []*types.AccountCurrency{}
	var staleAccountsMutex sync.Mutex

	// Concurrent execution limited to runtime.NumCPU
	for i := range changes {
		// We need to set variable before calling goroutine
		// to avoid getting an updated pointer as loop iteration
		// continues.
		change := changes[i]
		g.Go(func() error {
			shouldRemove, err := b.OrphanBalance(
				ctx,
				transaction,
				change,
			)
			if err != nil {
				return err
			}

			if !shouldRemove {
				return nil
			}

			staleAccountsMutex.Lock()
			staleAccounts = append(staleAccounts, &types.AccountCurrency{
				Account:  change.Account,
				Currency: change.Currency,
			})
			staleAccountsMutex.Unlock()

			return nil
		})
	}

	return func(ctx context.Context) error {
		if err := b.handler.BlockRemoved(ctx, block, changes); err != nil {
			return fmt.Errorf("unable to remove block %s: %w", types.PrintStruct(block), err)
		}

		if len(staleAccounts) == 0 {
			return nil
		}

		dbTx := b.db.Transaction(ctx)
		defer dbTx.Discard(ctx)
		for _, account := range staleAccounts {
			if err := b.deleteAccountRecords(ctx, dbTx, account.Account, account.Currency); err != nil {
				return err
			}
		}

		return dbTx.Commit(ctx)
	}, nil
}

// SetBalance allows a client to set the balance of an account in a database
// transaction (removing all historical states). This is particularly useful
// for bootstrapping balances.
func (b *BalanceStorage) SetBalance(
	ctx context.Context,
	dbTransaction database.Transaction,
	account *types.AccountIdentifier,
	amount *types.Amount,
	block *types.BlockIdentifier,
) error {
	if b.handler == nil {
		return storageErrs.ErrHelperHandlerMissing
	}

	// Remove all account-related items
	if err := b.deleteAccountRecords(
		ctx,
		dbTransaction,
		account,
		amount.Currency,
	); err != nil {
		return fmt.Errorf(
			"unable to delete account records for account %s: %w",
			types.PrintStruct(account),
			err,
		)
	}

	// Mark as new account seen
	if err := b.handler.AccountsSeen(ctx, dbTransaction, 1); err != nil {
		return fmt.Errorf("unable to update the total accounts seen by count: %w", err)
	}

	// Serialize account entry
	serialAcc, err := b.db.Encoder().EncodeAccountCurrency(&types.AccountCurrency{
		Account:  account,
		Currency: amount.Currency,
	})
	if err != nil {
		return fmt.Errorf(
			"unable to encode account currency for currency %s of account %s: %w",
			types.PrintStruct(amount.Currency),
			types.PrintStruct(account),
			err,
		)
	}

	// Set account
	key := GetAccountKey(accountNamespace, account, amount.Currency)
	if err := dbTransaction.Set(ctx, key, serialAcc, true); err != nil {
		return fmt.Errorf("unable to set account: %w", err)
	}

	// Set current balance
	key = GetAccountKey(balanceNamespace, account, amount.Currency)
	value, ok := new(big.Int).SetString(amount.Value, 10) // nolint
	if !ok {
		return storageErrs.ErrInvalidValue
	}

	valueBytes := value.Bytes()
	if err := dbTransaction.Set(ctx, key, valueBytes, false); err != nil {
		return fmt.Errorf("unable to set current balance: %w", err)
	}

	// Set historical balance
	key = GetHistoricalBalanceKey(account, amount.Currency, block.Index)
	if err := dbTransaction.Set(ctx, key, valueBytes, true); err != nil {
		return fmt.Errorf(
			"unable to set historical balance for currency %s of account %s: %w",
			types.PrintStruct(amount.Currency),
			types.PrintStruct(account),
			err,
		)
	}

	return nil
}

// Reconciled updates the LastReconciled field on a particular
// balance. Tracking reconciliation coverage is an important
// end condition.
func (b *BalanceStorage) Reconciled(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) error {
	key := GetAccountKey(reconciliationNamepace, account, currency)
	dbTx := b.db.WriteTransaction(ctx, string(key), false)
	defer dbTx.Discard(ctx)

	// Return nil if account record does not exist (could have
	// occurred during a reorg at tip
	acctKey := GetAccountKey(accountNamespace, account, currency)
	acctExists, _, err := dbTx.Get(ctx, acctKey)
	if err != nil {
		return fmt.Errorf("unable to get account: %w", err)
	}
	if !acctExists {
		return nil
	}

	exists, lastReconciled, err := BigIntGet(ctx, key, dbTx)
	if err != nil {
		return fmt.Errorf("unable to get reconciliation: %w", err)
	}

	if exists {
		// Skip update if we already reconciled the account
		// after the index.
		if block.Index <= lastReconciled.Int64() {
			return nil
		}
	} else {
		b.pendingReconciliationMutex.Lock(false)
		b.pendingReconciliations++
		b.pendingReconciliationMutex.Unlock()
	}

	if err := dbTx.Set(ctx, key, new(big.Int).SetInt64(block.Index).Bytes(), true); err != nil {
		return fmt.Errorf("unable to set reconciliation: %w", err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit last reconciliation update: %w", err)
	}

	return nil
}

// EstimatedReconciliationCoverage returns an estimated
// reconciliation coverage metric. This can be used to
// get an idea of the reconciliation coverage without
// doing an expensive DB scan across all accounts.
func (b *BalanceStorage) EstimatedReconciliationCoverage(ctx context.Context) (float64, error) {
	if b.helper == nil {
		return -1, storageErrs.ErrHelperHandlerMissing
	}

	dbTx := b.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	reconciled, err := b.helper.AccountsReconciled(ctx, dbTx)
	if err != nil {
		return -1, fmt.Errorf("unable to update the total accounts reconciled by count: %w", err)
	}

	accounts, err := b.helper.AccountsSeen(ctx, dbTx)
	if err != nil {
		return -1, fmt.Errorf("unable to update the total accounts seen by count: %w", err)
	}

	if accounts.Sign() == 0 {
		return 0, nil
	}

	return float64(reconciled.Int64()) / float64(accounts.Int64()), nil
}

// ReconciliationCoverage returns the proportion of accounts [0.0, 1.0] that
// have been reconciled at an index >= to a minimumIndex.
func (b *BalanceStorage) ReconciliationCoverage(
	ctx context.Context,
	minimumIndex int64,
) (float64, error) {
	seen := 0
	validCoverage := 0
	err := b.getAllAccountEntries(
		ctx,
		func(txn database.Transaction, entry *types.AccountCurrency) error {
			seen++

			// Fetch last reconciliation index in same database.Transaction
			key := GetAccountKey(reconciliationNamepace, entry.Account, entry.Currency)
			exists, lastReconciled, err := BigIntGet(ctx, key, txn)
			if err != nil {
				return fmt.Errorf("unable to get reconciliation: %w", err)
			}

			if !exists {
				return nil
			}

			if lastReconciled.Int64() >= minimumIndex {
				validCoverage++
			}

			return nil
		},
	)
	if err != nil {
		return -1, fmt.Errorf("unable to get all account entries: %w", err)
	}

	if seen == 0 {
		return 0, nil
	}

	return float64(validCoverage) / float64(seen), nil
}

// existingValue finds the existing value for
// a given *types.AccountIdentifier and *types.Currency.
//
// If there are matching balance exemptions,
// we update the passed in existing value.
func (b *BalanceStorage) existingValue(
	ctx context.Context,
	exists bool,
	change *parser.BalanceChange,
	parentBlock *types.BlockIdentifier,
	existingValue string,
) (string, error) {
	if b.helper == nil {
		return "", storageErrs.ErrHelperHandlerMissing
	}

	if exists {
		return existingValue, nil
	}

	// Don't attempt to use the helper if we are going to query the same
	// block we are processing (causes the duplicate issue).
	//
	// We also ensure we don't exit with 0 if the value already exists,
	// which could be true if balances are bootstrapped.
	if parentBlock != nil && change.Block.Hash == parentBlock.Hash {
		return "0", nil
	}

	// Use helper to fetch existing balance.
	amount, err := b.helper.AccountBalance(
		ctx,
		change.Account,
		change.Currency,
		parentBlock,
	)
	if err != nil {
		return "", fmt.Errorf(
			"unable to get previous account balance for %s %s at %s: %w",
			types.PrintStruct(change.Account),
			types.PrintStruct(change.Currency),
			types.PrintStruct(parentBlock),
			err,
		)
	}

	return amount.Value, nil
}

// applyExemptions compares the computed balance of an account
// with the live balance of an account at the *types.BlockIdentifier
// of the *parser.BalanceChange, if any exemptions apply.
//
// We compare the balance of the provided account after the
// new balance has been calculated instead of at the parent block
// because many blockchains apply rewards at the start
// of each block, which can be spent in the block. This
// means it is possible for an account balance to go negative
// if the balance change is applied to the balance of the account
// at the parent block.
func (b *BalanceStorage) applyExemptions(
	ctx context.Context,
	change *parser.BalanceChange,
	newVal string,
) (string, error) {
	if b.helper == nil {
		return "", storageErrs.ErrHelperHandlerMissing
	}

	// Find exemptions that are applicable to the *parser.BalanceChange
	exemptions := b.parser.FindExemptions(change.Account, change.Currency)
	if len(exemptions) == 0 {
		return newVal, nil
	}

	// Use helper to fetch live balance.
	liveAmount, err := b.helper.AccountBalance(
		ctx,
		change.Account,
		change.Currency,
		change.Block,
	)
	if err != nil {
		return "", fmt.Errorf(
			"unable to get current account balance for %s %s at %s: %w",
			types.PrintStruct(change.Account),
			types.PrintStruct(change.Currency),
			types.PrintStruct(change.Block),
			err,
		)
	}

	// Determine if new live balance complies
	// with any balance exemption.
	difference, err := types.SubtractValues(liveAmount.Value, newVal)
	if err != nil {
		return "", fmt.Errorf(
			"unable to calculate difference between live balance %s and computed balance %s: %w",
			liveAmount.Value,
			newVal,
			err,
		)
	}

	exemption := parser.MatchBalanceExemption(
		exemptions,
		difference,
	)
	if exemption == nil {
		return "", fmt.Errorf(
			"account %s balance difference (live - computed) %s at %s is not allowed by any balance exemption: %w",
			types.PrintStruct(change.Account),
			difference,
			types.PrintStruct(change.Block),
			storageErrs.ErrInvalidLiveBalance,
		)
	}

	return liveAmount.Value, nil
}

// deleteAccountRecords is a convenience method that deletes
// all traces of an account. This method should be called
// within a global write transaction as it could contend with
// other database.Transactions.
//
// WARNING: DO NOT RUN THIS CONCURRENTLY! IT CAN LEAD TO COUNTER
// CORRUPTION!
func (b *BalanceStorage) deleteAccountRecords(
	ctx context.Context,
	dbTx database.Transaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
) error {
	if b.handler == nil {
		return storageErrs.ErrHelperHandlerMissing
	}

	// Remove historical balance records
	if err := b.removeHistoricalBalances(
		ctx,
		dbTx,
		account,
		currency,
		-1,
		true, // We want everything >= -1
	); err != nil {
		return fmt.Errorf("unable to remove historical balances: %w", err)
	}

	// Remove single key records
	for _, namespace := range []string{
		accountNamespace,
		reconciliationNamepace,
		pruneNamespace,
		balanceNamespace,
	} {
		key := GetAccountKey(namespace, account, currency)

		// Determine if we should decrement the accounts
		// seen counter
		if namespace == accountNamespace {
			exists, _, err := dbTx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("unable to get account: %w", err)
			}

			if exists {
				if err := b.handler.AccountsSeen(ctx, dbTx, -1); err != nil {
					return fmt.Errorf("unable to update the total accounts seen by count: %w", err)
				}
			}
		}

		// Determine if we should decrement the reconciled
		// accounts counter
		if namespace == reconciliationNamepace {
			exists, _, err := dbTx.Get(ctx, key)
			if err != nil {
				return fmt.Errorf("unable to get reconciliation: %w", err)
			}

			if exists {
				if err := b.handler.AccountsReconciled(ctx, dbTx, -1); err != nil {
					return fmt.Errorf(
						"unable to update the total accounts reconciled by count: %w",
						err,
					)
				}
			}
		}

		if err := dbTx.Delete(ctx, key); err != nil {
			return fmt.Errorf("unable to delete transaction: %w", err)
		}
	}

	return nil
}

// OrphanBalance removes all saved
// states for a *types.Account and *types.Currency
// at blocks >= the provided block.
func (b *BalanceStorage) OrphanBalance(
	ctx context.Context,
	dbTransaction database.Transaction,
	change *parser.BalanceChange,
) (bool, error) {
	err := b.removeHistoricalBalances(
		ctx,
		dbTransaction,
		change.Account,
		change.Currency,
		change.Block.Index,
		true,
	)
	if err != nil {
		return false, fmt.Errorf("unable to remove historical balances: %w", err)
	}

	// Check if we should remove the account record
	// so that balance can be re-fetched, if necessary.
	_, err = b.getHistoricalBalance(
		ctx,
		dbTransaction,
		change.Account,
		change.Currency,
		change.Block.Index,
	)
	switch {
	case errors.Is(err, storageErrs.ErrAccountMissing):
		return true, nil
	case err != nil:
		return false, fmt.Errorf(
			"unable to get historical balance for currency %s of account %s: %w",
			types.PrintStruct(change.Currency),
			types.PrintStruct(change.Account),
			err,
		)
	}

	// Update current balance
	key := GetAccountKey(balanceNamespace, change.Account, change.Currency)
	exists, lastBalance, err := BigIntGet(ctx, key, dbTransaction)
	if err != nil {
		return false, fmt.Errorf("unable to get balance: %w", err)
	}

	if !exists {
		return false, storageErrs.ErrAccountMissing
	}

	difference, ok := new(big.Int).SetString(change.Difference, 10) // nolint
	if !ok {
		return false, storageErrs.ErrInvalidChangeValue
	}

	newBalance := new(big.Int).Add(lastBalance, difference)
	if err := dbTransaction.Set(ctx, key, newBalance.Bytes(), true); err != nil {
		return false, fmt.Errorf("unable to set current balance: %w", err)
	}

	return false, nil
}

// PruneBalances removes all historical balance states
// <= some index. This can significantly reduce storage
// usage in scenarios where historical balances are only
// retrieved once (like reconciliation).
func (b *BalanceStorage) PruneBalances(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) error {
	key := GetAccountKey(pruneNamespace, account, currency)
	dbTx := b.db.WriteTransaction(ctx, string(key), false)
	defer dbTx.Discard(ctx)

	err := b.removeHistoricalBalances(
		ctx,
		dbTx,
		account,
		currency,
		index,
		false,
	)
	if err != nil {
		return fmt.Errorf("unable to remove historical balances: %w", err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit historical balance removal: %w", err)
	}

	return nil
}

// UpdateBalance updates a types.AccountIdentifer
// by a types.Amount and sets the account's most
// recent accessed block.
func (b *BalanceStorage) UpdateBalance(
	ctx context.Context,
	dbTransaction database.Transaction,
	change *parser.BalanceChange,
	parentBlock *types.BlockIdentifier,
) (bool, error) {
	if change.Currency == nil {
		return false, storageErrs.ErrInvalidCurrency
	}

	// If the balance key does not exist, the account
	// does not exist.
	key := GetAccountKey(balanceNamespace, change.Account, change.Currency)
	exists, currentBalance, err := BigIntGet(ctx, key, dbTransaction)
	if err != nil {
		return false, fmt.Errorf("unable to get balance: %w", err)
	}

	// Find account existing value whether the account is new, has an
	// existing balance, or is subject to additional accounting from
	// a balance exemption.
	//
	// currentBalance is equal to 0 when it doesn't exist
	// so we don't need to apply any conditional logic here.
	existingValue, err := b.existingValue(
		ctx,
		exists,
		change,
		parentBlock,
		currentBalance.String(),
	)
	if err != nil {
		return false, fmt.Errorf(
			"unable to find account existing value for currency %s of account %s: %w",
			types.PrintStruct(change.Currency),
			types.PrintStruct(change.Account),
			err,
		)
	}

	newVal, err := types.AddValues(change.Difference, existingValue)
	if err != nil {
		return false, err
	}

	// If any exemptions apply, the returned new value will
	// reflect the live balance for the *types.AccountIdentifier
	// and *types.Currency.
	newVal, err = b.applyExemptions(ctx, change, newVal)
	if err != nil {
		return false, fmt.Errorf("unable to apply exemptions: %w", err)
	}

	bigNewVal, ok := new(big.Int).SetString(newVal, 10) // nolint
	if !ok {
		return false, fmt.Errorf("%s is not an integer", newVal)
	}

	if bigNewVal.Sign() == -1 {
		return false, fmt.Errorf(
			"account balance %s of currency %s for account %s at block %s is invalid: %w",
			newVal,
			types.PrintStruct(change.Currency),
			types.PrintStruct(change.Account),
			types.PrintStruct(change.Block),
			storageErrs.ErrNegativeBalance,
		)
	}

	// Add account entry if doesn't exist
	var newAccount bool
	if !exists {
		newAccount = true
		key := GetAccountKey(accountNamespace, change.Account, change.Currency)
		serialAcc, err := b.db.Encoder().EncodeAccountCurrency(&types.AccountCurrency{
			Account:  change.Account,
			Currency: change.Currency,
		})
		if err != nil {
			return false, fmt.Errorf(
				"unable to encode account currency %s of account %s: %w",
				types.PrintStruct(change.Currency),
				types.PrintStruct(change.Account),
				err,
			)
		}
		if err := dbTransaction.Set(ctx, key, serialAcc, true); err != nil {
			return false, fmt.Errorf("unable to set account: %w", err)
		}
	}

	// Update current balance
	if err := dbTransaction.Set(ctx, key, bigNewVal.Bytes(), true); err != nil {
		return false, fmt.Errorf("unable to set current balance: %w", err)
	}

	// Add a new historical record for the balance.
	historicalKey := GetHistoricalBalanceKey(
		change.Account,
		change.Currency,
		change.Block.Index,
	)
	if err := dbTransaction.Set(ctx, historicalKey, bigNewVal.Bytes(), true); err != nil {
		return false, fmt.Errorf(
			"unable to set historical balance for currency %s of account %s: %w",
			types.PrintStruct(change.Currency),
			types.PrintStruct(change.Account),
			err,
		)
	}

	return newAccount, nil
}

// GetBalance returns the balance of a types.AccountIdentifier
// at the canonical block of a certain index.
func (b *BalanceStorage) GetBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, error) {
	dbTx := b.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	amount, err := b.GetBalanceTransactional(
		ctx,
		dbTx,
		account,
		currency,
		index,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to get balance: %w", err)
	}

	return amount, nil
}

// GetBalanceTransactional returns all the balances of a types.AccountIdentifier
// and the types.BlockIdentifier it was last updated at in a database transaction.
func (b *BalanceStorage) GetBalanceTransactional(
	ctx context.Context,
	dbTx database.Transaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, error) {
	key := GetAccountKey(balanceNamespace, account, currency)
	exists, _, err := dbTx.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("unable to get balance: %w", err)
	}

	if !exists {
		return nil, storageErrs.ErrAccountMissing
	}

	key = GetAccountKey(pruneNamespace, account, currency)
	exists, lastPruned, err := BigIntGet(ctx, key, dbTx)
	if err != nil {
		return nil, fmt.Errorf("unable to get prune: %w", err)
	}

	if exists && lastPruned.Int64() >= index {
		return nil, fmt.Errorf(
			"desired %d last pruned %d: %w",
			index,
			lastPruned.Int64(),
			storageErrs.ErrBalancePruned,
		)
	}

	amount, err := b.getHistoricalBalance(
		ctx,
		dbTx,
		account,
		currency,
		index,
	)
	// If account record exists but we don't
	// find any records for the index, we assume
	// the balance to be 0 (i.e. before any balance
	// changes applied). If syncing starts after
	// genesis, this behavior could cause issues.
	if errors.Is(err, storageErrs.ErrAccountMissing) {
		return &types.Amount{
			Value:    "0",
			Currency: currency,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf(
			"unable to get historical balance for currency %s of account %s: %w",
			types.PrintStruct(currency),
			types.PrintStruct(account),
			err,
		)
	}

	return amount, nil
}

func (b *BalanceStorage) fetchAndSetBalance(
	ctx context.Context,
	dbTx database.Transaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) {
	amount, err := b.helper.AccountBalance(ctx, account, currency, block)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to get account balance for currency %s of account %s: %w",
			types.PrintStruct(currency),
			types.PrintStruct(account),
			err,
		)
	}

	err = b.SetBalance(
		ctx,
		dbTx,
		account,
		amount,
		block,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to set account balance of account %s: %w",
			types.PrintStruct(account),
			err,
		)
	}

	return amount, nil
}

// GetOrSetBalance returns the balance of a types.AccountIdentifier
// at the canonical block of a certain index, setting it if it
// doesn't exist.
func (b *BalanceStorage) GetOrSetBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) {
	dbTx := b.db.Transaction(ctx)
	defer dbTx.Discard(ctx)

	amount, err := b.GetOrSetBalanceTransactional(
		ctx,
		dbTx,
		account,
		currency,
		block,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"unable to get balance for currency %s of account %s: %w",
			types.PrintStruct(currency),
			types.PrintStruct(account),
			err,
		)
	}

	// We commit any changes made during the balance lookup.
	if err := dbTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("unable to commit account balance transaction: %w", err)
	}

	return amount, nil
}

// GetOrSetBalanceTransactional returns the balance of a types.AccountIdentifier
// at the canonical block of a certain index, setting it if it
// doesn't exist.
func (b *BalanceStorage) GetOrSetBalanceTransactional(
	ctx context.Context,
	dbTx database.Transaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) {
	if block == nil {
		return nil, storageErrs.ErrBlockNil
	}

	amount, err := b.GetBalanceTransactional(
		ctx,
		dbTx,
		account,
		currency,
		block.Index,
	)
	if errors.Is(err, storageErrs.ErrAccountMissing) {
		amount, err = b.fetchAndSetBalance(ctx, dbTx, account, currency, block)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to set balance for currency %s of account %s: %w",
				types.PrintStruct(currency),
				types.PrintStruct(account),
				err,
			)
		}

		return amount, nil
	}
	if err != nil {
		return nil, fmt.Errorf(
			"unable to get balance for currency %s of account %s: %w",
			types.PrintStruct(currency),
			types.PrintStruct(account),
			err,
		)
	}

	return amount, nil
}

// BootstrapBalance represents a balance of
// a *types.AccountIdentifier and a *types.Currency in the
// genesis block.
type BootstrapBalance struct {
	Account  *types.AccountIdentifier `json:"account_identifier,omitempty"`
	Currency *types.Currency          `json:"currency,omitempty"`
	Value    string                   `json:"value,omitempty"`
}

// BootstrapBalances is utilized to set the balance of
// any number of AccountIdentifiers at the genesis blocks.
// This is particularly useful for setting the value of
// accounts that received an allocation in the genesis block.
func (b *BalanceStorage) BootstrapBalances(
	ctx context.Context,
	bootstrapBalancesFile string,
	genesisBlockIdentifier *types.BlockIdentifier,
) error {
	// Read bootstrap file
	balances := []*BootstrapBalance{}
	if err := utils.LoadAndParse(bootstrapBalancesFile, &balances); err != nil {
		return fmt.Errorf("unable to load and parse %s: %w", bootstrapBalancesFile, err)
	}

	// Update balances in database
	dbTransaction := b.db.Transaction(ctx)
	defer dbTransaction.Discard(ctx)

	for i, balance := range balances {
		// Commit transaction batch by batch rather than commit at one time.
		// This helps reduce memory usage and improve running time when bootstrap_balances.json
		// contains huge number of accounts.
		if i != 0 && i%utils.MaxEntrySizePerTxn == 0 {
			if err := dbTransaction.Commit(ctx); err != nil {
				return fmt.Errorf("unable to commit bootstrap balance update batch")
			}
			dbTransaction = b.db.Transaction(ctx)
		}
		// Ensure change.Difference is valid
		amountValue, ok := new(big.Int).SetString(balance.Value, 10) // nolint
		if !ok {
			return fmt.Errorf("%s is not an integer", balance.Value)
		}

		if amountValue.Sign() < 1 {
			return fmt.Errorf("cannot bootstrap zero or negative balance %s", amountValue.String())
		}

		log.Printf(
			"Setting account %s balance to %s %+v\n",
			balance.Account.Address,
			balance.Value,
			balance.Currency,
		)

		err := b.SetBalance(
			ctx,
			dbTransaction,
			balance.Account,
			&types.Amount{
				Value:    balance.Value,
				Currency: balance.Currency,
			},
			genesisBlockIdentifier,
		)
		if err != nil {
			return fmt.Errorf(
				"unable to set balance for currency %s of account %s: %w",
				types.PrintStruct(balance.Currency),
				types.PrintStruct(balance.Account),
				err,
			)
		}
	}

	if err := dbTransaction.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit bootstrap balance")
	}

	log.Printf("%d Balances Bootstrapped\n", len(balances))
	return nil
}

func (b *BalanceStorage) getAllAccountEntries(
	ctx context.Context,
	handler func(database.Transaction, *types.AccountCurrency) error,
) error {
	txn := b.db.ReadTransaction(ctx)
	defer txn.Discard(ctx)
	_, err := txn.Scan(
		ctx,
		[]byte(accountNamespace),
		[]byte(accountNamespace),
		func(k []byte, v []byte) error {
			var accCurrency types.AccountCurrency
			// We should not reclaim memory during a scan!!
			err := b.db.Encoder().DecodeAccountCurrency(v, &accCurrency, false)
			if err != nil {
				return fmt.Errorf(
					"unable to parse balance entry for %s: %w",
					string(v),
					err,
				)
			}

			return handler(txn, &accCurrency)
		},
		false,
		false,
	)
	if err != nil {
		return fmt.Errorf("database scan failed: %w", err)
	}

	return nil
}

// GetAllAccountCurrency scans the db for all balances and returns a slice
// of reconciler.AccountCurrency. This is useful for bootstrapping the reconciler
// after restart.
func (b *BalanceStorage) GetAllAccountCurrency(
	ctx context.Context,
) ([]*types.AccountCurrency, error) {
	log.Println("Loading previously seen accounts (this could take a while)...")

	accounts := []*types.AccountCurrency{}
	if err := b.getAllAccountEntries(ctx, func(_ database.Transaction, account *types.AccountCurrency) error {
		accounts = append(accounts, account)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("unable to get all balance entries: %w", err)
	}

	return accounts, nil
}

// SetBalanceImported sets the balances of a set of addresses by
// getting their balances from the tip block, and populating the database.
// This is used when importing prefunded addresses.
func (b *BalanceStorage) SetBalanceImported(
	ctx context.Context,
	helper BalanceStorageHelper,
	accountBalances []*utils.AccountBalance,
) error {
	// Update balances in database
	transaction := b.db.Transaction(ctx)
	defer transaction.Discard(ctx)

	for _, accountBalance := range accountBalances {
		log.Printf(
			"Setting account %s balance to %s %+v\n",
			accountBalance.Account.Address,
			accountBalance.Amount.Value,
			accountBalance.Amount.Currency,
		)

		err := b.SetBalance(
			ctx,
			transaction,
			accountBalance.Account,
			accountBalance.Amount,
			accountBalance.Block,
		)
		if err != nil {
			return fmt.Errorf(
				"unable to set balance for currency %s of account %s: %w",
				types.PrintStruct(accountBalance.Amount.Currency),
				types.PrintStruct(accountBalance.Account),
				err,
			)
		}
	}

	if err := transaction.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit balance imported")
	}

	log.Printf("%d Balances Updated\n", len(accountBalances))
	return nil
}

// getHistoricalBalance returns the balance of an account
// at a particular *types.BlockIdentifier.
func (b *BalanceStorage) getHistoricalBalance(
	ctx context.Context,
	dbTx database.Transaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, error) {
	var foundValue string
	_, err := dbTx.Scan(
		ctx,
		GetHistoricalBalancePrefix(account, currency),
		GetHistoricalBalanceKey(account, currency, index),
		func(k []byte, v []byte) error {
			foundValue = new(big.Int).SetBytes(v).String()
			return errAccountFound
		},
		false,
		true,
	)
	if errors.Is(err, errAccountFound) {
		return &types.Amount{
			Value:    foundValue,
			Currency: currency,
		}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("database scan failed: %w", err)
	}

	return nil, storageErrs.ErrAccountMissing
}

// removeHistoricalBalances deletes all historical balances
// >= (used during reorg) or <= (used during pruning) a particular
// index.
func (b *BalanceStorage) removeHistoricalBalances(
	ctx context.Context,
	dbTx database.Transaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
	orphan bool,
) error {
	foundKeys := [][]byte{}
	_, err := dbTx.Scan(
		ctx,
		GetHistoricalBalancePrefix(account, currency),
		GetHistoricalBalanceKey(account, currency, index),
		func(k []byte, v []byte) error {
			thisK := make([]byte, len(k))
			copy(thisK, k)

			// If we don't limit the max pruning size,
			// we may accidentally create a commit too large for
			// the database.
			if !orphan && len(foundKeys) >= maxBalancePruneSize {
				return errTooManyKeys
			}

			foundKeys = append(foundKeys, thisK)
			return nil
		},
		false,

		// If we are orphaning blocks, we should sort
		// from greatest to least (i.e. reverse). If we
		// are pruning, we want to sort least to greatest.
		!orphan,
	)
	if err != nil && !errors.Is(err, errTooManyKeys) {
		return fmt.Errorf("database scan failed: %w", err)
	}

	for _, k := range foundKeys {
		if err := dbTx.Delete(ctx, k); err != nil {
			return fmt.Errorf("unable to delete transaction: %w", err)
		}
	}

	// We don't update the pruned value when index is less
	// than 0 because big.Int conversion doesn't support signed values.
	if orphan || index < 0 {
		return nil
	}

	// Update the last pruned index
	key := GetAccountKey(pruneNamespace, account, currency)
	exists, lastPruned, err := BigIntGet(ctx, key, dbTx)
	if err != nil {
		return fmt.Errorf("unable to get prune: %w", err)
	}

	if exists && lastPruned.Int64() > index {
		return nil
	}

	return dbTx.Set(ctx, key, big.NewInt(index).Bytes(), true)
}
