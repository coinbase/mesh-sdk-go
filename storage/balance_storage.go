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
	"log"
	"math/big"

	"github.com/sidhujag/rosetta-sdk-go/asserter"
	"github.com/sidhujag/rosetta-sdk-go/parser"
	"github.com/sidhujag/rosetta-sdk-go/types"
	"github.com/sidhujag/rosetta-sdk-go/utils"

	"golang.org/x/sync/errgroup"
)

var _ BlockWorker = (*BalanceStorage)(nil)

const (
	// accountNamespace is prepended to any stored account.
	accountNamespace = "account"

	// historicalBalanceNamespace is prepended to any stored
	// historical balance.
	historicalBalanceNamespace = "balance"
)

var (
	errAccountFound = errors.New("account found")
)

/*
  Key Construction
*/

// GetAccountKey returns a deterministic hash of a types.Account + types.Currency.
func GetAccountKey(account *types.AccountIdentifier, currency *types.Currency) []byte {
	return []byte(
		fmt.Sprintf("%s/%s/%s", accountNamespace, types.Hash(account), types.Hash(currency)),
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
}

// BalanceStorage implements block specific storage methods
// on top of a Database and DatabaseTransaction interface.
type BalanceStorage struct {
	db      Database
	helper  BalanceStorageHelper
	handler BalanceStorageHandler

	parser *parser.Parser
}

// NewBalanceStorage returns a new BalanceStorage.
func NewBalanceStorage(
	db Database,
) *BalanceStorage {
	return &BalanceStorage{
		db: db,
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
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	log.Printf("BalanceStorage::AddingBlock\n")
	changes, err := b.parser.BalanceChanges(ctx, block, false)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to calculate balance changes", err)
	}
	log.Printf("BalanceStorage::AddingBlock1\n")
	g, gctx := errgroup.WithContext(ctx)
	log.Printf("BalanceStorage::AddingBlock2\n")
	for i := range changes {
		// We need to set variable before calling goroutine
		// to avoid getting an updated pointer as loop iteration
		// continues.
		change := changes[i]
		log.Printf("BalanceStorage::AddingBlock3 %d\n", i)
		g.Go(func() error {
			log.Printf("BalanceStorage::AddingBlock3a %d\n", i)
			return b.UpdateBalance(gctx, transaction, change, block.ParentBlockIdentifier)
		})
	}
	log.Printf("BalanceStorage::AddingBlock4\n")

	if err := g.Wait(); err != nil {
		log.Printf("BalanceStorage::AddingBlock4a\n")
		return nil, err
	}
	log.Printf("BalanceStorage::AddingBlock5\n")
	return func(ctx context.Context) error {
		log.Printf("BalanceStorage::AddingBlock5a\n")
		return b.handler.BlockAdded(ctx, block, changes)
	}, nil
}

// RemovingBlock is called by BlockStorage when removing a block from storage.
func (b *BalanceStorage) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	changes, err := b.parser.BalanceChanges(ctx, block, true)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to calculate balance changes", err)
	}

	g, gctx := errgroup.WithContext(ctx)
	for i := range changes {
		// We need to set variable before calling goroutine
		// to avoid getting an updated pointer as loop iteration
		// continues.
		change := changes[i]
		g.Go(func() error {
			return b.OrphanBalance(
				gctx,
				transaction,
				change.Account,
				change.Currency,
				block.BlockIdentifier,
			)
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		return b.handler.BlockRemoved(ctx, block, changes)
	}, nil
}

type accountEntry struct {
	Account        *types.AccountIdentifier `json:"account"`
	Currency       *types.Currency          `json:"currency"`
	LastReconciled *types.BlockIdentifier   `json:"last_reconciled"`
	LastPruned     *int64                   `json:"last_pruned"`
}

// SetBalance allows a client to set the balance of an account in a database
// transaction (removing all historical states). This is particularly useful
// for bootstrapping balances.
func (b *BalanceStorage) SetBalance(
	ctx context.Context,
	dbTransaction DatabaseTransaction,
	account *types.AccountIdentifier,
	amount *types.Amount,
	block *types.BlockIdentifier,
) error {
	// Remove all historical records
	if err := b.removeHistoricalBalances(
		ctx,
		dbTransaction,
		account,
		amount.Currency,
		-1,
		true, // We want everything >= -1
	); err != nil {
		return err
	}

	serialAcc, err := b.db.Encoder().Encode(historicalBalanceNamespace, accountEntry{
		Account:  account,
		Currency: amount.Currency,
	})
	if err != nil {
		return err
	}

	// Set current record
	key := GetAccountKey(account, amount.Currency)
	if err := dbTransaction.Set(ctx, key, serialAcc, true); err != nil {
		return err
	}

	// Set historical record
	key = GetHistoricalBalanceKey(account, amount.Currency, block.Index)
	if err := dbTransaction.Set(ctx, key, []byte(amount.Value), true); err != nil {
		return err
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
	dbTx := b.db.NewDatabaseTransaction(ctx, true)
	defer dbTx.Discard(ctx)

	err := b.updateAccountEntry(
		ctx,
		dbTx,
		account,
		currency,
		func(accEntry accountEntry) *accountEntry {
			// Don't update last reconciled if the most recent reconciliation was
			// lower than the last reconciliation. This can occur when inactive
			// reconciliation gets ahead of the active reconciliation backlog.
			if accEntry.LastReconciled != nil && accEntry.LastReconciled.Index > block.Index {
				return nil
			}

			accEntry.LastReconciled = block

			return &accEntry
		},
	)
	if err != nil {
		return err
	}

	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: unable to commit last reconciliation update", err)
	}

	return nil
}

// ReconciliationCoverage returns the proportion of accounts [0.0, 1.0] that
// have been reconciled at an index >= to a minimumIndex.
func (b *BalanceStorage) ReconciliationCoverage(
	ctx context.Context,
	minimumIndex int64,
) (float64, error) {
	seen := 0
	validCoverage := 0
	err := b.getAllAccountEntries(ctx, func(entry accountEntry) {
		seen++
		if entry.LastReconciled == nil {
			return
		}

		if entry.LastReconciled.Index >= minimumIndex {
			validCoverage++
		}
	})
	if err != nil {
		return -1, fmt.Errorf("%w: unable to get all account entries", err)
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
	change *parser.BalanceChange,
	parentBlock *types.BlockIdentifier,
	existingValue string,
) (string, error) {
	exists := len(existingValue) > 0

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
			"%w: unable to get previous account balance for %s %s at %s",
			err,
			types.PrintStruct(change.Account),
			types.PrintStruct(change.Currency),
			types.PrintStruct(parentBlock),
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
			"%w: unable to get current account balance for %s %s at %s",
			err,
			types.PrintStruct(change.Account),
			types.PrintStruct(change.Currency),
			types.PrintStruct(change.Block),
		)
	}

	// Determine if new live balance complies
	// with any balance exemption.
	difference, err := types.SubtractValues(liveAmount.Value, newVal)
	if err != nil {
		return "", fmt.Errorf(
			"%w: unable to calculate difference between live and computed balances",
			err,
		)
	}

	exemption := parser.MatchBalanceExemption(
		exemptions,
		difference,
	)
	if exemption == nil {
		return "", fmt.Errorf(
			"%w: account %s balance difference (live - computed) %s at %s is not allowed by any balance exemption",
			ErrInvalidLiveBalance,
			types.PrintStruct(change.Account),
			difference,
			types.PrintStruct(change.Block),
		)
	}

	return liveAmount.Value, nil
}

// OrphanBalance removes all saved
// states for a *types.Account and *types.Currency
// at blocks >= the provided block.
func (b *BalanceStorage) OrphanBalance(
	ctx context.Context,
	dbTransaction DatabaseTransaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) error {
	err := b.removeHistoricalBalances(
		ctx,
		dbTransaction,
		account,
		currency,
		block.Index,
		true,
	)
	if err != nil {
		return err
	}

	// Check if we should remove the account record
	// so that balance can be re-fetched, if necessary.
	_, err = b.getHistoricalBalance(
		ctx,
		dbTransaction,
		account,
		currency,
		block.Index,
	)
	switch {
	case errors.Is(err, ErrAccountMissing):
		key := GetAccountKey(account, currency)
		return dbTransaction.Delete(ctx, key)
	case err != nil:
		return err
	default:
		return nil
	}
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
	dbTx := b.db.NewDatabaseTransaction(ctx, true)
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
		return fmt.Errorf("%w: unable to remove historical balances", err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: unable to commit historical balance removal", err)
	}

	return nil
}

// UpdateBalance updates a types.AccountIdentifer
// by a types.Amount and sets the account's most
// recent accessed block.
func (b *BalanceStorage) UpdateBalance(
	ctx context.Context,
	dbTransaction DatabaseTransaction,
	change *parser.BalanceChange,
	parentBlock *types.BlockIdentifier,
) error {
	log.Printf("BalanceStorage::UpdateBalance\n")
	if change.Currency == nil {
		return errors.New("invalid currency")
	}
	log.Printf("BalanceStorage::UpdateBalance1\n")
	// Get existing account key to determine if
	// balance should be fetched.
	key := GetAccountKey(change.Account, change.Currency)
	log.Printf("BalanceStorage::UpdateBalance2\n")
	exists, _, err := dbTransaction.Get(ctx, key)
	log.Printf("BalanceStorage::UpdateBalance3\n")
	if err != nil {
		log.Printf("BalanceStorage::UpdateBalance3a\n")
		return err
	}
	log.Printf("BalanceStorage::UpdateBalance4\n")
	var storedValue string
	if exists {
		log.Printf("BalanceStorage::UpdateBalance4a\n")
		// Get most recent historical balance
		balance, err := b.getHistoricalBalance(
			ctx,
			dbTransaction,
			change.Account,
			change.Currency,
			change.Block.Index,
		)
		log.Printf("BalanceStorage::UpdateBalance4b\n")
		switch {
		case errors.Is(err, ErrAccountMissing):
			storedValue = "0"
		case err != nil:
			return err
		default:
			storedValue = balance.Value
		}
	}
	log.Printf("BalanceStorage::UpdateBalance5\n")

	// Find account existing value whether the account is new, has an
	// existing balance, or is subject to additional accounting from
	// a balance exemption.
	existingValue, err := b.existingValue(
		ctx,
		change,
		parentBlock,
		storedValue,
	)
	log.Printf("BalanceStorage::UpdateBalance6\n")
	if err != nil {
		log.Printf("BalanceStorage::UpdateBalance6a\n")
		return err
	}
	log.Printf("BalanceStorage::UpdateBalance7\n")
	newVal, err := types.AddValues(change.Difference, existingValue)
	if err != nil {
		return err
		log.Printf("BalanceStorage::UpdateBalance7a\n")
	}
	log.Printf("BalanceStorage::UpdateBalance8\n")
	// If any exemptions apply, the returned new value will
	// reflect the live balance for the *types.AccountIdentifier
	// and *types.Currency.
	newVal, err = b.applyExemptions(ctx, change, newVal)
	if err != nil {
		return err
	}
	log.Printf("BalanceStorage::UpdateBalance9\n")
	bigNewVal, ok := new(big.Int).SetString(newVal, 10)
	if !ok {
		return fmt.Errorf("%s is not an integer", newVal)
	}
	log.Printf("BalanceStorage::UpdateBalance10\n")
	if bigNewVal.Sign() == -1 {
		log.Printf("BalanceStorage::UpdateBalance10a\n")
		return fmt.Errorf(
			"%w %s:%+v for %+v at %+v",
			ErrNegativeBalance,
			newVal,
			change.Currency,
			change.Account,
			change.Block,
		)
	}
	log.Printf("BalanceStorage::UpdateBalance11\n")
	// Add account entry if doesn't exist
	if !exists {
		log.Printf("BalanceStorage::UpdateBalance11a\n")
		serialAcc, err := b.db.Encoder().Encode(accountNamespace, accountEntry{
			Account:  change.Account,
			Currency: change.Currency,
		})
		log.Printf("BalanceStorage::UpdateBalance11b\n")
		if err != nil {
			log.Printf("BalanceStorage::UpdateBalance11ba\n")
			return err
		}
		log.Printf("BalanceStorage::UpdateBalance11c\n")
		if err := dbTransaction.Set(ctx, key, serialAcc, true); err != nil {
			log.Printf("BalanceStorage::UpdateBalance11ca\n")
			return err
		}
		log.Printf("BalanceStorage::UpdateBalance11d\n")
	}
	log.Printf("BalanceStorage::UpdateBalance12\n")
	// Add a new historical record for the balance.
	historicalKey := GetHistoricalBalanceKey(
		change.Account,
		change.Currency,
		change.Block.Index,
	)
	log.Printf("BalanceStorage::UpdateBalance13\n")
	if err := dbTransaction.Set(ctx, historicalKey, []byte(newVal), true); err != nil {
		log.Printf("BalanceStorage::UpdateBalance13a\n")
		return err
	}
	log.Printf("BalanceStorage::UpdateBalance14\n")
	return nil
}

// GetBalance returns the balance of a types.AccountIdentifier
// at the canonical block of a certain index.
func (b *BalanceStorage) GetBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, error) {
	dbTx := b.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	amount, err := b.GetBalanceTransactional(
		ctx,
		dbTx,
		account,
		currency,
		index,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get balance", err)
	}

	return amount, nil
}

// GetBalanceTransactional returns all the balances of a types.AccountIdentifier
// and the types.BlockIdentifier it was last updated at in a database transaction.
func (b *BalanceStorage) GetBalanceTransactional(
	ctx context.Context,
	dbTx DatabaseTransaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, error) {
	key := GetAccountKey(account, currency)
	exists, acct, err := dbTx.Get(ctx, key)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, ErrAccountMissing
	}

	var accEntry accountEntry
	if err := b.db.Encoder().Decode(accountNamespace, acct, &accEntry, true); err != nil {
		return nil, fmt.Errorf(
			"%w: unable to parse balance entry for %s",
			err,
			string(acct),
		)
	}

	if accEntry.LastPruned != nil && *accEntry.LastPruned >= index {
		return nil, fmt.Errorf(
			"%w: last pruned %d",
			ErrBalancePruned,
			*accEntry.LastPruned,
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
	if errors.Is(err, ErrAccountMissing) {
		return &types.Amount{
			Value:    "0",
			Currency: currency,
		}, nil
	}
	if err != nil {
		return nil, err
	}

	return amount, nil
}

func (b *BalanceStorage) fetchAndSetBalance(
	ctx context.Context,
	dbTx DatabaseTransaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) {
	amount, err := b.helper.AccountBalance(ctx, account, currency, block)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get account balance from helper", err)
	}

	err = b.SetBalance(
		ctx,
		dbTx,
		account,
		amount,
		block,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to set account balance", err)
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
	dbTx := b.db.NewDatabaseTransaction(ctx, true)
	defer dbTx.Discard(ctx)

	amount, err := b.GetOrSetBalanceTransactional(
		ctx,
		dbTx,
		account,
		currency,
		block,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get balance", err)
	}

	// We commit any changes made during the balance lookup.
	if err := dbTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%w: unable to commit account balance transaction", err)
	}

	return amount, nil
}

// GetOrSetBalanceTransactional returns the balance of a types.AccountIdentifier
// at the canonical block of a certain index, setting it if it
// doesn't exist.
func (b *BalanceStorage) GetOrSetBalanceTransactional(
	ctx context.Context,
	dbTx DatabaseTransaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, error) {
	if block == nil {
		return nil, ErrBlockNil
	}

	amount, err := b.GetBalanceTransactional(
		ctx,
		dbTx,
		account,
		currency,
		block.Index,
	)
	if errors.Is(err, ErrAccountMissing) {
		amount, err = b.fetchAndSetBalance(ctx, dbTx, account, currency, block)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to set balance", err)
		}

		return amount, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get balance", err)
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
		return err
	}

	// Update balances in database
	dbTransaction := b.db.NewDatabaseTransaction(ctx, true)
	defer dbTransaction.Discard(ctx)

	for _, balance := range balances {
		// Ensure change.Difference is valid
		amountValue, ok := new(big.Int).SetString(balance.Value, 10)
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
			return err
		}
	}

	err := dbTransaction.Commit(ctx)
	if err != nil {
		return err
	}

	log.Printf("%d Balances Bootstrapped\n", len(balances))
	return nil
}

func (b *BalanceStorage) getAllAccountEntries(
	ctx context.Context,
	handler func(accountEntry),
) error {
	txn := b.db.NewDatabaseTransaction(ctx, false)
	defer txn.Discard(ctx)
	_, err := txn.Scan(
		ctx,
		[]byte(accountNamespace),
		[]byte(accountNamespace),
		func(k []byte, v []byte) error {
			var accEntry accountEntry
			// We should not reclaim memory during a scan!!
			err := b.db.Encoder().Decode(accountNamespace, v, &accEntry, false)
			if err != nil {
				return fmt.Errorf(
					"%w: unable to parse balance entry for %s",
					err,
					string(v),
				)
			}

			handler(accEntry)

			return nil
		},
		false,
		false,
	)
	if err != nil {
		return fmt.Errorf("%w: database scan failed", err)
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

	accountEntries := []*accountEntry{}
	if err := b.getAllAccountEntries(ctx, func(entry accountEntry) {
		accountEntries = append(accountEntries, &entry)
	}); err != nil {
		return nil, fmt.Errorf("%w: unable to get all balance entries", err)
	}

	accounts := make([]*types.AccountCurrency, len(accountEntries))
	for i, account := range accountEntries {
		accounts[i] = &types.AccountCurrency{
			Account:  account.Account,
			Currency: account.Currency,
		}
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
	transaction := b.db.NewDatabaseTransaction(ctx, true)
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
			return err
		}
	}

	if err := transaction.Commit(ctx); err != nil {
		return err
	}

	log.Printf("%d Balances Updated\n", len(accountBalances))
	return nil
}

// getHistoricalBalance returns the balance of an account
// at a particular *types.BlockIdentifier.
func (b *BalanceStorage) getHistoricalBalance(
	ctx context.Context,
	dbTx DatabaseTransaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, error) {
	log.Printf("getHistoricalBalance\n")
	var foundValue string
	_, err := dbTx.Scan(
		ctx,
		GetHistoricalBalancePrefix(account, currency),
		GetHistoricalBalanceKey(account, currency, index),
		func(k []byte, v []byte) error {
			foundValue = string(v)
			return errAccountFound
		},
		false,
		true,
	)
	log.Printf("getHistoricalBalance1\n")
	if errors.Is(err, errAccountFound) {
		log.Printf("getHistoricalBalance2 %s\n", foundValue)
		return &types.Amount{
			Value:    foundValue,
			Currency: currency,
		}, nil
	}
	log.Printf("getHistoricalBalance3\n")
	if err != nil {
		log.Printf("getHistoricalBalance3a\n")
		return nil, fmt.Errorf("%w: database scan failed", err)
	}
	log.Printf("getHistoricalBalance4\n")
	return nil, ErrAccountMissing
}

func (b *BalanceStorage) updateAccountEntry(
	ctx context.Context,
	dbTx DatabaseTransaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	handler func(accountEntry) *accountEntry,
) error {
	key := GetAccountKey(account, currency)
	exists, acc, err := dbTx.Get(ctx, key)
	if err != nil {
		return fmt.Errorf(
			"%w: unable to get account entry for %s:%s",
			err,
			types.PrettyPrintStruct(account),
			types.PrettyPrintStruct(currency),
		)
	}

	if !exists {
		return fmt.Errorf(
			"account entry is missing for %s:%s",
			types.PrettyPrintStruct(account),
			types.PrettyPrintStruct(currency),
		)
	}

	var accEntry accountEntry
	if err := b.db.Encoder().Decode(accountNamespace, acc, &accEntry, true); err != nil {
		return fmt.Errorf("%w: unable to decode account entry", err)
	}

	newAcctEntry := handler(accEntry)
	if newAcctEntry == nil {
		return nil
	}

	serialAcc, err := b.db.Encoder().Encode(accountNamespace, newAcctEntry)
	if err != nil {
		return fmt.Errorf("%w: unable to encode account entry", err)
	}

	if err := dbTx.Set(ctx, key, serialAcc, true); err != nil {
		return fmt.Errorf("%w: unable to set account entry", err)
	}

	return nil
}

// removeHistoricalBalances deletes all historical balances
// >= (used during reorg) or <= (used during pruning) a particular
// index.
func (b *BalanceStorage) removeHistoricalBalances(
	ctx context.Context,
	dbTx DatabaseTransaction,
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

			foundKeys = append(foundKeys, thisK)
			return nil
		},
		false,

		// If we are orphaning blocks, we should sort
		// from greatest to least (i.e. reverse). If we
		// are pruning, we want to sort least to greatest.
		!orphan,
	)
	if err != nil {
		return fmt.Errorf("%w: database scan failed", err)
	}

	for _, k := range foundKeys {
		if err := dbTx.Delete(ctx, k); err != nil {
			return err
		}
	}

	// Update the last pruned index
	if !orphan {
		err := b.updateAccountEntry(
			ctx,
			dbTx,
			account,
			currency,
			func(accEntry accountEntry) *accountEntry {
				// Don't update last pruned if the most recent pruning was
				// lower than the last prune.
				if accEntry.LastPruned != nil && *accEntry.LastPruned > index {
					return nil
				}

				accEntry.LastPruned = &index

				return &accEntry
			},
		)
		if err != nil {
			return err
		}
	}

	return nil
}
