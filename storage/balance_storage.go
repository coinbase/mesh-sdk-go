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

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/reconciler"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

var _ BlockWorker = (*BalanceStorage)(nil)

const (
	// balanceNamespace is prepended to any stored balance.
	balanceNamespace = "balance"
)

var (
	// ErrAccountNotFound is returned when an account
	// is not found in BalanceStorage.
	ErrAccountNotFound = errors.New("account not found")

	// ErrNegativeBalance is returned when an account
	// balance goes negative as the result of an operation.
	ErrNegativeBalance = errors.New("negative balance")
)

/*
  Key Construction
*/

// GetBalanceKey returns a deterministic hash of an types.Account + types.Currency.
func GetBalanceKey(account *types.AccountIdentifier, currency *types.Currency) (string, []byte) {
	return balanceNamespace, []byte(
		fmt.Sprintf("%s/%s/%s", balanceNamespace, types.Hash(account), types.Hash(currency)),
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
func (b *BalanceStorage) Initialize(helper BalanceStorageHelper, handler BalanceStorageHandler) {
	b.helper = helper
	b.handler = handler
	b.parser = parser.New(helper.Asserter(), helper.ExemptFunc())
}

// AddingBlock is called by BlockStorage when adding a block to storage.
func (b *BalanceStorage) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	changes, err := b.parser.BalanceChanges(ctx, block, false)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to calculate balance changes", err)
	}

	for _, change := range changes {
		if err := b.UpdateBalance(ctx, transaction, change, block.ParentBlockIdentifier); err != nil {
			return nil, err
		}
	}

	return func(ctx context.Context) error {
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

	for _, change := range changes {
		if err := b.UpdateBalance(ctx, transaction, change, block.BlockIdentifier); err != nil {
			return nil, err
		}
	}

	return func(ctx context.Context) error {
		return b.handler.BlockRemoved(ctx, block, changes)
	}, nil
}

type balanceEntry struct {
	Account        *types.AccountIdentifier `json:"account"`
	Amount         *types.Amount            `json:"amount"`
	Block          *types.BlockIdentifier   `json:"block"`
	LastReconciled *types.BlockIdentifier   `json:"last_reconciled"`
}

// SetBalance allows a client to set the balance of an account in a database
// transaction. This is particularly useful for bootstrapping balances.
func (b *BalanceStorage) SetBalance(
	ctx context.Context,
	dbTransaction DatabaseTransaction,
	account *types.AccountIdentifier,
	amount *types.Amount,
	block *types.BlockIdentifier,
) error {
	namespace, key := GetBalanceKey(account, amount.Currency)

	serialBal, err := b.db.Compressor().Encode(namespace, &balanceEntry{
		Account: account,
		Amount:  amount,
		Block:   block,
	})
	if err != nil {
		return err
	}

	if err := dbTransaction.Set(ctx, key, serialBal); err != nil {
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
	dbTransaction := b.db.NewDatabaseTransaction(ctx, true)
	defer dbTransaction.Discard(ctx)

	namespace, key := GetBalanceKey(account, currency)
	exists, balance, err := dbTransaction.Get(ctx, key)
	if err != nil {
		return fmt.Errorf(
			"%w: unable to get balance entry for account %s:%s",
			err,
			types.PrettyPrintStruct(account),
			types.PrettyPrintStruct(currency),
		)
	}

	if !exists {
		return fmt.Errorf(
			"balance entry is missing for account %s:%s",
			types.PrettyPrintStruct(account),
			types.PrettyPrintStruct(currency),
		)
	}

	var bal balanceEntry
	if err := b.db.Compressor().Decode(namespace, balance, &bal); err != nil {
		return fmt.Errorf("%w: unable to decode balance entry", err)
	}

	bal.LastReconciled = block

	serialBal, err := b.db.Compressor().Encode(namespace, bal)
	if err != nil {
		return fmt.Errorf("%w: unable to encod balance entry", err)
	}

	if err := dbTransaction.Set(ctx, key, serialBal); err != nil {
		return fmt.Errorf("%w: unable to set balance entry", err)
	}

	if err := dbTransaction.Commit(ctx); err != nil {
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
	balances, err := b.getAllBalanceEntries(ctx)
	if err != nil {
		return -1, fmt.Errorf("%w: unable to get all balance entries", err)
	}

	if len(balances) == 0 {
		return 0, nil
	}

	validCoverage := 0
	for _, b := range balances {
		if b.LastReconciled == nil {
			continue
		}

		if b.LastReconciled.Index >= minimumIndex {
			validCoverage++
		}
	}

	return float64(validCoverage) / float64(len(balances)), nil
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
	if change.Currency == nil {
		return errors.New("invalid currency")
	}

	namespace, key := GetBalanceKey(change.Account, change.Currency)
	// Get existing balance on key
	exists, balance, err := dbTransaction.Get(ctx, key)
	if err != nil {
		return err
	}

	var existingValue string
	switch {
	case exists:
		// This could happen if balances are bootstrapped and should not be
		// overridden.
		var bal balanceEntry
		err := b.db.Compressor().Decode(namespace, balance, &bal)
		if err != nil {
			return err
		}

		existingValue = bal.Amount.Value
	case parentBlock != nil && change.Block.Hash == parentBlock.Hash:
		// Don't attempt to use the helper if we are going to query the same
		// block we are processing (causes the duplicate issue).
		existingValue = "0"
	default:
		// Use helper to fetch existing balance.
		amount, err := b.helper.AccountBalance(ctx, change.Account, change.Currency, parentBlock)
		if err != nil {
			return fmt.Errorf("%w: unable to get previous account balance", err)
		}

		existingValue = amount.Value
	}

	newVal, err := types.AddValues(change.Difference, existingValue)
	if err != nil {
		return err
	}

	bigNewVal, ok := new(big.Int).SetString(newVal, 10)
	if !ok {
		return fmt.Errorf("%s is not an integer", newVal)
	}

	if bigNewVal.Sign() == -1 {
		return fmt.Errorf(
			"%w %s:%+v for %+v at %+v",
			ErrNegativeBalance,
			newVal,
			change.Currency,
			change.Account,
			change.Block,
		)
	}

	serialBal, err := b.db.Compressor().Encode(namespace, &balanceEntry{
		Account: change.Account,
		Amount: &types.Amount{
			Value:    newVal,
			Currency: change.Currency,
		},
		Block: change.Block,
	})
	if err != nil {
		return err
	}

	return dbTransaction.Set(ctx, key, serialBal)
}

// GetBalance returns all the balances of a types.AccountIdentifier
// and the types.BlockIdentifier it was last updated at.
func (b *BalanceStorage) GetBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	headBlock *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	// We use a write-ready transaction here in case we need to
	// inject a non-existent balance into storage.
	dbTx := b.db.NewDatabaseTransaction(ctx, true)
	defer dbTx.Discard(ctx)

	amount, headBlock, err := b.GetBalanceTransactional(ctx, dbTx, account, currency, headBlock)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to get balance", err)
	}

	// We commit any changes made during the balance lookup.
	if err := dbTx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("%w: unable to commit account balance transaction", err)
	}

	return amount, headBlock, nil
}

// GetBalanceTransactional returns all the balances of a types.AccountIdentifier
// and the types.BlockIdentifier it was last updated at in a database transaction.
func (b *BalanceStorage) GetBalanceTransactional(
	ctx context.Context,
	dbTx DatabaseTransaction,
	account *types.AccountIdentifier,
	currency *types.Currency,
	headBlock *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	namespace, key := GetBalanceKey(account, currency)
	exists, bal, err := dbTx.Get(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	// When beginning syncing from an arbitrary height, an account may
	// not yet have a cached balance when requested. If this is the case,
	// we fetch the balance from the node for the given height and persist
	// it. This is particularly useful when monitoring interesting accounts.
	if !exists {
		amount, err := b.helper.AccountBalance(ctx, account, currency, headBlock)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to get account balance from helper", err)
		}

		err = b.SetBalance(
			ctx,
			dbTx,
			account,
			amount,
			headBlock,
		)
		if err != nil {
			return nil, nil, fmt.Errorf("%w: unable to set account balance", err)
		}

		return amount, headBlock, nil
	}

	var popBal balanceEntry
	err = b.db.Compressor().Decode(namespace, bal, &popBal)
	if err != nil {
		return nil, nil, err
	}

	return popBal.Amount, popBal.Block, nil
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

func (b *BalanceStorage) getAllBalanceEntries(ctx context.Context) ([]*balanceEntry, error) {
	namespace := balanceNamespace
	rawBalances, err := b.db.Scan(ctx, []byte(namespace))
	if err != nil {
		return nil, fmt.Errorf("%w: database scan failed", err)
	}

	balances := make([]*balanceEntry, len(rawBalances))
	for i, rawBalance := range rawBalances {
		var deserialBal balanceEntry
		err := b.db.Compressor().Decode(namespace, rawBalance.Value, &deserialBal)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: unable to parse balance entry for %s",
				err,
				string(rawBalance.Value),
			)
		}

		balances[i] = &deserialBal
	}

	return balances, nil
}

// GetAllAccountCurrency scans the db for all balances and returns a slice
// of reconciler.AccountCurrency. This is useful for bootstrapping the reconciler
// after restart.
func (b *BalanceStorage) GetAllAccountCurrency(
	ctx context.Context,
) ([]*reconciler.AccountCurrency, error) {
	log.Println("Loading previously seen accounts (this could take a while)...")

	balances, err := b.getAllBalanceEntries(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get all balance entries", err)
	}

	accounts := make([]*reconciler.AccountCurrency, len(balances))
	for i, balance := range balances {
		accounts[i] = &reconciler.AccountCurrency{
			Account:  balance.Account,
			Currency: balance.Amount.Currency,
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
