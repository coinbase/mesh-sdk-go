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

	"github.com/neilotoole/errgroup"

	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

const (
	// BlockCounter is the number of added blocks.
	BlockCounter = "blocks"

	// OrphanCounter is the number of orphaned blocks.
	OrphanCounter = "orphans"

	// TransactionCounter is the number of processed transactions.
	TransactionCounter = "transactions"

	// OperationCounter is the number of processed operations.
	OperationCounter = "operations"

	// AddressesCreatedCounter is the number of created addresses.
	AddressesCreatedCounter = "addresses_created"

	// TransactionsCreatedCounter is the number of created transactions.
	TransactionsCreatedCounter = "transactions_created"

	// TransactionsConfirmedCounter is the number of confirmed transactions.
	TransactionsConfirmedCounter = "transactions_confirmed"

	// StaleBroadcastsCounter is the number of transaction broadcasts that
	// never appeared on-chain.
	StaleBroadcastsCounter = "stale_broadcasts"

	// FailedBroadcastsCounter is the number of transaction broadcasts that
	// never made it on-chain after retries.
	FailedBroadcastsCounter = "failed_broadcasts"

	// ActiveReconciliationCounter is the number of active
	// reconciliations performed.
	ActiveReconciliationCounter = "active_reconciliations"

	// InactiveReconciliationCounter is the number of inactive
	// reconciliations performed.
	InactiveReconciliationCounter = "inactive_reconciliations"

	// ExemptReconciliationCounter is the number of reconciliation
	// failures that were exempt.
	ExemptReconciliationCounter = "exempt_reconciliations"

	// FailedReconciliationCounter is the number of reconciliation
	// failures that were not exempt.
	FailedReconciliationCounter = "failed_reconciliations"

	// SkippedReconciliationsCounter is the number of reconciliation
	// attempts that were skipped. This typically occurs because an
	// account balance has been updated since being marked for reconciliation
	// or the block where an account was updated has been orphaned.
	SkippedReconciliationsCounter = "skipped_reconciliations"

	// SeenAccounts is the total number of accounts seen.
	SeenAccounts = "seen_accounts"

	// ReconciledAccounts is the total number of accounts seen.
	ReconciledAccounts = "reconciled_accounts"

	// counterNamespace is preprended to any counter.
	counterNamespace = "counter"
)

var _ BlockWorker = (*CounterStorage)(nil)

// CounterStorage implements counter-specific storage methods
// on top of a database.Database and database.Transaction interface.
type CounterStorage struct {
	db database.Database

	m *utils.MutexMap
}

// NewCounterStorage returns a new CounterStorage.
func NewCounterStorage(
	db database.Database,
) *CounterStorage {
	return &CounterStorage{
		db: db,
		m:  utils.NewMutexMap(utils.DefaultShards),
	}
}

func getCounterKey(counter string) []byte {
	return []byte(fmt.Sprintf("%s/%s", counterNamespace, counter))
}

// BigIntGet attempts to fetch a *big.Int
// from a given key in a database.Transaction.
func BigIntGet(
	ctx context.Context,
	key []byte,
	txn database.Transaction,
) (bool, *big.Int, error) {
	exists, val, err := txn.Get(ctx, key)
	if err != nil {
		return false, nil, fmt.Errorf("unable to get the value for key %s: %w", string(key), err)
	}

	if !exists {
		return false, big.NewInt(0), nil
	}

	return true, new(big.Int).SetBytes(val), nil
}

// UpdateTransactional updates the value of a counter by amount and returns the new
// value in a transaction.
func (c *CounterStorage) UpdateTransactional(
	ctx context.Context,
	dbTx database.Transaction,
	counter string,
	amount *big.Int,
) (*big.Int, error) {
	// Ensure that same counter is not incremented
	// concurrently. This is necessary because we concurrently
	// store account balances.
	c.m.Lock(counter, false)
	defer c.m.Unlock(counter)

	_, val, err := BigIntGet(ctx, getCounterKey(counter), dbTx)
	if err != nil {
		return nil, fmt.Errorf("unable to get counter: %w", err)
	}

	newVal := new(big.Int).Add(val, amount)

	if err := dbTx.Set(ctx, getCounterKey(counter), newVal.Bytes(), false); err != nil {
		return nil, fmt.Errorf("unable to set counter: %w", err)
	}

	return newVal, nil
}

// Update updates the value of a counter by amount and returns the new value.
func (c *CounterStorage) Update(
	ctx context.Context,
	counter string,
	amount *big.Int,
) (*big.Int, error) {
	dbTx := c.db.WriteTransaction(ctx, counter, false)
	defer dbTx.Discard(ctx)

	newVal, err := c.UpdateTransactional(ctx, dbTx, counter, amount)
	if err != nil {
		return nil, fmt.Errorf("unable to update counter: %w", err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("unable to commit counter update: %w", err)
	}

	return newVal, nil
}

// Get returns the current value of a counter.
func (c *CounterStorage) Get(ctx context.Context, counter string) (*big.Int, error) {
	transaction := c.db.ReadTransaction(ctx)
	defer transaction.Discard(ctx)

	_, value, err := BigIntGet(ctx, getCounterKey(counter), transaction)
	return value, err
}

// GetTransactional returns the current value of a counter in a database.Transaction.
func (c *CounterStorage) GetTransactional(
	ctx context.Context,
	dbTx database.Transaction,
	counter string,
) (*big.Int, error) {
	_, value, err := BigIntGet(ctx, getCounterKey(counter), dbTx)
	return value, err
}

// AddingBlock is called by BlockStorage when adding a block.
func (c *CounterStorage) AddingBlock(
	ctx context.Context,
	g *errgroup.Group,
	block *types.Block,
	transaction database.Transaction,
) (database.CommitWorker, error) {
	_, err := c.UpdateTransactional(
		ctx,
		transaction,
		BlockCounter,
		big.NewInt(1),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to update block counter: %w", err)
	}

	_, err = c.UpdateTransactional(
		ctx,
		transaction,
		TransactionCounter,
		big.NewInt(int64(len(block.Transactions))),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to update transaction counter: %w", err)
	}

	opCount := int64(0)
	for _, txn := range block.Transactions {
		opCount += int64(len(txn.Operations))
	}
	_, err = c.UpdateTransactional(
		ctx,
		transaction,
		OperationCounter,
		big.NewInt(opCount),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to update operation counter: %w", err)
	}

	return nil, nil
}

// RemovingBlock is called by BlockStorage when removing a block.
func (c *CounterStorage) RemovingBlock(
	ctx context.Context,
	g *errgroup.Group,
	block *types.Block,
	transaction database.Transaction,
) (database.CommitWorker, error) {
	_, err := c.UpdateTransactional(ctx, transaction, OrphanCounter, big.NewInt(1))
	return nil, err
}
