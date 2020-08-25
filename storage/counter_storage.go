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

	// counterNamespace is preprended to any counter.
	counterNamespace = "counter"
)

// CounterStorage implements counter-specific storage methods
// on top of a Database and DatabaseTransaction interface.
type CounterStorage struct {
	db Database
}

// NewCounterStorage returns a new CounterStorage.
func NewCounterStorage(
	db Database,
) *CounterStorage {
	return &CounterStorage{
		db: db,
	}
}

func getCounterKey(counter string) []byte {
	return []byte(fmt.Sprintf("%s/%s", counterNamespace, counter))
}

func transactionalGet(
	ctx context.Context,
	counter string,
	txn DatabaseTransaction,
) (*big.Int, error) {
	exists, val, err := txn.Get(ctx, getCounterKey(counter))
	if err != nil {
		return nil, err
	}

	if !exists {
		return big.NewInt(0), nil
	}

	return new(big.Int).SetBytes(val), nil
}

// UpdateTransactional updates the value of a counter by amount and returns the new
// value in a transaction.
func (c *CounterStorage) UpdateTransactional(
	ctx context.Context,
	dbTx DatabaseTransaction,
	counter string,
	amount *big.Int,
) (*big.Int, error) {
	val, err := transactionalGet(ctx, counter, dbTx)
	if err != nil {
		return nil, err
	}

	newVal := new(big.Int).Add(val, amount)

	if err := dbTx.Set(ctx, getCounterKey(counter), newVal.Bytes()); err != nil {
		return nil, err
	}

	return newVal, nil
}

// Update updates the value of a counter by amount and returns the new value.
func (c *CounterStorage) Update(
	ctx context.Context,
	counter string,
	amount *big.Int,
) (*big.Int, error) {
	dbTx := c.db.NewDatabaseTransaction(ctx, true)
	defer dbTx.Discard(ctx)

	newVal, err := c.UpdateTransactional(ctx, dbTx, counter, amount)
	if err != nil {
		return nil, err
	}

	if err := dbTx.Commit(ctx); err != nil {
		return nil, err
	}

	return newVal, nil
}

// Get returns the current value of a counter.
func (c *CounterStorage) Get(ctx context.Context, counter string) (*big.Int, error) {
	transaction := c.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	return transactionalGet(ctx, counter, transaction)
}
