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
	"sync"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
)

const (
	// DefaultCacheSize is 0 MB.
	DefaultCacheSize = 0

	// DefaultBfCacheSize is 10 MB.
	DefaultBfCacheSize = 10 << 20

	// DefaultValueLogFileSize is 100 MB.
	DefaultValueLogFileSize = 100 << 20
)

// BadgerStorage is a wrapper around Badger DB
// that implements the Database interface.
type BadgerStorage struct {
	db *badger.DB

	writer sync.Mutex
}

// lowMemoryOptions returns a set of BadgerDB configuration
// options that significantly reduce memory usage.
//
// Inspired by: https://github.com/dgraph-io/badger/issues/1304
func lowMemoryOptions(dir string) badger.Options {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	// Don't load tables into memory.
	opts.TableLoadingMode = options.FileIO
	opts.ValueLogLoadingMode = options.FileIO

	// To allow writes at a faster speed, we create a new memtable as soon as
	// an existing memtable is filled up. This option determines how many
	// memtables should be kept in memory.
	opts.NumMemtables = 1

	// This option will have a significant effect the memory. If the level is kept
	// in-memory, read are faster but the tables will be kept in memory.
	opts.KeepL0InMemory = false

	// LoadBloomsOnOpen=false will improve the db startup speed
	opts.LoadBloomsOnOpen = false

	// Bloom filters will be kept in memory if the following option is not set. Each
	// bloom filter takes up 5 MB of memory. A smaller bf cache would mean that
	// bloom filters will be evicted quickly from the cache and they will be read from
	// the disk (which is slow) and inserted into the cache.
	opts.MaxBfCacheSize = DefaultBfCacheSize

	// Don't cache blocks in memory. All reads should go to disk.
	opts.MaxCacheSize = DefaultCacheSize

	// Don't keep multiple memtables in memory.
	opts.NumLevelZeroTables = 1
	opts.NumLevelZeroTablesStall = 2
	opts.NumMemtables = 1

	// Limit ValueLogFileSize as log files
	// must be read into memory during compaction.
	opts.ValueLogFileSize = DefaultValueLogFileSize

	return opts
}

// performanceOptions returns a set of BadgerDB configuration
// options that don't attempt to reduce memory usage (can
// improve performance).
func performanceOptions(dir string) badger.Options {
	opts := badger.DefaultOptions(dir)
	opts.Logger = nil

	return opts
}

// NewBadgerStorage creates a new BadgerStorage.
func NewBadgerStorage(ctx context.Context, dir string, disableMemoryLimit bool) (Database, error) {
	options := lowMemoryOptions(dir)
	if disableMemoryLimit {
		options = performanceOptions(dir)
	}

	db, err := badger.Open(options)
	if err != nil {
		return nil, fmt.Errorf("%w could not open badger database", err)
	}

	return &BadgerStorage{
		db: db,
	}, nil
}

// Close closes the database to prevent corruption.
// The caller should defer this in main.
func (b *BadgerStorage) Close(ctx context.Context) error {
	if err := b.db.Close(); err != nil {
		return fmt.Errorf("%w unable to close database", err)
	}

	return nil
}

// BadgerTransaction is a wrapper around a Badger
// DB transaction that implements the DatabaseTransaction
// interface.
type BadgerTransaction struct {
	db  *BadgerStorage
	txn *badger.Txn

	holdsLock bool
}

// NewDatabaseTransaction creates a new BadgerTransaction.
// If the transaction will not modify any values, pass
// in false for the write parameter (this allows for
// optimization within the Badger DB).
func (b *BadgerStorage) NewDatabaseTransaction(
	ctx context.Context,
	write bool,
) DatabaseTransaction {
	if write {
		// To avoid database commit conflicts,
		// we need to lock the writer.
		//
		// Because we process blocks serially,
		// this doesn't lead to much lock contention.
		b.writer.Lock()
	}

	return &BadgerTransaction{
		db:        b,
		txn:       b.db.NewTransaction(write),
		holdsLock: write,
	}
}

// Commit attempts to commit and discard the transaction.
func (b *BadgerTransaction) Commit(context.Context) error {
	err := b.txn.Commit()
	b.holdsLock = false
	b.db.writer.Unlock()

	if err != nil {
		return fmt.Errorf("%w: unable to commit transaction", err)
	}

	return nil
}

// Discard discards an open transaction. All transactions
// must be either discarded or committed.
func (b *BadgerTransaction) Discard(context.Context) {
	b.txn.Discard()
	if b.holdsLock {
		b.db.writer.Unlock()
	}
}

// Set changes the value of the key to the value within a transaction.
func (b *BadgerTransaction) Set(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	return b.txn.Set(key, value)
}

// Get accesses the value of the key within a transaction.
func (b *BadgerTransaction) Get(
	ctx context.Context,
	key []byte,
) (bool, []byte, error) {
	var value []byte
	item, err := b.txn.Get(key)
	if err == badger.ErrKeyNotFound {
		return false, nil, nil
	} else if err != nil {
		return false, nil, err
	}

	err = item.Value(func(v []byte) error {
		value = make([]byte, len(v))
		copy(value, v)
		return nil
	})
	if err != nil {
		return false, nil, err
	}

	return true, value, nil
}

// Delete removes the key and its value within the transaction.
func (b *BadgerTransaction) Delete(ctx context.Context, key []byte) error {
	return b.txn.Delete(key)
}

// Scan retrieves all elements with a given prefix in a database
// transaction.
func (b *BadgerTransaction) Scan(
	ctx context.Context,
	prefix []byte,
) ([]*ScanItem, error) {
	values := []*ScanItem{}
	opts := badger.DefaultIteratorOptions
	opts.PrefetchValues = false

	it := b.txn.NewIterator(opts)
	defer it.Close()
	for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		k := item.KeyCopy(nil)
		v, err := item.ValueCopy(nil)
		if err != nil {
			return nil, fmt.Errorf("%w: unable to get value for key %s", err, string(k))
		}

		values = append(values, &ScanItem{
			Key:   k,
			Value: v,
		})
	}

	return values, nil
}

// Set changes the value of the key to the value in its own transaction.
func (b *BadgerStorage) Set(
	ctx context.Context,
	key []byte,
	value []byte,
) error {
	return b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get fetches the value of a key in its own transaction.
func (b *BadgerStorage) Get(
	ctx context.Context,
	key []byte,
) (bool, []byte, error) {
	var value []byte
	err := b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}

		err = item.Value(func(v []byte) error {
			value = make([]byte, len(v))
			copy(value, v)
			return nil
		})
		if err != nil {
			return err
		}

		return nil
	})
	if err == badger.ErrKeyNotFound {
		return false, nil, nil
	} else if err != nil {
		return false, nil, err
	}

	return true, value, nil
}

// Scan fetches all items at a given prefix. This is typically
// used to get all items in a namespace.
func (b *BadgerStorage) Scan(
	ctx context.Context,
	prefix []byte,
) ([]*ScanItem, error) {
	txn := b.NewDatabaseTransaction(ctx, false)
	defer txn.Discard(ctx)

	return txn.Scan(ctx, prefix)
}
