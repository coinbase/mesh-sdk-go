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

package database

import (
	"context"

	"github.com/coinbase/rosetta-sdk-go/storage/encoder"
)

// Database is an interface that provides transactional
// access to a KV store.
type Database interface {
	// Transaction acquires an exclusive write lock on the database.
	// This ensures all other calls to Transaction and WriteTransaction
	// will block until the returned DatabaseTransaction is committed or
	// discarded. This is useful for making changes across
	// multiple prefixes but incurs a large performance overhead.
	Transaction(context.Context) Transaction

	// ReadTransaction allows for consistent, read-only access
	// to the database. This does not acquire any lock
	// on the database.
	ReadTransaction(context.Context) Transaction

	// WriteTransaction acquires a granular write lock for a particular
	// identifier. All subsequent calls to WriteTransaction with the same
	// identifier will block until the DatabaseTransaction returned is either
	// committed or discarded.
	WriteTransaction(ctx context.Context, identifier string, priority bool) Transaction

	// Close shuts down the database.
	Close(context.Context) error

	// Encoder returns the *Encoder used to store/read data
	// in the database. This *Encoder often performs some
	// form of compression on data.
	Encoder() *encoder.Encoder

	// GetMetaData returns customized metaData for db's metaData
	GetMetaData() string
}

// Transaction is an interface that provides
// access to a KV store within some transaction
// context provided by a Database.
//
// When a Transaction is committed or discarded,
// all memory utilized is reclaimed. If you want to persist
// any data retrieved, make sure to make a copy!
type Transaction interface {
	Set(context.Context, []byte, []byte, bool) error
	Get(context.Context, []byte) (bool, []byte, error)
	Delete(context.Context, []byte) error

	Scan(
		context.Context,
		[]byte, // prefix restriction
		[]byte, // seek start
		func([]byte, []byte) error,
		bool, // log entries
		bool, // reverse == true means greatest to least
	) (int, error)

	Commit(context.Context) error
	Discard(context.Context)
}

// CommitWorker is returned by a module to be called after
// changes have been committed. It is common to put logging activities
// in here (that shouldn't be printed until the block is committed).
type CommitWorker func(context.Context) error
