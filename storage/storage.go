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
)

// Database is an interface that provides transactional
// access to a KV store.
type Database interface {
	GTransaction(context.Context) DatabaseTransaction
	RTransaction(context.Context) DatabaseTransaction
	WTransaction(ctx context.Context, identifier string, priority bool) DatabaseTransaction

	Close(context.Context) error
	Encoder() *Encoder
}

// DatabaseTransaction is an interface that provides
// access to a KV store within some transaction
// context provided by a Database.
//
// When a DatabaseTransaction is committed or discarded,
// all memory utilized is reclaimed. If you want to persist
// any data retrieved, make sure to make a copy!
type DatabaseTransaction interface {
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
