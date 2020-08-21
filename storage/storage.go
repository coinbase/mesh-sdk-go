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

// ScanItem is returned by a call to Scan.
type ScanItem struct {
	Key   []byte
	Value []byte
}

// Database is an interface that provides transactional
// access to a KV store.
type Database interface {
	NewDatabaseTransaction(context.Context, bool) DatabaseTransaction
	Close(context.Context) error
	Set(context.Context, []byte, []byte) error
	Get(context.Context, []byte) (bool, []byte, error)
	Scan(ctx context.Context, prefix []byte) ([]*ScanItem, error)
}

// DatabaseTransaction is an interface that provides
// access to a KV store within some transaction
// context provided by a Database.
type DatabaseTransaction interface {
	Set(context.Context, []byte, []byte) error
	Get(context.Context, []byte) (bool, []byte, error)
	Delete(context.Context, []byte) error
	Commit(context.Context) error
	Discard(context.Context)
	Scan(ctx context.Context, prefix []byte) ([]*ScanItem, error)
}
