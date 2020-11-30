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

package modules

import (
	"context"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/storage/errors"
)

func storeUniqueKey(
	ctx context.Context,
	transaction database.Transaction,
	key []byte,
	value []byte,
	reclaimValue bool,
) error {
	exists, _, err := transaction.Get(ctx, key)
	if err != nil {
		return err
	}

	if exists {
		return fmt.Errorf(
			"%w: duplicate key %s found",
			errors.ErrDuplicateKey,
			string(key),
		)
	}

	return transaction.Set(ctx, key, value, reclaimValue)
}

// newTestBadgerDatabase creates a new Badger Database at the following directory. This is
// used extensively in module tests.
func newTestBadgerDatabase(ctx context.Context, dir string) (database.Database, error) {
	return database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
}
