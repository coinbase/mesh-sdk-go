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
)

func storeUniqueKey(
	ctx context.Context,
	transaction DatabaseTransaction,
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
			ErrDuplicateKey,
			string(key),
		)
	}

	return transaction.Set(ctx, key, value, reclaimValue)
}
