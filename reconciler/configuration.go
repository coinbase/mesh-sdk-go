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

package reconciler

import (
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Option is used to overwrite default values in
// Reconciler construction. Any Option not provided
// falls back to the default value.
type Option func(r *Reconciler)

// WithInactiveConcurrency overrides the default inactive
// concurrency.
func WithInactiveConcurrency(concurrency int) Option {
	return func(r *Reconciler) {
		r.InactiveConcurrency = concurrency
	}
}

// WithActiveConcurrency overrides the default active
// concurrency.
func WithActiveConcurrency(concurrency int) Option {
	return func(r *Reconciler) {
		r.ActiveConcurrency = concurrency
	}
}

// WithInterestingAccounts adds interesting accounts
// to check at each block.
func WithInterestingAccounts(interesting []*types.AccountCurrency) Option {
	return func(r *Reconciler) {
		r.interestingAccounts = interesting
	}
}

// WithSeenAccounts adds accounts to the seenAccounts
// slice and inactiveQueue for inactive reconciliation.
func WithSeenAccounts(seen []*types.AccountCurrency) Option {
	return func(r *Reconciler) {
		for _, acct := range seen {
			// When block is not set, it is assumed that the account should
			// be checked as soon as possible.
			r.inactiveQueue = append(r.inactiveQueue, &InactiveEntry{
				Entry: acct,
			})
			r.seenAccounts[types.Hash(acct)] = struct{}{}
		}

		fmt.Printf(
			"Initialized reconciler with %d previously seen accounts\n",
			len(r.seenAccounts),
		)
	}
}

// WithLookupBalanceByBlock sets lookupBlockByBalance to false.
func WithLookupBalanceByBlock() Option {
	return func(r *Reconciler) {
		r.lookupBalanceByBlock = true
	}
}

// WithInactiveFrequency is how many blocks the reconciler
// should wait between inactive reconciliations on each account.
func WithInactiveFrequency(blocks int64) Option {
	return func(r *Reconciler) {
		r.inactiveFrequency = blocks
	}
}

// WithDebugLogging determines if verbose logs should
// be printed.
func WithDebugLogging() Option {
	return func(r *Reconciler) {
		r.debugLogging = true
	}
}

// WithBalancePruning determines if historical
// balance states should be pruned after they are used.
// This can prevent storage blowup if historical states
// are only ever used once.
func WithBalancePruning() Option {
	return func(r *Reconciler) {
		r.balancePruning = true
	}
}

// WithBacklogSize overrides the defaultBacklogSize
// to some new size. This is often useful for blockchains
// that have high network activity.
func WithBacklogSize(size int) Option {
	return func(r *Reconciler) {
		r.backlogSize = size
	}
}

// add a metaData map to fetcher
func WithMetaData(metaData string) Option {
	return func(r *Reconciler) {
		r.metaData = metaData
	}
}
