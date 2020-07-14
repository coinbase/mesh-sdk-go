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

package reconciler

import (
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/parser"
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
		r.inactiveConcurrency = concurrency
	}
}

// WithActiveConcurrency overrides the default active
// concurrency.
func WithActiveConcurrency(concurrency int) Option {
	return func(r *Reconciler) {
		r.activeConcurrency = concurrency
	}
}

// WithInterestingAccounts adds interesting accounts
// to check at each block.
func WithInterestingAccounts(interesting []*AccountCurrency) Option {
	return func(r *Reconciler) {
		r.interestingAccounts = interesting
	}
}

// WithSeenAccounts adds accounts to the seenAccounts
// slice and inactiveQueue for inactive reconciliation.
func WithSeenAccounts(seen []*AccountCurrency) Option {
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

// WithLookupBalanceByBlock sets lookupBlockByBalance
// and instantiates the correct changeQueue.
func WithLookupBalanceByBlock(lookup bool) Option {
	return func(r *Reconciler) {
		// When lookupBalanceByBlock is disabled, we must check
		// balance changes asynchronously. Using a buffered
		// channel allows us to add balance changes without blocking.
		if !lookup {
			r.changeQueue = make(chan *parser.BalanceChange, backlogThreshold)
		}

		// We don't do anything if lookup == true because the default
		// is already to create a non-buffered channel.
		r.lookupBalanceByBlock = lookup
	}
}

// WithInactiveFrequency is how many blocks the reconciler
// should wait between inactive reconciliations on each account.
func WithInactiveFrequency(blocks int64) Option {
	return func(r *Reconciler) {
		r.inactiveFrequency = blocks
	}
}
