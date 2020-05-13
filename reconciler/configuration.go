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

// WithReconcilerConcurrency overrides the default reconciler
// concurrency.
func WithReconcilerConcurrency(concurrency int) Option {
	return func(r *Reconciler) {
		r.reconcilerConcurrency = concurrency
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
				accountCurrency: acct,
			})
			r.seenAccounts = append(r.seenAccounts, acct)
			fmt.Printf("Adding to inactive queue: %s\n", types.PrettyPrintStruct(acct))
		}
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
