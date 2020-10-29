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
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"golang.org/x/sync/errgroup"
)

const (
	// ActiveReconciliation is included in the reconciliation
	// error message if reconciliation failed during active
	// reconciliation.
	ActiveReconciliation = "ACTIVE"

	// InactiveReconciliation is included in the reconciliation
	// error message if reconciliation failed during inactive
	// reconciliation.
	InactiveReconciliation = "INACTIVE"
)

const (
	// BlockGone is when the block where a reconciliation
	// is supposed to happen is orphaned.
	BlockGone = "BLOCK_GONE"

	// HeadBehind is when the synced tip (where balances
	// were last computed) is behind the *types.BlockIdentifier
	// returned by the call to /account/balance.
	HeadBehind = "HEAD_BEHIND"

	// BacklogFull is when the reconciliation backlog is full.
	BacklogFull = "BACKLOG_FULL"
)

const (
	// defaultBacklogSize is the limit of account lookups
	// that can be enqueued to reconcile before new
	// requests are dropped.
	defaultBacklogSize = 50000

	// waitToCheckDiff is the syncing difference (live-head)
	// to retry instead of exiting. In other words, if the
	// processed head is behind the live head by <
	// waitToCheckDiff we should try again after sleeping.
	// TODO: Make configurable
	waitToCheckDiff = 10

	// waitToCheckDiffSleep is the amount of time to wait
	// to check a balance difference if the syncer is within
	// waitToCheckDiff from the block a balance was queried at.
	waitToCheckDiffSleep = 5 * time.Second

	// zeroString is a string of value 0.
	zeroString = "0"

	// inactiveReconciliationSleep is used as the time.Duration
	// to sleep when there are no seen accounts to reconcile.
	inactiveReconciliationSleep = 1 * time.Second

	// defaultInactiveFrequency is the minimum
	// number of blocks the reconciler should wait between
	// inactive reconciliations for each account.
	defaultInactiveFrequency = 200

	// defaultReconcilerConcurrency is the number of goroutines
	// to start for reconciliation. Half of the goroutines are assigned
	// to inactive reconciliation and half are assigned to active
	// reconciliation.
	defaultReconcilerConcurrency = 8

	// safeBalancePruneDepth is the depth from the last balance
	// change that we consider safe to prune. We are very conservative
	// here to prevent removing balances we may need in a reorg.
	safeBalancePruneDepth = int64(100) // nolint:gomnd
)

// Helper functions are used by Reconciler to compare
// computed balances from a block with the balance calculated
// by the node. Defining an interface allows the client to determine
// what sort of storage layer they want to use to provide the required
// information.
type Helper interface {
	CurrentBlock(
		ctx context.Context,
	) (*types.BlockIdentifier, storage.DatabaseTransaction, error)

	CanonicalBlock(
		ctx context.Context,
		dbTx storage.DatabaseTransaction,
		block *types.BlockIdentifier,
	) (bool, error)

	ComputedBalance(
		ctx context.Context,
		dbTx storage.DatabaseTransaction,
		account *types.AccountIdentifier,
		currency *types.Currency,
		index int64,
	) (*types.Amount, error)

	LiveBalance(
		ctx context.Context,
		account *types.AccountIdentifier,
		currency *types.Currency,
		index int64,
	) (*types.Amount, *types.BlockIdentifier, error)

	// PruneBalances is invoked by the reconciler
	// to indicate that all historical balance states
	// <= to index can be removed.
	PruneBalances(
		ctx context.Context,
		account *types.AccountIdentifier,
		currency *types.Currency,
		index int64,
	) error
}

// Handler is called by Reconciler after a reconciliation
// is performed. When a reconciliation failure is observed,
// it is up to the client to halt syncing or log the result.
type Handler interface {
	ReconciliationFailed(
		ctx context.Context,
		reconciliationType string,
		account *types.AccountIdentifier,
		currency *types.Currency,
		computedBalance string,
		liveBalance string,
		block *types.BlockIdentifier,
	) error

	ReconciliationSucceeded(
		ctx context.Context,
		reconciliationType string,
		account *types.AccountIdentifier,
		currency *types.Currency,
		balance string,
		block *types.BlockIdentifier,
	) error

	ReconciliationExempt(
		ctx context.Context,
		reconciliationType string,
		account *types.AccountIdentifier,
		currency *types.Currency,
		computedBalance string,
		liveBalance string,
		block *types.BlockIdentifier,
		exemption *types.BalanceExemption,
	) error

	ReconciliationSkipped(
		ctx context.Context,
		reconciliationType string,
		account *types.AccountIdentifier,
		currency *types.Currency,
		cause string,
	) error
}

// InactiveEntry is used to track the last
// time that an *types.AccountCurrency was reconciled.
type InactiveEntry struct {
	Entry     *types.AccountCurrency
	LastCheck *types.BlockIdentifier
}

// Reconciler contains all logic to reconcile balances of
// types.AccountIdentifiers returned in types.Operations
// by a Rosetta Server.
type Reconciler struct {
	helper  Helper
	handler Handler
	parser  *parser.Parser

	lookupBalanceByBlock bool
	interestingAccounts  []*types.AccountCurrency
	backlogSize          int
	changeQueue          chan *parser.BalanceChange
	inactiveFrequency    int64
	debugLogging         bool
	balancePruning       bool

	// Reconciler concurrency is separated between
	// active and inactive concurrency to allow for
	// fine-grained tuning of reconciler behavior.
	// When there are many transactions in a block
	// on a resource-constrained machine (laptop),
	// it is useful to allocate more resources to
	// active reconciliation as it is synchronous
	// (when lookupBalanceByBlock is enabled).
	ActiveConcurrency   int
	InactiveConcurrency int

	// highWaterMark is used to skip requests when
	// we are very far behind the live head.
	highWaterMark int64

	// seenAccounts are stored for inactive account
	// reconciliation. seenAccounts must be stored
	// separately from inactiveQueue to prevent duplicate
	// accounts from being added to the inactive reconciliation
	// queue. If this is not done, it is possible a goroutine
	// could be processing an account (not in the queue) when
	// we do a lookup to determine if we should add to the queue.
	seenAccounts  map[string]struct{}
	inactiveQueue []*InactiveEntry

	// inactiveQueueMutex needed because we can't peek at the tip
	// of a channel to determine when it is ready to look at.
	inactiveQueueMutex sync.Mutex

	// LastIndexChecked is the last block index reconciled actively.
	lastIndexMutex   sync.Mutex
	lastIndexChecked int64
}

// New creates a new Reconciler.
func New(
	helper Helper,
	handler Handler,
	p *parser.Parser,
	options ...Option,
) *Reconciler {
	r := &Reconciler{
		helper:              helper,
		handler:             handler,
		parser:              p,
		inactiveFrequency:   defaultInactiveFrequency,
		ActiveConcurrency:   defaultReconcilerConcurrency,
		InactiveConcurrency: defaultReconcilerConcurrency,
		highWaterMark:       -1,
		seenAccounts:        map[string]struct{}{},
		inactiveQueue:       []*InactiveEntry{},
		backlogSize:         defaultBacklogSize,
		lastIndexChecked:    -1,
	}

	for _, opt := range options {
		opt(r)
	}

	// Create change queue
	r.changeQueue = make(chan *parser.BalanceChange, r.backlogSize)

	return r
}

// TODO: replace with structured logging
func (r *Reconciler) debugLog(
	format string,
	v ...interface{},
) {
	if r.debugLogging {
		log.Printf(format+"\n", v...)
	}
}

func (r *Reconciler) wrappedActiveEnqueue(
	ctx context.Context,
	change *parser.BalanceChange,
) {
	select {
	case r.changeQueue <- change:
	default:
		r.debugLog(
			"skipping active enqueue because backlog has %d items",
			r.backlogSize,
		)

		if err := r.handler.ReconciliationSkipped(
			ctx,
			ActiveReconciliation,
			change.Account,
			change.Currency,
			BacklogFull,
		); err != nil {
			log.Printf("%s: reconciliation skipped handling failed\n", err.Error())
		}
	}
}

func (r *Reconciler) wrappedInactiveEnqueue(
	accountCurrency *types.AccountCurrency,
	liveBlock *types.BlockIdentifier,
) {
	if err := r.inactiveAccountQueue(true, accountCurrency, liveBlock); err != nil {
		log.Printf(
			"%s: unable to queue account %s",
			err.Error(),
			types.PrintStruct(accountCurrency),
		)
	}
}

// QueueChanges enqueues a slice of *BalanceChanges
// for reconciliation.
func (r *Reconciler) QueueChanges(
	ctx context.Context,
	block *types.BlockIdentifier,
	balanceChanges []*parser.BalanceChange,
) error {
	// Ensure all interestingAccounts are checked
	for _, account := range r.interestingAccounts {
		skipAccount := false
		// Look through balance changes for account + currency
		for _, change := range balanceChanges {
			if types.Hash(account) == types.Hash(&types.AccountCurrency{
				Account:  change.Account,
				Currency: change.Currency,
			}) {
				skipAccount = true
				break
			}
		}

		// Account changed on this block
		if skipAccount {
			continue
		}

		// If account + currency not found, add with difference 0
		balanceChanges = append(balanceChanges, &parser.BalanceChange{
			Account:    account.Account,
			Currency:   account.Currency,
			Difference: zeroString,
			Block:      block,
		})
	}

	for _, change := range balanceChanges {
		// All changes will have the same block. Continue
		// if we are too far behind to start reconciling.
		if block.Index < r.highWaterMark {
			if err := r.handler.ReconciliationSkipped(
				ctx,
				ActiveReconciliation,
				change.Account,
				change.Currency,
				HeadBehind,
			); err != nil {
				return err
			}
		}

		// Add all seen accounts to inactive reconciler queue.
		//
		// Note: accounts are only added if they have not been seen before.
		err := r.inactiveAccountQueue(false, &types.AccountCurrency{
			Account:  change.Account,
			Currency: change.Currency,
		}, block)
		if err != nil {
			return err
		}

		r.wrappedActiveEnqueue(ctx, change)
	}

	return nil
}

// QueueSize is a helper that returns the total
// number of items currently enqueued for active
// reconciliation.
func (r *Reconciler) QueueSize() int {
	return len(r.changeQueue)
}

// LastIndexReconciled is the last block index
// reconciled. This is used to ensure all the
// enqueued accounts for a particular block have
// been reconciled.
func (r *Reconciler) LastIndexReconciled() int64 {
	return r.lastIndexChecked
}

// CompareBalance checks to see if the computed balance of an account
// is equal to the live balance of an account. This function ensures
// balance is checked correctly in the case of orphaned blocks.
//
// We must transactionally fetch the last synced head,
// whether the liveBlock is canonical, and the computed
// balance of the *types.AccountIdentifier and *types.Currency.
func (r *Reconciler) CompareBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	liveBalance string,
	liveBlock *types.BlockIdentifier,
) (string, string, int64, error) {
	// Head block should be set before we CompareBalance
	head, dbTx, err := r.helper.CurrentBlock(ctx)
	defer dbTx.Discard(ctx)
	if err != nil {
		return zeroString, "", 0, fmt.Errorf(
			"%w: %v",
			ErrGetCurrentBlockFailed,
			err,
		)
	}

	// Check if live block is < head (or wait)
	if liveBlock.Index > head.Index {
		return zeroString, "", head.Index, fmt.Errorf(
			"%w live block %d > head block %d",
			ErrHeadBlockBehindLive,
			liveBlock.Index,
			head.Index,
		)
	}

	// Check if live block is in store (ensure not reorged)
	canonical, err := r.helper.CanonicalBlock(ctx, dbTx, liveBlock)
	if err != nil {
		return zeroString, "", 0, fmt.Errorf(
			"%w: %v: on live block %+v",
			ErrBlockExistsFailed,
			err,
			liveBlock,
		)
	}
	if !canonical {
		return zeroString, "", head.Index, fmt.Errorf(
			"%w %+v",
			ErrBlockGone,
			liveBlock,
		)
	}

	// Get computed balance at live block
	computedBalance, err := r.helper.ComputedBalance(
		ctx,
		dbTx,
		account,
		currency,
		liveBlock.Index,
	)
	if err != nil {
		return zeroString, "", head.Index, fmt.Errorf(
			"%w for %+v:%+v: %v",
			ErrGetComputedBalanceFailed,
			account,
			currency,
			err,
		)
	}

	difference, err := types.SubtractValues(liveBalance, computedBalance.Value)
	if err != nil {
		return "", "", -1, err
	}

	return difference, computedBalance.Value, head.Index, nil
}

// bestLiveBalance returns the balance for an account
// at either the current block (if lookupBalanceByBlock is
// disabled) or at some historical block.
func (r *Reconciler) bestLiveBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, *types.BlockIdentifier, error) {
	// Use the current balance to reconcile balances when lookupBalanceByBlock
	// is disabled. This could be the case when a rosetta server does not
	// support historical balance lookups.
	lookupIndex := int64(-1)

	if r.lookupBalanceByBlock {
		lookupIndex = index
	}

	amount, liveBlock, err := r.helper.LiveBalance(
		ctx,
		account,
		currency,
		lookupIndex,
	)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: unable to get live balance for %s %s at %d",
			err,
			types.PrintStruct(account),
			types.PrintStruct(currency),
			lookupIndex,
		)
	}

	// It is up to the caller to determine if
	// liveBlock is considered canonical.
	return amount, liveBlock, nil
}

// handleBalanceMismatch determines if a mismatch
// is considered exempt and handles it accordingly.
func (r *Reconciler) handleBalanceMismatch(
	ctx context.Context,
	difference string,
	reconciliationType string,
	account *types.AccountIdentifier,
	currency *types.Currency,
	computedBalance string,
	liveBalance string,
	block *types.BlockIdentifier,
) error {
	// Check if the reconciliation was exempt (supports compound exemptions)
	exemption := parser.MatchBalanceExemption(
		r.parser.FindExemptions(account, currency),
		difference,
	)
	if exemption != nil {
		// Return handler result (regardless if error) so that we don't invoke the handler for
		// a failed reconciliation as well.
		return r.handler.ReconciliationExempt(
			ctx,
			reconciliationType,
			account,
			currency,
			computedBalance,
			liveBalance,
			block,
			exemption,
		)
	}

	// If we didn't find a matching exemption,
	// we should consider the reconciliation
	// a failure.
	err := r.handler.ReconciliationFailed(
		ctx,
		reconciliationType,
		account,
		currency,
		computedBalance,
		liveBalance,
		block,
	)
	if err != nil { // error only returned if we should exit on failure
		return err
	}

	return nil
}

// accountReconciliation returns an error if the provided
// AccountAndCurrency's live balance cannot be reconciled
// with the computed balance.
func (r *Reconciler) accountReconciliation(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	liveAmount string,
	liveBlock *types.BlockIdentifier,
	inactive bool,
) error {
	accountCurrency := &types.AccountCurrency{
		Account:  account,
		Currency: currency,
	}
	reconciliationType := ActiveReconciliation
	if inactive {
		reconciliationType = InactiveReconciliation
	}
	for ctx.Err() == nil {
		// If don't have previous balance because stateless, check diff on block
		// instead of comparing entire computed balance
		difference, computedBalance, headIndex, err := r.CompareBalance(
			ctx,
			account,
			currency,
			liveAmount,
			liveBlock,
		)
		if err != nil {
			if errors.Is(err, ErrHeadBlockBehindLive) {
				// This error will only occur when lookupBalanceByBlock
				// is disabled and the syncer is behind the current block of
				// the node. This error should never occur when
				// lookupBalanceByBlock is enabled.
				diff := liveBlock.Index - headIndex
				if diff < waitToCheckDiff {
					time.Sleep(waitToCheckDiffSleep)
					continue
				}

				// Don't wait to check if we are very far behind
				r.debugLog(
					"Skipping reconciliation for %s: %d blocks behind",
					types.PrettyPrintStruct(accountCurrency),
					diff,
				)

				// Set a highWaterMark to not accept any new
				// reconciliation requests unless they happened
				// after this new highWaterMark.
				r.highWaterMark = liveBlock.Index

				return r.handler.ReconciliationSkipped(
					ctx,
					reconciliationType,
					account,
					currency,
					HeadBehind,
				)
			}

			if errors.Is(err, ErrBlockGone) {
				// Either the block has not been processed in a re-org yet
				// or the block was orphaned
				r.debugLog(
					"skipping reconciliation because block %s gone",
					types.PrintStruct(liveBlock),
				)

				return r.handler.ReconciliationSkipped(
					ctx,
					reconciliationType,
					account,
					currency,
					BlockGone,
				)
			}

			return err
		}

		if difference != zeroString {
			return r.handleBalanceMismatch(
				ctx,
				difference,
				reconciliationType,
				accountCurrency.Account,
				accountCurrency.Currency,
				computedBalance,
				liveAmount,
				liveBlock,
			)
		}

		return r.handler.ReconciliationSucceeded(
			ctx,
			reconciliationType,
			accountCurrency.Account,
			accountCurrency.Currency,
			liveAmount,
			liveBlock,
		)
	}

	return ctx.Err()
}

func (r *Reconciler) inactiveAccountQueue(
	inactive bool,
	accountCurrency *types.AccountCurrency,
	liveBlock *types.BlockIdentifier,
) error {
	r.inactiveQueueMutex.Lock()

	// Only enqueue the first time we see an account on an active reconciliation.
	shouldEnqueueInactive := false
	if !inactive && !ContainsAccountCurrency(r.seenAccounts, accountCurrency) {
		r.seenAccounts[types.Hash(accountCurrency)] = struct{}{}
		shouldEnqueueInactive = true
	}

	if inactive || shouldEnqueueInactive {
		r.inactiveQueue = append(r.inactiveQueue, &InactiveEntry{
			Entry:     accountCurrency,
			LastCheck: liveBlock,
		})
	}

	r.inactiveQueueMutex.Unlock()

	return nil
}

func (r *Reconciler) updateLastChecked(index int64) {
	// Update the lastIndexChecked value if the block
	// index is greater. We don't acquire the lock
	// to make this check to improve performance.
	if index > r.lastIndexChecked {
		r.lastIndexMutex.Lock()

		// In the time since making the check and acquiring
		// the lock, the lastIndexChecked could've increased
		// so we check it again.
		if index > r.lastIndexChecked {
			r.lastIndexChecked = index
		}

		r.lastIndexMutex.Unlock()
	}
}

func (r *Reconciler) pruneBalances(ctx context.Context, change *parser.BalanceChange) error {
	if !r.balancePruning {
		return nil
	}

	return r.helper.PruneBalances(
		ctx,
		change.Account,
		change.Currency,
		change.Block.Index-safeBalancePruneDepth,
	)
}

// reconcileActiveAccounts selects an account
// from the Reconciler account queue and
// reconciles the balance. This is useful
// for detecting if balance changes in operations
// were correct.
func (r *Reconciler) reconcileActiveAccounts(ctx context.Context) error { // nolint:gocognit
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case balanceChange := <-r.changeQueue:
			if balanceChange.Block.Index < r.highWaterMark {
				r.debugLog(
					"waiting to continue active reconciliation until reaching high water mark...",
				)

				if err := r.handler.ReconciliationSkipped(
					ctx,
					ActiveReconciliation,
					balanceChange.Account,
					balanceChange.Currency,
					HeadBehind,
				); err != nil {
					return err
				}

				continue
			}

			amount, block, err := r.bestLiveBalance(
				ctx,
				balanceChange.Account,
				balanceChange.Currency,
				balanceChange.Block.Index,
			)
			if err != nil {
				// Ensure we don't leak reconciliations if
				// context is canceled.
				if errors.Is(err, context.Canceled) {
					r.wrappedActiveEnqueue(ctx, balanceChange)
					return err
				}

				return fmt.Errorf("%w: %v", ErrLiveBalanceLookupFailed, err)
			}

			err = r.accountReconciliation(
				ctx,
				balanceChange.Account,
				balanceChange.Currency,
				amount.Value,
				block,
				false,
			)
			if err != nil {
				// Ensure we don't leak reconciliations if
				// context is canceled.
				if errors.Is(err, context.Canceled) {
					r.wrappedActiveEnqueue(ctx, balanceChange)
				}

				return err
			}

			// Attempt to prune historical balances that will not be used
			// anymore.
			if err := r.pruneBalances(ctx, balanceChange); err != nil {
				return err
			}

			r.updateLastChecked(balanceChange.Block.Index)
		}
	}
}

// shouldAttemptInactiveReconciliation returns a boolean indicating whether
// inactive reconciliation should be attempted based on syncing status.
func (r *Reconciler) shouldAttemptInactiveReconciliation(
	ctx context.Context,
) (bool, *types.BlockIdentifier) {
	head, dbTx, err := r.helper.CurrentBlock(ctx)
	defer dbTx.Discard(ctx)
	// When first start syncing, this loop may run before the genesis block is synced.
	// If this is the case, we should sleep and try again later instead of exiting.
	if err != nil {
		r.debugLog("waiting to start intactive reconciliation until a block is synced...")

		return false, nil
	}

	if head.Index < r.highWaterMark {
		r.debugLog(
			"waiting to continue intactive reconciliation until reaching high water mark...",
		)

		return false, nil
	}

	return true, head
}

// reconcileInactiveAccounts selects a random account
// from all previously seen accounts and reconciles
// the balance. This is useful for detecting balance
// changes that were not returned in operations.
func (r *Reconciler) reconcileInactiveAccounts(
	ctx context.Context,
) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		shouldAttempt, head := r.shouldAttemptInactiveReconciliation(ctx)
		if !shouldAttempt {
			time.Sleep(inactiveReconciliationSleep)
			continue
		}

		r.inactiveQueueMutex.Lock()
		queueLen := len(r.inactiveQueue)
		if queueLen == 0 {
			r.inactiveQueueMutex.Unlock()
			r.debugLog(
				"no accounts ready for inactive reconciliation (0 accounts in queue)",
			)
			time.Sleep(inactiveReconciliationSleep)
			continue
		}

		nextAcct := r.inactiveQueue[0]
		nextValidIndex := int64(-1)
		if nextAcct.LastCheck != nil { // block is set to nil when loaded from previous run
			nextValidIndex = nextAcct.LastCheck.Index + r.inactiveFrequency
		}

		if nextValidIndex <= head.Index {
			r.inactiveQueue = r.inactiveQueue[1:]
			r.inactiveQueueMutex.Unlock()

			amount, block, err := r.bestLiveBalance(
				ctx,
				nextAcct.Entry.Account,
				nextAcct.Entry.Currency,
				head.Index,
			)
			switch {
			case err == nil:
				err = r.accountReconciliation(
					ctx,
					nextAcct.Entry.Account,
					nextAcct.Entry.Currency,
					amount.Value,
					block,
					true,
				)
				if err != nil {
					r.wrappedInactiveEnqueue(nextAcct.Entry, block)
					return err
				}
			case !errors.Is(err, ErrBlockGone):
				r.wrappedInactiveEnqueue(nextAcct.Entry, block)
				return fmt.Errorf("%w: %v", ErrLiveBalanceLookupFailed, err)
			}

			// Always re-enqueue accounts after they have been inactively
			// reconciled. If we don't re-enqueue, we will never check
			// these accounts again.
			err = r.inactiveAccountQueue(true, nextAcct.Entry, block)
			if err != nil {
				return err
			}
		} else {
			r.inactiveQueueMutex.Unlock()
			r.debugLog(
				"no accounts ready for inactive reconciliation (%d accounts in queue, will reconcile next account at index %d)",
				queueLen,
				nextValidIndex,
			)
			time.Sleep(inactiveReconciliationSleep)
		}
	}
}

// Reconcile starts the active and inactive Reconciler goroutines.
// If any goroutine errors, the function will return an error.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for j := 0; j < r.ActiveConcurrency; j++ {
		g.Go(func() error {
			return r.reconcileActiveAccounts(ctx)
		})
	}

	for j := 0; j < r.InactiveConcurrency; j++ {
		g.Go(func() error {
			return r.reconcileInactiveAccounts(ctx)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// ContainsAccountCurrency returns a boolean indicating if a
// AccountCurrency set already contains an Account and Currency combination.
func ContainsAccountCurrency(
	m map[string]struct{},
	change *types.AccountCurrency,
) bool {
	_, exists := m[types.Hash(change)]
	return exists
}
