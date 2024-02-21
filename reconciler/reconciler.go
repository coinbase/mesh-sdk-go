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
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/coinbase/rosetta-sdk-go/parser"
	storageErrors "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

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
		inactiveQueueMutex:  new(utils.PriorityMutex),
		backlogSize:         defaultBacklogSize,
		lastIndexChecked:    -1,
		processQueue:        make(chan *blockRequest, processQueueBacklog),
	}

	for _, opt := range options {
		opt(r)
	}

	// Create change queue
	r.changeQueue = make(chan *parser.BalanceChange, r.backlogSize)

	// Create queueMap
	desiredShardCount := shardBuffer * (r.ActiveConcurrency + r.InactiveConcurrency)
	r.queueMap = utils.NewShardedMap(desiredShardCount)

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
			log.Printf("reconciliation skipped handling failed: %s\n", err.Error())
		}
	}
}

func (r *Reconciler) wrappedInactiveEnqueue(
	accountCurrency *types.AccountCurrency,
	liveBlock *types.BlockIdentifier,
) {
	if err := r.inactiveAccountQueue(true, accountCurrency, liveBlock, false); err != nil {
		log.Printf(
			"unable to queue account %s: %s",
			types.PrintStruct(accountCurrency),
			err.Error(),
		)
	}
}

// addToQueueMap adds a *types.AccountCurrency
// to the prune map at the provided index.
func (r *Reconciler) addToQueueMap(
	m map[string]interface{},
	key string,
	index int64,
) {
	if _, ok := m[key]; !ok {
		m[key] = &utils.BST{}
	}

	bst := m[key].(*utils.BST)
	existing := bst.Get(index)
	if existing == nil {
		bst.Set(index, 1)
	} else {
		existing.Value++
	}
}

// QueueChanges enqueues a slice of *BalanceChanges
// for reconciliation.
func (r *Reconciler) QueueChanges(
	ctx context.Context,
	block *types.BlockIdentifier,
	balanceChanges []*parser.BalanceChange,
) error {
	// If the processQueue fills up, we block
	// until some items are dequeued.
	select {
	case r.processQueue <- &blockRequest{
		Block:   block,
		Changes: balanceChanges,
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// queueWorker processes items from the processQueue.
func (r *Reconciler) queueWorker(ctx context.Context) error {
	for {
		select {
		case req := <-r.processQueue:
			if err := r.queueChanges(ctx, req.Block, req.Changes); err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// queueChanges processes a block for reconciliation.
func (r *Reconciler) queueChanges(
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
		// Add all seen accounts to inactive reconciler queue.
		//
		// Note: accounts are only added if they have not been seen before.
		//
		// We always add accounts to the inactive reconciler queue even if we're
		// below the high water mark. Once we have synced all the blocks the inactive
		// queue will recognize we are at the tip and will begin reconciliation of all
		// accounts.
		acctCurrency := &types.AccountCurrency{
			Account:  change.Account,
			Currency: change.Currency,
		}

		r.inactiveQueueMutex.Lock(true)
		err := r.inactiveAccountQueue(false, acctCurrency, block, true)
		r.inactiveQueueMutex.Unlock()
		if err != nil {
			return fmt.Errorf("failed to enqueue inactive account: %w", err)
		}

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
				return fmt.Errorf("failed to call \"reconciliation skip\" action: %w", err)
			}

			continue
		}

		// Add change to queueMap before enqueuing to ensure
		// there is no possible race.
		key := types.Hash(acctCurrency)
		m := r.queueMap.Lock(key, true)
		r.addToQueueMap(m, key, change.Block.Index)
		r.queueMap.Unlock(key)

		// Add change to active queue
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
	dbTx := r.helper.DatabaseTransaction(ctx)
	defer dbTx.Discard(ctx)

	// Head block should be set before we CompareBalance
	head, err := r.helper.CurrentBlock(ctx, dbTx)
	if err != nil {
		return zeroString, "", 0, fmt.Errorf(
			"failed to get current block: %w",
			err,
		)
	}

	// Check if live block is < head (or wait)
	if liveBlock.Index > head.Index {
		return zeroString, "", head.Index, fmt.Errorf(
			"live block %d > head block %d: %w",
			liveBlock.Index,
			head.Index,
			ErrHeadBlockBehindLive,
		)
	}

	// Check if live block is in store (ensure not reorged)
	canonical, err := r.helper.CanonicalBlock(ctx, dbTx, liveBlock)
	if err != nil {
		return zeroString, "", 0, fmt.Errorf(
			"unable to check if live block %s is in canonical chain: %w",
			types.PrintStruct(liveBlock),
			err,
		)
	}
	if !canonical {
		return zeroString, "", head.Index, fmt.Errorf(
			"live block %s is invalid: %w",
			types.PrintStruct(liveBlock),
			ErrBlockGone,
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
		if errors.Is(err, storageErrors.ErrAccountMissing) {
			return zeroString, "", head.Index, fmt.Errorf(
				"account %s is invalid for currency %s: %w",
				types.PrintStruct(account),
				types.PrintStruct(currency),
				storageErrors.ErrAccountMissing,
			)
		}

		return zeroString, "", head.Index, fmt.Errorf(
			"failed to get computed balance for currency %s of account %s: %w",
			types.PrintStruct(currency),
			types.PrintStruct(account),
			err,
		)
	}

	difference, err := types.SubtractValues(liveBalance, computedBalance.Value)
	if err != nil {
		return "", "", -1, fmt.Errorf(
			"failed to subtract values %s - %s: %w",
			liveBalance,
			computedBalance.Value,
			err,
		)
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
			"unable to get live balance for currency %s of account %s at %d: %w",
			types.PrintStruct(currency),
			types.PrintStruct(account),
			lookupIndex,
			err,
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
		return fmt.Errorf("failed to call \"reconciliation fail\" action: %w", err)
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

			if errors.Is(err, storageErrors.ErrAccountMissing) {
				// When interesting accounts are specified,
				// we try to reconcile balances for each of these
				// accounts at each block height.
				// But, until we encounter a block with an interesting account,
				// balance storage will not have an entry for
				// it, leading to this error. So, we simply skip the reconciliation
				// in this case.
				r.debugLog(
					"skipping reconciliation because account %s is missing.",
					types.PrintStruct(account),
				)

				return r.handler.ReconciliationSkipped(
					ctx,
					reconciliationType,
					account,
					currency,
					AccountMissing,
				)
			}

			return fmt.Errorf("failed to compare computed balance with live balance: %w", err)
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
	hasLock bool,
) error {
	if !hasLock {
		r.inactiveQueueMutex.Lock(false)
		defer r.inactiveQueueMutex.Unlock()
	}

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

func (r *Reconciler) pruneBalances(
	ctx context.Context,
	acctCurrency *types.AccountCurrency,
	index int64,
) error {
	if !r.balancePruning {
		return nil
	}

	return r.helper.PruneBalances(
		ctx,
		acctCurrency.Account,
		acctCurrency.Currency,
		index-safeBalancePruneDepth,
	)
}

// skipAndPrune calls the ReconciliationSkipped
// handler and attempts to prune.
func (r *Reconciler) skipAndPrune(
	ctx context.Context,
	change *parser.BalanceChange,
	skipCause string,
) error {
	if err := r.handler.ReconciliationSkipped(
		ctx,
		ActiveReconciliation,
		change.Account,
		change.Currency,
		skipCause,
	); err != nil {
		return fmt.Errorf("failed to call \"reconciliation skip\" action: %w", err)
	}

	return r.updateQueueMap(
		ctx,
		&types.AccountCurrency{
			Account:  change.Account,
			Currency: change.Currency,
		},
		change.Block.Index,
		pruneActiveReconciliation,
	)
}

// updateQueueMap removes a *parser.BalanceChange
// from the queueMap and attempts to prune the associated
// *types.AccountCurrency's balances, if appropriate.
func (r *Reconciler) updateQueueMap(
	ctx context.Context,
	acctCurrency *types.AccountCurrency,
	index int64,
	prune bool,
) error {
	key := types.Hash(acctCurrency)
	m := r.queueMap.Lock(key, false)
	bst := m[key].(*utils.BST)

	existing := bst.Get(index)
	existing.Value--
	if existing.Value > 0 {
		r.queueMap.Unlock(key)
		return nil
	}

	// Cleanup indexes when we don't need them anymore
	bst.Delete(index)

	// Don't prune if there are items for this AccountCurrency
	// less than this change.
	if !bst.Empty() &&
		index >= bst.Min().Key {
		r.queueMap.Unlock(key)
		return nil
	}

	// Cleanup keys when we don't need them anymore
	if bst.Empty() {
		delete(m, key)
	}

	// Unlock before pruning as this could take some time
	r.queueMap.Unlock(key)

	// Attempt to prune historical balances that will not be used
	// anymore.
	if !prune {
		return nil
	}

	return r.pruneBalances(ctx, acctCurrency, index)
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

				if err := r.skipAndPrune(ctx, balanceChange, HeadBehind); err != nil {
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

				tip, tErr := r.helper.IndexAtTip(ctx, balanceChange.Block.Index)
				switch {
				case tErr == nil && tip:
					if err := r.skipAndPrune(ctx, balanceChange, TipFailure); err != nil {
						return err
					}

					continue
				case tErr != nil:
					fmt.Printf("%v: could not determine if at tip\n", tErr)
				}

				return fmt.Errorf(
					"failed to lookup balance for currency %s of account %s at height %d: %w",
					types.PrintStruct(balanceChange.Currency),
					types.PrintStruct(balanceChange.Account),
					balanceChange.Block.Index,
					err,
				)
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
			if err := r.updateQueueMap(
				ctx,
				&types.AccountCurrency{
					Account:  balanceChange.Account,
					Currency: balanceChange.Currency,
				},
				balanceChange.Block.Index,
				pruneActiveReconciliation,
			); err != nil {
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
	dbTx := r.helper.DatabaseTransaction(ctx)
	defer dbTx.Discard(ctx)

	head, err := r.helper.CurrentBlock(ctx, dbTx)
	// When first start syncing, this loop may run before the genesis block is synced.
	// If this is the case, we should sleep and try again later instead of exiting.
	if err != nil {
		r.debugLog("waiting to start inactive reconciliation until a block is synced...")

		return false, nil
	}

	if head.Index < r.highWaterMark {
		r.debugLog(
			"waiting to continue inactive reconciliation until reaching high water mark...",
		)

		return false, nil
	}

	return true, head
}

// reconcileInactiveAccounts selects a random account
// from all previously seen accounts and reconciles
// the balance. This is useful for detecting balance
// changes that were not returned in operations.
func (r *Reconciler) reconcileInactiveAccounts( // nolint:gocognit
	ctx context.Context,
) error {
	for ctx.Err() == nil {
		r.inactiveQueueMutex.Lock(false)
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
		key := types.Hash(nextAcct.Entry)

		// Lock BST while determining if we should attempt reconciliation
		// to ensure we don't allow any accounts to be pruned at retrieved
		// head index. Although this appears to be a long time to hold
		// this mutex, this lookup takes less than a millisecond.
		m := r.queueMap.Lock(key, false)

		shouldAttempt, head := r.shouldAttemptInactiveReconciliation(ctx)
		if !shouldAttempt {
			r.queueMap.Unlock(key)
			r.inactiveQueueMutex.Unlock()
			time.Sleep(inactiveReconciliationSleep)
			continue
		}

		nextValidIndex := int64(-1)
		if nextAcct.LastCheck != nil { // block is set to nil when loaded from previous run
			nextValidIndex = nextAcct.LastCheck.Index + r.inactiveFrequency
		}

		if nextValidIndex <= head.Index ||
			r.helper.ForceInactiveReconciliation(
				ctx,
				nextAcct.Entry.Account,
				nextAcct.Entry.Currency,
				nextAcct.LastCheck,
			) {
			r.inactiveQueue = r.inactiveQueue[1:]
			r.inactiveQueueMutex.Unlock()

			// Add nextAcct to queueMap before returning
			// queueMapMutex lock.
			r.addToQueueMap(m, key, head.Index)
			r.queueMap.Unlock(key)

			amount, block, err := r.bestLiveBalance(
				ctx,
				nextAcct.Entry.Account,
				nextAcct.Entry.Currency,
				head.Index,
			)
			if err != nil {
				// Ensure we don't leak reconciliations
				r.wrappedInactiveEnqueue(nextAcct.Entry, block)
				if errors.Is(err, context.Canceled) {
					return err
				}

				tip, tErr := r.helper.IndexAtTip(ctx, head.Index)
				switch {
				case tErr == nil && tip:
					if err := r.handler.ReconciliationSkipped(
						ctx,
						InactiveReconciliation,
						nextAcct.Entry.Account,
						nextAcct.Entry.Currency,
						TipFailure,
					); err != nil {
						return fmt.Errorf("failed to call \"reconciliation skip\" action: %w", err)
					}

					if err := r.updateQueueMap(
						ctx,
						nextAcct.Entry,
						head.Index,
						pruneInactiveReconciliation,
					); err != nil {
						return fmt.Errorf("failed to update queue map: %w", err)
					}

					continue
				case tErr != nil:
					fmt.Printf("%v: could not determine if at tip\n", tErr)
				}

				return fmt.Errorf(
					"failed to lookup balance for currency %s of account %s at height %d: %w",
					types.PrintStruct(nextAcct.Entry.Currency),
					types.PrintStruct(nextAcct.Entry.Account),
					head.Index,
					err,
				)
			}

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

			// We always prune relative to the index we inserted
			// into the BST. If we end up performing a reconciliation
			// at an index after head.Index (because historical balances
			// are disabled), we will not prune relative to it.
			if err := r.updateQueueMap(
				ctx,
				nextAcct.Entry,
				head.Index,
				pruneInactiveReconciliation,
			); err != nil {
				return err
			}

			// Always re-enqueue accounts after they have been inactively
			// reconciled. If we don't re-enqueue, we will never check
			// these accounts again.
			err = r.inactiveAccountQueue(true, nextAcct.Entry, block, false)
			if err != nil {
				return err
			}
		} else {
			r.inactiveQueueMutex.Unlock()
			r.queueMap.Unlock(key)
			r.debugLog(
				"no accounts ready for inactive reconciliation (%d accounts in queue, will reconcile next account at index %d)",
				queueLen,
				nextValidIndex,
			)
			time.Sleep(inactiveReconciliationSleep)
		}
	}

	return ctx.Err()
}

// Reconcile starts the active and inactive Reconciler goroutines.
// If any goroutine errors, the function will return an error.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return r.queueWorker(ctx)
	})

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
