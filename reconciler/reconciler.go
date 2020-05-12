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

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
	"golang.org/x/sync/errgroup"
)

const (
	// backlogThreshold is the limit of account lookups
	// that can be enqueued to reconcile before new
	// requests are dropped.
	// TODO: Make configurable
	backlogThreshold = 1000

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

	// activeReconciliation is included in the reconciliation
	// error message if reconciliation failed during active
	// reconciliation.
	activeReconciliation = "ACTIVE"

	// inactiveReconciliation is included in the reconciliation
	// error message if reconciliation failed during inactive
	// reconciliation.
	inactiveReconciliation = "INACTIVE"

	// zeroString is a string of value 0.
	zeroString = "0"

	// inactiveReconciliationSleep is used as the time.Duration
	// to sleep when there are no seen accounts to reconcile.
	inactiveReconciliationSleep = 5 * time.Second

	// inactiveReconciliationRequiredDepth is the minimum
	// number of blocks the reconciler should wait between
	// inactive reconciliations.
	// TODO: make configurable
	inactiveReconciliationRequiredDepth = 500
)

var (
	// ErrHeadBlockBehindLive is returned when the processed
	// head is behind the live head. Sometimes, it is
	// preferrable to sleep and wait to catch up when
	// we are close to the live head (waitToCheckDiff).
	ErrHeadBlockBehindLive = errors.New("head block behind")

	// ErrAccountUpdated is returned when the
	// account was updated at a height later than
	// the live height (when the account balance was fetched).
	ErrAccountUpdated = errors.New("account updated")

	// ErrBlockGone is returned when the processed block
	// head is greater than the live head but the block
	// does not exist in the store. This likely means
	// that the block was orphaned.
	ErrBlockGone = errors.New("block gone")
)

// Helper functions are used by Reconciler to compare
// computed balances from a block with the balance calculated
// by the node. Defining an interface allows the client to determine
// what sort of storage layer they want to use to provide the required
// information.
type Helper interface {
	BlockExists(
		ctx context.Context,
		block *types.BlockIdentifier,
	) (bool, error)

	CurrentBlock(
		ctx context.Context,
	) (*types.BlockIdentifier, error)

	AccountBalance(
		ctx context.Context,
		account *types.AccountIdentifier,
		currency *types.Currency,
		headBlock *types.BlockIdentifier,
	) (*types.Amount, *types.BlockIdentifier, error)
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
		nodeBalance string,
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
}

// Reconciler contains all logic to reconcile balances of
// types.AccountIdentifiers returned in types.Operations
// by a Rosetta Server.
type Reconciler struct {
	network              *types.NetworkIdentifier
	helper               Helper
	handler              Handler
	fetcher              *fetcher.Fetcher
	accountConcurrency   uint64
	lookupBalanceByBlock bool
	interestingAccounts  []*AccountCurrency
	changeQueue          chan *parser.BalanceChange

	// highWaterMark is used to skip requests when
	// we are very far behind the live head.
	highWaterMark int64

	// seenAccts are stored for inactive account
	// reconciliation.
	seenAccts     []*AccountCurrency
	inactiveQueue []*parser.BalanceChange

	// inactiveQueueMutex needed because we can't peek at the tip
	// of a channel to determine when it is ready to look at.
	inactiveQueueMutex sync.Mutex
}

// NewReconciler creates a new Reconciler.
func NewReconciler(
	network *types.NetworkIdentifier,
	helper Helper,
	handler Handler,
	fetcher *fetcher.Fetcher,
	accountConcurrency uint64,
	lookupBalanceByBlock bool,
	interestingAccounts []*AccountCurrency,
	// TODO: load seenAccts and inactiveQueue from prior run (if exists)
) *Reconciler {
	r := &Reconciler{
		network:              network,
		helper:               helper,
		handler:              handler,
		fetcher:              fetcher,
		accountConcurrency:   accountConcurrency,
		lookupBalanceByBlock: lookupBalanceByBlock,
		interestingAccounts:  interestingAccounts,
		highWaterMark:        -1,
		seenAccts:            make([]*AccountCurrency, 0),
		inactiveQueue:        make([]*parser.BalanceChange, 0),
	}

	if lookupBalanceByBlock {
		// When lookupBalanceByBlock is enabled, we check
		// balance changes synchronously.
		r.changeQueue = make(chan *parser.BalanceChange)
	} else {
		// When lookupBalanceByBlock is disabled, we must check
		// balance changes asynchronously. Using a buffered
		// channel allows us to add balance changes without blocking.
		r.changeQueue = make(chan *parser.BalanceChange, backlogThreshold)
	}

	return r
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
			if types.Hash(account) == types.Hash(change) {
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

	if !r.lookupBalanceByBlock {
		// All changes will have the same block. Return
		// if we are too far behind to start reconciling.
		if block.Index < r.highWaterMark {
			return nil
		}

		for _, change := range balanceChanges {
			select {
			case r.changeQueue <- change:
			default:
				log.Println("skipping active enqueue because backlog")
			}
		}
	} else {
		// Block until all checked for a block or context is Done
		for _, change := range balanceChanges {
			select {
			case r.changeQueue <- change:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

// CompareBalance checks to see if the computed balance of an account
// is equal to the live balance of an account. This function ensures
// balance is checked correctly in the case of orphaned blocks.
func (r *Reconciler) CompareBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	amount string,
	liveBlock *types.BlockIdentifier,
) (string, string, int64, error) {
	// Head block should be set before we CompareBalance
	head, err := r.helper.CurrentBlock(ctx)
	if err != nil {
		return zeroString, "", 0, fmt.Errorf(
			"%w: unable to get current block for reconciliation",
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
	exists, err := r.helper.BlockExists(ctx, liveBlock)
	if err != nil {
		return zeroString, "", 0, fmt.Errorf(
			"%w: unable to check if block exists: %+v",
			err,
			liveBlock,
		)
	}
	if !exists {
		return zeroString, "", head.Index, fmt.Errorf(
			"%w %+v",
			ErrBlockGone,
			liveBlock,
		)
	}

	// Check if live block < computed head
	cachedBalance, balanceBlock, err := r.helper.AccountBalance(
		ctx,
		account,
		currency,
		head,
	)
	if err != nil {
		return zeroString, "", head.Index, fmt.Errorf(
			"%w: unable to get cached balance for %+v:%+v",
			err,
			account,
			currency,
		)
	}

	if liveBlock.Index < balanceBlock.Index {
		return zeroString, "", head.Index, fmt.Errorf(
			"%w %+v updated at %d",
			ErrAccountUpdated,
			account,
			balanceBlock.Index,
		)
	}

	difference, err := types.SubtractValues(cachedBalance.Value, amount)
	if err != nil {
		return "", "", -1, err
	}

	return difference, cachedBalance.Value, head.Index, nil
}

// bestBalance returns the balance for an account
// at either the current block (if lookupBalanceByBlock is
// disabled) or at some historical block.
func (r *Reconciler) bestBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.PartialBlockIdentifier,
) (*types.BlockIdentifier, string, error) {
	if !r.lookupBalanceByBlock {
		// Use the current balance to reconcile balances when lookupBalanceByBlock
		// is disabled. This could be the case when a rosetta server does not
		// support historical balance lookups.
		block = nil
	}
	return GetCurrencyBalance(
		ctx,
		r.fetcher,
		r.network,
		account,
		currency,
		block,
	)
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
	accountCurrency := &AccountCurrency{
		Account:  account,
		Currency: currency,
	}
	for ctx.Err() == nil {
		// If don't have previous balance because stateless, check diff on block
		// instead of comparing entire computed balance
		difference, cachedBalance, headIndex, err := r.CompareBalance(
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
				log.Printf(
					"Skipping reconciliation for %s: %d blocks behind\n",
					simpleAccountCurrency(accountCurrency),
					diff,
				)

				// Set a highWaterMark to not accept any new
				// reconciliation requests unless they happened
				// after this new highWaterMark.
				r.highWaterMark = liveBlock.Index
				break
			}

			if errors.Is(err, ErrBlockGone) {
				// Either the block has not been processed in a re-org yet
				// or the block was orphaned
				break
			}

			if errors.Is(err, ErrAccountUpdated) {
				// If account was updated, it must be
				// enqueued again
				break
			}

			return err
		}

		reconciliationType := activeReconciliation
		if inactive {
			reconciliationType = inactiveReconciliation
		}

		if difference != zeroString {
			err := r.handler.ReconciliationFailed(
				ctx,
				reconciliationType,
				accountCurrency.Account,
				accountCurrency.Currency,
				cachedBalance,
				liveAmount,
				liveBlock,
			)
			if err != nil {
				return err
			}

			return nil
		}

		r.inactiveAccountQueue(inactive, accountCurrency, liveBlock)
		return r.handler.ReconciliationSucceeded(
			ctx,
			reconciliationType,
			accountCurrency.Account,
			accountCurrency.Currency,
			liveAmount,
			liveBlock,
		)
	}

	return nil
}

func (r *Reconciler) inactiveAccountQueue(
	inactive bool,
	accountCurrency *AccountCurrency,
	liveBlock *types.BlockIdentifier,
) {
	// Only enqueue the first time we see an account on an active reconciliation.
	shouldEnqueueInactive := false
	if !inactive && !ContainsAccountCurrency(r.seenAccts, accountCurrency) {
		r.seenAccts = append(r.seenAccts, accountCurrency)
		shouldEnqueueInactive = true
	}

	if inactive || shouldEnqueueInactive {
		r.inactiveQueueMutex.Lock()
		r.inactiveQueue = append(r.inactiveQueue, &parser.BalanceChange{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    liveBlock,
		})
		r.inactiveQueueMutex.Unlock()
	}
}

// simpleAccountCurrency returns a string that is a simple
// representation of an AccountCurrency struct.
func simpleAccountCurrency(
	accountCurrency *AccountCurrency,
) string {
	acctString := accountCurrency.Account.Address
	if accountCurrency.Account.SubAccount != nil {
		acctString += accountCurrency.Account.SubAccount.Address
	}

	acctString += accountCurrency.Currency.Symbol

	return acctString
}

// reconcileActiveAccounts selects an account
// from the Reconciler account queue and
// reconciles the balance. This is useful
// for detecting if balance changes in operations
// were correct.
func (r *Reconciler) reconcileActiveAccounts(
	ctx context.Context,
) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case balanceChange := <-r.changeQueue:
			if balanceChange.Block.Index < r.highWaterMark {
				continue
			}

			block, value, err := r.bestBalance(
				ctx,
				balanceChange.Account,
				balanceChange.Currency,
				types.ConstructPartialBlockIdentifier(balanceChange.Block),
			)
			if err != nil {
				return err
			}

			err = r.accountReconciliation(
				ctx,
				balanceChange.Account,
				balanceChange.Currency,
				value,
				block,
				false,
			)
			if err != nil {
				return err
			}
		}
	}
}

// reconcileInactiveAccounts selects a random account
// from all previously seen accounts and reconciles
// the balance. This is useful for detecting balance
// changes that were not returned in operations.
func (r *Reconciler) reconcileInactiveAccounts(
	ctx context.Context,
) error {
	for ctx.Err() == nil {
		head, err := r.helper.CurrentBlock(ctx)
		// When first start syncing, this loop may run before the genesis block is synced.
		// If this is the case, we should sleep and try again later instead of exiting.
		if err != nil {
			time.Sleep(inactiveReconciliationSleep)
			log.Printf(
				"%s: waiting to start inactive reconciliation until current block set\n",
				err.Error(),
			)
			continue
		}

		r.inactiveQueueMutex.Lock()
		if len(r.inactiveQueue) > 0 &&
			r.inactiveQueue[0].Block.Index+inactiveReconciliationRequiredDepth < head.Index {
			randAcct := r.inactiveQueue[0]
			r.inactiveQueue = r.inactiveQueue[1:]
			r.inactiveQueueMutex.Unlock()

			block, amount, err := r.bestBalance(
				ctx,
				randAcct.Account,
				randAcct.Currency,
				types.ConstructPartialBlockIdentifier(head),
			)
			if err != nil {
				return err
			}

			err = r.accountReconciliation(
				ctx,
				randAcct.Account,
				randAcct.Currency,
				amount,
				block,
				true,
			)
			if err != nil {
				return err
			}
		} else {
			r.inactiveQueueMutex.Unlock()
			time.Sleep(inactiveReconciliationSleep)
		}
	}

	return nil
}

// Reconcile starts the active and inactive Reconciler goroutines.
// If any goroutine errors, the function will return an error.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	for j := uint64(0); j < r.accountConcurrency/2; j++ {
		g.Go(func() error {
			return r.reconcileActiveAccounts(ctx)
		})

		g.Go(func() error {
			return r.reconcileInactiveAccounts(ctx)
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

// ExtractAmount returns the types.Amount from a slice of types.Balance
// pertaining to an AccountAndCurrency.
func ExtractAmount(
	balances []*types.Amount,
	currency *types.Currency,
) (*types.Amount, error) {
	for _, b := range balances {
		if types.Hash(b.Currency) != types.Hash(currency) {
			continue
		}

		return b, nil
	}

	return nil, fmt.Errorf("could not extract amount for %+v", currency)
}

// AccountCurrency is a simple struct combining
// a *types.Account and *types.Currency. This can
// be useful for looking up balances.
type AccountCurrency struct {
	Account  *types.AccountIdentifier `json:"account_identifier,omitempty"`
	Currency *types.Currency          `json:"currency,omitempty"`
}

// ContainsAccountCurrency returns a boolean indicating if a
// AccountCurrency slice already contains an Account and Currency combination.
func ContainsAccountCurrency(
	arr []*AccountCurrency,
	change *AccountCurrency,
) bool {
	for _, a := range arr {
		if types.Hash(a) == types.Hash(change) {
			return true
		}
	}

	return false
}

// GetCurrencyBalance fetches the balance of a *types.AccountIdentifier
// for a particular *types.Currency.
func GetCurrencyBalance(
	ctx context.Context,
	fetcher *fetcher.Fetcher,
	network *types.NetworkIdentifier,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.PartialBlockIdentifier,
) (*types.BlockIdentifier, string, error) {
	liveBlock, liveBalances, _, err := fetcher.AccountBalanceRetry(
		ctx,
		network,
		account,
		block,
	)
	if err != nil {
		return nil, "", err
	}

	liveAmount, err := ExtractAmount(liveBalances, currency)
	if err != nil {
		return nil, "", err
	}

	return liveBlock, liveAmount.Value, nil
}
