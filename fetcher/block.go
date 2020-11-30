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

package fetcher

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// goalRoutineReuse is the minimum number of requests we want to make
	// on each goroutine created to fetch block transactions.
	goalRoutineReuse = 8

	// maxRoutines is the maximum number of goroutines we should create
	// to fetch block transactions.
	maxRoutines = 32

	// minRoutines is the minimum number of goroutines we should create
	// to fetch block transactions.
	minRoutines = 1
)

// addTransactionIdentifiers appends a slice of
// types.TransactionIdentifiers to a channel.
// When all types.TransactionIdentifiers are added,
// the channel is closed.
func addTransactionIdentifiers(
	ctx context.Context,
	txsToFetch chan *types.TransactionIdentifier,
	identifiers []*types.TransactionIdentifier,
) error {
	defer close(txsToFetch)
	for _, txHash := range identifiers {
		select {
		case txsToFetch <- txHash:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// fetchChannelTransactions fetches transactions from a
// channel until there are no more transactions in the
// channel or there is an error. We always retry
// transaction failures.
func (f *Fetcher) fetchChannelTransactions(
	ctx context.Context,
	network *types.NetworkIdentifier,
	block *types.BlockIdentifier,
	txsToFetch chan *types.TransactionIdentifier,
	fetchedTxs chan *types.Transaction,
) *Error {
	// We keep the lock for all transactions we fetch in this goroutine.
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return &Error{
			Err: fmt.Errorf("%w: %s", ErrCouldNotAcquireSemaphore, err.Error()),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	for transactionIdentifier := range txsToFetch {
		backoffRetries := backoffRetries(
			f.retryElapsedTime,
			f.maxRetries,
		)

		var tx *types.BlockTransactionResponse
		for {
			var clientErr *types.Error
			var err error
			tx, clientErr, err = f.rosettaClient.BlockAPI.BlockTransaction(ctx,
				&types.BlockTransactionRequest{
					NetworkIdentifier:     network,
					BlockIdentifier:       block,
					TransactionIdentifier: transactionIdentifier,
				},
			)
			if err == nil {
				break
			}

			if ctx.Err() != nil {
				return &Error{Err: ctx.Err()}
			}

			fetchErr := f.RequestFailedError(clientErr, err, fmt.Sprintf(
				"/block/transaction %s at block %d:%s",
				transactionIdentifier.Hash,
				block.Index,
				block.Hash,
			))

			txFetchErr := fmt.Sprintf("transaction %s", types.PrintStruct(transactionIdentifier))
			if err := tryAgain(txFetchErr, backoffRetries, fetchErr); err != nil {
				return err
			}
		}

		select {
		case fetchedTxs <- tx.Transaction:
		case <-ctx.Done():
			return &Error{
				Err: ctx.Err(),
			}
		}
	}

	return nil
}

// UnsafeTransactions returns the unvalidated response
// from the BlockTransaction method. UnsafeTransactions
// fetches all provided types.TransactionIdentifiers
// concurrently (with the number of threads specified
// by txConcurrency). If any fetch fails, this function
// will return an error.
func (f *Fetcher) UnsafeTransactions(
	ctx context.Context,
	network *types.NetworkIdentifier,
	block *types.BlockIdentifier,
	transactionIdentifiers []*types.TransactionIdentifier,
) ([]*types.Transaction, *Error) {
	if len(transactionIdentifiers) == 0 {
		return nil, nil
	}

	txsToFetch := make(chan *types.TransactionIdentifier)
	fetchedTxs := make(chan *types.Transaction)
	var fetchErr *Error
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return addTransactionIdentifiers(ctx, txsToFetch, transactionIdentifiers)
	})

	// Calculate the concurrency we wish to use to fetch transactions. If we pick
	// a high number, we will still be limited by the fetcher connection semaphore.
	transactionConcurrency := int(float64(len(transactionIdentifiers)) / float64(goalRoutineReuse))
	if transactionConcurrency > maxRoutines {
		transactionConcurrency = maxRoutines
	}
	if transactionConcurrency < minRoutines {
		transactionConcurrency = minRoutines
	}

	for i := 0; i < transactionConcurrency; i++ {
		g.Go(func() error {
			err := f.fetchChannelTransactions(ctx, network, block, txsToFetch, fetchedTxs)
			if err != nil {
				// Only record the first error returned
				// by fetchChannelTransactions.
				if fetchErr == nil {
					fetchErr = err
				}

				return err.Err
			}

			return nil
		})
	}

	go func() {
		_ = g.Wait()
		close(fetchedTxs)
	}()

	txs := make([]*types.Transaction, 0)
	for tx := range fetchedTxs {
		txs = append(txs, tx)
	}

	if err := g.Wait(); err != nil {
		// If there exists a fetchErr, that
		// should be returned over whatever error
		// is returned to the errGroup.
		if fetchErr != nil {
			return nil, fetchErr
		}

		return nil, &Error{Err: err}
	}

	return txs, nil
}

// UnsafeBlock returns the unvalidated response
// from the Block method. This function will
// automatically fetch any transactions that
// were not returned by the call to fetch the
// block.
func (f *Fetcher) UnsafeBlock(
	ctx context.Context,
	network *types.NetworkIdentifier,
	blockIdentifier *types.PartialBlockIdentifier,
) (*types.Block, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, &Error{
			Err: fmt.Errorf("%w: %s", ErrCouldNotAcquireSemaphore, err.Error()),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	blockResponse, clientErr, err := f.rosettaClient.BlockAPI.Block(ctx, &types.BlockRequest{
		NetworkIdentifier: network,
		BlockIdentifier:   blockIdentifier,
	})
	if err != nil {
		return nil, f.RequestFailedError(clientErr, err, fmt.Sprintf(
			"/block %s",
			types.PrintStruct(blockIdentifier),
		))
	}

	// Exit early if no need to fetch txs
	if blockResponse.OtherTransactions == nil || len(blockResponse.OtherTransactions) == 0 {
		return blockResponse.Block, nil
	}

	batchFetch, fetchErr := f.UnsafeTransactions(
		ctx,
		network,
		blockResponse.Block.BlockIdentifier,
		blockResponse.OtherTransactions,
	)
	if fetchErr != nil {
		return nil, fetchErr
	}

	blockResponse.Block.Transactions = append(blockResponse.Block.Transactions, batchFetch...)

	return blockResponse.Block, nil
}

// Block returns the validated response from
// the block method. This function will
// automatically fetch any transactions that
// were not returned by the call to fetch the
// block.
func (f *Fetcher) Block(
	ctx context.Context,
	network *types.NetworkIdentifier,
	blockIdentifier *types.PartialBlockIdentifier,
) (*types.Block, *Error) {
	block, err := f.UnsafeBlock(ctx, network, blockIdentifier)
	if err != nil {
		return nil, err
	}

	// If a block is omitted, it will return a non-error
	// response with block equal to nil.
	if block == nil {
		return nil, nil
	}

	if err := f.Asserter.Block(block); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /block", err),
		}
		return nil, fetcherErr
	}

	return block, nil
}

// BlockRetry retrieves a validated Block
// with a specified number of retries and max elapsed time.
func (f *Fetcher) BlockRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	blockIdentifier *types.PartialBlockIdentifier,
) (*types.Block, *Error) {
	if err := asserter.PartialBlockIdentifier(blockIdentifier); err != nil {
		return nil, &Error{Err: err}
	}

	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		block, err := f.Block(
			ctx,
			network,
			blockIdentifier,
		)
		if err == nil {
			return block, nil
		}

		if ctx.Err() != nil {
			return nil, &Error{Err: ctx.Err()}
		}

		if is, _ := asserter.Err(err.Err); is {
			fetcherErr := &Error{
				Err:       fmt.Errorf("%w: /block not attempting retry", err.Err),
				ClientErr: err.ClientErr,
			}
			return nil, fetcherErr
		}

		blockFetchErr := fmt.Sprintf("block %s", types.PrintStruct(blockIdentifier))
		if err := tryAgain(blockFetchErr, backoffRetries, err); err != nil {
			return nil, err
		}
	}
}
