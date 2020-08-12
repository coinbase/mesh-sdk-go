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
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"

	"golang.org/x/sync/errgroup"
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
// channel or there is an error.
func (f *Fetcher) fetchChannelTransactions(
	ctx context.Context,
	network *types.NetworkIdentifier,
	block *types.BlockIdentifier,
	txsToFetch chan *types.TransactionIdentifier,
	fetchedTxs chan *types.Transaction,
) error {
	for transactionIdentifier := range txsToFetch {
		tx, _, err := f.rosettaClient.BlockAPI.BlockTransaction(ctx,
			&types.BlockTransactionRequest{
				NetworkIdentifier:     network,
				BlockIdentifier:       block,
				TransactionIdentifier: transactionIdentifier,
			},
		)

		if err != nil {
			return fmt.Errorf("%w: /block/transaction %s", ErrRequestFailed, err.Error())
		}

		select {
		case fetchedTxs <- tx.Transaction:
		case <-ctx.Done():
			return ctx.Err()
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
) ([]*types.Transaction, error) {
	if len(transactionIdentifiers) == 0 {
		return nil, nil
	}

	txsToFetch := make(chan *types.TransactionIdentifier)
	fetchedTxs := make(chan *types.Transaction)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return addTransactionIdentifiers(ctx, txsToFetch, transactionIdentifiers)
	})

	for i := uint64(0); i < f.transactionConcurrency; i++ {
		g.Go(func() error {
			return f.fetchChannelTransactions(ctx, network, block, txsToFetch, fetchedTxs)
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
		return nil, err
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
) (*types.Block, error) {
	blockResponse, _, err := f.rosettaClient.BlockAPI.Block(ctx, &types.BlockRequest{
		NetworkIdentifier: network,
		BlockIdentifier:   blockIdentifier,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: /block %s", ErrRequestFailed, err.Error())
	}

	// Exit early if no need to fetch txs
	if blockResponse.OtherTransactions == nil || len(blockResponse.OtherTransactions) == 0 {
		return blockResponse.Block, nil
	}

	batchFetch, err := f.UnsafeTransactions(
		ctx,
		network,
		blockResponse.Block.BlockIdentifier,
		blockResponse.OtherTransactions,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: /block/transaction %s", ErrRequestFailed, err.Error())
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
) (*types.Block, error) {
	block, err := f.UnsafeBlock(ctx, network, blockIdentifier)
	if err != nil {
		return nil, fmt.Errorf("%w: /block %s", ErrRequestFailed, err.Error())
	}

	// If a block is omitted, it will return a non-error
	// response with block equal to nil.
	if block == nil {
		return nil, nil
	}

	if err := f.Asserter.Block(block); err != nil {
		return nil, fmt.Errorf("%w: /block %s", ErrAssertionFailed, err.Error())
	}

	return block, nil
}

// BlockRetry retrieves a validated Block
// with a specified number of retries and max elapsed time.
func (f *Fetcher) BlockRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	blockIdentifier *types.PartialBlockIdentifier,
) (*types.Block, error) {
	if err := asserter.PartialBlockIdentifier(blockIdentifier); err != nil {
		return nil, err
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
		if errors.Is(err, ErrAssertionFailed) {
			return nil, fmt.Errorf("%w: /block not attempting retry", err)
		}

		if err == nil {
			return block, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var blockFetchErr string
		if blockIdentifier.Index != nil {
			blockFetchErr = fmt.Sprintf("block %d", *blockIdentifier.Index)
		} else {
			blockFetchErr = fmt.Sprintf("block %s", *blockIdentifier.Hash)
		}

		if !tryAgain(blockFetchErr, backoffRetries, err) {
			break
		}
	}

	return nil, fmt.Errorf(
		"%w: unable to fetch block %s",
		ErrExhaustedRetries,
		types.PrettyPrintStruct(blockIdentifier),
	)
}
