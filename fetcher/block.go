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
	"time"

	rosetta "github.com/coinbase/rosetta-sdk-go/gen"

	"golang.org/x/sync/errgroup"
)

// addTransactionIdentifiers appends a slice of
// rosetta.TransactionIdentifiers to a channel.
// When all rosetta.TransactionIdentifiers are added,
// the channel is closed.
func addTransactionIdentifiers(
	ctx context.Context,
	txsToFetch chan *rosetta.TransactionIdentifier,
	identifiers []*rosetta.TransactionIdentifier,
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
	network *rosetta.NetworkIdentifier,
	block *rosetta.BlockIdentifier,
	txsToFetch chan *rosetta.TransactionIdentifier,
	fetchedTxs chan *rosetta.Transaction,
) error {
	for transactionIdentifier := range txsToFetch {
		tx, _, err := f.rosettaClient.BlockAPI.BlockTransaction(ctx,
			rosetta.BlockTransactionRequest{
				NetworkIdentifier:     network,
				BlockIdentifier:       block,
				TransactionIdentifier: transactionIdentifier,
			},
		)

		if err != nil {
			return err
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
// fetches all provided rosetta.TransactionIdentifiers
// concurrently (with the number of threads specified
// by txConcurrency). If any fetch fails, this function
// will return an error.
func (f *Fetcher) UnsafeTransactions(
	ctx context.Context,
	network *rosetta.NetworkIdentifier,
	block *rosetta.BlockIdentifier,
	transactionIdentifiers []*rosetta.TransactionIdentifier,
) ([]*rosetta.Transaction, error) {
	if len(transactionIdentifiers) == 0 {
		return nil, nil
	}

	txsToFetch := make(chan *rosetta.TransactionIdentifier)
	fetchedTxs := make(chan *rosetta.Transaction)
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

	txs := make([]*rosetta.Transaction, 0)
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
	network *rosetta.NetworkIdentifier,
	blockIdentifier *rosetta.PartialBlockIdentifier,
) (*rosetta.Block, error) {
	blockResponse, _, err := f.rosettaClient.BlockAPI.Block(ctx, rosetta.BlockRequest{
		NetworkIdentifier: network,
		BlockIdentifier:   blockIdentifier,
	})
	if err != nil {
		return nil, err
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
		return nil, err
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
	network *rosetta.NetworkIdentifier,
	blockIdentifier *rosetta.PartialBlockIdentifier,
) (*rosetta.Block, error) {
	if f.Asserter == nil {
		return nil, errors.New("asserter not initialized")
	}

	block, err := f.UnsafeBlock(ctx, network, blockIdentifier)
	if err != nil {
		return nil, err
	}

	if err := f.Asserter.Block(ctx, block); err != nil {
		return nil, err
	}

	return block, nil
}

// BlockRetry retrieves a validated Block
// with a specified number of retries and max elapsed time.
func (f *Fetcher) BlockRetry(
	ctx context.Context,
	network *rosetta.NetworkIdentifier,
	blockIdentifier *rosetta.PartialBlockIdentifier,
	maxElapsedTime time.Duration,
	maxRetries uint64,
) (*rosetta.Block, error) {
	if f.Asserter == nil {
		return nil, errors.New("asserter not initialized")
	}

	backoffRetries := backoffRetries(maxElapsedTime, maxRetries)

	for ctx.Err() == nil {
		block, err := f.Block(
			ctx,
			network,
			blockIdentifier,
		)
		if err == nil {
			return block, nil
		}

		if !tryAgain(fmt.Sprintf("block %d", blockIdentifier.Index), backoffRetries, err) {
			break
		}
	}

	return nil, errors.New("exhausted retries for block")
}

// BlockAndLatency is utilized to track the latency
// of concurrent block fetches.
type BlockAndLatency struct {
	Block   *rosetta.Block
	Latency float64
}

// addIndicies appends a range of indicies (from
// startIndex to endIndex, inclusive) to the
// blockIndicies channel. When all indicies are added,
// the channel is closed.
func addBlockIndicies(
	ctx context.Context,
	blockIndicies chan int64,
	startIndex int64,
	endIndex int64,
) error {
	defer close(blockIndicies)
	for i := startIndex; i <= endIndex; i++ {
		select {
		case blockIndicies <- i:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// fetchChannelBlocks fetches blocks from a
// channel with retries until there are no
// more blocks in the channel or there is an
// error.
func (f *Fetcher) fetchChannelBlocks(
	ctx context.Context,
	network *rosetta.NetworkIdentifier,
	blockIndicies chan int64,
	results chan *BlockAndLatency,
) error {
	for b := range blockIndicies {
		start := time.Now()
		block, err := f.BlockRetry(
			ctx,
			network,
			&rosetta.PartialBlockIdentifier{
				Index: &b,
			},
			DefaultElapsedTime,
			DefaultRetries,
		)
		if err != nil {
			return err
		}

		select {
		case results <- &BlockAndLatency{
			Block:   block,
			Latency: time.Since(start).Seconds(),
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// BlockRange concurrently fetches blocks from startIndex to endIndex,
// inclusive. Blocks returned by this method may not contain a path
// from the endBlock to the startBlock over Block.ParentBlockIdentifers
// if a re-org occurs during the fetch. This should be handled gracefully
// by any callers.
func (f *Fetcher) BlockRange(
	ctx context.Context,
	network *rosetta.NetworkIdentifier,
	startIndex int64,
	endIndex int64,
) (map[int64]*BlockAndLatency, error) {
	blockIndicies := make(chan int64)
	results := make(chan *BlockAndLatency)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return addBlockIndicies(ctx, blockIndicies, startIndex, endIndex)
	})

	for j := uint64(0); j < f.blockConcurrency; j++ {
		g.Go(func() error {
			return f.fetchChannelBlocks(ctx, network, blockIndicies, results)
		})
	}

	// Wait for all block fetching goroutines to exit
	// before closing the results channel.
	go func() {
		_ = g.Wait()
		close(results)
	}()

	m := make(map[int64]*BlockAndLatency)
	for b := range results {
		m[b.Block.BlockIdentifier.Index] = b
	}

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	return m, nil
}
