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

package syncer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coinbase/rosetta-sdk-go/types"
	"golang.org/x/sync/errgroup"
)

const (
	// PastBlockSize is the maximum number of previously
	// processed blocks we keep in the syncer to handle
	// reorgs correctly. If there is a reorg greater than
	// PastBlockSize, it will not be handled correctly.
	//
	// TODO: make configurable
	PastBlockSize = 20

	// DefaultConcurrency is the default number of
	// blocks the syncer will try to get concurrently.
	DefaultConcurrency = 8
)

var (
	// defaultSyncSleep is the amount of time to sleep
	// when we are at tip but want to keep syncing.
	defaultSyncSleep = 2 * time.Second

	// defaultFetchSleep is the amount of time to sleep
	// when we are loading more blocks to fetch but we
	// already have a backlog >= to concurrency.
	defaultFetchSleep = 500 * time.Millisecond

	// ErrCannotRemoveGenesisBlock is returned when
	// a Rosetta implementation indicates that the
	// genesis block should be orphaned.
	ErrCannotRemoveGenesisBlock = errors.New("cannot remove genesis block")

	// ErrOutOfOrder is returned when the syncer examines
	// a block that is out of order. This typically
	// means the Helper has a bug.
	ErrOutOfOrder = errors.New("out of order")

	// ErrOrphanHead is returned by the Helper when
	// the current head should be orphaned. In some
	// cases, it may not be possible to populate a block
	// if the head of the canonical chain is not yet synced.
	ErrOrphanHead = errors.New("orphan head")

	// ErrBlockResultNil is returned by the syncer
	// when attempting to process a block and the block
	// result is nil.
	ErrBlockResultNil = errors.New("block result is nil")
)

// Handler is called at various times during the sync cycle
// to handle different events. It is common to write logs or
// perform reconciliation in the sync processor.
type Handler interface {
	BlockAdded(
		ctx context.Context,
		block *types.Block,
	) error

	BlockRemoved(
		ctx context.Context,
		block *types.BlockIdentifier,
	) error
}

// Helper is called at various times during the sync cycle
// to get information about a blockchain network. It is
// common to implement this helper using the Fetcher package.
type Helper interface {
	NetworkStatus(context.Context, *types.NetworkIdentifier) (*types.NetworkStatusResponse, error)

	Block(
		context.Context,
		*types.NetworkIdentifier,
		*types.PartialBlockIdentifier,
	) (*types.Block, error)
}

// Syncer coordinates blockchain syncing without relying on
// a storage interface. Instead, it calls a provided Handler
// whenever a block is added or removed. This provides the client
// the opportunity to define the logic used to handle each new block.
// In the rosetta-cli, we handle reconciliation, state storage, and
// logging in the handler.
type Syncer struct {
	network     *types.NetworkIdentifier
	helper      Helper
	handler     Handler
	cancel      context.CancelFunc
	concurrency uint64

	// Used to keep track of sync state
	genesisBlock *types.BlockIdentifier
	nextIndex    int64

	// To ensure reorgs are handled correctly, the syncer must be able
	// to observe blocks it has previously processed. Without this, the
	// syncer may process an index that is not connected to previously added
	// blocks (ParentBlockIdentifier != lastProcessedBlock.BlockIdentifier).
	//
	// If a blockchain does not have reorgs, it is not necessary to populate
	// the blockCache on creation.
	pastBlocks []*types.BlockIdentifier
}

// New creates a new Syncer. If pastBlocks is left nil, it will
// be set to an empty slice.
func New(
	network *types.NetworkIdentifier,
	helper Helper,
	handler Handler,
	cancel context.CancelFunc,
	options ...Option,
) *Syncer {
	s := &Syncer{
		network:     network,
		helper:      helper,
		handler:     handler,
		concurrency: DefaultConcurrency,
		cancel:      cancel,
		pastBlocks:  []*types.BlockIdentifier{},
	}

	// Override defaults with any provided options
	for _, opt := range options {
		opt(s)
	}

	return s
}

func (s *Syncer) setStart(
	ctx context.Context,
	index int64,
) error {
	networkStatus, err := s.helper.NetworkStatus(
		ctx,
		s.network,
	)
	if err != nil {
		return err
	}

	s.genesisBlock = networkStatus.GenesisBlockIdentifier

	if index != -1 {
		s.nextIndex = index
		return nil
	}

	s.nextIndex = networkStatus.GenesisBlockIdentifier.Index
	return nil
}

// nextSyncableRange returns the next range of indexes to sync
// based on what the last processed block in storage is and
// the contents of the network status response.
func (s *Syncer) nextSyncableRange(
	ctx context.Context,
	endIndex int64,
) (int64, bool, error) {
	if s.nextIndex == -1 {
		return -1, false, errors.New("unable to get current head")
	}

	// Always fetch network status to ensure endIndex is not
	// past tip
	networkStatus, err := s.helper.NetworkStatus(
		ctx,
		s.network,
	)
	if err != nil {
		return -1, false, fmt.Errorf("%w: unable to get network status", err)
	}

	if endIndex == -1 || endIndex > networkStatus.CurrentBlockIdentifier.Index {
		endIndex = networkStatus.CurrentBlockIdentifier.Index
	}

	if s.nextIndex > endIndex {
		return -1, true, nil
	}

	return endIndex, false, nil
}

func (s *Syncer) attemptOrphan(
	lastBlock *types.BlockIdentifier,
) (bool, *types.BlockIdentifier, error) {
	if types.Hash(s.genesisBlock) == types.Hash(lastBlock) {
		return false, nil, ErrCannotRemoveGenesisBlock
	}

	return true, lastBlock, nil
}

func (s *Syncer) checkRemove(
	br *blockResult,
) (bool, *types.BlockIdentifier, error) {
	if len(s.pastBlocks) == 0 {
		return false, nil, nil
	}

	lastBlock := s.pastBlocks[len(s.pastBlocks)-1]
	if br.orphanHead {
		return s.attemptOrphan(lastBlock)
	}

	// Ensure processing correct index
	block := br.block
	if block.BlockIdentifier.Index != s.nextIndex {
		return false, nil, fmt.Errorf(
			"%w: got block %d instead of %d",
			ErrOutOfOrder,
			block.BlockIdentifier.Index,
			s.nextIndex,
		)
	}

	// Check if block parent is head
	if types.Hash(block.ParentBlockIdentifier) != types.Hash(lastBlock) {
		return s.attemptOrphan(lastBlock)
	}

	return false, lastBlock, nil
}

func (s *Syncer) processBlock(
	ctx context.Context,
	br *blockResult,
) error {
	if br == nil {
		return ErrBlockResultNil
	}
	// If the block is omitted, increase
	// index and return.
	if br.block == nil && !br.orphanHead {
		s.nextIndex++
		return nil
	}

	shouldRemove, lastBlock, err := s.checkRemove(br)
	if err != nil {
		return err
	}

	if shouldRemove {
		err = s.handler.BlockRemoved(ctx, lastBlock)
		if err != nil {
			return err
		}
		s.pastBlocks = s.pastBlocks[:len(s.pastBlocks)-1]
		s.nextIndex = lastBlock.Index
		return nil
	}

	block := br.block
	err = s.handler.BlockAdded(ctx, block)
	if err != nil {
		return err
	}

	s.pastBlocks = append(s.pastBlocks, block.BlockIdentifier)
	if len(s.pastBlocks) > PastBlockSize {
		s.pastBlocks = s.pastBlocks[1:]
	}
	s.nextIndex = block.BlockIdentifier.Index + 1
	return nil
}

// addBlockIndicies appends a range of indicies (from
// startIndex to endIndex, inclusive) to the
// blockIndicies channel. When all indicies are added,
// the channel is closed.
func (s *Syncer) addBlockIndicies(
	ctx context.Context,
	blockIndicies chan int64,
	startIndex int64,
	endIndex int64,
) error {
	defer close(blockIndicies)
	i := startIndex
	for i <= endIndex {
		// Don't load if we already have a healthy backlog.
		if uint64(len(blockIndicies)) > s.concurrency {
			time.Sleep(defaultFetchSleep)
			continue
		}

		select {
		case blockIndicies <- i:
			i++
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *Syncer) fetchBlockResult(
	ctx context.Context,
	network *types.NetworkIdentifier,
	index int64,
) (*blockResult, error) {
	block, err := s.helper.Block(
		ctx,
		network,
		&types.PartialBlockIdentifier{
			Index: &index,
		},
	)

	br := &blockResult{index: index}
	switch {
	case errors.Is(err, ErrOrphanHead):
		br.orphanHead = true
	case err != nil:
		return nil, err
	default:
		br.block = block
	}

	return br, nil
}

// fetchChannelBlocks fetches blocks from a
// channel with retries until there are no
// more blocks in the channel or there is an
// error.
func (s *Syncer) fetchChannelBlocks(
	ctx context.Context,
	network *types.NetworkIdentifier,
	blockIndicies chan int64,
	results chan *blockResult,
) error {
	for b := range blockIndicies {
		br, err := s.fetchBlockResult(
			ctx,
			network,
			b,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to fetch block %d", err, b)
		}

		select {
		case results <- br:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// processBlocks is invoked whenever a new block is fetched. It attempts
// to process as many blocks as possible.
func (s *Syncer) processBlocks(
	ctx context.Context,
	cache map[int64]*blockResult,
	endIndex int64,
) error {
	// We need to determine if we are in a reorg
	// so that we can force blocks to be fetched
	// if they don't exist in the cache.
	reorgStart := int64(-1)

	for s.nextIndex <= endIndex {
		br, exists := cache[s.nextIndex]
		if !exists {
			// Wait for more blocks if we aren't
			// in a reorg.
			if reorgStart < s.nextIndex {
				break
			}

			// Fetch the nextIndex if we are
			// in a re-org.
			var err error
			br, err = s.fetchBlockResult(
				ctx,
				s.network,
				s.nextIndex,
			)
			if err != nil {
				return fmt.Errorf("%w: unable to fetch block during re-org", err)
			}
		} else {
			// Anytime we re-fetch an index, we
			// will need to make another call to the node
			// as it is likely in a reorg.
			delete(cache, s.nextIndex)
		}

		lastProcessed := s.nextIndex
		if err := s.processBlock(ctx, br); err != nil {
			return fmt.Errorf("%w: unable to process block", err)
		}

		if s.nextIndex < lastProcessed && reorgStart == -1 {
			reorgStart = lastProcessed
		}
	}

	return nil
}

// blockResult is returned by calls
// to fetch a particular index. We must
// use a separate index field in case
// the block is omitted and we can't
// determine the index of the request.
type blockResult struct {
	index      int64
	block      *types.Block
	orphanHead bool
}

// syncRange fetches and processes a range of blocks
// (from syncer.nextIndex to endIndex, inclusive)
// with syncer.concurrency.
func (s *Syncer) syncRange(
	ctx context.Context,
	endIndex int64,
) error {
	blockIndicies := make(chan int64)
	results := make(chan *blockResult)

	// We create a separate derivative context here instead of
	// replacing the provided ctx because the context returned
	// by errgroup.WithContext is canceled as soon as Wait returns.
	// If this canceled context is passed to a handler or helper,
	// it can have unintended consequences (some functions
	// return immediately if the context is canceled).
	//
	// Source: https://godoc.org/golang.org/x/sync/errgroup
	g, pipelineCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.addBlockIndicies(pipelineCtx, blockIndicies, s.nextIndex, endIndex)
	})

	for j := uint64(0); j < s.concurrency; j++ {
		g.Go(func() error {
			return s.fetchChannelBlocks(pipelineCtx, s.network, blockIndicies, results)
		})
	}

	// Wait for all block fetching goroutines to exit
	// before closing the results channel.
	go func() {
		_ = g.Wait()
		close(results)
	}()

	cache := make(map[int64]*blockResult)
	for b := range results {
		cache[b.index] = b

		if err := s.processBlocks(ctx, cache, endIndex); err != nil {
			return fmt.Errorf("%w: unable to process blocks", err)
		}
	}

	err := g.Wait()
	if err != nil {
		return fmt.Errorf("%w: unable to sync to %d", err, endIndex)
	}

	return nil
}

// Sync cycles endlessly until there is an error
// or the requested range is synced.
func (s *Syncer) Sync(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	defer s.cancel()

	if err := s.setStart(ctx, startIndex); err != nil {
		return fmt.Errorf("%w: unable to set start index", err)
	}

	for {
		rangeEnd, halt, err := s.nextSyncableRange(
			ctx,
			endIndex,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to get next syncable range", err)
		}

		if halt {
			if s.nextIndex > endIndex && endIndex != -1 {
				break
			}

			time.Sleep(defaultSyncSleep)
			continue
		}

		if s.nextIndex != rangeEnd {
			log.Printf("Syncing %d-%d\n", s.nextIndex, rangeEnd)
		} else {
			log.Printf("Syncing %d\n", s.nextIndex)
		}

		err = s.syncRange(ctx, rangeEnd)
		if err != nil {
			return fmt.Errorf("%w: unable to sync to %d", err, rangeEnd)
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}

	if startIndex == -1 {
		startIndex = s.genesisBlock.Index
	}

	log.Printf("Finished syncing %d-%d\n", startIndex, endIndex)
	return nil
}
