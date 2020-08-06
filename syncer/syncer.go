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
)

var (
	// defaultSyncSleep is the amount of time to sleep
	// when we are at tip but want to keep syncing.
	defaultSyncSleep = 5 * time.Second
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

type Helper interface {
	NetworkStatus(context.Context, *types.NetworkIdentifier) (*types.NetworkStatusResponse, error)

	Block(context.Context, *types.NetworkIdentifier, *types.PartialBlockIdentifier) (*types.Block, error)
}

// Syncer coordinates blockchain syncing without relying on
// a storage interface. Instead, it calls a provided Handler
// whenever a block is added or removed. This provides the client
// the opportunity to define the logic used to handle each new block.
// In the rosetta-cli, we handle reconciliation, state storage, and
// logging in the handler.
type Syncer struct {
	network          *types.NetworkIdentifier
	helper           Helper
	handler          Handler
	cancel           context.CancelFunc
	blockConcurrency uint64

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
	blockConcurrency uint64,
	pastBlocks []*types.BlockIdentifier,
) *Syncer {
	past := pastBlocks
	if past == nil {
		past = []*types.BlockIdentifier{}
	}

	return &Syncer{
		network:          network,
		helper:           helper,
		handler:          handler,
		blockConcurrency: blockConcurrency,
		cancel:           cancel,
		pastBlocks:       past,
	}
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

func (s *Syncer) checkRemove(
	block *types.Block,
) (bool, *types.BlockIdentifier, error) {
	if len(s.pastBlocks) == 0 {
		return false, nil, nil
	}

	// Ensure processing correct index
	if block.BlockIdentifier.Index != s.nextIndex {
		return false, nil, fmt.Errorf(
			"Got block %d instead of %d",
			block.BlockIdentifier.Index,
			s.nextIndex,
		)
	}

	// Check if block parent is head
	lastBlock := s.pastBlocks[len(s.pastBlocks)-1]
	if types.Hash(block.ParentBlockIdentifier) != types.Hash(lastBlock) {
		if types.Hash(s.genesisBlock) == types.Hash(lastBlock) {
			return false, nil, fmt.Errorf("cannot remove genesis block")
		}

		return true, lastBlock, nil
	}

	return false, lastBlock, nil
}

func (s *Syncer) processBlock(
	ctx context.Context,
	block *types.Block,
) error {
	shouldRemove, lastBlock, err := s.checkRemove(block)
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

// addIndicies appends a range of indicies (from
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
		if uint64(len(blockIndicies)) > s.blockConcurrency {
			time.Sleep(1 * time.Second)
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

// fetchChannelBlocks fetches blocks from a
// channel with retries until there are no
// more blocks in the channel or there is an
// error.
func (s *Syncer) fetchChannelBlocks(
	ctx context.Context,
	network *types.NetworkIdentifier,
	blockIndicies chan int64,
	results chan *types.Block,
) error {
	for b := range blockIndicies {
		block, err := s.helper.Block(
			ctx,
			network,
			&types.PartialBlockIdentifier{
				Index: &b,
			},
		)
		if err != nil {
			return err
		}

		select {
		case results <- block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (s *Syncer) syncRange(
	ctx context.Context,
	endIndex int64,
) error {
	highWaterMark := s.nextIndex
	blockIndicies := make(chan int64)
	results := make(chan *types.Block)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.addBlockIndicies(ctx, blockIndicies, highWaterMark, endIndex)
	})

	for j := uint64(0); j < s.blockConcurrency; j++ {
		g.Go(func() error {
			return s.fetchChannelBlocks(ctx, s.network, blockIndicies, results)
		})
	}

	// Wait for all block fetching goroutines to exit
	// before closing the results channel.
	go func() {
		_ = g.Wait()
		close(results)
	}()

	cache := make(map[int64]*types.Block)
	for b := range results {
		cache[b.BlockIdentifier.Index] = b

		reorgBlock := int64(-1)
		for s.nextIndex <= endIndex {
			block, exists := cache[s.nextIndex]
			if !exists {
				if highWaterMark == s.nextIndex && reorgBlock != s.nextIndex { // wait for more blocks
					break
				}

				if s.nextIndex < highWaterMark || reorgBlock == s.nextIndex { // reorg occuring
					newBlock, err := s.helper.Block(
						ctx,
						s.network,
						&types.PartialBlockIdentifier{
							Index: &s.nextIndex,
						},
					)
					if err != nil {
						return fmt.Errorf("%w: unable to fetch block %d", err, s.nextIndex)
					}

					block = newBlock
				}
			} else {
				// Anytime we re-fetch an index, we
				// will need to make another call to the node
				// as it is likely in a reorg.
				delete(cache, s.nextIndex)
			}

			start := s.nextIndex
			if err := s.processBlock(ctx, block); err != nil {
				return fmt.Errorf("%w: unable to process block", err)
			}
			if s.nextIndex < start && reorgBlock == -1 {
				reorgBlock = start
			}

			if s.nextIndex > highWaterMark {
				highWaterMark = s.nextIndex
			}
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
