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

	"golang.org/x/sync/errgroup"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

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
		network:          network,
		helper:           helper,
		handler:          handler,
		concurrency:      DefaultConcurrency,
		cacheSize:        DefaultCacheSize,
		maxConcurrency:   DefaultMaxConcurrency,
		sizeMultiplier:   DefaultSizeMultiplier,
		cancel:           cancel,
		pastBlocks:       []*types.BlockIdentifier{},
		pastBlockLimit:   DefaultPastBlockLimit,
		adjustmentWindow: DefaultAdjustmentWindow,
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
		return -1, false, ErrGetCurrentHeadBlockFailed
	}

	// Always fetch network status to ensure endIndex is not
	// past tip
	networkStatus, err := s.helper.NetworkStatus(
		ctx,
		s.network,
	)
	if err != nil {
		return -1, false, fmt.Errorf("%w: %v", ErrGetNetworkStatusFailed, err)
	}

	// Update the syncer's known tip
	s.tip = networkStatus.CurrentBlockIdentifier

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
	if len(s.pastBlocks) > s.pastBlockLimit {
		s.pastBlocks = s.pastBlocks[1:]
	}
	s.nextIndex = block.BlockIdentifier.Index + 1
	return nil
}

// addBlockIndices appends a range of indices (from
// startIndex to endIndex, inclusive) to the
// blockIndices channel. When all indices are added,
// the channel is closed.
func (s *Syncer) addBlockIndices(
	ctx context.Context,
	blockIndices chan int64,
	startIndex int64,
	endIndex int64,
) error {
	defer close(blockIndices)

	i := startIndex
	for i <= endIndex {
		s.concurrencyLock.Lock()
		currentConcurrency := s.concurrency
		s.concurrencyLock.Unlock()

		// Don't load if we already have a healthy backlog.
		if int64(len(blockIndices)) > currentConcurrency {
			time.Sleep(defaultFetchSleep)
			continue
		}

		select {
		case blockIndices <- i:
			i++
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// We populate doneLoading before exiting
	// to make sure we don't create more goroutines
	// when we are done. If we don't do this, we may accidentally
	// try to create a new goroutine after Wait has returned.
	// This will cause a panic.
	s.doneLoadingLock.Lock()
	defer s.doneLoadingLock.Unlock()
	s.doneLoading = true

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

	if err := s.handleSeenBlock(ctx, br); err != nil {
		return nil, err
	}

	return br, nil
}

// safeExit ensures we lower the concurrency in a lock while
// exiting. This prevents us from accidentally increasing concurrency
// when we are shutting down.
func (s *Syncer) safeExit(err error) error {
	s.concurrencyLock.Lock()
	s.concurrency--
	s.concurrencyLock.Unlock()

	return err
}

// fetchBlocks fetches blocks from a
// channel with retries until there are no
// more blocks in the channel or there is an
// error.
func (s *Syncer) fetchBlocks(
	ctx context.Context,
	network *types.NetworkIdentifier,
	blockIndices chan int64,
	results chan *blockResult,
) error {
	for b := range blockIndices {
		br, err := s.fetchBlockResult(
			ctx,
			network,
			b,
		)
		if err != nil {
			return s.safeExit(fmt.Errorf("%w %d: %v", ErrFetchBlockFailed, b, err))
		}

		select {
		case results <- br:
		case <-ctx.Done():
			return s.safeExit(ctx.Err())
		}

		// Exit if concurrency is greater than
		// goal concurrency.
		s.concurrencyLock.Lock()
		shouldExit := false
		if s.concurrency > s.goalConcurrency {
			shouldExit = true
			s.concurrency--
		}
		s.concurrencyLock.Unlock()
		if shouldExit {
			return nil
		}
	}

	return s.safeExit(nil)
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
				return fmt.Errorf("%w: %v", ErrFetchBlockReorgFailed, err)
			}
		} else {
			// Anytime we re-fetch an index, we
			// will need to make another call to the node
			// as it is likely in a reorg.
			delete(cache, s.nextIndex)
		}

		lastProcessed := s.nextIndex
		if err := s.processBlock(ctx, br); err != nil {
			return fmt.Errorf("%w: %v", ErrBlockProcessFailed, err)
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

func (s *Syncer) adjustWorkers() bool {
	// find max block size
	maxSize := 0
	for _, b := range s.recentBlockSizes {
		if b > maxSize {
			maxSize = b
		}
	}
	max := float64(maxSize) * s.sizeMultiplier

	// Check if we have entered shutdown
	// and return false if we have.
	if s.concurrency == 0 {
		return false
	}

	// multiply average block size by concurrency
	estimatedMaxCache := max * float64(s.concurrency)

	// If < cacheSize, increase concurrency by 1 up to MaxConcurrency
	shouldCreate := false
	if estimatedMaxCache+max < float64(s.cacheSize) &&
		s.concurrency < s.maxConcurrency &&
		s.lastAdjustment > s.adjustmentWindow {
		s.goalConcurrency++
		s.concurrency++
		s.lastAdjustment = 0
		shouldCreate = true
		log.Printf(
			"increasing syncer concurrency to %d (projected new cache size: %f MB)\n",
			s.goalConcurrency,
			utils.BtoMb(max*float64(s.goalConcurrency)),
		)
	}

	// If >= cacheSize, decrease concurrency however many necessary to fit max cache size.
	//
	// Note: We always will decrease size, regardless of last adjustment.
	if estimatedMaxCache > float64(s.cacheSize) {
		newGoalConcurrency := int64(float64(s.cacheSize) / max)
		if newGoalConcurrency < MinConcurrency {
			newGoalConcurrency = MinConcurrency
		}

		// Only log if s.goalConcurrency != newGoalConcurrency
		if s.goalConcurrency != newGoalConcurrency {
			s.goalConcurrency = newGoalConcurrency
			s.lastAdjustment = 0
			log.Printf(
				"reducing syncer concurrency to %d (projected new cache size: %f MB)\n",
				s.goalConcurrency,
				utils.BtoMb(max*float64(s.goalConcurrency)),
			)
		}
	}

	// Remove first element in array if
	// we are over our trailing window.
	if len(s.recentBlockSizes) > defaultTrailingWindow {
		s.recentBlockSizes = s.recentBlockSizes[1:]
	}

	return shouldCreate
}

func (s *Syncer) handleSeenBlock(
	ctx context.Context,
	result *blockResult,
) error {
	// If the helper returns ErrOrphanHead
	// for a block fetch, result.block will
	// be nil.
	if result.block == nil {
		return nil
	}

	return s.handler.BlockSeen(ctx, result.block)
}

func (s *Syncer) sequenceBlocks( // nolint:golint
	ctx context.Context,
	pipelineCtx context.Context,
	g *errgroup.Group,
	blockIndices chan int64,
	fetchedBlocks chan *blockResult,
	endIndex int64,
) error {
	cache := make(map[int64]*blockResult)
	for result := range fetchedBlocks {
		cache[result.index] = result

		if err := s.processBlocks(ctx, cache, endIndex); err != nil {
			return fmt.Errorf("%w: %v", ErrBlocksProcessMultipleFailed, err)
		}

		// Determine if concurrency should be adjusted.
		s.recentBlockSizes = append(s.recentBlockSizes, utils.SizeOf(result))
		s.lastAdjustment++

		s.concurrencyLock.Lock()
		shouldCreate := s.adjustWorkers()
		if !shouldCreate {
			s.concurrencyLock.Unlock()
			continue
		}

		// If we have finished loading blocks or the pipelineCtx
		// has an error (like context.Canceled), we should avoid
		// creating more goroutines (as there is a chance that
		// Wait has returned). Attempting to create more goroutines
		// after Wait has returned will cause a panic.
		s.doneLoadingLock.Lock()
		if !s.doneLoading && pipelineCtx.Err() == nil {
			g.Go(func() error {
				return s.fetchBlocks(
					pipelineCtx,
					s.network,
					blockIndices,
					fetchedBlocks,
				)
			})
		} else {
			s.concurrency--
		}
		s.doneLoadingLock.Unlock()

		// Hold concurrencyLock until after we attempt to create another
		// new goroutine in the case we accidentally go to 0 during shutdown.
		s.concurrencyLock.Unlock()
	}

	return nil
}

// syncRange fetches and processes a range of blocks
// (from syncer.nextIndex to endIndex, inclusive)
// with syncer.concurrency.
func (s *Syncer) syncRange(
	ctx context.Context,
	endIndex int64,
) error {
	blockIndices := make(chan int64)
	fetchedBlocks := make(chan *blockResult)

	// Ensure default concurrency is less than max concurrency.
	startingConcurrency := DefaultConcurrency
	if s.maxConcurrency < startingConcurrency {
		startingConcurrency = s.maxConcurrency
	}

	// Don't create more goroutines than there are blocks
	// to sync.
	blocksToSync := endIndex - s.nextIndex + 1
	if blocksToSync < startingConcurrency {
		startingConcurrency = blocksToSync
	}

	// Reset sync variables
	s.recentBlockSizes = []int{}
	s.lastAdjustment = 0
	s.doneLoading = false
	s.concurrency = startingConcurrency
	s.goalConcurrency = s.concurrency

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
		return s.addBlockIndices(pipelineCtx, blockIndices, s.nextIndex, endIndex)
	})

	for j := int64(0); j < s.concurrency; j++ {
		g.Go(func() error {
			return s.fetchBlocks(pipelineCtx, s.network, blockIndices, fetchedBlocks)
		})
	}

	// Wait for all block fetching goroutines to exit
	// before closing the fetchedBlocks channel.
	go func() {
		_ = g.Wait()
		close(fetchedBlocks)
	}()

	if err := s.sequenceBlocks(
		ctx,
		pipelineCtx,
		g,
		blockIndices,
		fetchedBlocks,
		endIndex,
	); err != nil {
		return err
	}

	if err := g.Wait(); err != nil {
		return fmt.Errorf("%w: unable to sync to %d", err, endIndex)
	}

	return nil
}

// Tip returns the last observed tip. The tip is recorded
// at the start of each sync range and should only be thought
// of as a best effort approximation of tip.
//
// This can be very helpful to callers who want to know
// an approximation of tip very frequently (~every second)
// but don't want to implement their own caching logic.
func (s *Syncer) Tip() *types.BlockIdentifier {
	return s.tip
}

// Sync cycles endlessly until there is an error
// or the requested range is synced. When the requested
// range is synced, context is canceled.
func (s *Syncer) Sync(
	ctx context.Context,
	startIndex int64,
	endIndex int64,
) error {
	if err := s.setStart(ctx, startIndex); err != nil {
		return fmt.Errorf("%w: %v", ErrSetStartIndexFailed, err)
	}

	for {
		rangeEnd, halt, err := s.nextSyncableRange(
			ctx,
			endIndex,
		)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrNextSyncableRangeFailed, err)
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

	s.cancel()
	log.Printf("Finished syncing %d-%d\n", startIndex, endIndex)
	return nil
}
