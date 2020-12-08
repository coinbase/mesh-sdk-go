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

package statefulsyncer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

var _ syncer.Handler = (*StatefulSyncer)(nil)
var _ syncer.Helper = (*StatefulSyncer)(nil)

const (
	// DefaultPruneSleepTime is how long we sleep between
	// pruning attempts.
	DefaultPruneSleepTime = 10 * time.Minute

	// pruneBuffer is the cushion we apply to pastBlockLimit
	// when pruning.
	pruneBuffer = 2

	// semaphoreWeight is the weight of each semaphore request.
	semaphoreWeight = int64(1)
)

// StatefulSyncer is an abstraction layer over
// the stateless syncer package. This layer
// handles sync restarts and provides
// fully populated blocks during reorgs (not
// provided by stateless syncer).
type StatefulSyncer struct {
	network        *types.NetworkIdentifier
	fetcher        *fetcher.Fetcher
	cancel         context.CancelFunc
	blockStorage   *modules.BlockStorage
	counterStorage *modules.CounterStorage
	logger         Logger
	workers        []modules.BlockWorker

	cacheSize        int
	maxConcurrency   int64
	pastBlockLimit   int
	adjustmentWindow int64
	pruneSleepTime   time.Duration

	// SeenSemaphore limits how many executions of
	// BlockSeen occur concurrently.
	seenSemaphore     *semaphore.Weighted
	seenSemaphoreSize int64
}

// Logger is used by the statefulsyncer to
// log the addition and removal of blocks.
type Logger interface {
	AddBlockStream(context.Context, *types.Block) error
	RemoveBlockStream(context.Context, *types.BlockIdentifier) error
}

// PruneHelper is used by the stateful syncer
// to determine the safe pruneable index. This is
// a helper instead of a static argument because the
// pruneable index is often a function of the state
// of some number of structs.
type PruneHelper interface {
	// PruneableIndex is the largest block
	// index that is considered safe to prune.
	PruneableIndex(ctx context.Context, headIndex int64) (int64, error)
}

// New returns a new *StatefulSyncer.
func New(
	ctx context.Context,
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	blockStorage *modules.BlockStorage,
	counterStorage *modules.CounterStorage,
	logger Logger,
	cancel context.CancelFunc,
	workers []modules.BlockWorker,
	options ...Option,
) *StatefulSyncer {
	s := &StatefulSyncer{
		network:        network,
		fetcher:        fetcher,
		cancel:         cancel,
		blockStorage:   blockStorage,
		counterStorage: counterStorage,
		workers:        workers,
		logger:         logger,

		// Optional args
		cacheSize:         syncer.DefaultCacheSize,
		maxConcurrency:    syncer.DefaultMaxConcurrency,
		pastBlockLimit:    syncer.DefaultPastBlockLimit,
		adjustmentWindow:  syncer.DefaultAdjustmentWindow,
		pruneSleepTime:    DefaultPruneSleepTime,
		seenSemaphoreSize: int64(runtime.NumCPU()),
	}

	// Override defaults with any provided options
	for _, opt := range options {
		opt(s)
	}

	// We set this after options because the caller
	// has the ability to set the max concurrency
	// of seen invocations.
	s.seenSemaphore = semaphore.NewWeighted(s.seenSemaphoreSize)

	return s
}

// Sync starts a new sync run after properly initializing blockStorage.
func (s *StatefulSyncer) Sync(ctx context.Context, startIndex int64, endIndex int64) error {
	s.blockStorage.Initialize(s.workers)

	// Ensure storage is in correct state for starting at index
	if startIndex != -1 { // attempt to remove blocks from storage (without handling)
		if err := s.blockStorage.SetNewStartIndex(ctx, startIndex); err != nil {
			return fmt.Errorf("%w: unable to set new start index", err)
		}
	} else { // attempt to load last processed index
		head, err := s.blockStorage.GetHeadBlockIdentifier(ctx)
		if err == nil {
			startIndex = head.Index + 1
		}
	}

	// Load in previous blocks into syncer cache to handle reorgs.
	// If previously processed blocks exist in storage, they are fetched.
	// Otherwise, none are provided to the cache (the syncer will not attempt
	// a reorg if the cache is empty).
	pastBlocks := s.blockStorage.CreateBlockCache(ctx, s.pastBlockLimit)

	syncer := syncer.New(
		s.network,
		s,
		s,
		s.cancel,
		syncer.WithPastBlocks(pastBlocks),
		syncer.WithCacheSize(s.cacheSize),
		syncer.WithMaxConcurrency(s.maxConcurrency),
		syncer.WithAdjustmentWindow(s.adjustmentWindow),
	)

	return syncer.Sync(ctx, startIndex, endIndex)
}

// Prune will repeatedly attempt to prune BlockStorage until
// the context is canceled or an error is encountered.
//
// PruneHelper is provided as an argument here instead of
// in the initializer because the caller may wish to change
// pruning strategies during syncing.
func (s *StatefulSyncer) Prune(ctx context.Context, helper PruneHelper) error {
	for ctx.Err() == nil {
		// We don't use a timer pattern because s.pruneSleepTime is defined
		// as the time between pruning runs. Using a timer would only guarantee
		// that the difference between starts of each pruning run are s.pruneSleepTime.
		if err := utils.ContextSleep(ctx, s.pruneSleepTime); err != nil {
			return err
		}

		headBlock, err := s.blockStorage.GetHeadBlockIdentifier(ctx)
		if headBlock == nil && errors.Is(err, storageErrs.ErrHeadBlockNotFound) {
			// this will occur when we are waiting for the first block to be synced
			continue
		}
		if err != nil {
			return err
		}

		oldestIndex, err := s.blockStorage.GetOldestBlockIndex(ctx)
		if oldestIndex == -1 && errors.Is(err, storageErrs.ErrOldestIndexMissing) {
			// this will occur when we have yet to store the oldest index
			continue
		}
		if err != nil {
			return err
		}

		pruneableIndex, err := helper.PruneableIndex(ctx, headBlock.Index)
		if err != nil {
			return fmt.Errorf("%w: could not determine pruneable index", err)
		}

		if pruneableIndex < oldestIndex {
			continue
		}

		firstPruned, lastPruned, err := s.blockStorage.Prune(
			ctx,
			pruneableIndex,
			int64(s.pastBlockLimit)*pruneBuffer, // we should be very cautious about pruning
		)
		if err != nil {
			return err
		}

		// firstPruned and lastPruned are -1 if there is nothing to prune
		if firstPruned != -1 && lastPruned != -1 {
			pruneMessage := fmt.Sprintf("pruned blocks %d-%d", firstPruned, lastPruned)
			if firstPruned == lastPruned {
				pruneMessage = fmt.Sprintf("pruned block %d", firstPruned)
			}

			log.Println(pruneMessage)
		}
	}

	return ctx.Err()
}

// BlockSeen is called by the syncer when a block is seen.
func (s *StatefulSyncer) BlockSeen(ctx context.Context, block *types.Block) error {
	if err := s.seenSemaphore.Acquire(ctx, semaphoreWeight); err != nil {
		return err
	}
	defer s.seenSemaphore.Release(semaphoreWeight)

	if err := s.blockStorage.SeeBlock(ctx, block); err != nil {
		return fmt.Errorf(
			"%w: unable to pre-store block %s:%d",
			err,
			block.BlockIdentifier.Hash,
			block.BlockIdentifier.Index,
		)
	}

	return nil
}

// BlockAdded is called by the syncer when a block is added.
func (s *StatefulSyncer) BlockAdded(ctx context.Context, block *types.Block) error {
	err := s.blockStorage.AddBlock(ctx, block)
	if err != nil {
		return fmt.Errorf(
			"%w: unable to add block to storage %s:%d",
			err,
			block.BlockIdentifier.Hash,
			block.BlockIdentifier.Index,
		)
	}

	_ = s.logger.AddBlockStream(ctx, block)
	return nil
}

// BlockRemoved is called by the syncer when a block is removed.
func (s *StatefulSyncer) BlockRemoved(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
) error {
	err := s.blockStorage.RemoveBlock(ctx, blockIdentifier)
	if err != nil {
		return fmt.Errorf(
			"%w: unable to remove block from storage %s:%d",
			err,
			blockIdentifier.Hash,
			blockIdentifier.Index,
		)
	}

	_ = s.logger.RemoveBlockStream(ctx, blockIdentifier)
	return nil
}

// NetworkStatus is called by the syncer to get the current
// network status.
func (s *StatefulSyncer) NetworkStatus(
	ctx context.Context,
	network *types.NetworkIdentifier,
) (*types.NetworkStatusResponse, error) {
	networkStatus, fetchErr := s.fetcher.NetworkStatusRetry(ctx, network, nil)
	if fetchErr != nil {
		return nil, fetchErr.Err
	}

	return networkStatus, nil
}

// Block is called by the syncer to fetch a block.
func (s *StatefulSyncer) Block(
	ctx context.Context,
	network *types.NetworkIdentifier,
	block *types.PartialBlockIdentifier,
) (*types.Block, error) {
	blockResponse, fetchErr := s.fetcher.BlockRetry(ctx, network, block)
	if fetchErr != nil {
		return nil, fetchErr.Err
	}
	return blockResponse, nil
}
