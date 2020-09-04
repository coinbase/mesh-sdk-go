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
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ syncer.Handler = (*StatefulSyncer)(nil)
var _ syncer.Helper = (*StatefulSyncer)(nil)

// StatefulSyncer is an abstraction layer over
// the stateless syncer package. This layer
// handles sync restarts and provides
// fully populated blocks during reorgs (not
// provided by stateless syncer).
type StatefulSyncer struct {
	network        *types.NetworkIdentifier
	fetcher        *fetcher.Fetcher
	cancel         context.CancelFunc
	blockStorage   *storage.BlockStorage
	counterStorage *storage.CounterStorage
	logger         Logger
	workers        []storage.BlockWorker
	cacheSize      int
}

// Logger is used by the statefulsyncer to
// log the addition and removal of blocks.
type Logger interface {
	AddBlockStream(context.Context, *types.Block) error
	RemoveBlockStream(context.Context, *types.BlockIdentifier) error
}

// New returns a new *StatefulSyncer.
func New(
	ctx context.Context,
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	blockStorage *storage.BlockStorage,
	counterStorage *storage.CounterStorage,
	logger Logger,
	cancel context.CancelFunc,
	workers []storage.BlockWorker,
	cacheSize int,
) *StatefulSyncer {
	return &StatefulSyncer{
		network:        network,
		fetcher:        fetcher,
		cancel:         cancel,
		blockStorage:   blockStorage,
		counterStorage: counterStorage,
		workers:        workers,
		logger:         logger,
		cacheSize:      cacheSize,
	}
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
	pastBlocks := s.blockStorage.CreateBlockCache(ctx)

	syncer := syncer.New(
		s.network,
		s,
		s,
		s.cancel,
		syncer.WithPastBlocks(pastBlocks),
		syncer.WithCacheSize(s.cacheSize),
	)

	return syncer.Sync(ctx, startIndex, endIndex)
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

	if err := s.logger.AddBlockStream(ctx, block); err != nil {
		return nil
	}

	// Update Counters
	_, _ = s.counterStorage.Update(ctx, storage.BlockCounter, big.NewInt(1))
	_, _ = s.counterStorage.Update(
		ctx,
		storage.TransactionCounter,
		big.NewInt(int64(len(block.Transactions))),
	)
	opCount := int64(0)
	for _, txn := range block.Transactions {
		opCount += int64(len(txn.Operations))
	}
	_, _ = s.counterStorage.Update(ctx, storage.OperationCounter, big.NewInt(opCount))

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

	if err := s.logger.RemoveBlockStream(ctx, blockIdentifier); err != nil {
		return nil
	}

	// Update Counters
	_, _ = s.counterStorage.Update(ctx, storage.OrphanCounter, big.NewInt(1))

	return err
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
