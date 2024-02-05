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

package syncer

import (
	"context"
	"sync"
	"time"

	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// DefaultPastBlockLimit is the maximum number of previously
	// processed block headers we keep in the syncer to handle
	// reorgs correctly. If there is a reorg greater than
	// DefaultPastBlockLimit, it will not be handled correctly.
	DefaultPastBlockLimit = 100

	// DefaultConcurrency is the default number of
	// blocks the syncer will try to get concurrently.
	DefaultConcurrency = int64(4) // nolint:gomnd

	// DefaultCacheSize is the default size of the preprocess
	// cache for the syncer.
	DefaultCacheSize = 2000 << 20 // 2 GB

	// LargeCacheSize will aim to use 5 GB of memory.
	LargeCacheSize = 5000 << 20 // 5 GB

	// SmallCacheSize will aim to use 500 MB of memory.
	SmallCacheSize = 500 << 20 // 500 MB

	// TinyCacheSize will aim to use 200 MB of memory.
	TinyCacheSize = 200 << 20 // 200 MB

	// DefaultMaxConcurrency is the maximum concurrency we will
	// attempt to sync with.
	DefaultMaxConcurrency = int64(256) // nolint:gomnd

	// MinConcurrency is the minimum concurrency we will
	// attempt to sync with.
	MinConcurrency = int64(1) // nolint:gomnd

	// defaultTrailingWindow is the size of the trailing window
	// of block sizes to keep when adjusting concurrency.
	defaultTrailingWindow = 1000

	// DefaultAdjustmentWindow is how frequently we will
	// consider increasing our concurrency.
	DefaultAdjustmentWindow = 5

	// DefaultSizeMultiplier is used to pad our average size adjustment.
	// This can be used to account for the overhead associated with processing
	// a particular block (i.e. balance adjustments, coins created, etc).
	DefaultSizeMultiplier = float64(10) // nolint:gomnd

	// defaultSyncSleep is the amount of time to sleep
	// when we are at tip but want to keep syncing.
	defaultSyncSleep = 2 * time.Second

	// defaultFetchSleep is the amount of time to sleep
	// when we are loading more blocks to fetch but we
	// already have a backlog >= to concurrency.
	defaultFetchSleep = 500 * time.Millisecond
)

// Handler is called at various times during the sync cycle
// to handle different events. It is common to write logs or
// perform reconciliation in the sync processor.
type Handler interface {
	// BlockSeen is invoked AT LEAST ONCE
	// by the syncer prior to calling BlockAdded
	// with the same arguments. This allows for
	// storing block data before it is sequenced.
	BlockSeen(
		ctx context.Context,
		block *types.Block,
	) error

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
	NetworkStatus(
		context.Context,
		*types.NetworkIdentifier,
	) (*types.NetworkStatusResponse, error)

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
	network *types.NetworkIdentifier
	helper  Helper
	handler Handler
	cancel  context.CancelFunc

	// Used to keep track of sync state
	genesisBlock *types.BlockIdentifier
	tip          *types.BlockIdentifier
	nextIndex    int64

	// To ensure reorgs are handled correctly, the syncer must be able
	// to observe blocks it has previously processed. Without this, the
	// syncer may process an index that is not connected to previously added
	// blocks (ParentBlockIdentifier != lastProcessedBlock.BlockIdentifier).
	//
	// If a blockchain does not have reorgs, it is not necessary to populate
	// the blockCache on creation.
	pastBlocks     []*types.BlockIdentifier
	pastBlockLimit int

	// Automatically manage concurrency based on the
	// provided max cache size. The algorithm used here
	// is a slow rise (to increase concurrency) and fast
	// fall (if we breach our max cache size).
	cacheSize        int
	sizeMultiplier   float64
	maxConcurrency   int64
	concurrency      int64
	goalConcurrency  int64
	recentBlockSizes []int
	lastAdjustment   int64
	adjustmentWindow int64
	concurrencyLock  sync.Mutex

	// store customized info
	metaData string

	// doneLoading is used to coordinate adding goroutines
	// when close to the end of syncing a range.
	doneLoading     bool
	doneLoadingLock sync.Mutex
}
