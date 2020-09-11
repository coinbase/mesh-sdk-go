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
	"errors"

	utils "github.com/coinbase/rosetta-sdk-go/errors"
)

// Named error types for Syncer errors
var (
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

	ErrGetCurrentHeadBlockFailed   = errors.New("unable to get current head")
	ErrGetNetworkStatusFailed      = errors.New("unable to get network status")
	ErrFetchBlockFailed            = errors.New("unable to fetch block")
	ErrFetchBlockReorgFailed       = errors.New("unable to fetch block during re-org")
	ErrBlockProcessFailed          = errors.New("unable to process block")
	ErrBlocksProcessMultipleFailed = errors.New("unable to process blocks")
	ErrSetStartIndexFailed         = errors.New("unable to set start index")
	ErrNextSyncableRangeFailed     = errors.New("unable to get next syncable range")
)

// Err takes an error as an argument and returns
// whether or not the error is one thrown by the syncer package
func Err(err error) bool {
	syncerErrors := []error{
		ErrCannotRemoveGenesisBlock,
		ErrOutOfOrder,
		ErrOrphanHead,
		ErrBlockResultNil,
		ErrGetCurrentHeadBlockFailed,
		ErrGetNetworkStatusFailed,
		ErrFetchBlockFailed,
		ErrFetchBlockReorgFailed,
		ErrBlockProcessFailed,
		ErrBlocksProcessMultipleFailed,
		ErrSetStartIndexFailed,
		ErrNextSyncableRangeFailed,
	}

	return utils.FindError(syncerErrors, err)
}
