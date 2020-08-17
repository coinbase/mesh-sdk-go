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

package storage

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

const (
	// headBlockKey is used to lookup the head block identifier.
	// The head block is the block with the largest index that is
	// not orphaned.
	headBlockKey = "head-block"

	// blockNamespace is prepended to any stored block.
	blockNamespace = "block"

	// blockIndexNamespace is prepended to any stored block index.
	blockIndexNamespace = "block-index"

	// transactionHashNamespace is prepended to any stored
	// transaction hash.
	transactionHashNamespace = "transaction-hash"
)

var (
	// ErrHeadBlockNotFound is returned when there is no
	// head block found in BlockStorage.
	ErrHeadBlockNotFound = errors.New("head block not found")

	// ErrBlockNotFound is returned when a block is not
	// found in BlockStorage.
	ErrBlockNotFound = errors.New("block not found")

	// ErrDuplicateKey is returned when a key
	// cannot be stored because it is a duplicate.
	ErrDuplicateKey = errors.New("duplicate key")

	// ErrDuplicateTransactionHash is returned when a transaction
	// hash cannot be stored because it is a duplicate.
	ErrDuplicateTransactionHash = errors.New("duplicate transaction hash")
)

func getHeadBlockKey() []byte {
	return []byte(headBlockKey)
}

func getBlockHashKey(hash string) []byte {
	return []byte(fmt.Sprintf("%s/%s", blockNamespace, hash))
}

func getBlockIndexKey(index int64) []byte {
	return []byte(fmt.Sprintf("%s/%d", blockIndexNamespace, index))
}

func getTransactionHashKey(transactionIdentifier *types.TransactionIdentifier) []byte {
	return []byte(fmt.Sprintf("%s/%s", transactionHashNamespace, transactionIdentifier.Hash))
}

// BlockWorker is an interface that allows for work
// to be done while a block is added/removed from storage
// in the same database transaction as the change.
type BlockWorker interface {
	AddingBlock(context.Context, *types.Block, DatabaseTransaction) (CommitWorker, error)
	RemovingBlock(context.Context, *types.Block, DatabaseTransaction) (CommitWorker, error)
}

// CommitWorker is returned by a BlockWorker to be called after
// changes have been committed. It is common to put logging activities
// in here (that shouldn't be printed until the block is committed).
type CommitWorker func(context.Context) error

// BlockStorage implements block specific storage methods
// on top of a Database and DatabaseTransaction interface.
type BlockStorage struct {
	db Database

	workers []BlockWorker
}

// NewBlockStorage returns a new BlockStorage.
func NewBlockStorage(
	db Database,
) *BlockStorage {
	return &BlockStorage{
		db: db,
	}
}

// Initialize adds a []BlockWorker to BlockStorage. Usually
// all block workers are not created by the time block storage
// is constructed.
//
// This must be called prior to syncing!
func (b *BlockStorage) Initialize(workers []BlockWorker) {
	b.workers = workers
}

// GetHeadBlockIdentifier returns the head block identifier,
// if it exists.
func (b *BlockStorage) GetHeadBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	transaction := b.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	return b.GetHeadBlockIdentifierTransactional(ctx, transaction)
}

// GetHeadBlockIdentifierTransactional returns the head block identifier,
// if it exists, in the context of a DatabaseTransaction.
func (b *BlockStorage) GetHeadBlockIdentifierTransactional(
	ctx context.Context,
	transaction DatabaseTransaction,
) (*types.BlockIdentifier, error) {
	exists, block, err := transaction.Get(ctx, getHeadBlockKey())
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, ErrHeadBlockNotFound
	}

	var blockIdentifier types.BlockIdentifier
	err = decode(block, &blockIdentifier)
	if err != nil {
		return nil, err
	}

	return &blockIdentifier, nil
}

// StoreHeadBlockIdentifier stores a block identifier
// or returns an error.
func (b *BlockStorage) StoreHeadBlockIdentifier(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
) error {
	buf, err := encode(blockIdentifier)
	if err != nil {
		return err
	}

	return transaction.Set(ctx, getHeadBlockKey(), buf)
}

// GetBlock returns a block, if it exists.
func (b *BlockStorage) GetBlock(
	ctx context.Context,
	blockIdentifier *types.PartialBlockIdentifier,
) (*types.Block, error) {
	transaction := b.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	var exists bool
	var block []byte
	var err error
	switch {
	case blockIdentifier == nil || (blockIdentifier.Hash == nil && blockIdentifier.Index == nil):
		// Get current block when no blockIdentifier is provided
		var head *types.BlockIdentifier
		head, err = b.GetHeadBlockIdentifierTransactional(ctx, transaction)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot get head block identifier", err)
		}

		exists, block, err = transaction.Get(ctx, getBlockHashKey(head.Hash))
	case blockIdentifier.Hash != nil:
		// Get block by hash if provided
		exists, block, err = transaction.Get(ctx, getBlockHashKey(*blockIdentifier.Hash))
	default:
		// Get block by index if hash not provided
		var blockKey []byte
		exists, blockKey, err = transaction.Get(ctx, getBlockIndexKey(*blockIdentifier.Index))
		if exists {
			exists, block, err = transaction.Get(ctx, blockKey)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("%w: unable to get block", err)
	}

	if !exists {
		return nil, fmt.Errorf("%w: %+v", ErrBlockNotFound, blockIdentifier)
	}

	var rosettaBlock types.Block
	err = decode(block, &rosettaBlock)
	if err != nil {
		return nil, err
	}

	return &rosettaBlock, nil
}

func (b *BlockStorage) storeBlock(
	ctx context.Context,
	transaction DatabaseTransaction,
	block *types.Block,
) error {
	buf, err := encode(block)
	if err != nil {
		return fmt.Errorf("%w: unable to encode block", err)
	}

	if err := b.storeUniqueKey(ctx, transaction, getBlockHashKey(block.BlockIdentifier.Hash), buf); err != nil {
		return fmt.Errorf("%w: unable to store block", err)
	}

	if err := b.storeUniqueKey(
		ctx,
		transaction,
		getBlockIndexKey(block.BlockIdentifier.Index),
		getBlockHashKey(block.BlockIdentifier.Hash),
	); err != nil {
		return fmt.Errorf("%w: unable to store block index", err)
	}

	if err := b.StoreHeadBlockIdentifier(ctx, transaction, block.BlockIdentifier); err != nil {
		return fmt.Errorf("%w: unable to update head block identifier", err)
	}

	return nil
}

// AddBlock stores a block or returns an error.
func (b *BlockStorage) AddBlock(
	ctx context.Context,
	block *types.Block,
) error {
	transaction := b.db.NewDatabaseTransaction(ctx, true)
	defer transaction.Discard(ctx)

	// Store block
	err := b.storeBlock(ctx, transaction, block)
	if err != nil {
		return fmt.Errorf("%w: unable to store block", err)
	}

	// Store all transaction hashes
	for _, txn := range block.Transactions {
		err = b.storeTransactionHash(
			ctx,
			transaction,
			block.BlockIdentifier,
			txn.TransactionIdentifier,
		)
		if err != nil {
			return fmt.Errorf("%w: unable to store transaction hash", err)
		}
	}

	return b.callWorkersAndCommit(ctx, block, transaction, true)
}

func (b *BlockStorage) deleteBlock(
	ctx context.Context,
	transaction DatabaseTransaction,
	block *types.Block,
) error {
	blockIdentifier := block.BlockIdentifier
	if err := transaction.Delete(ctx, getBlockHashKey(blockIdentifier.Hash)); err != nil {
		return fmt.Errorf("%w: unable to delete block", err)
	}

	if err := transaction.Delete(ctx, getBlockIndexKey(blockIdentifier.Index)); err != nil {
		return fmt.Errorf("%w: unable to delete block index", err)
	}

	if err := b.StoreHeadBlockIdentifier(ctx, transaction, block.ParentBlockIdentifier); err != nil {
		return fmt.Errorf("%w: unable to update head block identifier", err)
	}

	return nil
}

// RemoveBlock removes a block or returns an error.
// RemoveBlock also removes the block hash and all
// its transaction hashes to not break duplicate
// detection. This is called within a re-org.
func (b *BlockStorage) RemoveBlock(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
) error {
	block, err := b.GetBlock(ctx, types.ConstructPartialBlockIdentifier(blockIdentifier))
	if err != nil {
		return err
	}

	transaction := b.db.NewDatabaseTransaction(ctx, true)
	defer transaction.Discard(ctx)

	// Remove all transaction hashes
	for _, txn := range block.Transactions {
		err = b.removeTransactionHash(ctx, transaction, blockIdentifier, txn.TransactionIdentifier)
		if err != nil {
			return err
		}
	}

	// Delete block
	if err := b.deleteBlock(ctx, transaction, block); err != nil {
		return fmt.Errorf("%w: unable to delete block", err)
	}

	return b.callWorkersAndCommit(ctx, block, transaction, false)
}

func (b *BlockStorage) callWorkersAndCommit(
	ctx context.Context,
	block *types.Block,
	txn DatabaseTransaction,
	adding bool,
) error {
	commitWorkers := make([]CommitWorker, len(b.workers))
	for i, w := range b.workers {
		var cw CommitWorker
		var err error
		if adding {
			cw, err = w.AddingBlock(ctx, block, txn)
		} else {
			cw, err = w.RemovingBlock(ctx, block, txn)
		}
		if err != nil {
			return err
		}

		commitWorkers[i] = cw
	}

	if err := txn.Commit(ctx); err != nil {
		return err
	}

	for _, cw := range commitWorkers {
		if cw == nil {
			continue
		}

		if err := cw(ctx); err != nil {
			return err
		}
	}

	return nil
}

// SetNewStartIndex attempts to remove all blocks
// greater than or equal to the startIndex.
func (b *BlockStorage) SetNewStartIndex(
	ctx context.Context,
	startIndex int64,
) error {
	head, err := b.GetHeadBlockIdentifier(ctx)
	if errors.Is(err, ErrHeadBlockNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	if head.Index < startIndex {
		return fmt.Errorf(
			"last processed block %d is less than start index %d",
			head.Index,
			startIndex,
		)
	}

	currBlock := head
	for currBlock.Index >= startIndex {
		log.Printf("Removing block %+v\n", currBlock)
		block, err := b.GetBlock(ctx, types.ConstructPartialBlockIdentifier(currBlock))
		if err != nil {
			return err
		}

		if err := b.RemoveBlock(ctx, block.BlockIdentifier); err != nil {
			return err
		}

		currBlock = block.ParentBlockIdentifier
	}

	return nil
}

// CreateBlockCache populates a slice of blocks with the most recent
// ones in storage.
func (b *BlockStorage) CreateBlockCache(ctx context.Context) []*types.BlockIdentifier {
	cache := []*types.BlockIdentifier{}
	head, err := b.GetHeadBlockIdentifier(ctx)
	if err != nil {
		return cache
	}

	for len(cache) < syncer.PastBlockSize {
		block, err := b.GetBlock(ctx, types.ConstructPartialBlockIdentifier(head))
		if err != nil {
			return cache
		}

		log.Printf("Added %+v to cache\n", block.BlockIdentifier)

		cache = append([]*types.BlockIdentifier{block.BlockIdentifier}, cache...)
		head = block.ParentBlockIdentifier

		// We should break if we have reached genesis.
		if head.Index == block.BlockIdentifier.Index {
			break
		}
	}

	return cache
}

func (b *BlockStorage) storeUniqueKey(
	ctx context.Context,
	transaction DatabaseTransaction,
	key []byte,
	value []byte,
) error {
	exists, _, err := transaction.Get(ctx, key)
	if err != nil {
		return err
	}

	if exists {
		return fmt.Errorf("%w: duplicate key %s found", ErrDuplicateKey, string(key))
	}

	return transaction.Set(ctx, key, value)
}

func (b *BlockStorage) storeTransactionHash(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	hashKey := getTransactionHashKey(transactionIdentifier)
	exists, val, err := transaction.Get(ctx, hashKey)
	if err != nil {
		return err
	}

	var blocks map[string]int64
	if !exists {
		blocks = make(map[string]int64)
	} else {
		if err := decode(val, &blocks); err != nil {
			return fmt.Errorf("%w: could not decode transaction hash contents", err)
		}

		if _, exists := blocks[blockIdentifier.Hash]; exists {
			return fmt.Errorf(
				"%w: duplicate transaction %s found in block %s:%d",
				ErrDuplicateTransactionHash,
				transactionIdentifier.Hash,
				blockIdentifier.Hash,
				blockIdentifier.Index,
			)
		}
	}
	blocks[blockIdentifier.Hash] = blockIdentifier.Index

	encodedResult, err := encode(blocks)
	if err != nil {
		return fmt.Errorf("%w: unable to encode transaction data", err)
	}

	return transaction.Set(ctx, hashKey, encodedResult)
}

func (b *BlockStorage) removeTransactionHash(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	hashKey := getTransactionHashKey(transactionIdentifier)
	exists, val, err := transaction.Get(ctx, hashKey)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("could not remove transaction %s", transactionIdentifier.Hash)
	}

	var blocks map[string]int64
	if err := decode(val, &blocks); err != nil {
		return fmt.Errorf("%w: could not decode transaction hash contents", err)
	}

	if _, exists := blocks[blockIdentifier.Hash]; !exists {
		return fmt.Errorf("saved blocks at transaction does not contain %s", blockIdentifier.Hash)
	}

	delete(blocks, blockIdentifier.Hash)

	if len(blocks) == 0 {
		return transaction.Delete(ctx, hashKey)
	}

	encodedResult, err := encode(blocks)
	if err != nil {
		return fmt.Errorf("%w: unable to encode transaction data", err)
	}

	return transaction.Set(ctx, hashKey, encodedResult)
}

// FindTransaction returns the most recent *types.BlockIdentifier containing the
// transaction and the transaction.
func (b *BlockStorage) FindTransaction(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	txn DatabaseTransaction,
) (*types.BlockIdentifier, *types.Transaction, error) {
	txExists, tx, err := txn.Get(ctx, getTransactionHashKey(transactionIdentifier))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to query database for transaction", err)
	}

	if !txExists {
		return nil, nil, nil
	}

	var blocks map[string]int64
	if err := decode(tx, &blocks); err != nil {
		return nil, nil, fmt.Errorf("%w: unable to decode block data for transaction", err)
	}

	var newestBlock *types.BlockIdentifier
	for hash, index := range blocks {
		b := &types.BlockIdentifier{Hash: hash, Index: index}
		if newestBlock == nil || b.Index > newestBlock.Index {
			newestBlock = b
		}
	}

	blockExists, block, err := txn.Get(ctx, getBlockHashKey(newestBlock.Hash))
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to query database for block", err)
	}

	if !blockExists {
		return nil, nil, ErrBlockNotFound
	}

	var parsedBlock *types.Block
	err = decode(block, &parsedBlock)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: could not decode block", err)
	}

	for _, tx := range parsedBlock.Transactions {
		if types.Hash(tx.TransactionIdentifier) == types.Hash(transactionIdentifier) {
			return newestBlock, tx, nil
		}
	}

	return nil, nil, fmt.Errorf(
		"unable to find transaction %s in expected block %s:%d",
		transactionIdentifier.Hash,
		newestBlock.Hash,
		newestBlock.Index,
	)
}

// AtTip returns a boolean indicating if we
// are at tip (provided some acceptable
// tip delay).
func (b *BlockStorage) AtTip(
	ctx context.Context,
	tipDelay int64,
) (bool, *types.BlockIdentifier, error) {
	block, err := b.GetBlock(ctx, nil)
	if errors.Is(err, ErrHeadBlockNotFound) {
		return false, nil, nil
	}

	if err != nil {
		return false, nil, fmt.Errorf("%w: unable to get head block", err)
	}

	currentTime := utils.Milliseconds()
	tipCutoff := currentTime - (tipDelay * utils.MillisecondsInSecond)
	if block.Timestamp < tipCutoff {
		return false, nil, nil
	}

	return true, block.BlockIdentifier, nil
}
