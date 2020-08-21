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

	// transactionNamespace is prepended to any stored
	// transaction.
	transactionNamespace = "transaction"
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

type blockTransaction struct {
	Transaction *types.Transaction `json:"transaction"`
	BlockIndex  int64              `json:"block_index"`
}

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
	return []byte(fmt.Sprintf("%s/%s", transactionNamespace, transactionIdentifier.Hash))
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

func (b *BlockStorage) getBlockResponse(
	ctx context.Context,
	blockIdentifier *types.PartialBlockIdentifier,
	transaction DatabaseTransaction,
) (*types.BlockResponse, error) {
	var exists bool
	var blockResponse []byte
	var err error
	switch {
	case blockIdentifier == nil || (blockIdentifier.Hash == nil && blockIdentifier.Index == nil):
		// Get current block when no blockIdentifier is provided
		var head *types.BlockIdentifier
		head, err = b.GetHeadBlockIdentifierTransactional(ctx, transaction)
		if err != nil {
			return nil, fmt.Errorf("%w: cannot get head block identifier", err)
		}

		exists, blockResponse, err = transaction.Get(ctx, getBlockHashKey(head.Hash))
	case blockIdentifier.Hash != nil:
		// Get block by hash if provided
		exists, blockResponse, err = transaction.Get(ctx, getBlockHashKey(*blockIdentifier.Hash))
	default:
		// Get block by index if hash not provided
		var blockKey []byte
		exists, blockKey, err = transaction.Get(ctx, getBlockIndexKey(*blockIdentifier.Index))
		if exists {
			exists, blockResponse, err = transaction.Get(ctx, blockKey)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("%w: unable to get block", err)
	}

	if !exists {
		return nil, fmt.Errorf("%w: %+v", ErrBlockNotFound, blockIdentifier)
	}

	var rosettaBlockResponse types.BlockResponse
	err = decode(blockResponse, &rosettaBlockResponse)
	if err != nil {
		return nil, err
	}

	return &rosettaBlockResponse, nil
}

// GetBlockLazy returns a *types.BlockResponse
// with populated OtherTransactions array containing
// all the transactions the caller must retrieve.
// This is typically used to serve /block queries.
func (b *BlockStorage) GetBlockLazy(
	ctx context.Context,
	blockIdentifier *types.PartialBlockIdentifier,
) (*types.BlockResponse, error) {
	transaction := b.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	return b.getBlockResponse(ctx, blockIdentifier, transaction)
}

// GetBlock returns a block, if it exists. GetBlock
// will fetch all transactions contained in a block
// automatically. If you don't wish to do this for
// performance reasons, use GetBlockLazy.
func (b *BlockStorage) GetBlock(
	ctx context.Context,
	blockIdentifier *types.PartialBlockIdentifier,
) (*types.Block, error) {
	transaction := b.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	blockResponse, err := b.getBlockResponse(ctx, blockIdentifier, transaction)
	if err != nil {
		return nil, err
	}

	if len(blockResponse.OtherTransactions) == 0 {
		return blockResponse.Block, nil
	}

	// Fetch all transactions in block
	block := blockResponse.Block
	txs := make([]*types.Transaction, len(blockResponse.OtherTransactions))
	for i, transactionIdentifier := range blockResponse.OtherTransactions {
		tx, err := b.findBlockTransaction(ctx, block.BlockIdentifier, transactionIdentifier, transaction)
		if err != nil {
			return nil, fmt.Errorf("%w: could not get transaction %s", err, transactionIdentifier.Hash)
		}

		txs[i] = tx
	}
	block.Transactions = txs

	return block, nil
}

func (b *BlockStorage) storeBlock(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockResponse *types.BlockResponse,
) error {
	buf, err := encode(blockResponse)
	if err != nil {
		return fmt.Errorf("%w: unable to encode block", err)
	}

	blockIdentifier := blockResponse.Block.BlockIdentifier

	if err := b.storeUniqueKey(ctx, transaction, getBlockHashKey(blockIdentifier.Hash), buf); err != nil {
		return fmt.Errorf("%w: unable to store block", err)
	}

	if err := b.storeUniqueKey(
		ctx,
		transaction,
		getBlockIndexKey(blockIdentifier.Index),
		getBlockHashKey(blockIdentifier.Hash),
	); err != nil {
		return fmt.Errorf("%w: unable to store block index", err)
	}

	if err := b.StoreHeadBlockIdentifier(ctx, transaction, blockIdentifier); err != nil {
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

	// Store all transactions
	identifiers := make([]*types.TransactionIdentifier, len(block.Transactions))
	for i, txn := range block.Transactions {
		identifiers[i] = txn.TransactionIdentifier
	}

	// Make copy of block and remove all transactions
	var copyBlock types.Block
	if err := copyStruct(block, &copyBlock); err != nil {
		return fmt.Errorf("%w: unable to copy block", err)
	}

	copyBlock.Transactions = nil

	// Prepare block for storage
	blockWithoutTransactions := &types.BlockResponse{
		Block:             &copyBlock,
		OtherTransactions: identifiers,
	}

	// Store block
	err := b.storeBlock(ctx, transaction, blockWithoutTransactions)
	if err != nil {
		return fmt.Errorf("%w: unable to store block", err)
	}

	for _, txn := range block.Transactions {
		err := b.storeTransaction(
			ctx,
			transaction,
			block.BlockIdentifier,
			txn,
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
		err = b.removeTransaction(ctx, transaction, blockIdentifier, txn.TransactionIdentifier)
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

func (b *BlockStorage) storeTransaction(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
	tx *types.Transaction,
) error {
	hashKey := getTransactionHashKey(tx.TransactionIdentifier)
	exists, val, err := transaction.Get(ctx, hashKey)
	if err != nil {
		return err
	}

	var blocks map[string]*blockTransaction
	if !exists {
		blocks = make(map[string]*blockTransaction)
	} else {
		if err := decode(val, &blocks); err != nil {
			return fmt.Errorf("%w: could not decode transaction hash contents", err)
		}

		if _, exists := blocks[blockIdentifier.Hash]; exists {
			return fmt.Errorf(
				"%w: duplicate transaction %s found in block %s:%d",
				ErrDuplicateTransactionHash,
				tx.TransactionIdentifier.Hash,
				blockIdentifier.Hash,
				blockIdentifier.Index,
			)
		}
	}
	blocks[blockIdentifier.Hash] = &blockTransaction{
		Transaction: tx,
		BlockIndex:  blockIdentifier.Index,
	}

	encodedResult, err := encode(blocks)
	if err != nil {
		return fmt.Errorf("%w: unable to encode transaction data", err)
	}

	return transaction.Set(ctx, hashKey, encodedResult)
}

func (b *BlockStorage) removeTransaction(
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

	var blocks map[string]*blockTransaction
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

	var blocks map[string]*blockTransaction
	if err := decode(tx, &blocks); err != nil {
		return nil, nil, fmt.Errorf("%w: unable to decode block data for transaction", err)
	}

	var newestBlock *types.BlockIdentifier
	var newestTransaction *types.Transaction
	for hash, blockTransaction := range blocks {
		b := &types.BlockIdentifier{Hash: hash, Index: blockTransaction.BlockIndex}
		if newestBlock == nil || blockTransaction.BlockIndex > newestBlock.Index {
			newestBlock = b
			newestTransaction = blockTransaction.Transaction
		}
	}

	return newestBlock, newestTransaction, nil
}

func (b *BlockStorage) findBlockTransaction(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
	txn DatabaseTransaction,
) (*types.Transaction, error) {
	txExists, tx, err := txn.Get(ctx, getTransactionHashKey(transactionIdentifier))
	if err != nil {
		return nil, fmt.Errorf("%w: unable to query database for transaction", err)
	}

	if !txExists {
		return nil, fmt.Errorf("unable to find transaction %s", transactionIdentifier.Hash)
	}

	var blocks map[string]*blockTransaction
	if err := decode(tx, &blocks); err != nil {
		return nil, fmt.Errorf("%w: unable to decode block data for transaction", err)
	}

	val, ok := blocks[blockIdentifier.Hash]
	if !ok {
		return nil, fmt.Errorf("transaction %s does not exist in block %s", transactionIdentifier.Hash, blockIdentifier.Hash)
	}

	return val.Transaction, nil
}

// GetBlockTransaction retrieves a transaction belonging to a certain
// block in a database transaction. This is usually used to implement
// /block/transaction.
func (b *BlockStorage) GetBlockTransaction(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
) (*types.Transaction, error) {
	transaction := b.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	return b.findBlockTransaction(ctx, blockIdentifier, transactionIdentifier, transaction)
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
