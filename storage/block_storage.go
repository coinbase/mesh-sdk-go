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
	"strconv"

	"github.com/coinbase/rosetta-sdk-go/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"golang.org/x/sync/errgroup"
)

const (
	// headBlockKey is used to lookup the head block identifier.
	// The head block is the block with the largest index that is
	// not orphaned.
	headBlockKey = "head-block"

	// oldestBlockIndex is the last accessible block index. Anything
	// prior to this block index has been pruned.
	oldestBlockIndex = "oldest-block-index"

	// blockNamespace is prepended to any stored block.
	blockNamespace = "block"

	// blockIndexNamespace is prepended to any stored block index.
	blockIndexNamespace = "block-index"

	// transactionNamespace is prepended to any stored
	// transaction.
	transactionNamespace = "transaction"
)

type blockTransaction struct {
	Transaction *types.Transaction `json:"transaction"`
	BlockIndex  int64              `json:"block_index"`
}

func getHeadBlockKey() []byte {
	return []byte(headBlockKey)
}

func getOldestBlockIndexKey() []byte {
	return []byte(oldestBlockIndex)
}

func getBlockHashKey(hash string) (string, []byte) {
	return blockNamespace, []byte(fmt.Sprintf("%s/%s", blockNamespace, hash))
}

func getBlockIndexKey(index int64) []byte {
	return []byte(fmt.Sprintf("%s/%d", blockIndexNamespace, index))
}

func getTransactionHashKey(transactionIdentifier *types.TransactionIdentifier) (string, []byte) {
	return transactionNamespace, []byte(
		fmt.Sprintf("%s/%s", transactionNamespace, transactionIdentifier.Hash),
	)
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

func (b *BlockStorage) setOldestBlockIndex(
	ctx context.Context,
	dbTx DatabaseTransaction,
	update bool,
	index int64,
) error {
	key := getOldestBlockIndexKey()
	value := []byte(strconv.FormatInt(index, 10))
	if update {
		if err := dbTx.Set(ctx, key, value, true); err != nil {
			return err
		}

		return nil
	}

	err := storeUniqueKey(ctx, dbTx, key, value, true)
	if err == nil || errors.Is(err, ErrDuplicateKey) {
		return nil
	}

	return err
}

// GetOldestBlockIndexTransactional returns the oldest block index
// available in BlockStorage in a single database transaction.
func (b *BlockStorage) GetOldestBlockIndexTransactional(
	ctx context.Context,
	dbTx DatabaseTransaction,
) (int64, error) {
	exists, rawIndex, err := dbTx.Get(ctx, getOldestBlockIndexKey())
	if err != nil {
		return -1, err
	}

	if !exists {
		return -1, ErrOldestIndexMissing
	}

	index, err := strconv.ParseInt(string(rawIndex), 10, 64)
	if err != nil {
		return -1, err
	}

	return index, nil
}

// GetOldestBlockIndex returns the oldest block index
// available in BlockStorage.
func (b *BlockStorage) GetOldestBlockIndex(
	ctx context.Context,
) (int64, error) {
	dbTx := b.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	return b.GetOldestBlockIndexTransactional(ctx, dbTx)
}

// pruneBlock attempts to prune a single block in a database transaction.
// If a block is pruned, we return its index.
func (b *BlockStorage) pruneBlock(
	ctx context.Context,
	index int64,
) (int64, error) {
	// We create a separate transaction for each pruning attempt so that
	// we don't hit the database tx size maximum. As a result, it is possible
	// that we prune a collection of blocks, encounter an error, and cannot
	// rollback the pruning operations.
	dbTx := b.db.NewDatabaseTransaction(ctx, true)
	defer dbTx.Discard(ctx)

	oldestIndex, err := b.GetOldestBlockIndexTransactional(ctx, dbTx)
	if err != nil {
		return -1, fmt.Errorf("%w: %v", ErrOldestIndexRead, err)
	}

	if index < oldestIndex {
		return -1, ErrNothingToPrune
	}

	head, err := b.GetHeadBlockIdentifierTransactional(ctx, dbTx)
	if err != nil {
		return -1, fmt.Errorf("%w: cannot get head block identifier", err)
	}

	// Ensure we are only pruning blocks that could not be
	// accessed later in a reorg.
	if oldestIndex > head.Index-syncer.PastBlockSize*2 {
		return -1, ErrNothingToPrune
	}

	blockResponse, err := b.GetBlockLazyTransactional(
		ctx,
		&types.PartialBlockIdentifier{Index: &oldestIndex},
		dbTx,
	)
	if err != nil && !errors.Is(err, ErrBlockNotFound) {
		return -1, err
	}

	// If there is an omitted block, we will have a non-nil error. When
	// a block is omitted, we should not attempt to remove it because
	// it doesn't exist.
	if err == nil {
		blockIdentifier := blockResponse.Block.BlockIdentifier

		for _, tx := range blockResponse.OtherTransactions {
			if err := b.pruneTransaction(ctx, dbTx, blockIdentifier, tx); err != nil {
				return -1, fmt.Errorf("%w: %v", ErrCannotPruneTransaction, err)
			}
		}

		_, blockKey := getBlockHashKey(blockIdentifier.Hash)
		if err := dbTx.Set(ctx, blockKey, []byte(""), true); err != nil {
			return -1, err
		}
	}

	// Update prune index
	if err := b.setOldestBlockIndex(ctx, dbTx, true, oldestIndex+1); err != nil {
		return -1, fmt.Errorf("%w: %v", ErrOldestIndexUpdateFailed, err)
	}

	// Commit tx
	if err := dbTx.Commit(ctx); err != nil {
		return -1, err
	}

	return oldestIndex, nil
}

// Prune removes block and transaction data
// from all blocks with index <= index. Pruning
// leaves all keys associated with pruned data
// but overwrites their data to be empty. If pruning
// is successful, we return the range of pruned
// blocks.
//
// Prune is not invoked automatically because
// some applications prefer not to prune any
// block data.
func (b *BlockStorage) Prune(
	ctx context.Context,
	index int64,
) (int64, int64, error) {
	firstPruned := int64(-1)
	lastPruned := int64(-1)

	for ctx.Err() == nil {
		prunedBlock, err := b.pruneBlock(ctx, index)
		if errors.Is(err, ErrNothingToPrune) {
			return firstPruned, lastPruned, nil
		}
		if err != nil {
			return -1, -1, fmt.Errorf("%w: %v", ErrPruningFailed, err)
		}

		if firstPruned == -1 {
			firstPruned = prunedBlock
		}

		if lastPruned < prunedBlock {
			lastPruned = prunedBlock
		}
	}

	return -1, -1, ctx.Err()
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
	err = b.db.Encoder().Decode("", block, &blockIdentifier, true)
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
	buf, err := b.db.Encoder().Encode("", blockIdentifier)
	if err != nil {
		return err
	}

	if err := transaction.Set(ctx, getHeadBlockKey(), buf, true); err != nil {
		return err
	}

	return nil
}

// GetBlockLazyTransactional returns a *types.BlockResponse
// with populated OtherTransactions array containing
// all the transactions the caller must retrieve
// in a provided database transaction.
func (b *BlockStorage) GetBlockLazyTransactional(
	ctx context.Context,
	blockIdentifier *types.PartialBlockIdentifier,
	transaction DatabaseTransaction,
) (*types.BlockResponse, error) {
	var namespace string
	var key []byte
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

		namespace, key = getBlockHashKey(head.Hash)
		exists, blockResponse, err = transaction.Get(ctx, key)
	case blockIdentifier.Hash != nil:
		// Get block by hash if provided
		namespace, key = getBlockHashKey(*blockIdentifier.Hash)
		exists, blockResponse, err = transaction.Get(ctx, key)
	case blockIdentifier.Index != nil:
		// Get block by index if hash not provided
		exists, key, err = transaction.Get(ctx, getBlockIndexKey(*blockIdentifier.Index))
		namespace = blockIndexNamespace
		if exists {
			namespace = blockNamespace
			exists, blockResponse, err = transaction.Get(ctx, key)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBlockGetFailed, err)
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrBlockNotFound, types.PrintStruct(blockIdentifier))
	}

	if len(blockResponse) == 0 {
		return nil, ErrCannotAccessPrunedData
	}

	var rosettaBlockResponse types.BlockResponse
	err = b.db.Encoder().Decode(namespace, blockResponse, &rosettaBlockResponse, true)
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

	return b.GetBlockLazyTransactional(ctx, blockIdentifier, transaction)
}

// CanonicalBlock returns a boolean indicating if
// a block with the provided *types.BlockIdentifier
// is in the canonical chain (regardless if it has
// been pruned).
func (b *BlockStorage) CanonicalBlock(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
) (bool, error) {
	block, err := b.GetBlockLazy(
		ctx,
		types.ConstructPartialBlockIdentifier(blockIdentifier),
	)
	if errors.Is(err, ErrCannotAccessPrunedData) {
		return true, nil
	}
	if errors.Is(err, ErrBlockNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	if block == nil {
		return false, nil
	}

	return true, nil
}

// GetBlockTransactional gets a block in the context of a database
// transaction.
func (b *BlockStorage) GetBlockTransactional(
	ctx context.Context,
	dbTx DatabaseTransaction,
	blockIdentifier *types.PartialBlockIdentifier,
) (*types.Block, error) {
	blockResponse, err := b.GetBlockLazyTransactional(ctx, blockIdentifier, dbTx)
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
		tx, err := b.findBlockTransaction(
			ctx,
			block.BlockIdentifier,
			transactionIdentifier,
			dbTx,
		)
		if err != nil {
			return nil, fmt.Errorf(
				"%w %s: %v",
				ErrTransactionGetFailed,
				transactionIdentifier.Hash,
				err,
			)
		}

		txs[i] = tx
	}
	block.Transactions = txs

	return block, nil
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

	return b.GetBlockTransactional(ctx, transaction, blockIdentifier)
}

func (b *BlockStorage) storeBlock(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockResponse *types.BlockResponse,
) error {
	blockIdentifier := blockResponse.Block.BlockIdentifier
	namespace, key := getBlockHashKey(blockIdentifier.Hash)
	buf, err := b.db.Encoder().Encode(namespace, blockResponse)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBlockEncodeFailed, err)
	}

	if err := storeUniqueKey(ctx, transaction, key, buf, true); err != nil {
		return fmt.Errorf("%w: %v", ErrBlockStoreFailed, err)
	}

	if err := storeUniqueKey(
		ctx,
		transaction,
		getBlockIndexKey(blockIdentifier.Index),
		key,
		false,
	); err != nil {
		return fmt.Errorf("%w: %v", ErrBlockIndexStoreFailed, err)
	}

	if err := b.StoreHeadBlockIdentifier(ctx, transaction, blockIdentifier); err != nil {
		return fmt.Errorf("%w: %v", ErrBlockIdentifierUpdateFailed, err)
	}

	if err := b.setOldestBlockIndex(ctx, transaction, false, blockIdentifier.Index); err != nil {
		return fmt.Errorf("%w: %v", ErrOldestIndexUpdateFailed, err)
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

	// Store all transactions in order and check for duplicates
	identifiers := make([]*types.TransactionIdentifier, len(block.Transactions))
	identiferSet := map[string]struct{}{}
	for i, txn := range block.Transactions {
		if _, exists := identiferSet[txn.TransactionIdentifier.Hash]; exists {
			return fmt.Errorf(
				"%w: duplicate transaction %s found in block %s:%d",
				ErrDuplicateTransactionHash,
				txn.TransactionIdentifier.Hash,
				block.BlockIdentifier.Hash,
				block.BlockIdentifier.Index,
			)
		}

		identiferSet[txn.TransactionIdentifier.Hash] = struct{}{}
		identifiers[i] = txn.TransactionIdentifier
	}

	// Make copy of block and remove all transactions
	var copyBlock types.Block
	if err := copyStruct(block, &copyBlock); err != nil {
		return fmt.Errorf("%w: %v", ErrBlockCopyFailed, err)
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
		return fmt.Errorf("%w: %v", ErrBlockStoreFailed, err)
	}

	g, gctx := errgroup.WithContext(ctx)
	for _, thisTransaction := range block.Transactions {
		txn := thisTransaction
		g.Go(func() error {
			err := b.storeTransaction(
				gctx,
				transaction,
				block.BlockIdentifier,
				txn,
			)
			if err != nil {
				return fmt.Errorf("%w: %v", ErrTransactionHashStoreFailed, err)
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return b.callWorkersAndCommit(ctx, block, transaction, true)
}

func (b *BlockStorage) deleteBlock(
	ctx context.Context,
	transaction DatabaseTransaction,
	block *types.Block,
) error {
	blockIdentifier := block.BlockIdentifier

	// We return an error if we attempt to remove the oldest index. If we did
	// not error, it is possible that we could panic in the future as any
	// further block removals would involve decoding pruned blocks.
	oldestIndex, err := b.GetOldestBlockIndexTransactional(ctx, transaction)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOldestIndexRead, err)
	}

	if blockIdentifier.Index <= oldestIndex {
		return ErrCannotRemoveOldest
	}

	_, key := getBlockHashKey(blockIdentifier.Hash)
	if err := transaction.Delete(ctx, key); err != nil {
		return fmt.Errorf("%w: %v", ErrBlockDeleteFailed, err)
	}

	if err := transaction.Delete(ctx, getBlockIndexKey(blockIdentifier.Index)); err != nil {
		return fmt.Errorf("%w: %v", ErrBlockIndexDeleteFailed, err)
	}

	if err := b.StoreHeadBlockIdentifier(ctx, transaction, block.ParentBlockIdentifier); err != nil {
		return fmt.Errorf("%w: %v", ErrHeadBlockIdentifierUpdateFailed, err)
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
	transaction := b.db.NewDatabaseTransaction(ctx, true)
	defer transaction.Discard(ctx)

	block, err := b.GetBlockTransactional(
		ctx,
		transaction,
		types.ConstructPartialBlockIdentifier(blockIdentifier),
	)
	if err != nil {
		return err
	}

	// Remove all transaction hashes
	g, gctx := errgroup.WithContext(ctx)
	for _, thisTransaction := range block.Transactions {
		txn := thisTransaction
		g.Go(func() error {
			return b.removeTransaction(
				gctx,
				transaction,
				blockIdentifier,
				txn.TransactionIdentifier,
			)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Delete block
	if err := b.deleteBlock(ctx, transaction, block); err != nil {
		return fmt.Errorf("%w: %v", ErrBlockDeleteFailed, err)
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
			"%w: block index is %d but start index is %d",
			ErrLastProcessedBlockPrecedesStart,
			head.Index,
			startIndex,
		)
	}

	// Ensure we do not set a new start index less
	// than the oldest block.
	dbTx := b.db.NewDatabaseTransaction(ctx, false)
	oldestIndex, err := b.GetOldestBlockIndexTransactional(ctx, dbTx)
	dbTx.Discard(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrOldestIndexRead, err)
	}

	if oldestIndex > startIndex {
		return fmt.Errorf(
			"%w: oldest block index is %d but start index is %d",
			ErrCannotAccessPrunedData,
			oldestIndex,
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

func (b *BlockStorage) updateTransaction(
	ctx context.Context,
	dbTx DatabaseTransaction,
	hashKey []byte,
	namespace string,
	blocks map[string]*blockTransaction,
) error {
	encodedResult, err := b.db.Encoder().Encode(namespace, blocks)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrTransactionDataEncodeFailed, err)
	}

	if err := dbTx.Set(ctx, hashKey, encodedResult, true); err != nil {
		return err
	}

	return nil
}

func (b *BlockStorage) storeTransaction(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
	tx *types.Transaction,
) error {
	namespace, hashKey := getTransactionHashKey(tx.TransactionIdentifier)
	exists, val, err := transaction.Get(ctx, hashKey)
	if err != nil {
		return err
	}

	var blocks map[string]*blockTransaction
	if !exists {
		blocks = make(map[string]*blockTransaction)
	} else {
		err := b.db.Encoder().Decode(namespace, val, &blocks, true)
		if err != nil {
			return fmt.Errorf("%w: could not decode transaction hash contents", err)
		}
	}
	// We check for duplicates before storing transaction,
	// so this must be a new key.
	blocks[blockIdentifier.Hash] = &blockTransaction{
		Transaction: tx,
		BlockIndex:  blockIdentifier.Index,
	}

	return b.updateTransaction(ctx, transaction, hashKey, namespace, blocks)
}

func (b *BlockStorage) pruneTransaction(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
	txIdentifier *types.TransactionIdentifier,
) error {
	namespace, hashKey := getTransactionHashKey(txIdentifier)
	exists, val, err := transaction.Get(ctx, hashKey)
	if err != nil {
		return err
	}
	if !exists {
		return ErrTransactionNotFound
	}

	var blocks map[string]*blockTransaction
	if err := b.db.Encoder().Decode(namespace, val, &blocks, true); err != nil {
		return fmt.Errorf("%w: could not decode transaction hash contents", err)
	}

	blocks[blockIdentifier.Hash] = &blockTransaction{
		BlockIndex: blockIdentifier.Index,
	}

	return b.updateTransaction(ctx, transaction, hashKey, namespace, blocks)
}

func (b *BlockStorage) removeTransaction(
	ctx context.Context,
	transaction DatabaseTransaction,
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	namespace, hashKey := getTransactionHashKey(transactionIdentifier)
	exists, val, err := transaction.Get(ctx, hashKey)
	if err != nil {
		return err
	}

	if !exists {
		return fmt.Errorf("%w %s", ErrTransactionDeleteFailed, transactionIdentifier.Hash)
	}

	var blocks map[string]*blockTransaction
	if err := b.db.Encoder().Decode(namespace, val, &blocks, true); err != nil {
		return fmt.Errorf("%w: could not decode transaction hash contents", err)
	}

	if _, exists := blocks[blockIdentifier.Hash]; !exists {
		return fmt.Errorf("%w %s", ErrTransactionHashNotFound, blockIdentifier.Hash)
	}

	delete(blocks, blockIdentifier.Hash)

	if len(blocks) == 0 {
		return transaction.Delete(ctx, hashKey)
	}

	return b.updateTransaction(ctx, transaction, hashKey, namespace, blocks)
}

// FindTransaction returns the most recent *types.BlockIdentifier containing the
// transaction and the transaction.
func (b *BlockStorage) FindTransaction(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	txn DatabaseTransaction,
) (*types.BlockIdentifier, *types.Transaction, error) {
	namespace, key := getTransactionHashKey(transactionIdentifier)
	txExists, tx, err := txn.Get(ctx, key)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", ErrTransactionDBQueryFailed, err)
	}

	if !txExists {
		return nil, nil, nil
	}

	var blocks map[string]*blockTransaction
	if err := b.db.Encoder().Decode(namespace, tx, &blocks, true); err != nil {
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

	// If the transaction has been pruned, it will be nil.
	if newestTransaction == nil {
		return nil, nil, ErrCannotAccessPrunedData
	}

	return newestBlock, newestTransaction, nil
}

func (b *BlockStorage) findBlockTransaction(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
	txn DatabaseTransaction,
) (*types.Transaction, error) {
	oldestIndex, err := b.GetOldestBlockIndexTransactional(ctx, txn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrOldestIndexRead, err)
	}

	if blockIdentifier.Index < oldestIndex {
		return nil, ErrCannotAccessPrunedData
	}

	namespace, key := getTransactionHashKey(transactionIdentifier)
	txExists, tx, err := txn.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrTransactionDBQueryFailed, err)
	}

	if !txExists {
		return nil, fmt.Errorf("%w %s", ErrTransactionNotFound, transactionIdentifier.Hash)
	}

	var blocks map[string]*blockTransaction
	if err := b.db.Encoder().Decode(namespace, tx, &blocks, true); err != nil {
		return nil, fmt.Errorf("%w: unable to decode block data for transaction", err)
	}

	val, ok := blocks[blockIdentifier.Hash]
	if !ok {
		return nil, fmt.Errorf(
			"%w: did not find transaction %s in block %s",
			ErrTransactionDoesNotExistInBlock,
			transactionIdentifier.Hash,
			blockIdentifier.Hash,
		)
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
		return false, nil, fmt.Errorf("%w: %v", ErrHeadBlockGetFailed, err)
	}

	atTip := utils.AtTip(tipDelay, block.Timestamp)
	if !atTip {
		return false, nil, nil
	}

	return true, block.BlockIdentifier, nil
}
