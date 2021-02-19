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

package modules

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/neilotoole/errgroup"

	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/storage/encoder"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
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

	// blockSyncIdentifier is the identifier used to acquire
	// a database lock.
	blockSyncIdentifier = "blockSyncIdentifier"

	// backwardRelation is a relation from a child to a root transaction
	// the root is the destination and the child is the transaction listing the root as a backward
	// relation
	backwardRelation = "backwardRelation" // prefix/root/child
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

func getTransactionKey(
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
) (string, []byte) {
	return transactionNamespace, []byte(
		fmt.Sprintf(
			"%s/%s/%s",
			transactionNamespace,
			transactionIdentifier.Hash,
			blockIdentifier.Hash,
		),
	)
}

func getTransactionPrefix(
	transactionIdentifier *types.TransactionIdentifier,
) []byte {
	return []byte(
		fmt.Sprintf(
			"%s/%s/",
			transactionNamespace,
			transactionIdentifier.Hash,
		),
	)
}

// getBackwardRelationKey returns a db key for a backwards relation. passing nil in for the
// child returns a prefix key.
func getBackwardRelationKey(
	backwardTransaction *types.TransactionIdentifier,
	tx *types.TransactionIdentifier,
) []byte {
	childHash := ""
	if tx != nil {
		childHash = tx.Hash
	}
	return []byte(fmt.Sprintf("%s/%s/%s", backwardRelation, backwardTransaction.Hash, childHash))
}

// BlockWorker is an interface that allows for work
// to be done while a block is added/removed from storage
// in the same database transaction as the change.
type BlockWorker interface {
	AddingBlock(
		context.Context,
		*errgroup.Group,
		*types.Block,
		database.Transaction,
	) (database.CommitWorker, error)

	RemovingBlock(
		context.Context,
		*errgroup.Group,
		*types.Block,
		database.Transaction,
	) (database.CommitWorker, error)
}

// BlockStorage implements block specific storage methods
// on top of a database.Database and database.Transaction interface.
type BlockStorage struct {
	db database.Database

	workers           []BlockWorker
	workerConcurrency int
}

// NewBlockStorage returns a new BlockStorage.
func NewBlockStorage(
	db database.Database,
	workerConcurrency int,
) *BlockStorage {
	return &BlockStorage{
		db:                db,
		workerConcurrency: workerConcurrency,
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
	dbTx database.Transaction,
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
	if err == nil || errors.Is(err, storageErrs.ErrDuplicateKey) {
		return nil
	}

	return err
}

// GetOldestBlockIndexTransactional returns the oldest block index
// available in BlockStorage in a single database transaction.
func (b *BlockStorage) GetOldestBlockIndexTransactional(
	ctx context.Context,
	dbTx database.Transaction,
) (int64, error) {
	exists, rawIndex, err := dbTx.Get(ctx, getOldestBlockIndexKey())
	if err != nil {
		return -1, err
	}

	if !exists {
		return -1, storageErrs.ErrOldestIndexMissing
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
	dbTx := b.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return b.GetOldestBlockIndexTransactional(ctx, dbTx)
}

// pruneBlock attempts to prune a single block in a database transaction.
// If a block is pruned, we return its index.
func (b *BlockStorage) pruneBlock(
	ctx context.Context,
	index int64,
	minDepth int64,
) (int64, error) {
	// We create a separate transaction for each pruning attempt so that
	// we don't hit the database tx size maximum. As a result, it is possible
	// that we prune a collection of blocks, encounter an error, and cannot
	// rollback the pruning operations.
	dbTx := b.db.WriteTransaction(ctx, blockSyncIdentifier, false)
	defer dbTx.Discard(ctx)

	oldestIndex, err := b.GetOldestBlockIndexTransactional(ctx, dbTx)
	if err != nil {
		return -1, fmt.Errorf("%w: %v", storageErrs.ErrOldestIndexRead, err)
	}

	if index < oldestIndex {
		return -1, storageErrs.ErrNothingToPrune
	}

	head, err := b.GetHeadBlockIdentifierTransactional(ctx, dbTx)
	if err != nil {
		return -1, fmt.Errorf("%w: cannot get head block identifier", err)
	}

	// Ensure we are only pruning blocks that could not be
	// accessed later in a reorg.
	if oldestIndex > head.Index-minDepth {
		return -1, storageErrs.ErrNothingToPrune
	}

	blockResponse, err := b.GetBlockLazyTransactional(
		ctx,
		&types.PartialBlockIdentifier{Index: &oldestIndex},
		dbTx,
	)
	if err != nil && !errors.Is(err, storageErrs.ErrBlockNotFound) {
		return -1, err
	}

	// If there is an omitted block, we will have a non-nil error. When
	// a block is omitted, we should not attempt to remove it because
	// it doesn't exist.
	if err == nil {
		blockIdentifier := blockResponse.Block.BlockIdentifier

		// Remove all transaction hashes
		g, gctx := errgroup.WithContextN(ctx, b.workerConcurrency, b.workerConcurrency)
		for i := range blockResponse.OtherTransactions {
			// We need to set variable before calling goroutine
			// to avoid getting an updated pointer as loop iteration
			// continues.
			tx := blockResponse.OtherTransactions[i]
			g.Go(func() error {
				if err := b.pruneTransaction(gctx, dbTx, blockIdentifier, tx); err != nil {
					return fmt.Errorf("%w: %v", storageErrs.ErrCannotPruneTransaction, err)
				}

				return nil
			})
		}

		if err := g.Wait(); err != nil {
			return -1, err
		}

		_, blockKey := getBlockHashKey(blockIdentifier.Hash)
		if err := dbTx.Set(ctx, blockKey, []byte(""), true); err != nil {
			return -1, err
		}
	}

	// Update prune index
	if err := b.setOldestBlockIndex(ctx, dbTx, true, oldestIndex+1); err != nil {
		return -1, fmt.Errorf("%w: %v", storageErrs.ErrOldestIndexUpdateFailed, err)
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
	minDepth int64,
) (int64, int64, error) {
	firstPruned := int64(-1)
	lastPruned := int64(-1)

	for ctx.Err() == nil {
		prunedBlock, err := b.pruneBlock(ctx, index, minDepth)
		if errors.Is(err, storageErrs.ErrNothingToPrune) {
			return firstPruned, lastPruned, nil
		}
		if err != nil {
			return -1, -1, fmt.Errorf("%w: %v", storageErrs.ErrPruningFailed, err)
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
	transaction := b.db.ReadTransaction(ctx)
	defer transaction.Discard(ctx)

	return b.GetHeadBlockIdentifierTransactional(ctx, transaction)
}

// GetHeadBlockIdentifierTransactional returns the head block identifier,
// if it exists, in the context of a database.Transaction.
func (b *BlockStorage) GetHeadBlockIdentifierTransactional(
	ctx context.Context,
	transaction database.Transaction,
) (*types.BlockIdentifier, error) {
	exists, block, err := transaction.Get(ctx, getHeadBlockKey())
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, storageErrs.ErrHeadBlockNotFound
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
	transaction database.Transaction,
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
	transaction database.Transaction,
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
		return nil, fmt.Errorf("%w: %v", storageErrs.ErrBlockGetFailed, err)
	}

	if !exists {
		return nil, fmt.Errorf(
			"%w: %s",
			storageErrs.ErrBlockNotFound,
			types.PrintStruct(blockIdentifier),
		)
	}

	if len(blockResponse) == 0 {
		return nil, storageErrs.ErrCannotAccessPrunedData
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
	transaction := b.db.ReadTransaction(ctx)
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
	dbTx := b.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return b.CanonicalBlockTransactional(ctx, blockIdentifier, dbTx)
}

// CanonicalBlockTransactional returns a boolean indicating if
// a block with the provided *types.BlockIdentifier
// is in the canonical chain (regardless if it has
// been pruned) in a single storage.database.Transaction.
func (b *BlockStorage) CanonicalBlockTransactional(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
	dbTx database.Transaction,
) (bool, error) {
	block, err := b.GetBlockLazyTransactional(
		ctx,
		types.ConstructPartialBlockIdentifier(blockIdentifier),
		dbTx,
	)
	if errors.Is(err, storageErrs.ErrCannotAccessPrunedData) {
		return true, nil
	}
	if errors.Is(err, storageErrs.ErrBlockNotFound) {
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
	dbTx database.Transaction,
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
				storageErrs.ErrTransactionGetFailed,
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
	transaction := b.db.ReadTransaction(ctx)
	defer transaction.Discard(ctx)

	return b.GetBlockTransactional(ctx, transaction, blockIdentifier)
}

func (b *BlockStorage) seeBlock(
	ctx context.Context,
	transaction database.Transaction,
	blockResponse *types.BlockResponse,
) (bool, error) {
	blockIdentifier := blockResponse.Block.BlockIdentifier
	namespace, key := getBlockHashKey(blockIdentifier.Hash)
	buf, err := b.db.Encoder().Encode(namespace, blockResponse)
	if err != nil {
		return false, fmt.Errorf("%w: %v", storageErrs.ErrBlockEncodeFailed, err)
	}

	exists, val, err := transaction.Get(ctx, key)
	if err != nil {
		return false, err
	}

	if !exists {
		return false, transaction.Set(ctx, key, buf, true)
	}

	var rosettaBlockResponse types.BlockResponse
	err = b.db.Encoder().Decode(namespace, val, &rosettaBlockResponse, true)
	if err != nil {
		return false, err
	}

	// Exit early if block already exists!
	if blockResponse.Block.BlockIdentifier.Hash == rosettaBlockResponse.Block.BlockIdentifier.Hash &&
		blockResponse.Block.BlockIdentifier.Index == rosettaBlockResponse.Block.BlockIdentifier.Index {
		return true, nil
	}

	return false, fmt.Errorf(
		"%w: duplicate key %s found",
		storageErrs.ErrDuplicateKey,
		string(key),
	)
}

func (b *BlockStorage) storeBlock(
	ctx context.Context,
	transaction database.Transaction,
	blockIdentifier *types.BlockIdentifier,
) error {
	_, key := getBlockHashKey(blockIdentifier.Hash)

	if err := storeUniqueKey(
		ctx,
		transaction,
		getBlockIndexKey(blockIdentifier.Index),
		key,
		false,
	); err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrBlockIndexStoreFailed, err)
	}

	if err := b.StoreHeadBlockIdentifier(ctx, transaction, blockIdentifier); err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrBlockIdentifierUpdateFailed, err)
	}

	if err := b.setOldestBlockIndex(ctx, transaction, false, blockIdentifier.Index); err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrOldestIndexUpdateFailed, err)
	}

	return nil
}

// SeeBlock pre-stores a block or returns an error.
func (b *BlockStorage) SeeBlock(
	ctx context.Context,
	block *types.Block,
) error {
	_, key := getBlockHashKey(block.BlockIdentifier.Hash)
	transaction := b.db.WriteTransaction(ctx, string(key), true)
	defer transaction.Discard(ctx)

	// Store all transactions in order and check for duplicates
	identifiers := make([]*types.TransactionIdentifier, len(block.Transactions))
	identiferSet := map[string]struct{}{}
	for i, txn := range block.Transactions {
		if _, exists := identiferSet[txn.TransactionIdentifier.Hash]; exists {
			return fmt.Errorf(
				"%w: duplicate transaction %s found in block %s:%d",
				storageErrs.ErrDuplicateTransactionHash,
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
	if err := encoder.CopyStruct(block, &copyBlock); err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrBlockCopyFailed, err)
	}

	copyBlock.Transactions = nil

	// Prepare block for storage
	blockWithoutTransactions := &types.BlockResponse{
		Block:             &copyBlock,
		OtherTransactions: identifiers,
	}

	// Store block
	exists, err := b.seeBlock(ctx, transaction, blockWithoutTransactions)
	if err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrBlockStoreFailed, err)
	}

	if exists {
		return nil
	}

	g, gctx := errgroup.WithContextN(ctx, b.workerConcurrency, b.workerConcurrency)
	for i := range block.Transactions {
		// We need to set variable before calling goroutine
		// to avoid getting an updated pointer as loop iteration
		// continues.
		txn := block.Transactions[i]
		g.Go(func() error {
			err := b.storeTransaction(
				gctx,
				transaction,
				block.BlockIdentifier,
				txn,
			)
			if err != nil {
				return fmt.Errorf("%w: %v", storageErrs.ErrTransactionHashStoreFailed, err)
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	return transaction.Commit(ctx)
}

// AddBlock stores a block or returns an error.
func (b *BlockStorage) AddBlock(
	ctx context.Context,
	block *types.Block,
) error {
	transaction := b.db.WriteTransaction(ctx, blockSyncIdentifier, true)
	defer transaction.Discard(ctx)

	// Store block
	err := b.storeBlock(ctx, transaction, block.BlockIdentifier)
	if err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrBlockStoreFailed, err)
	}

	return b.callWorkersAndCommit(ctx, block, transaction, true)
}

func (b *BlockStorage) deleteBlock(
	ctx context.Context,
	transaction database.Transaction,
	block *types.Block,
) error {
	blockIdentifier := block.BlockIdentifier

	// We return an error if we attempt to remove the oldest index. If we did
	// not error, it is possible that we could panic in the future as any
	// further block removals would involve decoding pruned blocks.
	oldestIndex, err := b.GetOldestBlockIndexTransactional(ctx, transaction)
	if err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrOldestIndexRead, err)
	}

	if blockIdentifier.Index <= oldestIndex {
		return storageErrs.ErrCannotRemoveOldest
	}

	_, key := getBlockHashKey(blockIdentifier.Hash)
	if err := transaction.Delete(ctx, key); err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrBlockDeleteFailed, err)
	}

	if err := transaction.Delete(ctx, getBlockIndexKey(blockIdentifier.Index)); err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrBlockIndexDeleteFailed, err)
	}

	if err := b.StoreHeadBlockIdentifier(ctx, transaction, block.ParentBlockIdentifier); err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrHeadBlockIdentifierUpdateFailed, err)
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
	transaction := b.db.WriteTransaction(ctx, blockSyncIdentifier, true)
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
	g, gctx := errgroup.WithContextN(ctx, b.workerConcurrency, b.workerConcurrency)
	for i := range block.Transactions {
		// We need to set variable before calling goroutine
		// to avoid getting an updated pointer as loop iteration
		// continues.
		txn := block.Transactions[i]
		g.Go(func() error {
			return b.removeTransaction(
				gctx,
				transaction,
				blockIdentifier,
				txn,
			)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Delete block
	if err := b.deleteBlock(ctx, transaction, block); err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrBlockDeleteFailed, err)
	}

	return b.callWorkersAndCommit(ctx, block, transaction, false)
}

func (b *BlockStorage) callWorkersAndCommit(
	ctx context.Context,
	block *types.Block,
	txn database.Transaction,
	adding bool,
) error {
	commitWorkers := make([]database.CommitWorker, len(b.workers))

	// Provision global errgroup to use for all workers
	// so that we don't need to wait at the end of each worker
	// for all results.
	g, gctx := errgroup.WithContextN(ctx, b.workerConcurrency, b.workerConcurrency)
	for i, w := range b.workers {
		var cw database.CommitWorker
		var err error
		if adding {
			cw, err = w.AddingBlock(gctx, g, block, txn)
		} else {
			cw, err = w.RemovingBlock(gctx, g, block, txn)
		}
		if err != nil {
			return err
		}

		commitWorkers[i] = cw
	}

	if err := g.Wait(); err != nil {
		return err
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
	if errors.Is(err, storageErrs.ErrHeadBlockNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	if head.Index < startIndex {
		return fmt.Errorf(
			"%w: block index is %d but start index is %d",
			storageErrs.ErrLastProcessedBlockPrecedesStart,
			head.Index,
			startIndex,
		)
	}

	// Ensure we do not set a new start index less
	// than the oldest block.
	dbTx := b.db.ReadTransaction(ctx)
	oldestIndex, err := b.GetOldestBlockIndexTransactional(ctx, dbTx)
	dbTx.Discard(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrOldestIndexRead, err)
	}

	if oldestIndex > startIndex {
		return fmt.Errorf(
			"%w: oldest block index is %d but start index is %d",
			storageErrs.ErrCannotAccessPrunedData,
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
func (b *BlockStorage) CreateBlockCache(ctx context.Context, blocks int) []*types.BlockIdentifier {
	cache := []*types.BlockIdentifier{}
	head, err := b.GetHeadBlockIdentifier(ctx)
	if err != nil {
		return cache
	}

	for len(cache) < blocks {
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

func (b *BlockStorage) storeTransaction(
	ctx context.Context,
	transaction database.Transaction,
	blockIdentifier *types.BlockIdentifier,
	tx *types.Transaction,
) error {
	err := b.storeBackwardRelations(ctx, transaction, tx)
	if err != nil {
		return err
	}

	namespace, hashKey := getTransactionKey(blockIdentifier, tx.TransactionIdentifier)
	bt := &blockTransaction{
		Transaction: tx,
		BlockIndex:  blockIdentifier.Index,
	}

	encodedResult, err := b.db.Encoder().Encode(namespace, bt)
	if err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrTransactionDataEncodeFailed, err)
	}

	return storeUniqueKey(ctx, transaction, hashKey, encodedResult, true)
}

func (b *BlockStorage) storeBackwardRelations(
	ctx context.Context,
	transaction database.Transaction,
	tx *types.Transaction,
) error {
	fn := func(ctx context.Context, transaction database.Transaction, key []byte) error {
		err := transaction.Set(ctx, key, []byte{}, true)
		if err != nil {
			return fmt.Errorf("%v: %w", storageErrs.ErrCannotStoreBackwardRelation, err)
		}

		return nil
	}

	return b.modifyBackwardRelations(ctx, transaction, tx, fn)
}

func (b *BlockStorage) removeBackwardRelations(
	ctx context.Context,
	transaction database.Transaction,
	tx *types.Transaction,
) error {
	fn := func(ctx context.Context, transaction database.Transaction, key []byte) error {
		err := transaction.Delete(ctx, key)
		if err != nil {
			return fmt.Errorf("%w: %v", storageErrs.ErrCannotRemoveBackwardRelation, err)
		}

		return nil
	}

	return b.modifyBackwardRelations(ctx, transaction, tx, fn)
}

func (b *BlockStorage) modifyBackwardRelations(
	ctx context.Context,
	transaction database.Transaction,
	tx *types.Transaction,
	fn func(ctx context.Context, transaction database.Transaction, key []byte) error,
) error {
	var backwardRelationKeys [][]byte
	for _, relatedTx := range tx.RelatedTransactions {
		// skip if on another network
		if relatedTx.NetworkIdentifier != nil {
			continue
		}
		if relatedTx.Direction != types.Backward {
			continue
		}

		// skip if related block not found
		block, _, err := b.FindTransaction(ctx, relatedTx.TransactionIdentifier, transaction)
		if err != nil {
			return fmt.Errorf("%v: %w", storageErrs.ErrCannotStoreBackwardRelation, err)
		}
		if block == nil {
			continue
		}

		backwardRelationKeys = append(
			backwardRelationKeys,
			getBackwardRelationKey(relatedTx.TransactionIdentifier, tx.TransactionIdentifier),
		)
	}

	for _, key := range backwardRelationKeys {
		err := fn(ctx, transaction, key)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *BlockStorage) pruneTransaction(
	ctx context.Context,
	transaction database.Transaction,
	blockIdentifier *types.BlockIdentifier,
	txIdentifier *types.TransactionIdentifier,
) error {
	namespace, hashKey := getTransactionKey(blockIdentifier, txIdentifier)
	bt := &blockTransaction{
		BlockIndex: blockIdentifier.Index,
	}

	encodedResult, err := b.db.Encoder().Encode(namespace, bt)
	if err != nil {
		return fmt.Errorf("%w: %v", storageErrs.ErrTransactionDataEncodeFailed, err)
	}

	return transaction.Set(ctx, hashKey, encodedResult, true)
}

func (b *BlockStorage) removeTransaction(
	ctx context.Context,
	transaction database.Transaction,
	blockIdentifier *types.BlockIdentifier,
	tx *types.Transaction,
) error {
	err := b.removeBackwardRelations(ctx, transaction, tx)
	if err != nil {
		return err
	}

	_, hashKey := getTransactionKey(blockIdentifier, tx.TransactionIdentifier)
	return transaction.Delete(ctx, hashKey)
}

func (b *BlockStorage) getAllTransactionsByIdentifier(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	txn database.Transaction,
) ([]*types.BlockTransaction, error) {
	blockTransactions := []*types.BlockTransaction{}
	_, err := txn.Scan(
		ctx,
		getTransactionPrefix(transactionIdentifier),
		getTransactionPrefix(transactionIdentifier),
		func(k []byte, v []byte) error {
			// Decode blockTransaction
			var bt blockTransaction
			if err := b.db.Encoder().Decode(transactionNamespace, v, &bt, false); err != nil {
				return fmt.Errorf("%w: unable to decode block data for transaction", err)
			}

			// Extract hash from key
			splitKey := strings.Split(string(k), "/")
			blockHash := splitKey[len(splitKey)-1]

			blockTransactions = append(blockTransactions, &types.BlockTransaction{
				BlockIdentifier: &types.BlockIdentifier{
					Index: bt.BlockIndex,
					Hash:  blockHash,
				},
				Transaction: bt.Transaction,
			})
			return nil
		},
		false,
		false,
	)
	if err != nil {
		return nil, err
	}

	return blockTransactions, nil
}

// FindTransaction returns the most recent *types.BlockIdentifier containing the
// transaction and the transaction.
func (b *BlockStorage) FindTransaction(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	txn database.Transaction,
) (*types.BlockIdentifier, *types.Transaction, error) {
	blockTransactions, err := b.getAllTransactionsByIdentifier(ctx, transactionIdentifier, txn)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %v", storageErrs.ErrTransactionDBQueryFailed, err)
	}

	if len(blockTransactions) == 0 {
		return nil, nil, nil
	}

	head, err := b.GetHeadBlockIdentifierTransactional(ctx, txn)
	if err != nil {
		return nil, nil, err
	}

	var newestBlock *types.BlockIdentifier
	var newestTransaction *types.Transaction
	var transactionUnsequenced bool
	for _, blockTransaction := range blockTransactions {
		if newestBlock == nil || blockTransaction.BlockIdentifier.Index > newestBlock.Index {
			// Now that we are optimistically storing data, there is a chance
			// we may fetch a transaction from a seen but unsequenced block.
			if head != nil && blockTransaction.BlockIdentifier.Index > head.Index {
				// We have seen a transaction, but it's in a block that is not yet
				// sequenced.
				transactionUnsequenced = true
				continue
			}

			newestBlock = blockTransaction.BlockIdentifier
			// When `blockTransaction` is pruned, `blockTransaction.Transaction` is set to nil
			newestTransaction = blockTransaction.Transaction
		}
	}

	if newestTransaction != nil {
		return newestBlock, newestTransaction, nil
	}

	if !transactionUnsequenced {
		// All matching transaction have been pruned
		return nil, nil, storageErrs.ErrCannotAccessPrunedData
	}

	// A transaction exists but we have not yet sequenced the block it is in
	return nil, nil, nil
}

func (b *BlockStorage) FindRelatedTransactions(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	db database.Transaction,
) (*types.BlockIdentifier, *types.Transaction, []*types.Transaction, error) {
	rootBlock, tx, err := b.FindTransaction(ctx, transactionIdentifier, db)
	if err != nil {
		return nil, nil, nil, err
	}

	if rootBlock == nil {
		return nil, nil, nil, nil
	}

	childIds, err := b.getForwardRelatedTransactions(ctx, tx, db)
	if err != nil {
		return nil, nil, nil, err
	}

	// create map of seen transactions to avoid duplicates
	seen := make(map[string]struct{})
	children := []*types.Transaction{}

	i := 0
	for {
		if i >= len(childIds) {
			break
		}
		childID := childIds[i]
		i++

		// skip duplicates
		if _, ok := seen[childID.Hash]; !ok {
			seen[childID.Hash] = struct{}{}
		} else {
			continue
		}

		childBlock, childTx, err := b.FindTransaction(ctx, childID, db)
		if err != nil {
			return nil, nil, nil, err
		}

		if childBlock == nil {
			return nil, nil, nil, nil
		}

		children = append(children, childTx)
		if rootBlock.Index < childBlock.Index {
			rootBlock = childBlock
		}

		newChildren, err := b.getForwardRelatedTransactions(ctx, childTx, db)
		if err != nil {
			return nil, nil, nil, err
		}
		childIds = append(childIds, newChildren...)
	}

	return rootBlock, tx, children, nil
}

// TODO: add support for relations across multiple networks
func (b *BlockStorage) getForwardRelatedTransactions(
	ctx context.Context,
	tx *types.Transaction,
	db database.Transaction,
) ([]*types.TransactionIdentifier, error) {
	var children []*types.TransactionIdentifier
	for _, relatedTx := range tx.RelatedTransactions {
		// skip if on another network
		if relatedTx.NetworkIdentifier != nil {
			continue
		}

		if relatedTx.Direction == types.Forward {
			children = append(children, relatedTx.TransactionIdentifier)
		}
	}

	// scan db for all transactions where tx appears as a backward relation
	_, err := db.Scan(
		ctx,
		getBackwardRelationKey(tx.TransactionIdentifier, nil),
		getBackwardRelationKey(tx.TransactionIdentifier, nil),
		func(k []byte, v []byte) error {
			ss := strings.Split(string(k), "/")
			txHash := ss[len(ss)-1]
			txID := &types.TransactionIdentifier{Hash: txHash}
			children = append(children, txID)
			return nil
		},
		false,
		false,
	)

	if err != nil {
		return nil, err
	}

	return children, nil
}

func (b *BlockStorage) findBlockTransaction(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
	txn database.Transaction,
) (*types.Transaction, error) {
	oldestIndex, err := b.GetOldestBlockIndexTransactional(ctx, txn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", storageErrs.ErrOldestIndexRead, err)
	}

	if blockIdentifier.Index < oldestIndex {
		return nil, storageErrs.ErrCannotAccessPrunedData
	}

	namespace, key := getTransactionKey(blockIdentifier, transactionIdentifier)
	txExists, tx, err := txn.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", storageErrs.ErrTransactionDBQueryFailed, err)
	}

	if !txExists {
		return nil, fmt.Errorf(
			"%w %s",
			storageErrs.ErrTransactionNotFound,
			transactionIdentifier.Hash,
		)
	}

	var bt blockTransaction
	if err := b.db.Encoder().Decode(namespace, tx, &bt, true); err != nil {
		return nil, fmt.Errorf("%w: unable to decode block data for transaction", err)
	}

	return bt.Transaction, nil
}

// GetBlockTransaction retrieves a transaction belonging to a certain
// block in a database transaction. This is usually used to implement
// /block/transaction.
func (b *BlockStorage) GetBlockTransaction(
	ctx context.Context,
	blockIdentifier *types.BlockIdentifier,
	transactionIdentifier *types.TransactionIdentifier,
) (*types.Transaction, error) {
	transaction := b.db.ReadTransaction(ctx)
	defer transaction.Discard(ctx)

	return b.findBlockTransaction(ctx, blockIdentifier, transactionIdentifier, transaction)
}

// AtTipTransactional returns a boolean indicating if we
// are at tip (provided some acceptable
// tip delay) in a database transaction.
func (b *BlockStorage) AtTipTransactional(
	ctx context.Context,
	tipDelay int64,
	txn database.Transaction,
) (bool, *types.BlockIdentifier, error) {
	blockResponse, err := b.GetBlockLazyTransactional(ctx, nil, txn)
	if errors.Is(err, storageErrs.ErrHeadBlockNotFound) {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, fmt.Errorf("%w: %v", storageErrs.ErrHeadBlockGetFailed, err)
	}
	block := blockResponse.Block

	atTip := utils.AtTip(tipDelay, block.Timestamp)
	if !atTip {
		return false, nil, nil
	}

	return true, block.BlockIdentifier, nil
}

// AtTip returns a boolean indicating if we
// are at tip (provided some acceptable
// tip delay).
func (b *BlockStorage) AtTip(
	ctx context.Context,
	tipDelay int64,
) (bool, *types.BlockIdentifier, error) {
	transaction := b.db.ReadTransaction(ctx)
	defer transaction.Discard(ctx)

	return b.AtTipTransactional(ctx, tipDelay, transaction)
}

// IndexAtTip returns a boolean indicating if a block
// index is at tip (provided some acceptable
// tip delay). If the index is ahead of the head block
// and the head block is at tip, we consider the
// index at tip.
func (b *BlockStorage) IndexAtTip(
	ctx context.Context,
	tipDelay int64,
	index int64,
) (bool, error) {
	transaction := b.db.ReadTransaction(ctx)
	defer transaction.Discard(ctx)
	headBlockResponse, err := b.GetBlockLazyTransactional(ctx, nil, transaction)
	if errors.Is(err, storageErrs.ErrHeadBlockNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Check if index is greater than headBlock and if headBlock
	// is at tip. If so, this is a result of us querying ahead of
	// tip.
	headBlock := headBlockResponse.Block
	if headBlock.BlockIdentifier.Index < index {
		return utils.AtTip(tipDelay, headBlock.Timestamp), nil
	}

	// Query block at index
	blockResponse, err := b.GetBlockLazyTransactional(
		ctx,
		&types.PartialBlockIdentifier{Index: &index},
		transaction,
	)
	if err != nil {
		return false, err
	}
	block := blockResponse.Block

	return utils.AtTip(tipDelay, block.Timestamp), nil
}
