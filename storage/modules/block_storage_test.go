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

package modules

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

const (
	minPruningDepth        = 20
	blockWorkerConcurrency = 10
)

func TestHeadBlockIdentifier(t *testing.T) {
	var (
		newBlockIdentifier = &types.BlockIdentifier{
			Hash:  "blah",
			Index: 0,
		}
		newBlockIdentifier2 = &types.BlockIdentifier{
			Hash:  "blah2",
			Index: 1,
		}
	)

	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(database, blockWorkerConcurrency)

	t.Run("No head block set", func(t *testing.T) {
		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx)
		assert.EqualError(t, err, storageErrs.ErrHeadBlockNotFound.Error())
		assert.Nil(t, blockIdentifier)
	})

	t.Run("Set and get head block", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn, newBlockIdentifier))
		assert.NoError(t, txn.Commit(ctx))

		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlockIdentifier, blockIdentifier)
	})

	t.Run("Discard head block update", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn,
			&types.BlockIdentifier{
				Hash:  "no blah",
				Index: 10,
			}),
		)
		txn.Discard(ctx)

		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlockIdentifier, blockIdentifier)
	})

	t.Run("Multiple updates to head block", func(t *testing.T) {
		txn := storage.db.Transaction(ctx)
		assert.NoError(t, storage.StoreHeadBlockIdentifier(ctx, txn, newBlockIdentifier2))
		assert.NoError(t, txn.Commit(ctx))

		blockIdentifier, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		txn.Discard(ctx)
		assert.Equal(t, newBlockIdentifier2, blockIdentifier)
	})
}

func simpleTransactionFactory(
	hash string,
	address string,
	value string,
	currency *types.Currency,
) *types.Transaction {
	return &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: hash,
		},
		Operations: []*types.Operation{
			{
				OperationIdentifier: &types.OperationIdentifier{
					Index: 0,
				},
				Type:   "Transfer",
				Status: types.String("Success"),
				Account: &types.AccountIdentifier{
					Address: address,
				},
				Amount: &types.Amount{
					Value:    value,
					Currency: currency,
				},
			},
		},
	}
}

func addRelatedTransaction(
	transaction *types.Transaction,
	hash string,
	direction types.Direction,
) *types.Transaction {
	relatedTx := &types.RelatedTransaction{
		NetworkIdentifier: nil,
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: hash,
		},
		Direction: direction,
	}

	transaction.RelatedTransactions = append(transaction.RelatedTransactions, relatedTx)
	return transaction
}

var (
	genesisBlock = &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 0",
			Index: 0,
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 0",
			Index: 0,
		},
		Timestamp: 1,
	}

	newBlock = &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 1",
			Index: 1,
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 0",
			Index: 0,
		},
		Timestamp: 1,
		Transactions: []*types.Transaction{
			simpleTransactionFactory("blahTx", "addr1", "100", &types.Currency{Symbol: "hello"}),
		},
	}

	badBlockIdentifier = &types.BlockIdentifier{
		Hash:  "missing blah",
		Index: 0,
	}

	newBlock2 = &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 2",
			Index: 2,
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 1",
			Index: 1,
		},
		Timestamp: 1,
		Transactions: []*types.Transaction{
			simpleTransactionFactory("blahTx", "addr1", "100", &types.Currency{Symbol: "hello"}),
		},
	}
	lazyBlock2 = &types.BlockResponse{
		Block: &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "blah 2",
				Index: 2,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "blah 1",
				Index: 1,
			},
			Timestamp: 1,
		},
		OtherTransactions: []*types.TransactionIdentifier{
			{
				Hash: "blahTx",
			},
		},
	}

	complexBlock = &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 3",
			Index: 3,
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 2",
			Index: 2,
		},
		Timestamp: 1,
		Transactions: []*types.Transaction{
			{
				TransactionIdentifier: &types.TransactionIdentifier{
					Hash: "blahTx 2",
				},
				Operations: []*types.Operation{
					{
						OperationIdentifier: &types.OperationIdentifier{
							Index: 0,
						},
						Type:   "Transfer",
						Status: types.String("Success"),
						Account: &types.AccountIdentifier{
							Address: "addr1",
							SubAccount: &types.SubAccountIdentifier{
								Address: "staking",
								Metadata: map[string]interface{}{
									"other_complex_stuff": []interface{}{
										map[string]interface{}{
											"neat": "test",
											"more complex": map[string]interface{}{
												"neater": "testier",
											},
										},
										map[string]interface{}{
											"i love": "ice cream",
										},
									},
								},
							},
						},
						Amount: &types.Amount{
							Value: "100",
							Currency: &types.Currency{
								Symbol: "hello",
							},
						},
					},
				},
				Metadata: map[string]interface{}{
					"other_stuff":  []interface{}{"stuff"},
					"simple_stuff": "abc",
					"super_complex_stuff": map[string]interface{}{
						"neat": "test",
						"more complex": map[string]interface{}{
							"neater": "testier",
						},
					},
				},
			},
		},
	}

	duplicateTxBlock = &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 4",
			Index: 4,
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 3",
			Index: 3,
		},
		Timestamp: 1,
		Transactions: []*types.Transaction{
			simpleTransactionFactory("blahTx3", "addr2", "200", &types.Currency{Symbol: "hello"}),
			simpleTransactionFactory("blahTx3", "addr2", "200", &types.Currency{Symbol: "hello"}),
		},
	}

	gapBlock = &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Hash:  "block 10",
			Index: 10,
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Hash:  "blah 3",
			Index: 3,
		},
	}
)

func findTransactionWithDbTransaction(
	ctx context.Context,
	storage *BlockStorage,
	transactionIdentifier *types.TransactionIdentifier,
) (*types.BlockIdentifier, *types.Transaction, error) {
	txn := storage.db.ReadTransaction(ctx)
	defer txn.Discard(ctx)

	return storage.FindTransaction(
		ctx,
		transactionIdentifier,
		txn,
	)
}

func TestBlock(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(database, blockWorkerConcurrency)

	t.Run("Get non-existent tx", func(t *testing.T) {
		newestBlock, transaction, err := findTransactionWithDbTransaction(
			ctx,
			storage,
			newBlock.Transactions[0].TransactionIdentifier,
		)
		assert.NoError(t, err)
		assert.Nil(t, newestBlock)
		assert.Nil(t, transaction)
	})

	t.Run("Attempt Block Pruning Before Syncing", func(t *testing.T) {
		firstPruned, lastPruned, err := storage.Prune(ctx, 2, minPruningDepth)
		assert.Equal(t, int64(-1), firstPruned)
		assert.Equal(t, int64(-1), lastPruned)
		assert.True(t, errors.Is(err, storageErrs.ErrOldestIndexMissing))
	})

	t.Run("Set genesis block", func(t *testing.T) {
		oldestIndex, err := storage.GetOldestBlockIndex(ctx)
		assert.Equal(t, int64(-1), oldestIndex)
		assert.Error(t, storageErrs.ErrOldestIndexMissing, err)

		err = storage.SeeBlock(ctx, genesisBlock)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, genesisBlock)
		assert.NoError(t, err)

		oldestIndex, err = storage.GetOldestBlockIndex(ctx)
		assert.Equal(t, int64(0), oldestIndex)
		assert.NoError(t, err)

		// Ensure we error if trying to remove genesis
		err = storage.RemoveBlock(ctx, genesisBlock.BlockIdentifier)
		assert.Contains(t, err.Error(), storageErrs.ErrCannotRemoveOldest.Error())
	})

	t.Run("Set and get block", func(t *testing.T) {
		err = storage.SeeBlock(ctx, newBlock)
		assert.NoError(t, err)

		// Attempt to find transaction before it's synced
		transaction := storage.db.ReadTransaction(ctx)
		newestBlock, newestTransaction, err := storage.FindTransaction(
			ctx,
			newBlock.Transactions[0].TransactionIdentifier,
			transaction,
		)
		assert.NoError(t, err)
		assert.Nil(t, newestBlock)
		assert.Nil(t, newestTransaction)
		transaction.Discard(ctx)

		// Ensure we can FindTransaction in add block transaction.
		transaction = storage.db.WriteTransaction(ctx, blockSyncIdentifier, true)
		// Sequence the block so the transaction can be found
		err = storage.storeBlock(ctx, transaction, newBlock.BlockIdentifier)
		assert.NoError(t, err)

		newestBlock, newestTransaction, err = storage.FindTransaction(
			ctx,
			newBlock.Transactions[0].TransactionIdentifier,
			transaction,
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock.BlockIdentifier, newestBlock)
		assert.Equal(t, newBlock.Transactions[0], newestTransaction)

		assert.NoError(t, transaction.Commit(ctx))

		block, err := storage.GetBlock(
			ctx,
			types.ConstructPartialBlockIdentifier(newBlock.BlockIdentifier),
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock, block)

		block, err = storage.GetBlock(
			ctx,
			&types.PartialBlockIdentifier{Index: &newBlock.BlockIdentifier.Index},
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock, block)

		block, err = storage.GetBlock(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, newBlock, block)

		head, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlock.BlockIdentifier, head)

		oldestIndex, err := storage.GetOldestBlockIndex(ctx)
		assert.Equal(t, int64(0), oldestIndex)
		assert.NoError(t, err)
	})

	t.Run("Get non-existent block", func(t *testing.T) {
		identifier := types.ConstructPartialBlockIdentifier(badBlockIdentifier)
		block, err := storage.GetBlock(ctx, identifier)
		assert.True(
			t,
			errors.Is(err, storageErrs.ErrBlockNotFound),
		)
		assert.Nil(t, block)

		canonical, err := storage.CanonicalBlock(ctx, badBlockIdentifier)
		assert.False(t, canonical)
		assert.NoError(t, err)
	})

	t.Run("Get non-existent block index", func(t *testing.T) {
		badIndex := int64(100000)
		identifier := &types.PartialBlockIdentifier{Index: &badIndex}
		block, err := storage.GetBlock(ctx, identifier)
		assert.True(
			t,
			errors.Is(err, storageErrs.ErrBlockNotFound),
		)
		assert.Nil(t, block)
	})

	t.Run("Set duplicate block hash", func(t *testing.T) {
		err = storage.AddBlock(ctx, newBlock)
		assert.Contains(t, err.Error(), storageErrs.ErrDuplicateKey.Error())
	})

	t.Run("Set duplicate transaction hash (from prior block)", func(t *testing.T) {
		err = storage.SeeBlock(ctx, newBlock2)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, newBlock2)
		assert.NoError(t, err)

		oldestIndex, err := storage.GetOldestBlockIndex(ctx)
		assert.Equal(t, int64(0), oldestIndex)
		assert.NoError(t, err)

		block, err := storage.GetBlock(
			ctx,
			types.ConstructPartialBlockIdentifier(newBlock2.BlockIdentifier),
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2, block)

		block, err = storage.GetBlock(
			ctx,
			&types.PartialBlockIdentifier{Index: &newBlock2.BlockIdentifier.Index},
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2, block)

		block, err = storage.GetBlock(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2, block)

		blockLazy, err := storage.GetBlockLazy(ctx, nil)
		assert.NoError(t, err)
		assert.Equal(t, lazyBlock2, blockLazy)

		blockTransaction, err := storage.GetBlockTransaction(
			ctx,
			blockLazy.Block.BlockIdentifier,
			blockLazy.OtherTransactions[0],
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2.Transactions[0], blockTransaction)

		head, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2.BlockIdentifier, head)

		newestBlock, transaction, err := findTransactionWithDbTransaction(
			ctx,
			storage,
			newBlock.Transactions[0].TransactionIdentifier,
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2.BlockIdentifier, newestBlock)
		assert.Equal(t, newBlock2.Transactions[0], transaction)
	})

	t.Run("Remove block and re-set block of same hash", func(t *testing.T) {
		err := storage.RemoveBlock(ctx, newBlock2.BlockIdentifier)
		assert.NoError(t, err)

		canonical, err := storage.CanonicalBlock(ctx, newBlock2.BlockIdentifier)
		assert.False(t, canonical)
		assert.NoError(t, err)

		oldestIndex, err := storage.GetOldestBlockIndex(ctx)
		assert.Equal(t, int64(0), oldestIndex)
		assert.NoError(t, err)

		head, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2.ParentBlockIdentifier, head)

		err = storage.SeeBlock(ctx, newBlock2)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, newBlock2)
		assert.NoError(t, err)

		head, err = storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2.BlockIdentifier, head)

		newestBlock, transaction, err := findTransactionWithDbTransaction(
			ctx,
			storage,
			newBlock.Transactions[0].TransactionIdentifier,
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2.BlockIdentifier, newestBlock)
		assert.Equal(t, newBlock2.Transactions[0], transaction)
	})

	t.Run("Add block with complex metadata", func(t *testing.T) {
		err := storage.SeeBlock(ctx, complexBlock)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, complexBlock)
		assert.NoError(t, err)

		oldestIndex, err := storage.GetOldestBlockIndex(ctx)
		assert.Equal(t, int64(0), oldestIndex)
		assert.NoError(t, err)

		block, err := storage.GetBlock(
			ctx,
			types.ConstructPartialBlockIdentifier(complexBlock.BlockIdentifier),
		)
		assert.NoError(t, err)
		assert.Equal(t, complexBlock, block)

		head, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, complexBlock.BlockIdentifier, head)

		newestBlock, transaction, err := findTransactionWithDbTransaction(
			ctx,
			storage,
			newBlock.Transactions[0].TransactionIdentifier,
		)
		assert.NoError(t, err)
		assert.Equal(t, newBlock2.BlockIdentifier, newestBlock)
		assert.Equal(t, newBlock2.Transactions[0], transaction)
	})

	t.Run("Set duplicate transaction hash (same block)", func(t *testing.T) {
		err = storage.SeeBlock(ctx, duplicateTxBlock)
		assert.Contains(t, err.Error(), storageErrs.ErrDuplicateTransactionHash.Error())

		head, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, complexBlock.BlockIdentifier, head)
	})

	t.Run("Add block after omitted", func(t *testing.T) {
		err := storage.SeeBlock(ctx, gapBlock)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, gapBlock)
		assert.NoError(t, err)

		block, err := storage.GetBlock(
			ctx,
			types.ConstructPartialBlockIdentifier(gapBlock.BlockIdentifier),
		)
		assert.NoError(t, err)
		assert.Equal(t, gapBlock, block)

		head, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, gapBlock.BlockIdentifier, head)
	})

	t.Run("Orphan gap block", func(t *testing.T) {
		err := storage.RemoveBlock(ctx, gapBlock.BlockIdentifier)
		assert.NoError(t, err)

		head, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, gapBlock.ParentBlockIdentifier, head)

		err = storage.SeeBlock(ctx, gapBlock)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, gapBlock)
		assert.NoError(t, err)

		block, err := storage.GetBlock(
			ctx,
			types.ConstructPartialBlockIdentifier(gapBlock.BlockIdentifier),
		)
		assert.NoError(t, err)
		assert.Equal(t, gapBlock, block)

		head, err = storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, gapBlock.BlockIdentifier, head)
	})

	t.Run("Attempt Block Pruning", func(t *testing.T) {
		firstPruned, lastPruned, err := storage.Prune(ctx, 2, minPruningDepth)
		assert.Equal(t, int64(-1), firstPruned)
		assert.Equal(t, int64(-1), lastPruned)
		assert.NoError(t, err)

		firstPruned, lastPruned, err = storage.Prune(ctx, 100, minPruningDepth)
		assert.Equal(t, int64(-1), firstPruned)
		assert.Equal(t, int64(-1), lastPruned)
		assert.NoError(t, err)

		// Attempt to sync sufficient blocks so we can test pruning
		for i := gapBlock.BlockIdentifier.Index + 1; i < 200; i++ {
			blockIdentifier := &types.BlockIdentifier{
				Index: i,
				Hash:  fmt.Sprintf("block %d", i),
			}
			parentBlockIndex := blockIdentifier.Index - 1
			if parentBlockIndex < 0 {
				parentBlockIndex = 0
			}
			parentBlockIdentifier := &types.BlockIdentifier{
				Index: parentBlockIndex,
				Hash:  fmt.Sprintf("block %d", parentBlockIndex),
			}

			block := &types.Block{
				BlockIdentifier:       blockIdentifier,
				ParentBlockIdentifier: parentBlockIdentifier,
			}

			assert.NoError(t, storage.SeeBlock(ctx, block))
			assert.NoError(t, storage.AddBlock(ctx, block))
			head, err := storage.GetHeadBlockIdentifier(ctx)
			assert.NoError(t, err)
			assert.Equal(t, blockIdentifier, head)

			canonical, err := storage.CanonicalBlock(
				ctx,
				block.BlockIdentifier,
			)
			assert.True(t, canonical)
			assert.NoError(t, err)
		}

		firstPruned, lastPruned, err = storage.Prune(ctx, 100, minPruningDepth)
		assert.Equal(t, int64(0), firstPruned)
		assert.Equal(t, int64(100), lastPruned)
		assert.NoError(t, err)

		oldestIndex, err := storage.GetOldestBlockIndex(ctx)
		assert.Equal(t, int64(101), oldestIndex)
		assert.NoError(t, err)

		block, err := storage.GetBlock(
			ctx,
			types.ConstructPartialBlockIdentifier(newBlock.BlockIdentifier),
		)
		assert.True(t, errors.Is(err, storageErrs.ErrCannotAccessPrunedData))
		assert.Nil(t, block)

		canonical, err := storage.CanonicalBlock(
			ctx,
			newBlock.BlockIdentifier,
		)
		assert.True(t, canonical)
		assert.NoError(t, err)

		blockTransaction, err := storage.GetBlockTransaction(
			ctx,
			newBlock2.BlockIdentifier,
			newBlock2.Transactions[0].TransactionIdentifier,
		)
		assert.True(t, errors.Is(err, storageErrs.ErrCannotAccessPrunedData))
		assert.Nil(t, blockTransaction)

		newestBlock, transaction, err := findTransactionWithDbTransaction(
			ctx,
			storage,
			newBlock.Transactions[0].TransactionIdentifier,
		)
		assert.True(t, errors.Is(err, storageErrs.ErrCannotAccessPrunedData))
		assert.Nil(t, newestBlock)
		assert.Nil(t, transaction)
	})
}

func TestManyBlocks(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(database, blockWorkerConcurrency)

	for i := int64(0); i < 10000; i++ {
		blockIdentifier := &types.BlockIdentifier{
			Index: i,
			Hash:  fmt.Sprintf("block %d", i),
		}
		parentBlockIndex := blockIdentifier.Index - 1
		if parentBlockIndex < 0 {
			parentBlockIndex = 0
		}
		parentBlockIdentifier := &types.BlockIdentifier{
			Index: parentBlockIndex,
			Hash:  fmt.Sprintf("block %d", parentBlockIndex),
		}

		block := &types.Block{
			BlockIdentifier:       blockIdentifier,
			ParentBlockIdentifier: parentBlockIdentifier,
		}

		assert.NoError(t, storage.AddBlock(ctx, block))
		head, err := storage.GetHeadBlockIdentifier(ctx)
		assert.NoError(t, err)
		assert.Equal(t, blockIdentifier, head)
	}

	firstPruned, lastPruned, err := storage.Prune(ctx, 9959, minPruningDepth)
	assert.Equal(t, int64(0), firstPruned)
	assert.Equal(t, int64(9959), lastPruned)
	assert.NoError(t, err)

	oldestIndex, err := storage.GetOldestBlockIndex(ctx)
	assert.Equal(t, int64(9960), oldestIndex)
	assert.NoError(t, err)

	// Attempt to set new start index in pruned territory
	err = storage.SetNewStartIndex(ctx, 1000)
	assert.True(t, errors.Is(err, storageErrs.ErrCannotAccessPrunedData))
}

func TestCreateBlockCache(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(database, blockWorkerConcurrency)

	t.Run("no blocks processed", func(t *testing.T) {
		assert.Equal(t, []*types.BlockIdentifier{}, storage.CreateBlockCache(ctx, minPruningDepth))
	})

	t.Run("1 block processed", func(t *testing.T) {
		err = storage.SeeBlock(ctx, genesisBlock)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, genesisBlock)
		assert.NoError(t, err)
		assert.Equal(
			t,
			[]*types.BlockIdentifier{genesisBlock.BlockIdentifier},
			storage.CreateBlockCache(ctx, minPruningDepth),
		)
	})

	t.Run("2 blocks processed", func(t *testing.T) {
		err = storage.SeeBlock(ctx, newBlock)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, newBlock)
		assert.NoError(t, err)
		assert.Equal(
			t,
			[]*types.BlockIdentifier{genesisBlock.BlockIdentifier, newBlock.BlockIdentifier},
			storage.CreateBlockCache(ctx, minPruningDepth),
		)
	})

	t.Run("3 blocks processed (with gap)", func(t *testing.T) {
		simpleGap := &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "block 100",
				Index: 100,
			},
			ParentBlockIdentifier: newBlock.BlockIdentifier,
		}

		err = storage.SeeBlock(ctx, simpleGap)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, simpleGap)
		assert.NoError(t, err)
		assert.Equal(
			t,
			[]*types.BlockIdentifier{
				genesisBlock.BlockIdentifier,
				newBlock.BlockIdentifier,
				simpleGap.BlockIdentifier,
			},
			storage.CreateBlockCache(ctx, minPruningDepth),
		)
	})
}

func TestAtTip(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(database, blockWorkerConcurrency)
	tipDelay := int64(100)

	t.Run("no blocks processed", func(t *testing.T) {
		atTip, blockIdentifier, err := storage.AtTip(ctx, tipDelay)
		assert.NoError(t, err)
		assert.False(t, atTip)
		assert.Nil(t, blockIdentifier)

		atTip, err = storage.IndexAtTip(ctx, tipDelay, 1)
		assert.NoError(t, err)
		assert.False(t, atTip)
	})

	t.Run("Add old block", func(t *testing.T) {
		b := &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "block 0",
				Index: 0,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "block 0",
				Index: 0,
			},
			Timestamp: utils.Milliseconds() - (3 * tipDelay * utils.MillisecondsInSecond),
		}
		err := storage.SeeBlock(ctx, b)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, b)
		assert.NoError(t, err)

		atTip, blockIdentifier, err := storage.AtTip(ctx, tipDelay)
		assert.NoError(t, err)
		assert.False(t, atTip)
		assert.Nil(t, blockIdentifier)

		atTip, err = storage.IndexAtTip(ctx, tipDelay, 1)
		assert.NoError(t, err)
		assert.False(t, atTip)
	})

	t.Run("Add new block", func(t *testing.T) {
		b := &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "block 1",
				Index: 1,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "block 0",
				Index: 0,
			},
			Timestamp: utils.Milliseconds(),
		}
		err := storage.SeeBlock(ctx, b)
		assert.NoError(t, err)

		err = storage.AddBlock(ctx, b)
		assert.NoError(t, err)

		atTip, blockIdentifier, err := storage.AtTip(ctx, tipDelay)
		assert.NoError(t, err)
		assert.True(t, atTip)
		assert.Equal(t, &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}, blockIdentifier)

		atTip, err = storage.IndexAtTip(ctx, tipDelay, 1)
		assert.NoError(t, err)
		assert.True(t, atTip)

		atTip, err = storage.IndexAtTip(ctx, tipDelay, 2)
		assert.NoError(t, err)
		assert.True(t, atTip)
	})
}

func TestRelatedTransactions(t *testing.T) {
	// setup
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBlockStorage(database, blockWorkerConcurrency)

	t.Run("test forward and backward relations", func(t *testing.T) {
		err = storage.SeeBlock(ctx, genesisBlock)
		assert.NoError(t, err)
		err = storage.AddBlock(ctx, genesisBlock)
		assert.NoError(t, err)

		block1 := &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "blah 1",
				Index: 1,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "blah 0",
				Index: 0,
			},
			Timestamp: 1,
			Transactions: []*types.Transaction{
				addRelatedTransaction(
					simpleTransactionFactory(
						"parentTx",
						"addr1",
						"100",
						&types.Currency{Symbol: "hello"},
					),
					"childTx",
					types.Forward,
				),
				simpleTransactionFactory(
					"backwardRelative",
					"addr2",
					"100",
					&types.Currency{Symbol: "hello"},
				),
			},
		}
		err = storage.SeeBlock(ctx, block1)
		assert.NoError(t, err)
		err = storage.AddBlock(ctx, block1)
		assert.NoError(t, err)

		block2 := &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "blah 2",
				Index: 2,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "blah 1",
				Index: 1,
			},
			Timestamp: 1,
			Transactions: []*types.Transaction{
				simpleTransactionFactory(
					"childTx",
					"addr3",
					"100",
					&types.Currency{Symbol: "hello"},
				),
				addRelatedTransaction(
					simpleTransactionFactory(
						"backwardTx",
						"addr4",
						"100",
						&types.Currency{Symbol: "hello"},
					),
					"backwardRelative",
					types.Backward,
				),
				addRelatedTransaction(
					simpleTransactionFactory(
						"badForward",
						"addr5",
						"100",
						&types.Currency{Symbol: "hello"},
					),
					"invalid",
					types.Forward,
				),
			},
		}
		err = storage.SeeBlock(ctx, block2)
		assert.NoError(t, err)
		err = storage.AddBlock(ctx, block2)
		assert.NoError(t, err)

		_, _, related, err := storage.FindRelatedTransactions(
			ctx,
			block1.Transactions[0].TransactionIdentifier,
			storage.db.ReadTransaction(ctx),
		)
		assert.NoError(t, err)
		assert.Equal(t, len(related), 1)
		assert.Equal(
			t,
			related[0].TransactionIdentifier.Hash,
			block2.Transactions[0].TransactionIdentifier.Hash,
		)

		_, _, related, err = storage.FindRelatedTransactions(
			ctx,
			block1.Transactions[1].TransactionIdentifier,
			storage.db.ReadTransaction(ctx),
		)
		assert.NoError(t, err)
		assert.Equal(t, len(related), 1)
		assert.Equal(
			t,
			related[0].TransactionIdentifier.Hash,
			block2.Transactions[1].TransactionIdentifier.Hash,
		)

		blockID, tx, related, err := storage.FindRelatedTransactions(
			ctx,
			block2.Transactions[2].TransactionIdentifier,
			storage.db.ReadTransaction(ctx),
		)
		assert.NoError(t, err)
		assert.Nil(t, blockID)
		assert.Nil(t, tx)
		assert.Empty(t, related)
	})
}
