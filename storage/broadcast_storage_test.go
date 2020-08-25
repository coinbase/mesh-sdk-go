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
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/stretchr/testify/assert"
)

const (
	confirmationDepth   = int64(2)
	staleDepth          = int64(3)
	broadcastLimit      = 3
	broadcastTipDelay   = 10
	broadcastBehindTip  = false
	blockBroadcastLimit = 10
)

func blockFiller(start int64, end int64) []*types.Block {
	blocks := []*types.Block{}
	for i := start; i < end; i++ {
		parentIndex := i - 1
		if parentIndex < 0 {
			parentIndex = 0
		}
		blocks = append(blocks, &types.Block{
			BlockIdentifier: &types.BlockIdentifier{
				Index: i,
				Hash:  fmt.Sprintf("block %d", i),
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Index: parentIndex,
				Hash:  fmt.Sprintf("block %d", parentIndex),
			},
		})
	}

	return blocks
}

func opFiller(sender string, opNumber int) []*types.Operation {
	ops := make([]*types.Operation, opNumber)
	for i := 0; i < opNumber; i++ {
		ops[i] = &types.Operation{
			OperationIdentifier: &types.OperationIdentifier{
				Index: int64(i),
			},
			Account: &types.AccountIdentifier{
				Address: sender,
				SubAccount: &types.SubAccountIdentifier{
					Address: sender,
				},
			},
		}
	}

	return ops
}

func TestBroadcastStorageBroadcastSuccess(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBroadcastStorage(
		database,
		staleDepth,
		broadcastLimit,
		broadcastTipDelay,
		broadcastBehindTip,
		blockBroadcastLimit,
	)
	mockHelper := &MockBroadcastStorageHelper{}
	mockHandler := &MockBroadcastStorageHandler{}
	storage.Initialize(mockHelper, mockHandler)

	send1 := opFiller("addr 1", 11)
	send2 := opFiller("addr 2", 13)
	mockHelper.Transactions = map[string]*types.TransactionIdentifier{
		"payload 1": {Hash: "tx 1"},
		"payload 2": {Hash: "tx 2"},
	}
	network := &types.NetworkIdentifier{Blockchain: "Bitcoin", Network: "Testnet3"}

	mockHelper.AtSyncTip = true

	t.Run("broadcast send 1 before block exists", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, true)
		defer dbTx.Discard(ctx)

		err := storage.Broadcast(
			ctx,
			dbTx,
			"broadcast 1",
			network,
			send1,
			&types.TransactionIdentifier{Hash: "tx 1"},
			"payload 1",
			confirmationDepth,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 1)
		assert.ElementsMatch(t, []string{"addr 1"}, addresses)

		assert.NoError(t, dbTx.Commit(ctx))

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)
	})

	blocks := blockFiller(0, 6)
	t.Run("add block 0", func(t *testing.T) {
		block := blocks[0]

		txn := storage.db.NewDatabaseTransaction(ctx, true)
		commitWorker, err := storage.AddingBlock(ctx, block, txn)
		assert.NoError(t, err)
		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
		err = commitWorker(ctx)
		assert.NoError(t, err)

		addresses, err := storage.LockedAddresses(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, addresses, 1)
		assert.ElementsMatch(t, []string{"addr 1"}, addresses)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				LastBroadcast:         blocks[0].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)

		assert.ElementsMatch(t, []*types.TransactionIdentifier{}, mockHandler.Stale)
		assert.ElementsMatch(t, []*failedTx{}, mockHandler.Failed)
	})

	t.Run("add block 1", func(t *testing.T) {
		block := blocks[1]

		txn := storage.db.NewDatabaseTransaction(ctx, true)
		commitWorker, err := storage.AddingBlock(ctx, block, txn)
		assert.NoError(t, err)
		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
		err = commitWorker(ctx)
		assert.NoError(t, err)

		addresses, err := storage.LockedAddresses(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, addresses, 1)
		assert.ElementsMatch(t, []string{"addr 1"}, addresses)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				LastBroadcast:         blocks[0].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)

		assert.ElementsMatch(t, []*types.TransactionIdentifier{}, mockHandler.Stale)
		assert.ElementsMatch(t, []*failedTx{}, mockHandler.Failed)
	})

	t.Run("broadcast send 2 after adding a block", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, true)
		defer dbTx.Discard(ctx)

		err := storage.Broadcast(
			ctx,
			dbTx,
			"broadcast 2",
			network,
			send2,
			&types.TransactionIdentifier{Hash: "tx 2"},
			"payload 2",
			confirmationDepth,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 2)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2"}, addresses)

		assert.NoError(t, dbTx.Commit(ctx))
		assert.NoError(t, storage.BroadcastAll(ctx, true))

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				LastBroadcast:         blocks[0].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				LastBroadcast:         blocks[1].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)
	})

	t.Run("add block 2", func(t *testing.T) {
		block := blocks[2]

		txn := storage.db.NewDatabaseTransaction(ctx, true)
		commitWorker, err := storage.AddingBlock(ctx, block, txn)
		assert.NoError(t, err)

		addresses, err := storage.LockedAddresses(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, addresses, 2)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2"}, addresses)

		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
		err = commitWorker(ctx)
		assert.NoError(t, err)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				LastBroadcast:         blocks[2].BlockIdentifier,
				Broadcasts:            2,
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				LastBroadcast:         blocks[1].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)

		assert.ElementsMatch(t, []*types.TransactionIdentifier{
			{
				Hash: "tx 1",
			},
		}, mockHandler.Stale)
		assert.ElementsMatch(t, []*failedTx{}, mockHandler.Failed)
	})

	tx1 := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
		Operations:            send1,
	}
	tx2 := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
		Operations:            send2,
	}
	mockHelper.FindTransactions = map[string]*findTx{
		tx1.TransactionIdentifier.Hash: {
			blockIdentifier: blocks[3].BlockIdentifier,
			transaction:     tx1,
		},
		tx2.TransactionIdentifier.Hash: {
			blockIdentifier: blocks[4].BlockIdentifier,
			transaction:     tx2,
		},
	}
	t.Run("add block 3", func(t *testing.T) {
		block := blocks[3]
		block.Transactions = []*types.Transaction{tx1}

		txn := storage.db.NewDatabaseTransaction(ctx, true)
		commitWorker, err := storage.AddingBlock(ctx, block, txn)
		assert.NoError(t, err)
		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
		err = commitWorker(ctx)
		assert.NoError(t, err)

		addresses, err := storage.LockedAddresses(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, addresses, 2)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2"}, addresses)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				LastBroadcast:         blocks[2].BlockIdentifier,
				Broadcasts:            2,
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				LastBroadcast:         blocks[1].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)

		assert.ElementsMatch(t, []*types.TransactionIdentifier{
			{
				Hash: "tx 1",
			},
		}, mockHandler.Stale)
		assert.ElementsMatch(t, []*failedTx{}, mockHandler.Failed)
		assert.ElementsMatch(t, []*confirmedTx{}, mockHandler.Confirmed)
	})

	t.Run("add block 4", func(t *testing.T) {
		block := blocks[4]
		block.Transactions = []*types.Transaction{tx2}

		txn := storage.db.NewDatabaseTransaction(ctx, true)
		commitWorker, err := storage.AddingBlock(ctx, block, txn)
		assert.NoError(t, err)

		addresses, err := storage.LockedAddresses(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, addresses, 1)
		assert.ElementsMatch(t, []string{"addr 2"}, addresses)

		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
		err = commitWorker(ctx)
		assert.NoError(t, err)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				LastBroadcast:         blocks[1].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)
		assert.ElementsMatch(t, []*types.TransactionIdentifier{
			{
				Hash: "tx 1",
			},
		}, mockHandler.Stale)
		assert.ElementsMatch(t, []*failedTx{}, mockHandler.Failed)
		assert.ElementsMatch(t, []*confirmedTx{
			{
				blockIdentifier: blocks[3].BlockIdentifier,
				transaction:     tx1,
				intent:          send1,
			},
		}, mockHandler.Confirmed)
	})

	t.Run("add block 5", func(t *testing.T) {
		block := blocks[5]

		txn := storage.db.NewDatabaseTransaction(ctx, true)
		commitWorker, err := storage.AddingBlock(ctx, block, txn)
		assert.NoError(t, err)

		addresses, err := storage.LockedAddresses(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, addresses, 0)
		assert.ElementsMatch(t, []string{}, addresses)

		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
		err = commitWorker(ctx)
		assert.NoError(t, err)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{}, broadcasts)

		assert.ElementsMatch(t, []*types.TransactionIdentifier{
			{
				Hash: "tx 1",
			},
		}, mockHandler.Stale)
		assert.ElementsMatch(t, []*failedTx{}, mockHandler.Failed)
		assert.ElementsMatch(t, []*confirmedTx{
			{
				blockIdentifier: blocks[3].BlockIdentifier,
				transaction:     tx1,
				intent:          send1,
			},
			{
				blockIdentifier: blocks[4].BlockIdentifier,
				transaction:     tx2,
				intent:          send2,
			},
		}, mockHandler.Confirmed)
	})
}

func TestBroadcastStorageBroadcastFailure(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBroadcastStorage(
		database,
		staleDepth,
		broadcastLimit,
		broadcastTipDelay,
		broadcastBehindTip,
		blockBroadcastLimit,
	)
	mockHelper := &MockBroadcastStorageHelper{}
	mockHandler := &MockBroadcastStorageHandler{}
	storage.Initialize(mockHelper, mockHandler)

	t.Run("locked addresses with no broadcasts", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, false)
		defer dbTx.Discard(ctx)

		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 0)
		assert.ElementsMatch(t, []string{}, addresses)
	})

	send1 := opFiller("addr 1", 11)
	send1 = append(send1, &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: int64(len(send1)),
		},
		Account: &types.AccountIdentifier{
			Address: "addr 3",
		},
	})

	send2 := opFiller("addr 2", 13)
	network := &types.NetworkIdentifier{Blockchain: "Bitcoin", Network: "Testnet3"}
	t.Run("broadcast", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, true)
		defer dbTx.Discard(ctx)

		err := storage.Broadcast(
			ctx,
			dbTx,
			"broadcast 1",
			network,
			send1,
			&types.TransactionIdentifier{Hash: "tx 1"},
			"payload 1",
			confirmationDepth,
		)
		assert.NoError(t, err)

		err = storage.Broadcast(
			ctx,
			dbTx,
			"broadcast 2",
			network,
			send2,
			&types.TransactionIdentifier{Hash: "tx 2"},
			"payload 2",
			confirmationDepth,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 3)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2", "addr 3"}, addresses)

		assert.NoError(t, dbTx.Commit(ctx))
		assert.NoError(t, storage.BroadcastAll(ctx, true))

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)
	})

	t.Run("add blocks and expire", func(t *testing.T) {
		mockHelper.Transactions = map[string]*types.TransactionIdentifier{
			"payload 1": {Hash: "tx 1"},
			// payload 2 will fail
		}
		blocks := blockFiller(0, 10)
		mockHelper.AtSyncTip = true
		for _, block := range blocks {
			txn := storage.db.NewDatabaseTransaction(ctx, true)
			commitWorker, err := storage.AddingBlock(ctx, block, txn)
			assert.NoError(t, err)
			err = txn.Commit(ctx)
			assert.NoError(t, err)

			mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
			err = commitWorker(ctx)
			assert.NoError(t, err)
		}

		dbTx := database.NewDatabaseTransaction(ctx, false)
		defer dbTx.Discard(ctx)

		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 0)
		assert.ElementsMatch(t, []string{}, addresses)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{}, broadcasts)

		assert.ElementsMatch(t, []*types.TransactionIdentifier{
			{Hash: "tx 1"},
			{Hash: "tx 1"},
			{Hash: "tx 1"},
			{Hash: "tx 2"},
			{Hash: "tx 2"},
			{Hash: "tx 2"},
		}, mockHandler.Stale)

		assert.ElementsMatch(t, []*failedTx{
			{
				transaction: &types.TransactionIdentifier{Hash: "tx 1"},
				intent:      send1,
			},
			{
				transaction: &types.TransactionIdentifier{Hash: "tx 2"},
				intent:      send2,
			},
		}, mockHandler.Failed)

		assert.ElementsMatch(t, []*confirmedTx{}, mockHandler.Confirmed)
	})
}

func TestBroadcastStorageBehindTip(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBroadcastStorage(
		database,
		100,
		broadcastLimit,
		broadcastTipDelay,
		broadcastBehindTip,
		blockBroadcastLimit,
	)
	mockHelper := &MockBroadcastStorageHelper{}
	mockHandler := &MockBroadcastStorageHandler{}
	storage.Initialize(mockHelper, mockHandler)

	send1 := opFiller("addr 1", 1)
	send2 := opFiller("addr 2", 1)
	network := &types.NetworkIdentifier{Blockchain: "Bitcoin", Network: "Testnet3"}
	t.Run("broadcast", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, true)
		defer dbTx.Discard(ctx)

		err := storage.Broadcast(
			ctx,
			dbTx,
			"broadcast 1",
			network,
			send1,
			&types.TransactionIdentifier{Hash: "tx 1"},
			"payload 1",
			confirmationDepth,
		)
		assert.NoError(t, err)

		err = storage.Broadcast(
			ctx,
			dbTx,
			"broadcast 2",
			network,
			send2,
			&types.TransactionIdentifier{Hash: "tx 2"},
			"payload 2",
			confirmationDepth,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 2)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2"}, addresses)

		assert.NoError(t, dbTx.Commit(ctx))

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)
	})

	blocks := blockFiller(0, 81)
	mockHelper.AtSyncTip = false
	mockHelper.Transactions = map[string]*types.TransactionIdentifier{
		"payload 1": {Hash: "tx 1"},
		"payload 2": {Hash: "tx 2"},
	}

	t.Run("add blocks behind tip", func(t *testing.T) {
		for _, block := range blocks[:60] {
			txn := storage.db.NewDatabaseTransaction(ctx, true)
			commitWorker, err := storage.AddingBlock(ctx, block, txn)
			assert.NoError(t, err)
			err = txn.Commit(ctx)
			assert.NoError(t, err)

			mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
			err = commitWorker(ctx)
			assert.NoError(t, err)
		}

		dbTx := database.NewDatabaseTransaction(ctx, false)
		defer dbTx.Discard(ctx)

		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 2)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2"}, addresses)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)
		assert.ElementsMatch(t, []*types.TransactionIdentifier{}, mockHandler.Stale)
		assert.ElementsMatch(t, []*failedTx{}, mockHandler.Failed)
		assert.ElementsMatch(t, []*confirmedTx{}, mockHandler.Confirmed)
	})

	mockHelper.AtSyncTip = true
	t.Run("add blocks close to tip", func(t *testing.T) {
		for _, block := range blocks[60:71] {
			txn := storage.db.NewDatabaseTransaction(ctx, true)
			commitWorker, err := storage.AddingBlock(ctx, block, txn)
			assert.NoError(t, err)
			err = txn.Commit(ctx)
			assert.NoError(t, err)

			mockHelper.SyncedBlockIdentifier = block.BlockIdentifier
			err = commitWorker(ctx)
			assert.NoError(t, err)
		}

		dbTx := database.NewDatabaseTransaction(ctx, false)
		defer dbTx.Discard(ctx)

		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 2)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2"}, addresses)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				LastBroadcast:         blocks[60].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				LastBroadcast:         blocks[60].BlockIdentifier,
				Broadcasts:            1,
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)
		assert.ElementsMatch(t, []*types.TransactionIdentifier{}, mockHandler.Stale)
		assert.ElementsMatch(t, []*failedTx{}, mockHandler.Failed)
		assert.ElementsMatch(t, []*confirmedTx{}, mockHandler.Confirmed)
	})
}

func TestBroadcastStorageClearBroadcasts(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewBroadcastStorage(
		database,
		staleDepth,
		broadcastLimit,
		broadcastTipDelay,
		broadcastBehindTip,
		blockBroadcastLimit,
	)
	mockHelper := &MockBroadcastStorageHelper{}
	mockHandler := &MockBroadcastStorageHandler{}
	storage.Initialize(mockHelper, mockHandler)

	network := &types.NetworkIdentifier{Blockchain: "Bitcoin", Network: "Testnet3"}
	t.Run("locked addresses with no broadcasts", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, false)
		defer dbTx.Discard(ctx)

		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 0)
		assert.ElementsMatch(t, []string{}, addresses)
	})

	send1 := opFiller("addr 1", 11)
	send2 := opFiller("addr 2", 13)
	t.Run("broadcast", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, true)
		defer dbTx.Discard(ctx)

		err := storage.Broadcast(
			ctx,
			dbTx,
			"broadcast 1",
			network,
			send1,
			&types.TransactionIdentifier{Hash: "tx 1"},
			"payload 1",
			confirmationDepth,
		)
		assert.NoError(t, err)

		err = storage.Broadcast(
			ctx,
			dbTx,
			"broadcast 2",
			network,
			send2,
			&types.TransactionIdentifier{Hash: "tx 2"},
			"payload 2",
			confirmationDepth,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 2)
		assert.ElementsMatch(t, []string{"addr 1", "addr 2"}, addresses)

		assert.NoError(t, dbTx.Commit(ctx))

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)
	})

	t.Run("clear broadcasts", func(t *testing.T) {
		broadcasts, err := storage.ClearBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{
			{
				Identifier:            "broadcast 1",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
				Intent:                send1,
				Payload:               "payload 1",
				ConfirmationDepth:     confirmationDepth,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
			},
		}, broadcasts)

		dbTx := database.NewDatabaseTransaction(ctx, false)
		defer dbTx.Discard(ctx)

		addresses, err := storage.LockedAddresses(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, addresses, 0)
		assert.ElementsMatch(t, []string{}, addresses)
	})
}

var _ BroadcastStorageHelper = (*MockBroadcastStorageHelper)(nil)

type findTx struct {
	blockIdentifier *types.BlockIdentifier
	transaction     *types.Transaction
}

type MockBroadcastStorageHelper struct {
	AtSyncTip             bool
	SyncedBlockIdentifier *types.BlockIdentifier
	Transactions          map[string]*types.TransactionIdentifier
	FindTransactions      map[string]*findTx
}

func (m *MockBroadcastStorageHelper) AtTip(
	ctx context.Context,
	tipDelay int64,
) (bool, error) {
	return m.AtSyncTip, nil
}

func (m *MockBroadcastStorageHelper) CurrentBlockIdentifier(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	return m.SyncedBlockIdentifier, nil
}

func (m *MockBroadcastStorageHelper) FindTransaction(
	ctx context.Context,
	transactionIdentifier *types.TransactionIdentifier,
	txn DatabaseTransaction,
) (*types.BlockIdentifier, *types.Transaction, error) {
	val, exists := m.FindTransactions[transactionIdentifier.Hash]
	if !exists {
		return nil, nil, nil
	}

	return val.blockIdentifier, val.transaction, nil
}

func (m *MockBroadcastStorageHelper) BroadcastTransaction(
	ctx context.Context,
	network *types.NetworkIdentifier,
	payload string,
) (*types.TransactionIdentifier, error) {
	val, exists := m.Transactions[payload]
	if !exists {
		return nil, errors.New("broadcast error")
	}

	return val, nil
}

var _ BroadcastStorageHandler = (*MockBroadcastStorageHandler)(nil)

type confirmedTx struct {
	blockIdentifier *types.BlockIdentifier
	transaction     *types.Transaction
	intent          []*types.Operation
}

type failedTx struct {
	transaction *types.TransactionIdentifier
	intent      []*types.Operation
}

type MockBroadcastStorageHandler struct {
	Confirmed []*confirmedTx
	Stale     []*types.TransactionIdentifier
	Failed    []*failedTx
}

func (m *MockBroadcastStorageHandler) TransactionConfirmed(
	ctx context.Context,
	identifier string,
	blockIdentifier *types.BlockIdentifier,
	transaction *types.Transaction,
	intent []*types.Operation,
) error {
	if m.Confirmed == nil {
		m.Confirmed = []*confirmedTx{}
	}

	m.Confirmed = append(m.Confirmed, &confirmedTx{
		blockIdentifier: blockIdentifier,
		transaction:     transaction,
		intent:          intent,
	})

	return nil
}

func (m *MockBroadcastStorageHandler) TransactionStale(
	ctx context.Context,
	identifier string,
	transactionIdentifier *types.TransactionIdentifier,
) error {
	if m.Stale == nil {
		m.Stale = []*types.TransactionIdentifier{}
	}

	m.Stale = append(m.Stale, transactionIdentifier)

	return nil
}

func (m *MockBroadcastStorageHandler) BroadcastFailed(
	ctx context.Context,
	identifier string,
	transactionIdentifier *types.TransactionIdentifier,
	intent []*types.Operation,
) error {
	if m.Failed == nil {
		m.Failed = []*failedTx{}
	}

	m.Failed = append(m.Failed, &failedTx{
		transaction: transactionIdentifier,
		intent:      intent,
	})

	return nil
}
