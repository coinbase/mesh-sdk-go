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

	"github.com/neilotoole/errgroup"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	mocks "github.com/coinbase/rosetta-sdk-go/mocks/storage/modules"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

const (
	confirmationDepth   = int64(2)
	staleDepth          = int64(3)
	broadcastLimit      = 3
	broadcastTipDelay   = 10
	broadcastBehindTip  = false
	blockBroadcastLimit = 10
)

var (
	broadcastMetadata = map[string]interface{}{}
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

	database, err := newTestBadgerDatabase(ctx, newDir)
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
	send1 := opFiller("addr 1", 11)
	send2 := opFiller("addr 2", 13)
	network := &types.NetworkIdentifier{Blockchain: "Bitcoin", Network: "Testnet3"}

	t.Run("broadcast send 1 before block exists", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)

		dbTx := database.Transaction(ctx)
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
			broadcastMetadata,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of accounts aren't reported
		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 1)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
		}, accounts)

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
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	blocks := blockFiller(0, 6)
	t.Run("add block 0", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)
		block := blocks[0]

		txn := storage.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil)
		mockHelper.On(
			"BroadcastTransaction",
			ctx,
			network,
			"payload 1",
		).Return(
			&types.TransactionIdentifier{Hash: "tx 1"},
			nil,
		).Once()
		err = commitWorker(ctx)
		assert.NoError(t, err)

		accounts, err := storage.LockedAccounts(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, accounts, 1)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
		}, accounts)

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
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	t.Run("add block 1", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)
		block := blocks[1]

		txn := storage.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"FindTransaction",
			gctx,
			&types.TransactionIdentifier{Hash: "tx 1"},
			txn,
		).Return(
			nil,
			nil,
			nil,
		).Once()
		commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())
		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil)
		err = commitWorker(ctx)
		assert.NoError(t, err)

		accounts, err := storage.LockedAccounts(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, accounts, 1)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
		}, accounts)

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
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)
		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	t.Run("broadcast send 2 after adding a block", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)
		mockHelper.On("CurrentBlockIdentifier", ctx).Return(blocks[1].BlockIdentifier, nil)

		dbTx := database.Transaction(ctx)
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
			broadcastMetadata,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
			{Address: "addr 2"},
		}, accounts)

		mockHelper.On(
			"BroadcastTransaction",
			ctx,
			network,
			"payload 2",
		).Return(
			&types.TransactionIdentifier{Hash: "tx 2"},
			nil,
		).Once()
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
				TransactionMetadata:   broadcastMetadata,
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
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	t.Run("add block 2", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)

		block := blocks[2]
		mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)

		txn := storage.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"FindTransaction",
			gctx,
			&types.TransactionIdentifier{Hash: "tx 1"},
			txn,
		).Return(
			nil,
			nil,
			nil,
		).Once()
		mockHelper.On(
			"FindTransaction",
			gctx,
			&types.TransactionIdentifier{Hash: "tx 2"},
			txn,
		).Return(
			nil,
			nil,
			nil,
		).Once()
		mockHandler.On(
			"TransactionStale",
			gctx,
			txn,
			"broadcast 1",
			&types.TransactionIdentifier{Hash: "tx 1"},
		).Return(
			nil,
		).Once()
		mockHelper.On(
			"BroadcastTransaction",
			ctx,
			network,
			"payload 1",
		).Return(
			&types.TransactionIdentifier{Hash: "tx 1"},
			nil,
		).Once()
		commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())

		accounts, err := storage.LockedAccounts(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
			{Address: "addr 2"},
		}, accounts)

		err = txn.Commit(ctx)
		assert.NoError(t, err)

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
				TransactionMetadata:   broadcastMetadata,
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
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	tx1 := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 1"},
		Operations:            send1,
	}
	tx2 := &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
		Operations:            send2,
	}

	t.Run("add block 3", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)

		block := blocks[3]
		block.Transactions = []*types.Transaction{tx1}

		txn := storage.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"FindTransaction",
			gctx,
			&types.TransactionIdentifier{Hash: "tx 1"},
			txn,
		).Return(
			blocks[3].BlockIdentifier,
			tx1,
			nil,
		).Once()
		mockHelper.On(
			"FindTransaction",
			gctx,
			&types.TransactionIdentifier{Hash: "tx 2"},
			txn,
		).Return(
			nil,
			nil,
			nil,
		).Once()
		mockHandler.On(
			"TransactionStale",
			gctx,
			txn,
			"broadcast 2",
			&types.TransactionIdentifier{Hash: "tx 2"},
		).Return(
			nil,
		).Once()
		mockHelper.On(
			"BroadcastTransaction",
			ctx,
			network,
			"payload 2",
		).Return(
			&types.TransactionIdentifier{Hash: "tx 2"},
			nil,
		).Once()
		commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())

		accounts, err := storage.LockedAccounts(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
			{Address: "addr 2"},
		}, accounts)

		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil)
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
				TransactionMetadata:   broadcastMetadata,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				LastBroadcast:         blocks[3].BlockIdentifier,
				Broadcasts:            2,
				ConfirmationDepth:     confirmationDepth,
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	t.Run("add block 4", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)

		block := blocks[4]
		block.Transactions = []*types.Transaction{tx2}

		txn := storage.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"FindTransaction",
			gctx,
			&types.TransactionIdentifier{Hash: "tx 1"},
			txn,
		).Return(
			blocks[3].BlockIdentifier,
			tx1,
			nil,
		).Once()
		mockHelper.On(
			"FindTransaction",
			gctx,
			&types.TransactionIdentifier{Hash: "tx 2"},
			txn,
		).Return(
			blocks[4].BlockIdentifier,
			tx2,
			nil,
		).Once()
		mockHandler.On(
			"TransactionConfirmed",
			gctx,
			txn,
			"broadcast 1",
			blocks[3].BlockIdentifier,
			tx1,
			send1,
			broadcastMetadata,
		).Return(
			nil,
		).Once()
		commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())

		accounts, err := storage.LockedAccounts(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, accounts, 1)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 2"},
		}, accounts)

		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil)
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
				LastBroadcast:         blocks[3].BlockIdentifier,
				Broadcasts:            2,
				ConfirmationDepth:     confirmationDepth,
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	t.Run("add block 5", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)

		block := blocks[5]

		txn := storage.db.Transaction(ctx)
		g, gctx := errgroup.WithContext(ctx)
		mockHelper.On(
			"FindTransaction",
			gctx,
			&types.TransactionIdentifier{Hash: "tx 2"},
			txn,
		).Return(
			blocks[4].BlockIdentifier,
			tx2,
			nil,
		).Once()
		mockHandler.On(
			"TransactionConfirmed",
			gctx,
			txn,
			"broadcast 2",
			blocks[4].BlockIdentifier,
			tx2,
			send2,
			broadcastMetadata,
		).Return(
			nil,
		).Once()
		commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
		assert.NoError(t, err)
		assert.NoError(t, g.Wait())

		accounts, err := storage.LockedAccounts(ctx, txn)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
		assert.ElementsMatch(t, []*types.AccountIdentifier{}, accounts)

		err = txn.Commit(ctx)
		assert.NoError(t, err)

		mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil)
		err = commitWorker(ctx)
		assert.NoError(t, err)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})
}

func TestBroadcastStorageBroadcastFailure(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
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
	t.Run("locked addresses with no broadcasts", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)

		dbTx := database.ReadTransaction(ctx)
		defer dbTx.Discard(ctx)

		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
		assert.ElementsMatch(t, []*types.AccountIdentifier{}, accounts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
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
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("CurrentBlockIdentifier", ctx).Return(nil, nil)

		dbTx := database.Transaction(ctx)
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
			broadcastMetadata,
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
			broadcastMetadata,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 3)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
			{Address: "addr 2"},
			{Address: "addr 3"},
		}, accounts)

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
				TransactionMetadata:   broadcastMetadata,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	t.Run("add blocks and expire", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)

		blocks := blockFiller(0, 10)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)
		mockHelper.On(
			"BroadcastTransaction",
			ctx,
			network,
			"payload 1",
		).Return(
			&types.TransactionIdentifier{Hash: "tx 1"},
			nil,
		).Times(
			3,
		)
		mockHandler.On(
			"TransactionStale",
			mock.Anything,
			mock.Anything,
			"broadcast 1",
			&types.TransactionIdentifier{Hash: "tx 1"},
		).Return(
			nil,
		).Times(
			3,
		)
		mockHandler.On(
			"BroadcastFailed",
			ctx,
			mock.Anything,
			"broadcast 1",
			&types.TransactionIdentifier{Hash: "tx 1"},
			send1,
		).Return(
			nil,
		).Once()
		mockHelper.On(
			"BroadcastTransaction",
			ctx,
			network,
			"payload 2",
		).Return(
			nil,
			errors.New("broadcast error"),
		).Times(
			3,
		)
		mockHandler.On(
			"TransactionStale",
			mock.Anything,
			mock.Anything,
			"broadcast 2",
			&types.TransactionIdentifier{Hash: "tx 2"},
		).Return(
			nil,
		).Times(
			3,
		)
		mockHandler.On(
			"BroadcastFailed",
			mock.Anything,
			mock.Anything,
			"broadcast 2",
			&types.TransactionIdentifier{Hash: "tx 2"},
			send2,
		).Return(
			nil,
		).Once()

		// Never find in block
		mockHelper.On(
			"FindTransaction",
			mock.Anything,
			&types.TransactionIdentifier{Hash: "tx 1"},
			mock.Anything,
		).Return(
			nil,
			nil,
			nil,
		)
		mockHelper.On(
			"FindTransaction",
			mock.Anything,
			&types.TransactionIdentifier{Hash: "tx 2"},
			mock.Anything,
		).Return(
			nil,
			nil,
			nil,
		)
		for _, block := range blocks {
			mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil).Once()
			txn := storage.db.Transaction(ctx)
			g, gctx := errgroup.WithContext(ctx)
			commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
			assert.NoError(t, err)
			assert.NoError(t, g.Wait())
			err = txn.Commit(ctx)
			assert.NoError(t, err)

			err = commitWorker(ctx)
			assert.NoError(t, err)
		}

		dbTx := database.ReadTransaction(ctx)
		defer dbTx.Discard(ctx)

		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
		assert.ElementsMatch(t, []*types.AccountIdentifier{}, accounts)

		broadcasts, err := storage.GetAllBroadcasts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*Broadcast{}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})
}

func TestBroadcastStorageBehindTip(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
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
	send1 := opFiller("addr 1", 1)
	send2 := opFiller("addr 2", 1)
	network := &types.NetworkIdentifier{Blockchain: "Bitcoin", Network: "Testnet3"}
	t.Run("broadcast", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)

		dbTx := database.Transaction(ctx)
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
			broadcastMetadata,
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
			broadcastMetadata,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
			{Address: "addr 2"},
		}, accounts)

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
				TransactionMetadata:   broadcastMetadata,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	blocks := blockFiller(0, 81)

	t.Run("add blocks behind tip", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(false, nil)

		for _, block := range blocks[:60] {
			mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil).Once()
			txn := storage.db.Transaction(ctx)
			g, gctx := errgroup.WithContext(ctx)
			commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
			assert.NoError(t, err)
			assert.NoError(t, g.Wait())
			err = txn.Commit(ctx)
			assert.NoError(t, err)

			err = commitWorker(ctx)
			assert.NoError(t, err)
		}

		dbTx := database.ReadTransaction(ctx)
		defer dbTx.Discard(ctx)

		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
			{Address: "addr 2"},
		}, accounts)

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
				TransactionMetadata:   broadcastMetadata,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	t.Run("add blocks close to tip", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)
		mockHelper.On("AtTip", ctx, mock.Anything).Return(true, nil)

		mockHelper.On(
			"BroadcastTransaction",
			ctx,
			network,
			"payload 1",
		).Return(
			&types.TransactionIdentifier{Hash: "tx 1"},
			nil,
		).Once()
		mockHelper.On(
			"BroadcastTransaction",
			ctx,
			network,
			"payload 2",
		).Return(
			&types.TransactionIdentifier{Hash: "tx 2"},
			nil,
		).Once()
		// Never find in block
		mockHelper.On(
			"FindTransaction",
			mock.Anything,
			&types.TransactionIdentifier{Hash: "tx 1"},
			mock.Anything,
		).Return(
			nil,
			nil,
			nil,
		)
		mockHelper.On(
			"FindTransaction",
			mock.Anything,
			&types.TransactionIdentifier{Hash: "tx 2"},
			mock.Anything,
		).Return(
			nil,
			nil,
			nil,
		)
		for _, block := range blocks[60:71] {
			mockHelper.On("CurrentBlockIdentifier", ctx).Return(block.BlockIdentifier, nil).Once()
			txn := storage.db.Transaction(ctx)
			g, gctx := errgroup.WithContext(ctx)
			commitWorker, err := storage.AddingBlock(gctx, g, block, txn)
			assert.NoError(t, err)
			assert.NoError(t, g.Wait())
			err = txn.Commit(ctx)
			assert.NoError(t, err)

			err = commitWorker(ctx)
			assert.NoError(t, err)
		}

		dbTx := database.ReadTransaction(ctx)
		defer dbTx.Discard(ctx)

		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
			{Address: "addr 2"},
		}, accounts)

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
				TransactionMetadata:   broadcastMetadata,
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
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})
}

func TestBroadcastStorageClearBroadcasts(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
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
	network := &types.NetworkIdentifier{Blockchain: "Bitcoin", Network: "Testnet3"}
	t.Run("locked addresses with no broadcasts", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)

		dbTx := database.ReadTransaction(ctx)
		defer dbTx.Discard(ctx)

		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
		assert.ElementsMatch(t, []*types.AccountIdentifier{}, accounts)
		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	send1 := opFiller("addr 1", 11)
	send2 := opFiller("addr 2", 13)
	t.Run("broadcast", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)

		dbTx := database.Transaction(ctx)
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
			broadcastMetadata,
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
			broadcastMetadata,
		)
		assert.NoError(t, err)

		// Check to make sure duplicate instances of address aren't reported
		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 2)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr 1"},
			{Address: "addr 2"},
		}, accounts)

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
				TransactionMetadata:   broadcastMetadata,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})

	t.Run("clear broadcasts", func(t *testing.T) {
		mockHelper := &mocks.BroadcastStorageHelper{}
		mockHandler := &mocks.BroadcastStorageHandler{}
		storage.Initialize(mockHelper, mockHandler)

		mockHandler.On(
			"BroadcastFailed",
			ctx,
			mock.Anything,
			"broadcast 1",
			&types.TransactionIdentifier{Hash: "tx 1"},
			send1,
		).Return(
			nil,
		).Once()
		mockHandler.On(
			"BroadcastFailed",
			ctx,
			mock.Anything,
			"broadcast 2",
			&types.TransactionIdentifier{Hash: "tx 2"},
			send2,
		).Return(
			nil,
		).Once()

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
				TransactionMetadata:   broadcastMetadata,
			},
			{
				Identifier:            "broadcast 2",
				NetworkIdentifier:     network,
				TransactionIdentifier: &types.TransactionIdentifier{Hash: "tx 2"},
				Intent:                send2,
				Payload:               "payload 2",
				ConfirmationDepth:     confirmationDepth,
				TransactionMetadata:   broadcastMetadata,
			},
		}, broadcasts)

		dbTx := database.ReadTransaction(ctx)
		defer dbTx.Discard(ctx)

		accounts, err := storage.LockedAccounts(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
		assert.ElementsMatch(t, []*types.AccountIdentifier{}, accounts)

		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})
}
