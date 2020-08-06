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
	"context"
	"testing"

	mocks "github.com/coinbase/rosetta-sdk-go/mocks/syncer"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

var (
	networkIdentifier = &types.NetworkIdentifier{
		Blockchain: "blah",
		Network:    "testnet",
	}

	currency = &types.Currency{
		Symbol:   "Blah",
		Decimals: 2,
	}

	recipient = &types.AccountIdentifier{
		Address: "acct1",
	}

	recipientAmount = &types.Amount{
		Value:    "100",
		Currency: currency,
	}

	recipientOperation = &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: 0,
		},
		Type:    "Transfer",
		Status:  "Success",
		Account: recipient,
		Amount:  recipientAmount,
	}

	recipientFailureOperation = &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: 1,
		},
		Type:    "Transfer",
		Status:  "Failure",
		Account: recipient,
		Amount:  recipientAmount,
	}

	recipientTransaction = &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "tx1",
		},
		Operations: []*types.Operation{
			recipientOperation,
			recipientFailureOperation,
		},
	}

	sender = &types.AccountIdentifier{
		Address: "acct2",
	}

	senderAmount = &types.Amount{
		Value:    "-100",
		Currency: currency,
	}

	senderOperation = &types.Operation{
		OperationIdentifier: &types.OperationIdentifier{
			Index: 0,
		},
		Type:    "Transfer",
		Status:  "Success",
		Account: sender,
		Amount:  senderAmount,
	}

	senderTransaction = &types.Transaction{
		TransactionIdentifier: &types.TransactionIdentifier{
			Hash: "tx2",
		},
		Operations: []*types.Operation{
			senderOperation,
		},
	}

	orphanGenesis = &types.Block{
		BlockIdentifier: &types.BlockIdentifier{
			Hash:  "1",
			Index: 1,
		},
		ParentBlockIdentifier: &types.BlockIdentifier{
			Hash:  "0a",
			Index: 0,
		},
		Transactions: []*types.Transaction{},
	}

	blockSequence = []*types.Block{
		{ // genesis
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "0",
				Index: 0,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "0",
				Index: 0,
			},
		},
		{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "1",
				Index: 1,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "0",
				Index: 0,
			},
			Transactions: []*types.Transaction{
				recipientTransaction,
			},
		},
		{ // reorg
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "2",
				Index: 2,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "1a",
				Index: 1,
			},
		},
		{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "1a",
				Index: 1,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "0",
				Index: 0,
			},
		},
		{
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "3",
				Index: 3,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "2",
				Index: 2,
			},
			Transactions: []*types.Transaction{
				senderTransaction,
			},
		},
		{ // invalid block
			BlockIdentifier: &types.BlockIdentifier{
				Hash:  "5",
				Index: 5,
			},
			ParentBlockIdentifier: &types.BlockIdentifier{
				Hash:  "4",
				Index: 4,
			},
		},
	}
)

func lastBlockIdentifier(syncer *Syncer) *types.BlockIdentifier {
	return syncer.pastBlocks[len(syncer.pastBlocks)-1]
}

func TestProcessBlock(t *testing.T) {
	ctx := context.Background()

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	syncer := New(networkIdentifier, mockHelper, mockHandler, nil, 1, nil)
	syncer.genesisBlock = blockSequence[0].BlockIdentifier

	t.Run("No block exists", func(t *testing.T) {
		mockHandler.On("BlockAdded", ctx, blockSequence[0]).Return(nil)
		assert.Equal(
			t,
			[]*types.BlockIdentifier{},
			syncer.pastBlocks,
		)
		err := syncer.processBlock(
			ctx,
			blockSequence[0],
		)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), syncer.nextIndex)
		assert.Equal(t, blockSequence[0].BlockIdentifier, lastBlockIdentifier(syncer))
		assert.Equal(
			t,
			[]*types.BlockIdentifier{blockSequence[0].BlockIdentifier},
			syncer.pastBlocks,
		)
	})

	t.Run("Orphan genesis", func(t *testing.T) {
		err := syncer.processBlock(
			ctx,
			orphanGenesis,
		)

		assert.EqualError(t, err, "cannot remove genesis block")
		assert.Equal(t, int64(1), syncer.nextIndex)
		assert.Equal(t, blockSequence[0].BlockIdentifier, lastBlockIdentifier(syncer))
		assert.Equal(
			t,
			[]*types.BlockIdentifier{blockSequence[0].BlockIdentifier},
			syncer.pastBlocks,
		)
	})

	t.Run("Block exists, no reorg", func(t *testing.T) {
		mockHandler.On("BlockAdded", ctx, blockSequence[1]).Return(nil)
		err := syncer.processBlock(
			ctx,
			blockSequence[1],
		)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), syncer.nextIndex)
		assert.Equal(t, blockSequence[1].BlockIdentifier, lastBlockIdentifier(syncer))
		assert.Equal(
			t,
			[]*types.BlockIdentifier{
				blockSequence[0].BlockIdentifier,
				blockSequence[1].BlockIdentifier,
			},
			syncer.pastBlocks,
		)
	})

	t.Run("Orphan block", func(t *testing.T) {
		mockHandler.On("BlockRemoved", ctx, blockSequence[1].BlockIdentifier).Return(nil)
		err := syncer.processBlock(
			ctx,
			blockSequence[2],
		)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), syncer.nextIndex)
		assert.Equal(t, blockSequence[0].BlockIdentifier, lastBlockIdentifier(syncer))
		assert.Equal(
			t,
			[]*types.BlockIdentifier{blockSequence[0].BlockIdentifier},
			syncer.pastBlocks,
		)

		mockHandler.On("BlockAdded", ctx, blockSequence[3]).Return(nil)
		err = syncer.processBlock(
			ctx,
			blockSequence[3],
		)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), syncer.nextIndex)
		assert.Equal(t, blockSequence[3].BlockIdentifier, lastBlockIdentifier(syncer))
		assert.Equal(
			t,
			[]*types.BlockIdentifier{
				blockSequence[0].BlockIdentifier,
				blockSequence[3].BlockIdentifier,
			},
			syncer.pastBlocks,
		)

		mockHandler.On("BlockAdded", ctx, blockSequence[2]).Return(nil)
		err = syncer.processBlock(
			ctx,
			blockSequence[2],
		)
		assert.NoError(t, err)
		assert.Equal(t, int64(3), syncer.nextIndex)
		assert.Equal(t, blockSequence[2].BlockIdentifier, lastBlockIdentifier(syncer))
		assert.Equal(
			t,
			[]*types.BlockIdentifier{
				blockSequence[0].BlockIdentifier,
				blockSequence[3].BlockIdentifier,
				blockSequence[2].BlockIdentifier,
			},
			syncer.pastBlocks,
		)
	})

	t.Run("Out of order block", func(t *testing.T) {
		err := syncer.processBlock(
			ctx,
			blockSequence[5],
		)
		assert.EqualError(t, err, "Got block 5 instead of 3")
		assert.Equal(t, int64(3), syncer.nextIndex)
		assert.Equal(t, blockSequence[2].BlockIdentifier, lastBlockIdentifier(syncer))
		assert.Equal(
			t,
			[]*types.BlockIdentifier{
				blockSequence[0].BlockIdentifier,
				blockSequence[3].BlockIdentifier,
				blockSequence[2].BlockIdentifier,
			},
			syncer.pastBlocks,
		)
	})

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}
