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
	"fmt"
	"log"
	"sync"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var _ BlockWorker = (*BroadcastStorage)(nil)

const (
	transactionBroadcastNamespace = "transaction-broadcast"

	// depthOffset is used for adjusting depth checks because
	// depth is "indexed by 1". Meaning, if a transaction is in
	// tip it has depth 1.
	depthOffset = 1
)

func getBroadcastKey(transactionIdentifier *types.TransactionIdentifier) (string, []byte) {
	return transactionBroadcastNamespace, []byte(
		fmt.Sprintf("%s/%s", transactionBroadcastNamespace, transactionIdentifier.Hash),
	)
}

// BroadcastStorage implements storage methods for managing
// transaction broadcast.
type BroadcastStorage struct {
	db      Database
	helper  BroadcastStorageHelper
	handler BroadcastStorageHandler

	staleDepth          int64
	broadcastLimit      int
	tipDelay            int64
	broadcastBehindTip  bool
	blockBroadcastLimit int

	// Running BroadcastAll concurrently
	// could cause corruption.
	broadcastAllMutex sync.Mutex
}

// BroadcastStorageHelper is used by BroadcastStorage to submit transactions
// and find said transaction in blocks on-chain.
type BroadcastStorageHelper interface {
	// CurrentBlockIdentifier is called before transaction broadcast and is used
	// to determine if a transaction broadcast is stale.
	CurrentBlockIdentifier(
		context.Context,
	) (*types.BlockIdentifier, error) // used to determine if should rebroadcast

	// AtTip is called before transaction broadcast to determine if we are at tip.
	AtTip(
		context.Context,
		int64,
	) (bool, error)

	// FindTransaction looks for the provided TransactionIdentifier in processed
	// blocks and returns the block identifier containing the most recent sighting
	// and the transaction seen in that block.
	FindTransaction(
		context.Context,
		*types.TransactionIdentifier,
		DatabaseTransaction,
	) (*types.BlockIdentifier, *types.Transaction, error) // used to confirm

	// BroadcastTransaction broadcasts a transaction to a Rosetta implementation
	// and returns the *types.TransactionIdentifier returned by the implementation.
	BroadcastTransaction(
		context.Context,
		*types.NetworkIdentifier,
		string,
	) (*types.TransactionIdentifier, error) // handle initial broadcast + confirm matches provided + rebroadcast if stale
}

// BroadcastStorageHandler is invoked when a transaction is confirmed on-chain
// or when a transaction is considered stale.
type BroadcastStorageHandler interface {
	// TransactionConfirmed is called when a transaction is observed on-chain for the
	// last time at a block height < current block height - confirmationDepth.
	TransactionConfirmed(
		context.Context,
		DatabaseTransaction,
		string, // identifier
		*types.BlockIdentifier,
		*types.Transaction,
		[]*types.Operation,
	) error // can use locked account again + confirm matches intent + update logger

	// TransactionStale is called when a transaction has not yet been
	// seen on-chain and is considered stale. This occurs when
	// current block height - last broadcast > staleDepth.
	TransactionStale(
		context.Context,
		DatabaseTransaction,
		string, // identifier
		*types.TransactionIdentifier,
	) error // log in counter (rebroadcast should occur here)

	// BroadcastFailed is called when another transaction broadcast would
	// put it over the provided broadcast limit.
	BroadcastFailed(
		context.Context,
		DatabaseTransaction,
		string, // identifier
		*types.TransactionIdentifier,
		[]*types.Operation,
	) error
}

// Broadcast is persisted to the db to track transaction broadcast.
type Broadcast struct {
	Identifier            string                       `json:"identifier"`
	NetworkIdentifier     *types.NetworkIdentifier     `json:"network_identifier"`
	TransactionIdentifier *types.TransactionIdentifier `json:"transaction_identifier"`
	ConfirmationDepth     int64                        `json:"confirmation_depth"`
	Intent                []*types.Operation           `json:"intent"`
	Payload               string                       `json:"payload"`
	LastBroadcast         *types.BlockIdentifier       `json:"broadcast_at"`
	Broadcasts            int                          `json:"broadcasts"`
}

// NewBroadcastStorage returns a new BroadcastStorage.
func NewBroadcastStorage(
	db Database,
	staleDepth int64,
	broadcastLimit int,
	tipDelay int64,
	broadcastBehindTip bool,
	blockBroadcastLimit int,
) *BroadcastStorage {
	return &BroadcastStorage{
		db:                  db,
		staleDepth:          staleDepth,
		broadcastLimit:      broadcastLimit,
		tipDelay:            tipDelay,
		broadcastBehindTip:  broadcastBehindTip,
		blockBroadcastLimit: blockBroadcastLimit,
	}
}

// Initialize adds a BroadcastStorageHelper and BroadcastStorageHandler to BroadcastStorage.
// This must be called prior to syncing!
func (b *BroadcastStorage) Initialize(
	helper BroadcastStorageHelper,
	handler BroadcastStorageHandler,
) {
	b.helper = helper
	b.handler = handler
}

func (b *BroadcastStorage) invokeAddBlockHandlers(
	ctx context.Context,
	dbTx DatabaseTransaction,
	staleBroadcasts []*Broadcast,
	confirmedTransactions []*Broadcast,
	foundBlocks []*types.BlockIdentifier,
	foundTransactions []*types.Transaction,
) error {
	for _, stale := range staleBroadcasts {
		if err := b.handler.TransactionStale(ctx, dbTx, stale.Identifier, stale.TransactionIdentifier); err != nil {
			return fmt.Errorf(
				"%w %s: %v",
				ErrBroadcastTxStale,
				stale.TransactionIdentifier.Hash,
				err,
			)
		}
	}

	for i, broadcast := range confirmedTransactions {
		err := b.handler.TransactionConfirmed(
			ctx,
			dbTx,
			broadcast.Identifier,
			foundBlocks[i],
			foundTransactions[i],
			broadcast.Intent,
		)
		if err != nil {
			return fmt.Errorf(
				"%w %s: %v",
				ErrBroadcastTxConfirmed,
				broadcast.TransactionIdentifier.Hash,
				err,
			)
		}
	}

	return nil
}

// AddingBlock is called by BlockStorage when adding a block.
func (b *BroadcastStorage) AddingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	broadcasts, err := b.GetAllBroadcasts(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: unable to get all broadcasts", err)
	}

	staleBroadcasts := []*Broadcast{}
	confirmedTransactions := []*Broadcast{}
	foundTransactions := []*types.Transaction{}
	foundBlocks := []*types.BlockIdentifier{}

	for _, broadcast := range broadcasts {
		if broadcast.LastBroadcast == nil {
			continue
		}

		namespace, key := getBroadcastKey(broadcast.TransactionIdentifier)

		// We perform the FindTransaction search in the context of the block database
		// transaction so we can access any transactions of depth 1 (in the current
		// block).
		foundBlock, foundTransaction, err := b.helper.FindTransaction(
			ctx,
			broadcast.TransactionIdentifier,
			transaction,
		)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrBroadcastFindTxFailed, err)
		}

		// Check if we should mark the broadcast as stale
		if foundBlock == nil &&
			block.BlockIdentifier.Index-broadcast.LastBroadcast.Index >= b.staleDepth-depthOffset {
			staleBroadcasts = append(staleBroadcasts, broadcast)
			broadcast.LastBroadcast = nil
			bytes, err := b.db.Encoder().Encode(namespace, broadcast)
			if err != nil {
				return nil, fmt.Errorf("%w: %v", ErrBroadcastEncodeUpdateFailed, err)
			}

			if err := transaction.Set(ctx, key, bytes, true); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrBroadcastUpdateFailed, err)
			}

			continue
		}

		// Continue if we are still waiting for a broadcast to appear and it isn't stale
		if foundBlock == nil {
			continue
		}

		// Check if we should mark the transaction as confirmed
		if block.BlockIdentifier.Index-foundBlock.Index >= broadcast.ConfirmationDepth-depthOffset {
			confirmedTransactions = append(confirmedTransactions, broadcast)
			foundTransactions = append(foundTransactions, foundTransaction)
			foundBlocks = append(foundBlocks, foundBlock)

			if err := transaction.Delete(ctx, key); err != nil {
				return nil, fmt.Errorf("%w: %v", ErrBroadcastDeleteConfirmedTxFailed, err)
			}
		}
	}

	if err := b.invokeAddBlockHandlers(
		ctx,
		transaction,
		staleBroadcasts,
		confirmedTransactions,
		foundBlocks,
		foundTransactions,
	); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBroadcastInvokeBlockHandlersFailed, err)
	}

	return func(ctx context.Context) error {
		if err := b.BroadcastAll(ctx, true); err != nil {
			return fmt.Errorf("%w: %v", ErrBroadcastFailed, err)
		}

		return nil
	}, nil
}

// RemovingBlock is called by BlockStorage when removing a block.
// TODO: error if transaction removed after confirmed (means confirmation depth not deep enough)
func (b *BroadcastStorage) RemovingBlock(
	ctx context.Context,
	block *types.Block,
	transaction DatabaseTransaction,
) (CommitWorker, error) {
	return nil, nil
}

// Broadcast is called when a caller wants a transaction to be broadcast and tracked.
// The caller SHOULD NOT broadcast the transaction before calling this function.
func (b *BroadcastStorage) Broadcast(
	ctx context.Context,
	dbTx DatabaseTransaction,
	identifier string,
	network *types.NetworkIdentifier,
	intent []*types.Operation,
	transactionIdentifier *types.TransactionIdentifier,
	payload string,
	confirmationDepth int64,
) error {
	namespace, broadcastKey := getBroadcastKey(transactionIdentifier)

	exists, _, err := dbTx.Get(ctx, broadcastKey)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastDBGetFailed, err)
	}

	if exists {
		return fmt.Errorf("%w %s", ErrBroadcastAlreadyExists, transactionIdentifier.Hash)
	}

	bytes, err := b.db.Encoder().Encode(namespace, Broadcast{
		Identifier:            identifier,
		NetworkIdentifier:     network,
		TransactionIdentifier: transactionIdentifier,
		Intent:                intent,
		Payload:               payload,
		Broadcasts:            0,
		ConfirmationDepth:     confirmationDepth,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastEncodeFailed, err)
	}

	if err := dbTx.Set(ctx, broadcastKey, bytes, true); err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastSetFailed, err)
	}

	return nil
}

func (b *BroadcastStorage) getAllBroadcasts(
	ctx context.Context,
	dbTx DatabaseTransaction,
) ([]*Broadcast, error) {
	namespace := transactionBroadcastNamespace
	broadcasts := []*Broadcast{}
	_, err := dbTx.Scan(
		ctx,
		[]byte(namespace),
		[]byte(namespace),
		func(k []byte, v []byte) error {
			var broadcast Broadcast
			// We should not reclaim memory during a scan!!
			if err := b.db.Encoder().Decode(namespace, v, &broadcast, false); err != nil {
				return fmt.Errorf("%w: %v", ErrBroadcastDecodeFailed, err)
			}

			broadcasts = append(broadcasts, &broadcast)
			return nil
		},
		false,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBroadcastScanFailed, err)
	}

	return broadcasts, nil
}

// GetAllBroadcasts returns all currently in-process broadcasts.
func (b *BroadcastStorage) GetAllBroadcasts(ctx context.Context) ([]*Broadcast, error) {
	dbTx := b.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	return b.getAllBroadcasts(ctx, dbTx)
}

func (b *BroadcastStorage) performBroadcast(
	ctx context.Context,
	broadcast *Broadcast,
	onlyEligible bool,
) error {
	namespace, key := getBroadcastKey(broadcast.TransactionIdentifier)
	bytes, err := b.db.Encoder().Encode(namespace, broadcast)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastEncodeFailed, err)
	}

	txn := b.db.NewDatabaseTransaction(ctx, true)
	defer txn.Discard(ctx)

	if err := txn.Set(ctx, key, bytes, true); err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastUpdateFailed, err)
	}

	if err := txn.Commit(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastCommitUpdateFailed, err)
	}

	if !onlyEligible {
		log.Printf("Broadcasting: %s\n", types.PrettyPrintStruct(broadcast))
	}

	broadcastIdentifier, err := b.helper.BroadcastTransaction(
		ctx,
		broadcast.NetworkIdentifier,
		broadcast.Payload,
	)
	if err != nil {
		// Don't error on broadcast failure, retries will automatically be handled.
		log.Printf(
			"%s: unable to broadcast transaction %s",
			err.Error(),
			broadcast.TransactionIdentifier.Hash,
		)

		return nil
	}

	if types.Hash(broadcastIdentifier) != types.Hash(broadcast.TransactionIdentifier) {
		return fmt.Errorf(
			"%w: expected %s but got %s",
			ErrBroadcastIdentifierMismatch,
			broadcast.TransactionIdentifier.Hash,
			broadcastIdentifier.Hash,
		)
	}

	return nil
}

// BroadcastAll broadcasts all transactions in BroadcastStorage. If onlyEligible
// is set to true, then only transactions that should be broadcast again
// are actually broadcast.
func (b *BroadcastStorage) BroadcastAll(ctx context.Context, onlyEligible bool) error {
	// Corruption can occur if we run this concurrently.
	b.broadcastAllMutex.Lock()
	defer b.broadcastAllMutex.Unlock()

	currBlock, err := b.helper.CurrentBlockIdentifier(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastGetCurrentBlockIdentifierFailed, err)
	}

	// We have not yet synced a block and should wait to broadcast
	// until we do so (otherwise we can't track last broadcast correctly).
	if currBlock == nil {
		return nil
	}

	// Wait to broadcast transaction until close to tip
	atTip, err := b.helper.AtTip(ctx, b.tipDelay)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastAtTipFailed, err)
	}

	if (!atTip && !b.broadcastBehindTip) && onlyEligible {
		return nil
	}

	broadcasts, err := b.GetAllBroadcasts(ctx)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrBroadcastGetAllFailed, err)
	}

	attemptedBroadcasts := 0
	for _, broadcast := range broadcasts {
		// When a transaction should be broadcast, its last broadcast field must
		// be set to nil.
		if broadcast.LastBroadcast != nil && onlyEligible {
			continue
		}

		if broadcast.Broadcasts >= b.broadcastLimit {
			txn := b.db.NewDatabaseTransaction(ctx, true)
			defer txn.Discard(ctx)

			_, key := getBroadcastKey(broadcast.TransactionIdentifier)
			if err := txn.Delete(ctx, key); err != nil {
				return fmt.Errorf("%w: %v", ErrBroadcastDeleteFailed, err)
			}

			if err := b.handler.BroadcastFailed(
				ctx,
				txn,
				broadcast.Identifier,
				broadcast.TransactionIdentifier,
				broadcast.Intent,
			); err != nil {
				return fmt.Errorf("%w: %v", ErrBroadcastHandleFailureUnsuccessful, err)
			}

			if err := txn.Commit(ctx); err != nil {
				return fmt.Errorf("%w: %v", ErrBroadcastCommitDeleteFailed, err)
			}

			continue
		}

		// Limit the number of transactions we attempt to broadcast
		// at a given block.
		if attemptedBroadcasts >= b.blockBroadcastLimit {
			continue
		}
		attemptedBroadcasts++

		// We set the last broadcast value before broadcast so we don't accidentally
		// re-broadcast if exiting between broadcasting the transaction and updating
		// the value in the database. If the transaction is never really broadcast,
		// it will be rebroadcast when it is considered stale!
		broadcast.LastBroadcast = currBlock
		broadcast.Broadcasts++

		if err := b.performBroadcast(ctx, broadcast, onlyEligible); err != nil {
			return fmt.Errorf("%w: %v", ErrBroadcastPerformFailed, err)
		}
	}

	return nil
}

// LockedAccounts returns all *types.AccountIdentifier currently active in transaction broadcasts.
// The caller SHOULD NOT broadcast a transaction from an account if it is
// considered locked!
func (b *BroadcastStorage) LockedAccounts(
	ctx context.Context,
	dbTx DatabaseTransaction,
) ([]*types.AccountIdentifier, error) {
	broadcasts, err := b.getAllBroadcasts(ctx, dbTx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBroadcastGetAllFailed, err)
	}

	// De-duplicate accounts present in broadcast storage.
	accountMap := map[string]*types.AccountIdentifier{}
	for _, broadcast := range broadcasts {
		for _, op := range broadcast.Intent {
			if op.Account == nil {
				continue
			}

			accountMap[types.Hash(op.Account)] = op.Account
		}
	}

	accounts := []*types.AccountIdentifier{}
	for _, v := range accountMap {
		accounts = append(accounts, v)
	}

	return accounts, nil
}

// ClearBroadcasts deletes all in-progress broadcasts from BroadcastStorage. This
// is useful when there is some construction error and all pending broadcasts
// will fail and should be cleared instead of re-attempting.
func (b *BroadcastStorage) ClearBroadcasts(ctx context.Context) ([]*Broadcast, error) {
	broadcasts, err := b.GetAllBroadcasts(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBroadcastGetAllFailed, err)
	}

	txn := b.db.NewDatabaseTransaction(ctx, true)
	for _, broadcast := range broadcasts {
		_, key := getBroadcastKey(broadcast.TransactionIdentifier)
		if err := txn.Delete(ctx, key); err != nil {
			return nil, fmt.Errorf(
				"%w %s: %v",
				ErrBroadcastDeleteFailed,
				broadcast.Identifier,
				err,
			)
		}

		// When clearing broadcasts, make sure to invoke the handler
		// so other services can be updated.
		if err := b.handler.BroadcastFailed(
			ctx,
			txn,
			broadcast.Identifier,
			broadcast.TransactionIdentifier,
			broadcast.Intent,
		); err != nil {
			return nil, fmt.Errorf(
				"%w %s: %v",
				ErrBroadcastHandleFailureUnsuccessful,
				broadcast.Identifier,
				err,
			)
		}
	}

	if err := txn.Commit(ctx); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBroadcastCommitDeleteFailed, err)
	}

	return broadcasts, nil
}
