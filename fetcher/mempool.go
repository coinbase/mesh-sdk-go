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

package fetcher

import (
	"context"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Mempool returns the validated response
// from the Mempool method.
func (f *Fetcher) Mempool(
	ctx context.Context,
	network *types.NetworkIdentifier,
) ([]*types.TransactionIdentifier, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.MempoolAPI.Mempool(
		ctx,
		&types.NetworkRequest{
			NetworkIdentifier: network,
		},
	)
	if err != nil {
		return nil, f.RequestFailedError(clientErr, err, "/mempool")
	}

	mempool := response.TransactionIdentifiers
	if err := asserter.MempoolTransactions(mempool); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/mempool response is invalid: %w", err),
		}
		return nil, fetcherErr
	}

	return mempool, nil
}

// MempoolTransaction returns the validated response
// from the MempoolTransaction method.
func (f *Fetcher) MempoolTransaction(
	ctx context.Context,
	network *types.NetworkIdentifier,
	transaction *types.TransactionIdentifier,
) (*types.Transaction, map[string]interface{}, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.MempoolAPI.MempoolTransaction(
		ctx,
		&types.MempoolTransactionRequest{
			NetworkIdentifier:     network,
			TransactionIdentifier: transaction,
		},
	)
	if err != nil {
		return nil, nil, f.RequestFailedError(clientErr, err, "/mempool/transaction")
	}

	mempoolTransaction := response.Transaction
	if err := f.Asserter.Transaction(mempoolTransaction); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("/mempool/transaction response is invalid: %w", err),
		}
		return nil, nil, fetcherErr
	}

	return mempoolTransaction, response.Metadata, nil
}
