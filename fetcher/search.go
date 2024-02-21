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

// SearchTransactions returns the validated response
// from the SearchTransactions method.
func (f *Fetcher) SearchTransactions(
	ctx context.Context,
	request *types.SearchTransactionsRequest,
) (*int64, []*types.BlockTransaction, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, nil, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.SearchAPI.SearchTransactions(ctx, request)
	if err != nil {
		return nil, nil, f.RequestFailedError(clientErr, err, "/search/transactions")
	}

	if err := f.Asserter.SearchTransactionsResponse(
		response,
	); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf(
				"/search/transactions response is invalid: %w",
				err,
			),
		}
		return nil, nil, fetcherErr
	}

	return response.NextOffset, response.Transactions, nil
}

// SearchTransactionsRetry retrieves the valided SearchTransactions
// with a specified number of retries and max elapsed time.
func (f *Fetcher) SearchTransactionsRetry(
	ctx context.Context,
	request *types.SearchTransactionsRequest,
) (*int64, []*types.BlockTransaction, *Error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		nextOffset, transactions, err := f.SearchTransactions(
			ctx,
			request,
		)
		if err == nil {
			return nextOffset, transactions, nil
		}

		if ctx.Err() != nil {
			return nil, nil, &Error{
				Err: ctx.Err(),
			}
		}

		if is, _ := asserter.Err(err.Err); is {
			fetcherErr := &Error{
				Err:       fmt.Errorf("/search/transactions not attempting retry: %w", err.Err),
				ClientErr: err.ClientErr,
			}
			return nil, nil, fetcherErr
		}

		if err := tryAgain(
			fmt.Sprintf("/search/transactions %s", types.PrintStruct(request)),
			backoffRetries,
			err,
		); err != nil {
			return nil, nil, err
		}
	}
}
