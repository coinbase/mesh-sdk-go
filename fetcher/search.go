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
			Err: fmt.Errorf("%w: %s", ErrCouldNotAcquireSemaphore, err.Error()),
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
				"%w: /search/transactions",
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
				Err:       fmt.Errorf("%w: /search/transactions not attempting retry", err.Err),
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
