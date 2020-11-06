package fetcher

import (
	"context"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// EventsBlocks returns the validated response
// from the EventsBlocks method.
func (f *Fetcher) EventsBlocks(
	ctx context.Context,
	network *types.NetworkIdentifier,
	offset *int64,
	limit *int64,
) (int64, []*types.BlockEvent, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return -1, nil, &Error{
			Err: fmt.Errorf("%w: %s", ErrCouldNotAcquireSemaphore, err.Error()),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.EventsAPI.EventsBlocks(ctx,
		&types.EventsBlocksRequest{
			NetworkIdentifier: network,
			Offset:            offset,
			Limit:             limit,
		},
	)
	if err != nil {
		return -1, nil, f.RequestFailedError(clientErr, err, "/events/blocks")
	}

	if err := asserter.EventsBlocksResponse(
		response,
	); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf(
				"%w: /events/blocks",
				err,
			),
		}
		return -1, nil, fetcherErr
	}

	return response.MaxSequence, response.Events, nil
}

// EventsBlocksRetry retrieves the valided EventsBlocks
// with a specified number of retries and max elapsed time.
func (f *Fetcher) EventsBlocksRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	offset *int64,
	limit *int64,
) (int64, []*types.BlockEvent, *Error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		maxSequence, events, err := f.EventsBlocks(
			ctx,
			network,
			offset,
			limit,
		)
		if err == nil {
			return maxSequence, events, nil
		}

		if ctx.Err() != nil {
			return -1, nil, &Error{
				Err: ctx.Err(),
			}
		}

		if is, _ := asserter.Err(err.Err); is {
			fetcherErr := &Error{
				Err:       fmt.Errorf("%w: /events/blocks not attempting retry", err.Err),
				ClientErr: err.ClientErr,
			}
			return -1, nil, fetcherErr
		}

		if err := tryAgain(
			fmt.Sprintf("/events/blocks %+v %+v", offset, limit),
			backoffRetries,
			err,
		); err != nil {
			return -1, nil, err
		}
	}
}
