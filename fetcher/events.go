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
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
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
				"/events/blocks response is invalid: %w",
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
				Err:       fmt.Errorf("/events/blocks not attempting retry: %w", err.Err),
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
