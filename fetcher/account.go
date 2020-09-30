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

package fetcher

import (
	"context"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/asserter"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// AccountBalance returns the validated response
// from the AccountBalance method. If a block
// is provided, a historical lookup is performed.
func (f *Fetcher) AccountBalance(
	ctx context.Context,
	network *types.NetworkIdentifier,
	account *types.AccountIdentifier,
	block *types.PartialBlockIdentifier,
) (*types.BlockIdentifier, []*types.Amount, []*types.Coin, map[string]interface{}, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, nil, nil, nil, &Error{
			Err: fmt.Errorf("%w: %s", ErrCouldNotAcquireSemaphore, err.Error()),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.AccountAPI.AccountBalance(ctx,
		&types.AccountBalanceRequest{
			NetworkIdentifier: network,
			AccountIdentifier: account,
			BlockIdentifier:   block,
		},
	)
	if err != nil {
		return nil, nil, nil, nil, f.RequestFailedError(clientErr, err, "/account/balance")
	}

	if err := asserter.AccountBalanceResponse(
		block,
		response,
	); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf(
				"%w: /account/balance",
				err,
			),
		}
		return nil, nil, nil, nil, fetcherErr
	}

	return response.BlockIdentifier, response.Balances, response.Coins, response.Metadata, nil
}

// AccountBalanceRetry retrieves the validated AccountBalance
// with a specified number of retries and max elapsed time.
// If a block is provided, a historical lookup is performed.
func (f *Fetcher) AccountBalanceRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	account *types.AccountIdentifier,
	block *types.PartialBlockIdentifier,
) (*types.BlockIdentifier, []*types.Amount, []*types.Coin, map[string]interface{}, *Error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		responseBlock, balances, coins, metadata, err := f.AccountBalance(
			ctx,
			network,
			account,
			block,
		)
		if err == nil {
			return responseBlock, balances, coins, metadata, nil
		}

		if ctx.Err() != nil {
			return nil, nil, nil, nil, &Error{
				Err: ctx.Err(),
			}
		}

		if is, _ := asserter.Err(err.Err); is {
			fetcherErr := &Error{
				Err:       fmt.Errorf("%w: /account/balance not attempting retry", err.Err),
				ClientErr: err.ClientErr,
			}
			return nil, nil, nil, nil, fetcherErr
		}

		if err := tryAgain(
			fmt.Sprintf("account %s", types.PrintStruct(account)),
			backoffRetries,
			err,
		); err != nil {
			return nil, nil, nil, nil, err
		}
	}
}
