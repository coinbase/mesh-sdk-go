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
	"errors"
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
	response, clientErr, err := f.rosettaClient.AccountAPI.AccountBalance(ctx,
		&types.AccountBalanceRequest{
			NetworkIdentifier: network,
			AccountIdentifier: account,
			BlockIdentifier:   block,
		},
	)
	if err != nil {
		res := &Error{
			Err:       fmt.Errorf("%w: /account/balance %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, nil, nil, nil, res
	}

	if err := asserter.AccountBalanceResponse(
		block,
		response,
	); err != nil {
		res := &Error{
			Err: fmt.Errorf(
				"%w: /account/balance %s",
				ErrAssertionFailed,
				err.Error(),
			),
		}
		return nil, nil, nil, nil, res
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
) (*types.BlockIdentifier, []*types.Amount, []*types.Coin, map[string]interface{}, error) {
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

		if errors.Is(err.Err, ErrAssertionFailed) {
			return nil, nil, nil, nil, fmt.Errorf(
				"%w: /account/balance not attempting retry",
				err.Err,
			)
		}

		if ctx.Err() != nil {
			return nil, nil, nil, nil, ctx.Err()
		}

		if !tryAgain(
			fmt.Sprintf("account %s", types.PrettyPrintStruct(account)),
			backoffRetries,
			err.Err,
		) {
			break
		}
	}

	return nil, nil, nil, nil, fmt.Errorf(
		"%w: unable to fetch account %s",
		ErrExhaustedRetries,
		types.PrettyPrintStruct(account),
	)
}
