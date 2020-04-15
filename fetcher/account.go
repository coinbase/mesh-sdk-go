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
// from the AccountBalance method.
func (f *Fetcher) AccountBalance(
	ctx context.Context,
	network *types.NetworkIdentifier,
	account *types.AccountIdentifier,
) (*types.BlockIdentifier, []*types.Amount, error) {
	response, _, err := f.rosettaClient.AccountAPI.AccountBalance(ctx,
		&types.AccountBalanceRequest{
			NetworkIdentifier: network,
			AccountIdentifier: account,
		},
	)
	if err != nil {
		return nil, nil, err
	}

	block := response.BlockIdentifier
	balances := response.Balances
	if err := asserter.AccountBalance(block, balances); err != nil {
		return nil, nil, err
	}

	return block, balances, nil
}

// AccountBalanceRetry retrieves the validated AccountBalance
// with a specified number of retries and max elapsed time.
func (f *Fetcher) AccountBalanceRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	account *types.AccountIdentifier,
) (*types.BlockIdentifier, []*types.Amount, error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for ctx.Err() == nil {
		block, balances, err := f.AccountBalance(
			ctx,
			network,
			account,
		)
		if err == nil {
			return block, balances, nil
		}

		if !tryAgain(fmt.Sprintf("account %s", account.Address), backoffRetries, err) {
			break
		}
	}

	return nil, nil, errors.New("exhausted retries for account")
}
