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

type Error struct {
	err      error
	clientErr *types.Error
}

// NetworkStatus returns the validated response
// from the NetworkStatus method.
func (f *Fetcher) NetworkStatus(
	ctx context.Context,
	network *types.NetworkIdentifier,
	metadata map[string]interface{},
) (*types.NetworkStatusResponse, *Error) {
	networkStatus, clientErr, err := f.rosettaClient.NetworkAPI.NetworkStatus(
		ctx,
		&types.NetworkRequest{
			NetworkIdentifier: network,
			Metadata:          metadata,
		},
	)
	if err != nil {
		res := &Error{
			err:      fmt.Errorf("%w: /network/status %s", ErrRequestFailed, err.Error()),
			clientErr: clientErr,
		}
		return nil, res
	}

	if err := asserter.NetworkStatusResponse(networkStatus); err != nil {
		res := &Error{
			err:      fmt.Errorf("%w: /network/status %s", ErrAssertionFailed, err.Error()),
			clientErr: clientErr,
		}
		return nil, res
	}

	return networkStatus, nil
}

// NetworkStatusRetry retrieves the validated NetworkStatus
// with a specified number of retries and max elapsed time.
func (f *Fetcher) NetworkStatusRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	metadata map[string]interface{},
) (*types.NetworkStatusResponse, error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		networkStatus, errRes := f.NetworkStatus(
			ctx,
			network,
			metadata,
		)
		if errRes == nil {
			continue
		}

		if errors.Is(errRes.err, ErrAssertionFailed) {
			return nil, fmt.Errorf("%w: /network/status not attempting retry", errRes.err)
		}

		if errRes.err == nil {
			return networkStatus, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if !tryAgain(
			fmt.Sprintf("network status %s", types.PrettyPrintStruct(network)),
			backoffRetries,
			errRes.err,
		) {
			break
		}
	}

	return nil, fmt.Errorf(
		"%w: unable to fetch network status %s",
		ErrExhaustedRetries,
		types.PrettyPrintStruct(network),
	)
}

// NetworkList returns the validated response
// from the NetworkList method.
func (f *Fetcher) NetworkList(
	ctx context.Context,
	metadata map[string]interface{},
) (*types.NetworkListResponse, *Error) {
	networkList, clientErr, err := f.rosettaClient.NetworkAPI.NetworkList(
		ctx,
		&types.MetadataRequest{
			Metadata: metadata,
		},
	)

	if err != nil {
		res := &Error{
			err:      fmt.Errorf("%w: /network/list %s", ErrRequestFailed, err.Error()),
			clientErr: clientErr,
		}
		return nil, res
	}

	if err := asserter.NetworkListResponse(networkList); err != nil {
		res := &Error{
			err:      fmt.Errorf("%w: /network/list %s", ErrAssertionFailed, err.Error()),
			clientErr: clientErr,
		}
		return nil, res
	}

	return networkList, nil
}

// NetworkListRetry retrieves the validated NetworkList
// with a specified number of retries and max elapsed time.
func (f *Fetcher) NetworkListRetry(
	ctx context.Context,
	metadata map[string]interface{},
) (*types.NetworkListResponse, error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		networkList, errRes := f.NetworkList(
			ctx,
			metadata,
		)
		if errRes == nil {
			continue
		}

		if errors.Is(errRes.err, ErrAssertionFailed) {
			return nil, fmt.Errorf("%w: /network/list not attempting retry", errRes.err)
		}

		if errRes.err == nil {
			return networkList, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if !tryAgain("NetworkList", backoffRetries, errRes.err) {
			break
		}
	}

	return nil, fmt.Errorf(
		"%w: unable to fetch network list",
		ErrExhaustedRetries,
	)
}

// NetworkOptions returns the validated response
// from the NetworkOptions method.
func (f *Fetcher) NetworkOptions(
	ctx context.Context,
	network *types.NetworkIdentifier,
	metadata map[string]interface{},
) (*types.NetworkOptionsResponse, *Error) {
	networkOptions, clientErr, err := f.rosettaClient.NetworkAPI.NetworkOptions(
		ctx,
		&types.NetworkRequest{
			NetworkIdentifier: network,
			Metadata:          metadata,
		},
	)

	if err != nil {
		res := &Error{
			err:      fmt.Errorf("%w: /network/options %s", ErrRequestFailed, err.Error()),
			clientErr: clientErr,
		}
		return nil, res
	}

	if err := asserter.NetworkOptionsResponse(networkOptions); err != nil {
		res := &Error{
			err:      fmt.Errorf("%w: /network/options %s", ErrAssertionFailed, err.Error()),
			clientErr: clientErr,
		}
		return nil, res
	}

	return networkOptions, nil
}

// NetworkOptionsRetry retrieves the validated NetworkOptions
// with a specified number of retries and max elapsed time.
func (f *Fetcher) NetworkOptionsRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	metadata map[string]interface{},
) (*types.NetworkOptionsResponse, error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		networkOptions, errRes := f.NetworkOptions(
			ctx,
			network,
			metadata,
		)
		if errRes == nil {
			continue
		}

		if errors.Is(errRes.err, ErrAssertionFailed) {
			return nil, fmt.Errorf("%w: /network/options not attempting retry", errRes.err)
		}

		if errRes.err == nil {
			return networkOptions, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if !tryAgain(
			fmt.Sprintf("network options %s", types.PrettyPrintStruct(network)),
			backoffRetries,
			errRes.err,
		) {
			break
		}
	}

	return nil, fmt.Errorf(
		"%w: unable to fetch network options %s",
		ErrExhaustedRetries,
		types.PrettyPrintStruct(network),
	)
}
