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
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /network/status %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, fetcherErr
	}

	if err := asserter.NetworkStatusResponse(networkStatus); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /network/status %s", ErrAssertionFailed, err.Error()),
		}
		return nil, fetcherErr
	}

	return networkStatus, nil
}

// NetworkStatusRetry retrieves the validated NetworkStatus
// with a specified number of retries and max elapsed time.
func (f *Fetcher) NetworkStatusRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	metadata map[string]interface{},
) (*types.NetworkStatusResponse, *Error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		networkStatus, err := f.NetworkStatus(
			ctx,
			network,
			metadata,
		)
		if err == nil {
			return networkStatus, nil
		}

		if errors.Is(err.Err, ErrAssertionFailed) {
			fetcherErr := &Error{
				Err:       fmt.Errorf("%w: /network/status not attempting retry", err.Err),
				ClientErr: err.ClientErr,
			}
			return nil, fetcherErr
		}

		if ctx.Err() != nil {
			fetcherErr := &Error{
				Err:       ctx.Err(),
				ClientErr: err.ClientErr,
			}
			return nil, fetcherErr
		}

		if !tryAgain(
			fmt.Sprintf("network status %s", types.PrettyPrintStruct(network)),
			backoffRetries,
			err.Err,
		) {
			break
		}
	}

	return nil, &Error{
		Err: fmt.Errorf(
			"%w: unable to fetch network status %s",
			ErrExhaustedRetries,
			types.PrettyPrintStruct(network),
		)}
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
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /network/list %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, fetcherErr
	}

	if err := asserter.NetworkListResponse(networkList); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /network/list %s", ErrAssertionFailed, err.Error()),
		}
		return nil, fetcherErr
	}

	return networkList, nil
}

// NetworkListRetry retrieves the validated NetworkList
// with a specified number of retries and max elapsed time.
func (f *Fetcher) NetworkListRetry(
	ctx context.Context,
	metadata map[string]interface{},
) (*types.NetworkListResponse, *Error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		networkList, err := f.NetworkList(
			ctx,
			metadata,
		)
		if err == nil {
			return networkList, nil
		}

		if errors.Is(err.Err, ErrAssertionFailed) {
			fetcherErr := &Error{
				Err:       fmt.Errorf("%w: /network/list not attempting retry", err.Err),
				ClientErr: err.ClientErr,
			}
			return nil, fetcherErr
		}

		if ctx.Err() != nil {
			fetcherErr := &Error{
				Err:       ctx.Err(),
				ClientErr: err.ClientErr,
			}
			return nil, fetcherErr
		}

		if !tryAgain("NetworkList", backoffRetries, err.Err) {
			break
		}
	}

	return nil, &Error{
		Err: fmt.Errorf(
			"%w: unable to fetch network list",
			ErrExhaustedRetries,
		)}
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
		fetcherErr := &Error{
			Err:       fmt.Errorf("%w: /network/options %s", ErrRequestFailed, err.Error()),
			ClientErr: clientErr,
		}
		return nil, fetcherErr
	}

	if err := asserter.NetworkOptionsResponse(networkOptions); err != nil {
		fetcherErr := &Error{
			Err: fmt.Errorf("%w: /network/options %s", ErrAssertionFailed, err.Error()),
		}
		return nil, fetcherErr
	}

	return networkOptions, nil
}

// NetworkOptionsRetry retrieves the validated NetworkOptions
// with a specified number of retries and max elapsed time.
func (f *Fetcher) NetworkOptionsRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	metadata map[string]interface{},
) (*types.NetworkOptionsResponse, *Error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		networkOptions, err := f.NetworkOptions(
			ctx,
			network,
			metadata,
		)
		if err == nil {
			return networkOptions, nil
		}

		if errors.Is(err.Err, ErrAssertionFailed) {
			fetcherErr := &Error{
				Err:       fmt.Errorf("%w: /network/options not attempting retry", err.Err),
				ClientErr: err.ClientErr,
			}
			return nil, fetcherErr
		}

		if ctx.Err() != nil {
			fetcherErr := &Error{
				Err:       ctx.Err(),
				ClientErr: err.ClientErr,
			}
			return nil, fetcherErr
		}

		if !tryAgain(
			fmt.Sprintf("network options %s", types.PrettyPrintStruct(network)),
			backoffRetries,
			err.Err,
		) {
			break
		}
	}

	return nil, &Error{
		Err: fmt.Errorf(
			"%w: unable to fetch network options %s",
			ErrExhaustedRetries,
			types.PrettyPrintStruct(network)),
	}
}
