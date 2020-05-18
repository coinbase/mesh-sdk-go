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

	"github.com/coinbase/rosetta-sdk-go/asserter"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// NetworkStatus returns the validated response
// from the NetworkStatus method.
func (f *Fetcher) NetworkStatus(
	ctx context.Context,
	network *types.NetworkIdentifier,
	metadata map[string]interface{},
) (*types.NetworkStatusResponse, error) {
	networkStatus, _, err := f.rosettaClient.NetworkAPI.NetworkStatus(
		ctx,
		&types.NetworkRequest{
			NetworkIdentifier: network,
			Metadata:          metadata,
		},
	)
	if err != nil {
		return nil, err
	}

	if err := asserter.NetworkStatusResponse(networkStatus); err != nil {
		return nil, err
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
		networkStatus, err := f.NetworkStatus(
			ctx,
			network,
			metadata,
		)
		if err == nil {
			return networkStatus, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if !tryAgain("NetworkStatus", backoffRetries, err) {
			break
		}
	}

	return nil, errors.New("exhausted retries for NetworkStatus")
}

// NetworkList returns the validated response
// from the NetworList method.
func (f *Fetcher) NetworkList(
	ctx context.Context,
	metadata map[string]interface{},
) (*types.NetworkListResponse, error) {
	networkList, _, err := f.rosettaClient.NetworkAPI.NetworkList(
		ctx,
		&types.MetadataRequest{
			Metadata: metadata,
		},
	)
	if err != nil {
		return nil, err
	}

	if err := asserter.NetworkListResponse(networkList); err != nil {
		return nil, err
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
		networkList, err := f.NetworkList(
			ctx,
			metadata,
		)
		if err == nil {
			return networkList, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if !tryAgain("NetworkList", backoffRetries, err) {
			break
		}
	}

	return nil, errors.New("exhausted retries for NetworkList")
}

// NetworkOptions returns the validated response
// from the NetworList method.
func (f *Fetcher) NetworkOptions(
	ctx context.Context,
	network *types.NetworkIdentifier,
	metadata map[string]interface{},
) (*types.NetworkOptionsResponse, error) {
	NetworkOptions, _, err := f.rosettaClient.NetworkAPI.NetworkOptions(
		ctx,
		&types.NetworkRequest{
			NetworkIdentifier: network,
			Metadata:          metadata,
		},
	)
	if err != nil {
		return nil, err
	}

	if err := asserter.NetworkOptionsResponse(NetworkOptions); err != nil {
		return nil, err
	}

	return NetworkOptions, nil
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
		networkOptions, err := f.NetworkOptions(
			ctx,
			network,
			metadata,
		)
		if err == nil {
			return networkOptions, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if !tryAgain("NetworkOptions", backoffRetries, err) {
			break
		}
	}

	return nil, errors.New("exhausted retries for NetworkOptions")
}
