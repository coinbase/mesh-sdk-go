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
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"

	"github.com/coinbase/rosetta-sdk-go/models"
)

// UnsafeNetworkStatus returns the unvalidated response
// from the NetworkStatus method.
func (f *Fetcher) UnsafeNetworkStatus(
	ctx context.Context,
	metadata *map[string]interface{},
) (*models.NetworkStatusResponse, error) {
	networkStatus, _, err := f.rosettaClient.NetworkAPI.NetworkStatus(
		ctx,
		models.NetworkStatusRequest{
			Metadata: metadata,
		},
	)
	if err != nil {
		return nil, err
	}

	return networkStatus, nil
}

// NetworkStatus returns the validated response
// from the NetworkStatus method.
func (f *Fetcher) NetworkStatus(
	ctx context.Context,
	metadata *map[string]interface{},
) (*models.NetworkStatusResponse, error) {
	networkStatus, err := f.UnsafeNetworkStatus(ctx, metadata)
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
	metadata *map[string]interface{},
	maxElapsedTime time.Duration,
	maxRetries uint64,
) (*models.NetworkStatusResponse, error) {
	backoffRetries := backoffRetries(maxElapsedTime, maxRetries)

	for ctx.Err() == nil {
		networkStatus, err := f.NetworkStatus(ctx, metadata)
		if err == nil {
			return networkStatus, nil
		}

		if !tryAgain("network", backoffRetries, err) {
			break
		}
	}

	return nil, errors.New("exhausted retries for network")
}
