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
	"errors"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Error wraps the two possible types of error responses returned
// by the Rosetta Client
type Error struct {
	Err       error        `json:"err"`
	ClientErr *types.Error `json:"client_err"`
}

var (
	// ErrNoNetworks is returned when there are no
	// networks available for syncing.
	ErrNoNetworks = errors.New("no networks available")

	// ErrNetworkMissing is returned during asserter initialization
	// when the provided *types.NetworkIdentifier is not in the
	// *types.NetworkListResponse.
	ErrNetworkMissing = errors.New("network missing")

	// ErrRequestFailed is returned when a request fails.
	ErrRequestFailed = errors.New("request failed")

	// ErrExhaustedRetries is returned when a request with retries
	// fails because it was attempted too many times.
	ErrExhaustedRetries = errors.New("retries exhausted")

	// ErrCouldNotAcquireSemaphore is returned when acquiring
	// the connection semaphore returns an error.
	ErrCouldNotAcquireSemaphore = errors.New("could not acquire semaphore")
)
