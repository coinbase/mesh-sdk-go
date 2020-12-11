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
	"log"

	utils "github.com/coinbase/rosetta-sdk-go/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Error wraps the two possible types of error responses returned
// by the Rosetta Client
type Error struct {
	Err       error        `json:"err"`
	ClientErr *types.Error `json:"client_err"`

	// Retry is a boolean that indicates if the request should be retried.
	// It is the combination of the *types.Error.Retriable status and a
	// collection of transient errors.
	Retry bool `json:"retry"`
}

// RequestFailedError creates a new *Error and asserts the provided
// rosettaErr was provided in /network/options.
func (f *Fetcher) RequestFailedError(
	rosettaErr *types.Error,
	err error,
	message string,
) *Error {
	// Only check for error correctness if err is not context.Canceled
	// and it is not transient (usually caused by the client failing
	// the request).
	if !errors.Is(err, context.Canceled) && !transientError(err) {
		// If there is a *types.Error assertion error, we log it instead
		// of exiting. Exiting abruptly here may cause unintended consequences.
		if assertionErr := f.Asserter.Error(rosettaErr); assertionErr != nil {
			log.Printf("error %s assertion failed: %s", types.PrintStruct(rosettaErr), assertionErr)
		}
	}

	return &Error{
		Err:       fmt.Errorf("%w: %s %s", ErrRequestFailed, message, err.Error()),
		ClientErr: rosettaErr,
		Retry: ((rosettaErr != nil && rosettaErr.Retriable) || transientError(err) || f.forceRetry) &&
			!errors.Is(err, context.Canceled),
	}
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

// Err takes an error as an argument and returns
// whether or not the error is one thrown by the fetcher package
func Err(err error) bool {
	fetcherErrors := []error{
		ErrNoNetworks,
		ErrNetworkMissing,
		ErrRequestFailed,
		ErrExhaustedRetries,
		ErrCouldNotAcquireSemaphore,
	}

	return utils.FindError(fetcherErrors, err)
}
