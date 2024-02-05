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
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/fatih/color"

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
			msg := fmt.Sprintf(
				"error %s assertion failed: %s%s",
				types.PrintStruct(rosettaErr),
				assertionErr,
				f.metaData,
			)
			color.Cyan(msg)
			log.Println(msg)
		}
	}
	// if the err is context.Canceled, do not print it
	// context.Canceled could because of validation succeed,
	// print an error in succeed situation will be confused
	if !strings.Contains(err.Error(), context.Canceled.Error()) {
		errForPrint := fmt.Errorf("%s %s: %w%s", message, err.Error(), ErrRequestFailed, f.metaData)
		color.Red(errForPrint.Error())
	}

	return &Error{
		Err:       fmt.Errorf("%s %s: %w", message, err.Error(), ErrRequestFailed),
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
)

// Err takes an error as an argument and returns
// whether or not the error is one thrown by the fetcher package
func Err(err error) bool {
	fetcherErrors := []error{
		ErrNoNetworks,
		ErrNetworkMissing,
		ErrRequestFailed,
		ErrExhaustedRetries,
	}

	return utils.FindError(fetcherErrors, err)
}
