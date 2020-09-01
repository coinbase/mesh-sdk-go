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
	"fmt"
	"log"
	"time"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/cenkalti/backoff"
)

// backoffRetries creates the backoff.BackOff struct used by all
// *Retry functions in the fetcher.
func backoffRetries(
	maxElapsedTime time.Duration,
	maxRetries uint64,
) backoff.BackOff {
	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.MaxElapsedTime = maxElapsedTime
	return backoff.WithMaxRetries(exponentialBackoff, maxRetries)
}

// tryAgain handles a backoff and prints error messages depending
// on the fetchMsg.
func tryAgain(fetchMsg string, thisBackoff backoff.BackOff, err *Error) *Error {
	// Only retry if an error is explicitly retriable.
	if err.ClientErr == nil || !err.ClientErr.Retriable {
		return err
	}

	nextBackoff := thisBackoff.NextBackOff()
	if nextBackoff == backoff.Stop {
		return &Error{
			Err: fmt.Errorf(
				"%w: %s",
				ErrExhaustedRetries,
				fetchMsg,
			),
		}
	}

	errMessage := err.Err.Error()
	if err.ClientErr != nil {
		errMessage = types.PrintStruct(err.ClientErr)
	}

	log.Printf("%s: retrying fetch for %s after %fs\n", errMessage, fetchMsg, nextBackoff.Seconds())
	time.Sleep(nextBackoff)

	return nil
}

// checkError compares a *fetcher.Error to a simple type error and returns
// a boolean indicating if they are equivalent
func checkError(fetcherErr *Error, err error) bool {
	if fetcherErr == nil {
		return err == nil
	}
	return errors.Is(fetcherErr.Err, err)
}

// CheckNetworkListForNetwork returns a boolean
// indicating if a *types.NetworkIdentifier is present
// in the list of supported networks.
func CheckNetworkListForNetwork(
	networkList *types.NetworkListResponse,
	networkIdentifier *types.NetworkIdentifier,
) (bool, []*types.NetworkIdentifier) {
	networkMatched := false
	supportedNetworks := []*types.NetworkIdentifier{}
	for _, availableNetwork := range networkList.NetworkIdentifiers {
		if types.Hash(availableNetwork) == types.Hash(networkIdentifier) {
			networkMatched = true
			break
		}

		supportedNetworks = append(supportedNetworks, availableNetwork)
	}

	return networkMatched, supportedNetworks
}
