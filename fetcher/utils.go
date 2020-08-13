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
	"log"
	"strings"
	"time"

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
func tryAgain(fetchMsg string, thisBackoff backoff.BackOff, err error) bool {
	fetchMsg = strings.Replace(fetchMsg, "\n", "", -1)
	log.Printf("%s fetch error: %s\n", fetchMsg, err.Error())

	nextBackoff := thisBackoff.NextBackOff()
	if nextBackoff == backoff.Stop {
		return false
	}

	log.Printf("retrying fetch for %s after %fs\n", fetchMsg, nextBackoff.Seconds())
	time.Sleep(nextBackoff)

	return true
}

// checkError compares a *fetcher.Error to a simple type error and returns
// a boolean indicating if they are equivalent
func checkError(fetcherErr *Error, err error) bool {
	if fetcherErr == nil {
		return err == nil
	}
	return errors.Is(fetcherErr.Err, err)
}
