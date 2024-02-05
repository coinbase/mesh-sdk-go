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
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/cenkalti/backoff"

	"github.com/coinbase/rosetta-sdk-go/client"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// connectionResetByPeer is returned when the server resets a connection
	// because of high memory usage or because a client has opened too many
	// connections.
	connectionResetByPeer = "connection reset by peer"

	// clientTimeout is returned when a request exceeds the set
	// HTTP timeout setting.
	clientTimeout = "Client.Timeout exceeded"

	// serverClosedIdleConnection is returned when the client
	// attempts to make a request on a connection that was closed
	// by the server.
	serverClosedIdleConnection = "server closed idle connection"
)

// Backoff wraps backoff.BackOff so we can
// access the retry count (which is private
// on backoff.BackOff).
type Backoff struct {
	backoff  backoff.BackOff
	attempts int
}

// backoffRetries creates the backoff.BackOff struct used by all
// *Retry functions in the fetcher.
func backoffRetries(
	maxElapsedTime time.Duration,
	maxRetries uint64,
) *Backoff {
	exponentialBackoff := backoff.NewExponentialBackOff()
	exponentialBackoff.MaxElapsedTime = maxElapsedTime
	return &Backoff{backoff: backoff.WithMaxRetries(exponentialBackoff, maxRetries)}
}

// transientError returns a boolean indicating if a particular
// error is considered transient (so the request should be
// retried). Currently, we consider EOF, connection reset by
// peer, and timeout to be transient.
func transientError(err error) bool {
	if errors.Is(err, client.ErrRetriable) ||
		strings.Contains(err.Error(), io.EOF.Error()) ||
		strings.Contains(err.Error(), connectionResetByPeer) ||
		strings.Contains(err.Error(), serverClosedIdleConnection) ||
		strings.Contains(err.Error(), clientTimeout) {
		return true
	}

	return false
}

// tryAgain handles a backoff and prints error messages depending
// on the fetchMsg.
func tryAgain(fetchMsg string, thisBackoff *Backoff, err *Error) *Error {
	if !err.Retry {
		return err
	}

	nextBackoff := thisBackoff.backoff.NextBackOff()
	if nextBackoff == backoff.Stop {
		return &Error{
			Err: fmt.Errorf(
				"fetch message %s: %w",
				fetchMsg,
				ErrExhaustedRetries,
			),
		}
	}

	errMessage := err.Err.Error()
	if err.ClientErr != nil {
		errMessage = types.PrintStruct(err.ClientErr)
	}

	thisBackoff.attempts++
	log.Printf(
		"retrying fetch for %s after %fs (prior attempts: %d): %s\n",
		fetchMsg,
		nextBackoff.Seconds(),
		thisBackoff.attempts,
		errMessage,
	)
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
