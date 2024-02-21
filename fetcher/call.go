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
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Call returns the validated response
// from the Call method.
func (f *Fetcher) Call(
	ctx context.Context,
	network *types.NetworkIdentifier,
	method string,
	parameters map[string]interface{},
) (map[string]interface{}, bool, *Error) {
	if err := f.connectionSemaphore.Acquire(ctx, semaphoreRequestWeight); err != nil {
		return nil, false, &Error{
			Err: fmt.Errorf("failed to acquire semaphore: %w", err),
		}
	}
	defer f.connectionSemaphore.Release(semaphoreRequestWeight)

	response, clientErr, err := f.rosettaClient.CallAPI.Call(
		ctx,
		&types.CallRequest{
			NetworkIdentifier: network,
			Method:            method,
			Parameters:        parameters,
		},
	)
	if err != nil {
		return nil, false, f.RequestFailedError(clientErr, err, "/call")
	}

	return response.Result, response.Idempotent, nil
}

// CallRetry invokes the /call endpoint with a specified
// number of retries and max elapsed time.
func (f *Fetcher) CallRetry(
	ctx context.Context,
	network *types.NetworkIdentifier,
	method string,
	parameters map[string]interface{},
) (map[string]interface{}, bool, *Error) {
	backoffRetries := backoffRetries(
		f.retryElapsedTime,
		f.maxRetries,
	)

	for {
		result, idempotent, err := f.Call(
			ctx,
			network,
			method,
			parameters,
		)
		if err == nil {
			return result, idempotent, nil
		}

		if ctx.Err() != nil {
			return nil, false, &Error{
				Err: ctx.Err(),
			}
		}

		if is, _ := asserter.Err(err.Err); is {
			fetcherErr := &Error{
				Err:       fmt.Errorf("/call not attempting retry: %w", err.Err),
				ClientErr: err.ClientErr,
			}
			return nil, false, fetcherErr
		}

		if err := tryAgain(
			fmt.Sprintf("/call %s:%s", method, types.PrintStruct(parameters)),
			backoffRetries,
			err,
		); err != nil {
			return nil, false, err
		}
	}
}
