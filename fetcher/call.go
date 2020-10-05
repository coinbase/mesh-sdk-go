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
			Err: fmt.Errorf("%w: %s", ErrCouldNotAcquireSemaphore, err.Error()),
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
				Err:       fmt.Errorf("%w: /call not attempting retry", err.Err),
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
