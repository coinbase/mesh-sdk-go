// Copyright 2025 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Generated by: OpenAPI Generator (https://openapi-generator.tech)

package client

import (
	_context "context"
	"fmt"
	"io"
	_nethttp "net/http"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Linger please
var (
	_ _context.Context
)

// SearchAPIService SearchAPI service
type SearchAPIService service

// SearchTransactions /search/transactions allows the caller to search for transactions that meet
// certain conditions. Some conditions include matching a transaction hash, containing an operation
// with a certain status, or containing an operation that affects a certain account.
// /search/transactions is considered an \indexer\ endpoint and Rosetta implementations are not
// required to complete it to adhere to the Rosetta spec. However, any Rosetta \indexer\ MUST
// support this endpoint.
func (a *SearchAPIService) SearchTransactions(
	ctx _context.Context,
	searchTransactionsRequest *types.SearchTransactionsRequest,
) (*types.SearchTransactionsResponse, *types.Error, error) {
	var (
		localVarPostBody interface{}
	)

	// create path and map variables
	localVarPath := a.client.cfg.BasePath + "/search/transactions"
	localVarHeaderParams := make(map[string]string)

	// to determine the Content-Type header
	localVarHTTPContentTypes := []string{"application/json"}

	// set Content-Type header
	localVarHTTPContentType := selectHeaderContentType(localVarHTTPContentTypes)
	if localVarHTTPContentType != "" {
		localVarHeaderParams["Content-Type"] = localVarHTTPContentType
	}

	// to determine the Accept header
	localVarHTTPHeaderAccepts := []string{"application/json"}

	// set Accept header
	localVarHTTPHeaderAccept := selectHeaderAccept(localVarHTTPHeaderAccepts)
	if localVarHTTPHeaderAccept != "" {
		localVarHeaderParams["Accept"] = localVarHTTPHeaderAccept
	}
	// body params
	localVarPostBody = searchTransactionsRequest

	r, err := a.client.prepareRequest(ctx, localVarPath, localVarPostBody, localVarHeaderParams)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare request: %w", err)
	}

	localVarHTTPResponse, err := a.client.callAPI(ctx, r)
	if err != nil || localVarHTTPResponse == nil {
		return nil, nil, fmt.Errorf("failed to call API: %w", err)
	}

	localVarBody, err := io.ReadAll(localVarHTTPResponse.Body)
	defer func() {
		_, _ = io.Copy(io.Discard, localVarHTTPResponse.Body)
		_ = localVarHTTPResponse.Body.Close()
	}()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	switch localVarHTTPResponse.StatusCode {
	case _nethttp.StatusOK:
		var v types.SearchTransactionsResponse
		err = a.client.decode(&v, localVarBody, localVarHTTPResponse.Header.Get("Content-Type"))
		if err != nil {
			return nil, nil, fmt.Errorf(
				"failed to decode when hit status code 200, response body %s: %w",
				string(localVarBody),
				err,
			)
		}

		return &v, nil, nil
	case _nethttp.StatusInternalServerError:
		var v types.Error
		err = a.client.decode(&v, localVarBody, localVarHTTPResponse.Header.Get("Content-Type"))
		if err != nil {
			return nil, nil, fmt.Errorf(
				"failed to decode when hit status code 500, response body %s: %w",
				string(localVarBody),
				err,
			)
		}

		return nil, &v, fmt.Errorf("error %+v", v)
	case _nethttp.StatusBadGateway,
		_nethttp.StatusServiceUnavailable,
		_nethttp.StatusGatewayTimeout,
		_nethttp.StatusRequestTimeout:
		return nil, nil, fmt.Errorf(
			"status code %d, response body %s: %w",
			localVarHTTPResponse.StatusCode,
			string(localVarBody),
			ErrRetriable,
		)
	default:
		return nil, nil, fmt.Errorf(
			"invalid status code %d, response body %s",
			localVarHTTPResponse.StatusCode,
			string(localVarBody),
		)
	}
}
