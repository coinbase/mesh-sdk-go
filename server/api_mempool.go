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

package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// A MempoolAPIController binds http requests to an api service and writes the service results to
// the http response
type MempoolAPIController struct {
	service  MempoolAPIServicer
	asserter *asserter.Asserter
}

// NewMempoolAPIController creates a default api controller
func NewMempoolAPIController(
	s MempoolAPIServicer,
	asserter *asserter.Asserter,
) Router {
	return &MempoolAPIController{
		service:  s,
		asserter: asserter,
	}
}

// Routes returns all of the api route for the MempoolAPIController
func (c *MempoolAPIController) Routes() Routes {
	return Routes{
		{
			"Mempool",
			strings.ToUpper("Post"),
			"/mempool",
			c.Mempool,
		},
		{
			"MempoolTransaction",
			strings.ToUpper("Post"),
			"/mempool/transaction",
			c.MempoolTransaction,
		},
	}
}

// Mempool - Get All Mempool Transactions
func (c *MempoolAPIController) Mempool(w http.ResponseWriter, r *http.Request) {
	networkRequest := &types.NetworkRequest{}
	if err := json.NewDecoder(r.Body).Decode(&networkRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	// Assert that NetworkRequest is correct
	if err := c.asserter.NetworkRequest(networkRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	result, serviceErr := c.service.Mempool(r.Context(), networkRequest)
	if serviceErr != nil {
		EncodeJSONResponse(serviceErr, http.StatusInternalServerError, w)

		return
	}

	EncodeJSONResponse(result, http.StatusOK, w)
}

// MempoolTransaction - Get a Mempool Transaction
func (c *MempoolAPIController) MempoolTransaction(w http.ResponseWriter, r *http.Request) {
	mempoolTransactionRequest := &types.MempoolTransactionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&mempoolTransactionRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	// Assert that MempoolTransactionRequest is correct
	if err := c.asserter.MempoolTransactionRequest(mempoolTransactionRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	result, serviceErr := c.service.MempoolTransaction(r.Context(), mempoolTransactionRequest)
	if serviceErr != nil {
		EncodeJSONResponse(serviceErr, http.StatusInternalServerError, w)

		return
	}

	EncodeJSONResponse(result, http.StatusOK, w)
}
