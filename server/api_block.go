// Copyright 2023 Coinbase, Inc.
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

// Generated by: OpenAPI Generator (https://openapi-generator.tech)

package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// A BlockAPIController binds http requests to an api service and writes the service results to the
// http response
type BlockAPIController struct {
	service  BlockAPIServicer
	asserter *asserter.Asserter
}

// NewBlockAPIController creates a default api controller
func NewBlockAPIController(
	s BlockAPIServicer,
	asserter *asserter.Asserter,
) Router {
	return &BlockAPIController{
		service:  s,
		asserter: asserter,
	}
}

// Routes returns all of the api route for the BlockAPIController
func (c *BlockAPIController) Routes() Routes {
	return Routes{
		{
			"Block",
			strings.ToUpper("Post"),
			"/block",
			c.Block,
		},
		{
			"BlockTransaction",
			strings.ToUpper("Post"),
			"/block/transaction",
			c.BlockTransaction,
		},
	}
}

// Block - Get a Block
func (c *BlockAPIController) Block(w http.ResponseWriter, r *http.Request) {
	blockRequest := &types.BlockRequest{}
	if err := json.NewDecoder(r.Body).Decode(&blockRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	// Assert that BlockRequest is correct
	if err := c.asserter.BlockRequest(blockRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	result, serviceErr := c.service.Block(r.Context(), blockRequest)
	if serviceErr != nil {
		EncodeJSONResponse(serviceErr, http.StatusInternalServerError, w)

		return
	}

	EncodeJSONResponse(result, http.StatusOK, w)
}

// BlockTransaction - Get a Block Transaction
func (c *BlockAPIController) BlockTransaction(w http.ResponseWriter, r *http.Request) {
	blockTransactionRequest := &types.BlockTransactionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&blockTransactionRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	// Assert that BlockTransactionRequest is correct
	if err := c.asserter.BlockTransactionRequest(blockTransactionRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	result, serviceErr := c.service.BlockTransaction(r.Context(), blockTransactionRequest)
	if serviceErr != nil {
		EncodeJSONResponse(serviceErr, http.StatusInternalServerError, w)

		return
	}

	EncodeJSONResponse(result, http.StatusOK, w)
}
