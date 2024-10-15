// Copyright 2024 Coinbase, Inc.
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
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// A EventsAPIController binds http requests to an api service and writes the service results to the
// http response
type EventsAPIController struct {
	service            EventsAPIServicer
	asserter           *asserter.Asserter
	contextFromRequest func(*http.Request) context.Context
}

// NewEventsAPIController creates a default api controller
func NewEventsAPIController(
	s EventsAPIServicer,
	asserter *asserter.Asserter,
	contextFromRequest func(*http.Request) context.Context,
) Router {
	return &EventsAPIController{
		service:            s,
		asserter:           asserter,
		contextFromRequest: contextFromRequest,
	}
}

// Routes returns all of the api route for the EventsAPIController
func (c *EventsAPIController) Routes() Routes {
	return Routes{
		{
			"EventsBlocks",
			strings.ToUpper("Post"),
			"/events/blocks",
			c.EventsBlocks,
		},
	}
}

func (c *EventsAPIController) ContextFromRequest(r *http.Request) context.Context {
	ctx := r.Context()

	if c.contextFromRequest != nil {
		ctx = c.contextFromRequest(r)
	}

	return ctx
}

// EventsBlocks - [INDEXER] Get a range of BlockEvents
func (c *EventsAPIController) EventsBlocks(w http.ResponseWriter, r *http.Request) {
	eventsBlocksRequest := &types.EventsBlocksRequest{}
	if err := json.NewDecoder(r.Body).Decode(&eventsBlocksRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	// Assert that EventsBlocksRequest is correct
	if err := c.asserter.EventsBlocksRequest(eventsBlocksRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)

		return
	}

	result, serviceErr := c.service.EventsBlocks(c.ContextFromRequest(r), eventsBlocksRequest)
	if serviceErr != nil {
		EncodeJSONResponse(serviceErr, http.StatusInternalServerError, w)

		return
	}

	EncodeJSONResponse(result, http.StatusOK, w)
}
