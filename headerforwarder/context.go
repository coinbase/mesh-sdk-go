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

package headerforwarder

import (
	"context"
	"net/http"

	"github.com/google/uuid"
)

type contextKey string

const requestIDKey = contextKey("request_id")

const outgoingHeadersKey = contextKey("outgoing_headers")

func ContextWithRosettaID(ctx context.Context) context.Context {
	return context.WithValue(ctx, requestIDKey, uuid.NewString())
}

func RosettaIDFromContext(ctx context.Context) string {
	switch val := ctx.Value(requestIDKey).(type) {
	case string:
		return val
	default:
		return ""
	}
}

func RosettaIDFromRequest(r *http.Request) string {
	switch value := r.Context().Value(requestIDKey).(type) {
	case string:
		return value
	default:
		return ""
	}
}

// RequestWithRequestID adds a unique ID to the request context. A new request is returned that contains the
// new context
func RequestWithRequestID(req *http.Request) *http.Request {
	ctx := req.Context()
	ctxWithID := ContextWithRosettaID(ctx)
	requestWithID := req.WithContext(ctxWithID)

	return requestWithID
}

func ContextWithOutgoingHeaders(ctx context.Context, headers http.Header) context.Context {
	return context.WithValue(ctx, outgoingHeadersKey, headers)
}

func OutgoingHeadersFromContext(ctx context.Context) http.Header {
	switch val := ctx.Value(outgoingHeadersKey).(type) {
	case http.Header:
		return val
	default:
		return nil
	}
}
