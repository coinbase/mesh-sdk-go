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
	"fmt"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// HeaderExtractingTransport is a utility to help a rosetta server forward headers to and from
// native node requests. It implements several interfaces to achieve that:
//   - http.RoundTripper: this can be used to create an http Client that will automatically save headers
//     if necessary
//   - func(http.Handler) http.Handler: this can be used to wrap an http.Handler to set headers
//     on the response
//
// the headers can be requested later.
//
// TODO: this should expire entries after a certain amount of time
type HeaderForwarder struct {
	incomingHeaders    map[string]http.Header
	outgoingHeaders    map[string]http.Header
	interestingHeaders []string
	actualTransport    http.RoundTripper
}

func NewHeaderForwarder(
	interestingHeaders []string,
	transport http.RoundTripper,
) (*HeaderForwarder, error) {
	if len(interestingHeaders) == 0 {
		return nil, fmt.Errorf("must provide at least one interesting header")
	}

	return &HeaderForwarder{
		incomingHeaders:    make(map[string]http.Header),
		outgoingHeaders:    make(map[string]http.Header),
		interestingHeaders: interestingHeaders,
		actualTransport:    transport,
	}, nil
}

func (hf *HeaderForwarder) WithTransport(transport http.RoundTripper) *HeaderForwarder {
	hf.actualTransport = transport
	return hf
}

func (hf *HeaderForwarder) captureOutgoingHeaders(req *http.Request) {
	ctx := req.Context()
	rosettaRequestID := RosettaIDFromContext(ctx)

	hf.outgoingHeaders[rosettaRequestID] = make(http.Header)

	// Only capture interesting headers
	for _, interestingHeader := range hf.interestingHeaders {
		if _, requestHasHeader := req.Header[http.CanonicalHeaderKey(interestingHeader)]; requestHasHeader {
			hf.outgoingHeaders[rosettaRequestID].Set(interestingHeader, req.Header.Get(interestingHeader))
		}
	}
}

// shouldRememberHeaders reports whether response headers should be remembered for a
// given request. Response headers will only be remembered if the request does not contain all of
// the interesting headers and the response contains at least one of the interesting headers.
//
// It should be noted that the request and response here are for a request to the native node,
// not a request to the Rosetta server.
func (hf *HeaderForwarder) shouldRememberHeaders(req *http.Request, resp *http.Response) bool {
	requestHasAllHeaders := true
	responseHasSomeHeaders := false

	for _, interestingHeader := range hf.interestingHeaders {
		_, requestHasHeader := req.Header[http.CanonicalHeaderKey(interestingHeader)]
		_, responseHasHeader := resp.Header[http.CanonicalHeaderKey(interestingHeader)]

		if !requestHasHeader {
			requestHasAllHeaders = false
		}

		if responseHasHeader {
			responseHasSomeHeaders = true
		}
	}

	// only remember headers if the request does not contain all of the interesting headers and the
	// response contains at least one
	return !requestHasAllHeaders && responseHasSomeHeaders
}

// rememberHeaders saves the native node response headers. The request object here is a
// native node request (e.g. one constructed by go-ethereum for geth-based rosetta implementations).
// The response object is a native node response.
func (hf *HeaderForwarder) rememberHeaders(req *http.Request, resp *http.Response) {
	ctx := req.Context()
	rosettaRequestID := RosettaIDFromContext(ctx)

	// For multiple requests with the same rosetta ID, we want to remember all of the headers
	// For repeated response headers, later values will overwrite earlier ones
	headersToRemember, exists := hf.incomingHeaders[rosettaRequestID]
	if !exists {
		headersToRemember = make(http.Header)
	}

	// Only remember interesting headers
	for _, interestingHeader := range hf.interestingHeaders {
		headersToRemember.Set(interestingHeader, resp.Header.Get(interestingHeader))
	}

	hf.incomingHeaders[rosettaRequestID] = headersToRemember
}

// shouldRememberMetadata reports whether response metadata should be remembered for a grpc unary
// RPC call. Response metadata will only be remembered if it contains any of the interesting headers.
func (hf *HeaderForwarder) shouldRememberMetadata(ctx context.Context, resp metadata.MD) bool {
	rosettaID := RosettaIDFromContext(ctx)
	if rosettaID == "" {
		return false
	}

	// If any of the interesting headers are in the response metadata, remember it
	// grpc metadata uses lowercase keys rather than http canonicalized keys
	for _, interestingHeader := range hf.interestingHeaders {
		if _, responseHasHeader := resp[strings.ToLower(interestingHeader)]; responseHasHeader {
			return true
		}
	}

	return false
}

// rememberMetadata saves the native node response metadata. The response object is metadata retrieved
// from a native node GRPC unary RPC call.
func (hf *HeaderForwarder) rememberMetadata(ctx context.Context, resp metadata.MD) {
	rosettaID := RosettaIDFromContext(ctx)

	// For multiple requests with the same rosetta ID, we want to remember all of the headers
	// For repeated response headers, later values will overwrite earlier ones
	headersToRemember, exists := hf.incomingHeaders[rosettaID]
	if !exists {
		headersToRemember = make(http.Header)
	}

	for _, interestingHeader := range hf.interestingHeaders {
		for _, value := range resp.Get(strings.ToLower(interestingHeader)) {
			headersToRemember.Set(interestingHeader, value)
		}
	}

	hf.incomingHeaders[rosettaID] = headersToRemember
}

// GetResponseHeaders returns any headers that should be returned to a rosetta response. These
// consist of native node response headers/metadata that were remembered for a request ID.
func (hf *HeaderForwarder) getResponseHeaders(rosettaRequestID string) (http.Header, bool) {
	headers, ok := hf.incomingHeaders[rosettaRequestID]

	// Delete the headers from the map after they are retrieved
	// This is safe to call even if the key doesn't exist
	delete(hf.incomingHeaders, rosettaRequestID)

	// Also delete the outgoing headers from the map since we are done with them
	delete(hf.outgoingHeaders, rosettaRequestID)

	return headers, ok
}

// HeaderForwarderHandler will allow the next handler to serve the request, and then checks
// if there are any native node response headers recorded for the request. If there are, it will set
// those headers on the response
func (hf *HeaderForwarder) HeaderForwarderHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// add a unique ID to the request context, and make a new request for it
		requestWithID := RequestWithRequestID(r)

		// Capture outgoing interesting headers
		hf.captureOutgoingHeaders(requestWithID)

		// NOTE: for servers using github.com/coinbase/mesh-geth-sdk, ResponseWriter::WriteHeader() WILL
		// be called internally, so we can't set headers after this happens. We include a wrapper around the
		// response writer that allows us to set headers just before WriteHeader is called
		wrappedResponseWriter := NewResponseWriter(
			w,
			RosettaIDFromRequest(requestWithID),
			hf.getResponseHeaders,
		)

		// Serve the request
		next.ServeHTTP(wrappedResponseWriter, requestWithID)
	})
}

// RoundTrip implements http.RoundTripper and will be used to construct an http Client which
// saves the native node response headers if necessary.
func (hf *HeaderForwarder) RoundTrip(req *http.Request) (*http.Response, error) {
	// add outgoing headers to the request
	if outgoingHeaders, ok := hf.outgoingHeaders[RosettaIDFromRequest(req)]; ok {
		for header, values := range outgoingHeaders {
			for _, value := range values {
				req.Header.Add(header, value)
			}
		}
	}

	resp, err := hf.actualTransport.RoundTrip(req)

	if err == nil && hf.shouldRememberHeaders(req, resp) {
		hf.rememberHeaders(req, resp)
	}

	return resp, err
}

func (hf *HeaderForwarder) UnaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// Capture incoming headers from the grpc call
	var header metadata.MD
	opts = append(opts, grpc.Header(&header))

	// Add outgoing headers to the context
	if outgoingHeaders, ok := hf.outgoingHeaders[RosettaIDFromContext(ctx)]; ok {
		for header, values := range outgoingHeaders {
			for _, value := range values {
				ctx = metadata.AppendToOutgoingContext(ctx, strings.ToLower(header), value)
			}
		}
	}

	// Invoke the grpc call
	err := invoker(ctx, method, req, reply, cc, opts...)

	if hf.shouldRememberMetadata(ctx, header) {
		hf.rememberMetadata(ctx, header)
	}

	return err
}
