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
	requestHeaders     map[string]http.Header
	interestingHeaders []string
	actualTransport    http.RoundTripper
}

// TODO: make transport an optional parameter, add "WithTransport" style functions to make it easier
// to add the actual RPC clients to this struct
func NewHeaderForwarder(interestingHeaders []string, transport http.RoundTripper) (*HeaderForwarder, error) {
	if len(interestingHeaders) == 0 {
		return nil, fmt.Errorf("must provide at least one interesting header")
	}

	return &HeaderForwarder{
		requestHeaders:     make(map[string]http.Header),
		interestingHeaders: interestingHeaders,
		actualTransport:    transport,
	}, nil
}

func (hf *HeaderForwarder) WithTransport(transport http.RoundTripper) *HeaderForwarder {
	hf.actualTransport = transport
	return hf
}

// RequestWithRequestID adds a unique ID to the request context. A new request is returned that contains the
// new context
func (hf *HeaderForwarder) RequestWithRequestID(req *http.Request) *http.Request {
	ctx := req.Context()
	ctxWithID := ContextWithRosettaID(ctx)
	requestWithID := req.WithContext(ctxWithID)

	return requestWithID
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

	// Only remember interesting headers
	headersToRemember := make(http.Header)
	for _, interestingHeader := range hf.interestingHeaders {
		headersToRemember.Set(interestingHeader, resp.Header.Get(interestingHeader))
	}

	hf.requestHeaders[rosettaRequestID] = headersToRemember
}

// shouldRememberMetadata reports whether response metadata should be remembered for a grpc unary
// RPC call. Response metadata will only be remembered if it contains any of the interesting headers.
func (hf *HeaderForwarder) shouldRememberMetadata(ctx context.Context, req any, resp metadata.MD) bool {
	rosettaID := RosettaIDFromContext(ctx)
	if rosettaID == "" {
		return false
	}

	// If any of the interesting headers are in the response metadata, remember it
	for _, interestingHeader := range hf.interestingHeaders {
		if _, responseHasHeader := resp[http.CanonicalHeaderKey(interestingHeader)]; responseHasHeader {
			return true
		}
	}

	return false
}

// rememberMetadata saves the native node response metadata. The response object is metadata retrieved
// from a native node GRPC unary RPC call.
func (hf *HeaderForwarder) rememberMetadata(ctx context.Context, req any, resp metadata.MD) {
	rosettaID := RosettaIDFromContext(ctx)

	headersToRemember := make(http.Header)
	for _, interestingHeader := range hf.interestingHeaders {
		for _, value := range resp.Get(interestingHeader) {
			headersToRemember.Set(interestingHeader, value)
		}
	}

	hf.requestHeaders[rosettaID] = headersToRemember
}

// GetResponseHeaders returns any headers that should be returned to a rosetta response. These
// consist of native node response headers/metadata that were remembered for a request ID.
func (hf *HeaderForwarder) getResponseHeaders(rosettaRequestID string) (http.Header, bool) {
	headers, ok := hf.requestHeaders[rosettaRequestID]

	// Delete the headers from the map after they are retrieved
	// This is safe to call even if the key doesn't exist
	delete(hf.requestHeaders, rosettaRequestID)

	return headers, ok
}

// HeaderForwarderHandler will allow the next handler to serve the request, and then checks
// if there are any native node response headers recorded for the request. If there are, it will set
// those headers on the response
func (hf *HeaderForwarder) HeaderForwarderHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("HeaderForwarder Handler")
		// add a unique ID to the request context, and make a new request for it
		requestWithID := hf.RequestWithRequestID(r)

		// Serve the request
		// NOTE: for servers using github.com/coinbase/mesh-geth-sdk, ResponseWriter::WriteHeader() WILL
		// be called here, so we can't set headers after this happens. We include a wrapper around the
		// response writer that allows us to set headers just before WriteHeader is called
		wrappedResponseWriter := NewResponseWriter(
			w,
			RosettaIDFromRequest(requestWithID),
			hf.getResponseHeaders,
		)
		next.ServeHTTP(wrappedResponseWriter, requestWithID)
	})
}

// RoundTrip implements http.RoundTripper and will be used to construct an http Client which
// saves the native node response headers if necessary.
func (hf *HeaderForwarder) RoundTrip(req *http.Request) (*http.Response, error) {
	fmt.Println("HeaderForwarder RoundTrip")
	resp, err := hf.actualTransport.RoundTrip(req)

	fmt.Println("HeaderForwarder RoundTrip: response headers", resp.Header)

	if err == nil && hf.shouldRememberHeaders(req, resp) {
		hf.rememberHeaders(req, resp)
	}

	return resp, err
}

func (hf *HeaderForwarder) UnaryClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	fmt.Println("HeaderForwarder grpc interceptor")

	fmt.Println("request id: ", RosettaIDFromContext(ctx))

	// append a header DialOption to the request
	var responseMD metadata.MD
	opts = append(opts, grpc.Header(&responseMD))

	err := invoker(ctx, method, req, reply, cc, opts...)

	if hf.shouldRememberMetadata(ctx, req, responseMD) {
		hf.rememberMetadata(ctx, req, responseMD)
	}

	// get headers from response
	fmt.Println("HeaderForwarder grpc interceptor: headers from response", responseMD)

	return err
}
