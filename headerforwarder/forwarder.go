package headerforwarder

import (
	"net/http"
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

func NewHeaderForwarder(interestingHeaders []string, transport http.RoundTripper) *HeaderForwarder {
	return &HeaderForwarder{
		requestHeaders:     make(map[string]http.Header),
		interestingHeaders: interestingHeaders,
		actualTransport:    transport,
	}
}

// RoundTrip implements http.RoundTripper and will be used to construct an http Client which
// saves the native node response headers if necessary.
func (hf *HeaderForwarder) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := hf.actualTransport.RoundTrip(req)

	if err == nil && hf.shouldRememberHeaders(req, resp) {
		hf.rememberHeaders(req, resp)
	}

	return resp, err
}

// shouldRememberHeaders is called to determine if response headers should be remembered for a
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

// rememberHeaders is called to save the native node response headers. The request object
// here is a native node request (constructed by go-ethereum for geth-based rosetta implementations).
// The response object is a native node response.
func (hf *HeaderForwarder) rememberHeaders(req *http.Request, resp *http.Response) {
	ctx := req.Context()
	// rosettaRequestID := services.osettaIdFromContext(ctx)
	rosettaRequestID := RosettaIDFromContext(ctx)

	// Only remember interesting headers
	headersToRemember := make(http.Header)
	for _, interestingHeader := range hf.interestingHeaders {
		headersToRemember.Set(interestingHeader, resp.Header.Get(interestingHeader))
	}

	hf.requestHeaders[rosettaRequestID] = headersToRemember
}

// GetResponseHeaders returns any native node response headers that were recorded for a request ID.
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

		// add a unique ID to the request context, and make a new request for it
		requestWithID := hf.WithRequestID(r)

		// Serve the request
		// NOTE: ResponseWriter::WriteHeader() WILL be called here, so we can't set headers after this happens
		// We include a wrapper around the response writer that allows us to set headers just before
		// WriteHeader is called
		wrappedResponseWriter := NewHeaderForwarderResponseWriter(
			w,
			RosettaIDFromRequest(requestWithID),
			hf.getResponseHeaders,
		)
		next.ServeHTTP(wrappedResponseWriter, requestWithID)
	})
}

// WithRequestID adds a unique ID to the request context. A new request is returned that contains the
// new context
func (hf *HeaderForwarder) WithRequestID(req *http.Request) *http.Request {
	ctx := req.Context()
	ctxWithID := ContextWithRosettaID(ctx)
	requestWithID := req.WithContext(ctxWithID)

	return requestWithID
}
