package headerforwarder

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var defaultInterestingHeaders = []string{
	"X-Client-Id",
	"X-Client-Type",
}

//
// Helpers/Mocks
//

// for whatever reason this isn't allowed to be named mockTransport
type fakeTransport struct {
	LastRequest *http.Request
	response    *http.Response
	err         error
}

func (f *fakeTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.LastRequest = r
	return f.response, f.err
}

func newMockTransport(incomingHeaders http.Header, err error) http.RoundTripper {
	resp := &http.Response{
		Header: incomingHeaders,
	}

	return &fakeTransport{
		response: resp,
		err:      err,
	}
}

type mockInvoker struct {
	LastRequestMD metadata.MD
	responseMD    metadata.MD
	err           error
}

func (m *mockInvoker) Invoke(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
	m.LastRequestMD, _ = metadata.FromOutgoingContext(ctx)

	// add response metadata to pointer from call option
	var headerOpt grpc.HeaderCallOption
	for _, opt := range opts {
		if o, ok := opt.(grpc.HeaderCallOption); ok {
			headerOpt = o
		}
	}

	for k, v := range m.responseMD {
		headerOpt.HeaderAddr.Append(k, v...)
	}

	return m.err
}

func newMockInvoker(responseMD metadata.MD, err error) *mockInvoker {
	return &mockInvoker{
		err:        err,
		responseMD: responseMD,
	}
}

func contextWithID() (context.Context, string) {
	ctx := ContextWithRosettaID(context.Background())
	return ctx, RosettaIDFromContext(ctx)
}

func newRequest(ctx context.Context, header http.Header) *http.Request {
	req := &http.Request{}

	if header == nil {
		req.Header = make(http.Header)
	} else {
		req.Header = header
	}

	return req.WithContext(ctx)
}

type mockHandler struct {
	LastRequestID string
}

func (m *mockHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.LastRequestID = RosettaIDFromRequest(r)
}

//
// Tests
//

func TestRoundTrip(t *testing.T) {
	tests := map[string]struct {
		interestingHeaders []string
		// a mock for headers already captured from a rosetta request by the forwarder
		outgoingHeaders http.Header
		// a mock for headers coming from a native node response to the forwarder
		incomingHeaders http.Header

		ExpectedError error
		// expected headers on the request sent to the native node
		ExpectedRequestHeaders http.Header
		// expected headers to be captured from the native node response
		ExpectedResponseHeaders http.Header
	}{
		"calls actual transport": {
			ExpectedError: nil,
		},

		"adds outgoing header to request": {
			outgoingHeaders: map[string][]string{
				"X-Client-Id": {"test"},
			},
			ExpectedRequestHeaders: map[string][]string{
				"X-Client-Id": {"test"},
			},
		},

		"adds multiple outgoing headers to request": {
			outgoingHeaders: map[string][]string{
				"X-Client-Id":   {"test"},
				"X-Client-Type": {"testclient"},
			},
			ExpectedRequestHeaders: map[string][]string{
				"X-Client-Id":   {"test"},
				"X-Client-Type": {"testclient"},
			},
		},

		"adds outgoing header with multiple values to request": {
			outgoingHeaders: map[string][]string{
				"X-Client-Id": {"test", "test2"},
			},
			ExpectedRequestHeaders: map[string][]string{
				"X-Client-Id": {"test", "test2"},
			},
		},

		"captures headers from response": {
			incomingHeaders: map[string][]string{
				"X-Client-Id":  {"test", "test2"},
				"Content-Type": {"application/json"},
			},
			ExpectedResponseHeaders: map[string][]string{
				"X-Client-Id": {"test", "test2"},
			},
		},

		"captures no headers if none are interesting": {
			interestingHeaders: []string{},
			incomingHeaders: map[string][]string{
				"X-Client-Id":  {"test", "test2"},
				"Content-Type": {"application/json"},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// default values because http Request always have a non-nil Header
			// We don't need to do this for expected response because it's compared to the
			// internal headers map in HeaderForwarder, which is nil if no headers are captured
			if test.ExpectedRequestHeaders == nil {
				test.ExpectedRequestHeaders = make(http.Header)
			}

			// for convenience we use default interesting headers
			if test.interestingHeaders == nil {
				test.interestingHeaders = defaultInterestingHeaders
			}

			ctx, requestID := contextWithID()
			mockTransport := newMockTransport(test.incomingHeaders, nil)

			hf := &HeaderForwarder{
				interestingHeaders: test.interestingHeaders,
				incomingHeaders:    make(map[string]http.Header),
				actualTransport:    mockTransport,
				outgoingHeaders: map[string]http.Header{
					requestID: test.outgoingHeaders,
				},
			}

			testReq := newRequest(ctx, nil)

			_, err := hf.RoundTrip(testReq)
			assert.Equal(t, test.ExpectedError, err)

			outgoingReq := mockTransport.(*fakeTransport).LastRequest
			capturedHeaders, _ := hf.GetResponseHeaders(requestID)

			assert.Equal(t, test.ExpectedRequestHeaders, outgoingReq.Header)
			assert.Equal(t, test.ExpectedResponseHeaders, capturedHeaders)
		})
	}
}

func TestUnaryClientInterceptor(t *testing.T) {
	tests := map[string]struct {
		interestingHeaders []string
		// a mock for headers already captured from a rosetta request by the forwarder
		outgoingHeaders http.Header
		// a mock for headers coming from a native node response to the forwarder
		incomingMD metadata.MD

		ExpectedError error
		// expected headers on the request sent to the native node
		ExpectedRequestMD metadata.MD
		// expected headers to be captured from the native node response
		ExpectedResponseHeaders http.Header
	}{
		"calls invoker": {},

		"adds outgoing headers to context": {
			outgoingHeaders: map[string][]string{
				"X-Client-Id": {"test"},
			},
			ExpectedRequestMD: map[string][]string{
				"x-client-id": {"test"},
			},
		},

		"adds multiple outgoing headers to context": {
			outgoingHeaders: map[string][]string{
				"X-Client-Id":   {"test"},
				"X-Client-Type": {"testclient"},
			},
			ExpectedRequestMD: map[string][]string{
				"x-client-id":   {"test"},
				"x-client-type": {"testclient"},
			},
		},

		"adds outgoing header with multiple values to context": {
			outgoingHeaders: map[string][]string{
				"X-Client-Id": {"test", "test2"},
			},
			ExpectedRequestMD: map[string][]string{
				"x-client-id": {"test", "test2"},
			},
		},

		"captures headers from response metadata": {
			incomingMD: map[string][]string{
				"x-client-id":  {"test", "test2"},
				"content-type": {"application/json"},
			},
			ExpectedResponseHeaders: map[string][]string{
				"X-Client-Id": {"test", "test2"},
			},
		},

		"captures headers with multiple values from response metadata": {
			incomingMD: map[string][]string{
				"x-client-id":  {"test", "test2"},
				"content-type": {"application/json"},
			},
			ExpectedResponseHeaders: map[string][]string{
				"X-Client-Id": {"test", "test2"},
			},
		},

		"captures no headers if none are interesting": {
			interestingHeaders: []string{},
			incomingMD: map[string][]string{
				"X-Client-Id":  {"test", "test2"},
				"Content-Type": {"application/json"},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// for convenience we use default interesting headers
			if test.interestingHeaders == nil {
				test.interestingHeaders = defaultInterestingHeaders
			}

			ctx, requestID := contextWithID()

			mock := newMockInvoker(test.incomingMD, nil)

			hf := &HeaderForwarder{
				interestingHeaders: test.interestingHeaders,
				incomingHeaders:    make(map[string]http.Header),
				actualTransport:    nil,
				outgoingHeaders: map[string]http.Header{
					requestID: test.outgoingHeaders,
				},
			}

			err := hf.UnaryClientInterceptor(ctx, "test", nil, nil, nil, mock.Invoke)
			assert.Nil(t, err)

			outgoingReqMD := mock.LastRequestMD
			capturedHeaders, _ := hf.GetResponseHeaders(requestID)

			assert.Equal(t, test.ExpectedRequestMD, outgoingReqMD)
			assert.Equal(t, test.ExpectedResponseHeaders, capturedHeaders)
		})
	}
}

func TestHeaderForwarderHandler(t *testing.T) {
	tests := map[string]struct {
		interestingHeaders []string
		// a mock for headers on the rosetta request
		requestHeaders http.Header

		// expected headers to be captured from the rosetta request
		ExpectedOutgoingHeaders http.Header
	}{
		"calls next handler": {},

		"adds request ID to context": {},

		"captures request headers": {
			requestHeaders: map[string][]string{
				"X-Client-Id":  {"test"},
				"Content-Type": {"application/json"},
			},
			ExpectedOutgoingHeaders: map[string][]string{
				"X-Client-Id": {"test"},
			},
		},

		"captures multiple request headers": {
			requestHeaders: map[string][]string{
				"X-Client-Id":  {"test"},
				"Content-Type": {"application/json"},
			},
			ExpectedOutgoingHeaders: map[string][]string{
				"X-Client-Id": {"test"},
			},
		},

		"captures request headers with multiple values": {
			requestHeaders: map[string][]string{
				"X-Client-Id":  {"test"},
				"Content-Type": {"application/json"},
			},
			ExpectedOutgoingHeaders: map[string][]string{
				"X-Client-Id": {"test"},
			},
		},

		"captures no headers if none are interesting": {
			interestingHeaders: []string{},
			requestHeaders: map[string][]string{
				"X-Client-Id":  {"test"},
				"Content-Type": {"application/json"},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// default values because http Request always have a non-nil Header
			if test.ExpectedOutgoingHeaders == nil {
				test.ExpectedOutgoingHeaders = make(http.Header)
			}

			if test.interestingHeaders == nil {
				test.interestingHeaders = defaultInterestingHeaders
			}

			mockHandler := &mockHandler{}
			r := &http.Request{
				Header: test.requestHeaders,
			}
			w := httptest.NewRecorder()

			hf := &HeaderForwarder{
				interestingHeaders: test.interestingHeaders,
				incomingHeaders:    make(map[string]http.Header),
				outgoingHeaders:    make(map[string]http.Header),
				actualTransport:    nil,
			}

			handler := hf.HeaderForwarderHandler(mockHandler)
			handler.ServeHTTP(w, r)

			requestID := mockHandler.LastRequestID
			assert.NotEmpty(t, requestID)

			// Check that the uuid can be parsed. We don't actually care about this
			// we just care that it's a string that looks like a uuid and this is easy
			_, err := uuid.Parse(requestID)
			assert.Nil(t, err)

			assert.Equal(t, test.ExpectedOutgoingHeaders, hf.outgoingHeaders[requestID])
		})
	}
}
