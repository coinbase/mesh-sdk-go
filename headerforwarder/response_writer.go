package headerforwarder

import (
	"net/http"
)

// HeaderForwarderResponseWriter is a wrapper around a http.ResponseWriter that allows us to set headers
// just before the WriteHeader function is called. These headers will be extracted from native node
// responses, and set on the rosetta response.
type HeaderForwarderResponseWriter struct {
	writer               http.ResponseWriter
	RosettaRequestID     string
	GetAdditionalHeaders func(string) (http.Header, bool)
}

func NewHeaderForwarderResponseWriter(
	writer http.ResponseWriter,
	rosettaRequestID string,
	getAdditionalHeaders func(string) (http.Header, bool),
) *HeaderForwarderResponseWriter {
	return &HeaderForwarderResponseWriter{
		writer:               writer,
		RosettaRequestID:     rosettaRequestID,
		GetAdditionalHeaders: getAdditionalHeaders,
	}
}

// Header passes through to the underlying ResponseWriter instance
func (hfrw *HeaderForwarderResponseWriter) Header() http.Header {
	return hfrw.writer.Header()
}

// Write passes through to the underlying ResponseWriter instance
func (hfrw *HeaderForwarderResponseWriter) Write(b []byte) (int, error) {
	return hfrw.writer.Write(b)
}

// WriteHeader will add any final extracted headers, and then pass through to the underlying ResponseWriter instance
func (hfrw *HeaderForwarderResponseWriter) WriteHeader(statusCode int) {
	hfrw.AddExtractedHeaders()
	hfrw.writer.WriteHeader(statusCode)
}

func (hfrw *HeaderForwarderResponseWriter) AddExtractedHeaders() {
	headers, hasAdditionalHeaders := hfrw.GetAdditionalHeaders(hfrw.RosettaRequestID)

	if hasAdditionalHeaders {
		for key, values := range headers {
			for _, value := range values {
				hfrw.writer.Header().Add(key, value)
			}
		}
	}
}
