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
	"net/http"
)

// ResponseWriter is a wrapper around a http.ResponseWriter that allows us to set headers
// just before the WriteHeader function is called. These headers will be extracted from native node
// responses, and set on the rosetta response.
type ResponseWriter struct {
	writer               http.ResponseWriter
	calledWriteHeaders   bool
	RosettaRequestID     string
	GetAdditionalHeaders func(string) (http.Header, bool)
}

func NewResponseWriter(
	writer http.ResponseWriter,
	rosettaRequestID string,
	getAdditionalHeaders func(string) (http.Header, bool),
) *ResponseWriter {
	return &ResponseWriter{
		writer:               writer,
		RosettaRequestID:     rosettaRequestID,
		GetAdditionalHeaders: getAdditionalHeaders,
	}
}

// Header passes through to the underlying ResponseWriter instance
func (hfrw *ResponseWriter) Header() http.Header {
	return hfrw.writer.Header()
}

// Write passes through to the underlying ResponseWriter instance
func (hfrw *ResponseWriter) Write(b []byte) (int, error) {
	if !hfrw.calledWriteHeaders {
		hfrw.AddExtractedHeaders()
	}

	return hfrw.writer.Write(b)
}

// WriteHeader will add any final extracted headers, and then pass through to the underlying ResponseWriter instance
func (hfrw *ResponseWriter) WriteHeader(statusCode int) {
	hfrw.calledWriteHeaders = true
	hfrw.AddExtractedHeaders()
	hfrw.writer.WriteHeader(statusCode)
}

func (hfrw *ResponseWriter) AddExtractedHeaders() {
	headers, hasAdditionalHeaders := hfrw.GetAdditionalHeaders(hfrw.RosettaRequestID)

	if hasAdditionalHeaders {
		for key, values := range headers {
			for _, value := range values {
				hfrw.writer.Header().Add(key, value)
			}
		}
	}
}
