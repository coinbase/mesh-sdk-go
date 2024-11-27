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
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func mockGetter(id string, headers http.Header) func(string) (http.Header, bool) {
	return func(s string) (http.Header, bool) {
		if s != id {
			return nil, false
		}

		return headers, true
	}
}

var mockHeader http.Header = http.Header{
	"Test-Header": []string{"test-value"},
}

func TestWriteHeader(t *testing.T) {
	writer := httptest.NewRecorder()
	hfrw := NewResponseWriter(writer, "test-id", mockGetter("test-id", mockHeader))

	hfrw.WriteHeader(200)

	result := writer.Result()
	assert.Equal(t, 200, result.StatusCode)
}

func TestWriteHeaderAddsHeaders(t *testing.T) {
	writer := httptest.NewRecorder()
	hfrw := NewResponseWriter(writer, "test-id", mockGetter("test-id", mockHeader))

	hfrw.WriteHeader(200)

	result := writer.Result()
	assert.Equal(t, 200, result.StatusCode)
	assert.Equal(t, mockHeader.Values("test-header"), result.Header.Values("test-header"))
}

// Tests that headers are written if Write is called before WriteHeader
func TestWriteAddsHeaders(t *testing.T) {
	writer := httptest.NewRecorder()
	hfrw := NewResponseWriter(writer, "test-id", mockGetter("test-id", mockHeader))

	_, err := hfrw.Write([]byte("test-bytes"))
	assert.Nil(t, err)

	result := writer.Result()
	assert.Equal(t, 200, result.StatusCode)
	assert.Equal(t, mockHeader.Values("test-header"), result.Header.Values("test-header"))
}
