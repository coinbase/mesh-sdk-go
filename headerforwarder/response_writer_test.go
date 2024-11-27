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

	hfrw.Write([]byte("test-bytes"))

	result := writer.Result()
	assert.Equal(t, 200, result.StatusCode)
	assert.Equal(t, mockHeader.Values("test-header"), result.Header.Values("test-header"))
}
