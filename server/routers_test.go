// Copyright 2025 Coinbase, Inc.
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

package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

type badJSON struct{}

func (badJSON) MarshalJSON() ([]byte, error) {
	return nil, assert.AnError
}

func TestEncodeJSONResponse_Success(t *testing.T) {
	rec := httptest.NewRecorder()
	EncodeJSONResponse(map[string]string{"hello": "world"}, http.StatusOK, rec)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json; charset=UTF-8", rec.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"hello":"world"}`, rec.Body.String())
}

func TestEncodeJSONResponse_CustomStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	EncodeJSONResponse(map[string]string{"hello": "world"}, http.StatusCreated, rec)

	assert.Equal(t, http.StatusCreated, rec.Code)
	assert.JSONEq(t, `{"hello":"world"}`, rec.Body.String())
}

func TestEncodeJSONResponse_EncodeError(t *testing.T) {
	rec := httptest.NewRecorder()
	EncodeJSONResponse(badJSON{}, http.StatusOK, rec)

	// When encoding fails we should return 500, not the caller-provided status,
	// and we must not trigger a superfluous WriteHeader call.
	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), assert.AnError.Error())
}
