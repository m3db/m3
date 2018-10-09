// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package xhttp

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestWriteJSONResponse(t *testing.T) {
	recorder := httptest.NewRecorder()
	WriteJSONResponse(recorder, struct {
		Foo string `json:"foo"`
	}{
		Foo: "bar",
	}, zap.NewNop())

	assert.Equal(t, http.StatusOK, recorder.Code)
	assert.Equal(t, `{"foo":"bar"}`, recorder.Body.String())
}

func TestWriteJSONResponseError(t *testing.T) {
	recorder := httptest.NewRecorder()
	WriteJSONResponse(recorder, math.Inf(1), zap.NewNop())

	assertWroteJSONError(t, recorder, http.StatusInternalServerError)
}

func TestWriteUninitializedResponse(t *testing.T) {
	recorder := httptest.NewRecorder()
	WriteUninitializedResponse(recorder, zap.NewNop())

	assertWroteJSONError(t, recorder, http.StatusServiceUnavailable)
}

func assertWroteJSONError(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	code int,
) {
	assert.Equal(t, code, recorder.Code)
	var resp errorResponse
	err := json.NewDecoder(recorder.Body).Decode(&resp)
	require.NoError(t, err)
	assert.NotEqual(t, "", resp.Error)
}
