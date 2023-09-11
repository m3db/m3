// Copyright (c) 2019 Uber Technologies, Inc.
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

package topic

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTopicDeleteHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockService := setupTest(t, ctrl)
	handler := newDeleteHandler(nil, config.Configuration{}, instrument.NewOptions())
	handler.(*DeleteHandler).serviceFn = testServiceFn(mockService)

	// Test successful get
	w := httptest.NewRecorder()
	req := httptest.NewRequest("DELETE", "/topic", nil)
	require.NotNil(t, req)

	mockService.
		EXPECT().
		Delete(DefaultTopicName).
		Return(nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	w = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/topic", nil)
	req.Header.Add(HeaderTopicName, "foobar")
	require.NotNil(t, req)

	mockService.
		EXPECT().
		Delete("foobar").
		Return(nil)
	handler.ServeHTTP(w, req)

	resp = w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	w = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/topic", nil)
	require.NotNil(t, req)

	mockService.
		EXPECT().
		Delete(DefaultTopicName).
		Return(kv.ErrNotFound)
	handler.ServeHTTP(w, req)

	resp = w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode,
		fmt.Sprintf("response: %s", w.Body.String()))
}
