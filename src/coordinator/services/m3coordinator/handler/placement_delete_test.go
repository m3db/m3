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

package handler

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementDeleteHandler(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewPlacementDeleteHandler(mockClient)

	// Test delete success
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/placement/delete", nil)
	require.NotNil(t, req)
	mockPlacementService.EXPECT().Delete()
	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Test delete error
	w = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/placement/delete", nil)
	require.NotNil(t, req)
	mockPlacementService.EXPECT().Delete().Return(errors.New("error"))
	handler.ServeHTTP(w, req)

	resp = w.Result()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
