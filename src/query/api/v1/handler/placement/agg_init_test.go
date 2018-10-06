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

package placement

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3cluster/placement"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggPlacementInitHandler(t *testing.T) {
	var (
		mockClient, mockPlacementService = SetupPlacementTest(t)
		handler                          = &AggInitHandler{mockClient, config.Configuration{}}
		w                                = httptest.NewRecorder()
	)

	// Test placement init success
	req := httptest.NewRequest(AggInitHTTPMethod, AggInitURL, strings.NewReader(initTestSuccessRequestBody))
	require.NotNil(t, req)

	newPlacement, err := placement.NewPlacementFromProto(initTestPlacementProto)
	require.NoError(t, err)

	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Not(nil), 16, 1).Return(newPlacement, nil)
	handler.ServeHTTP(w, req)
	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, initTestSuccessResponseBody, string(body))

	// Test error response
	w = httptest.NewRecorder()
	req = httptest.NewRequest(AggInitHTTPMethod, AggInitURL, strings.NewReader(initTestInvalidRequestBody))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Not(nil), 64, 2).Return(nil, errors.New("unable to build initial placement"))
	handler.ServeHTTP(w, req)
	resp = w.Result()
	body, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, initTestInvalidRequestResponse, string(body))
}
