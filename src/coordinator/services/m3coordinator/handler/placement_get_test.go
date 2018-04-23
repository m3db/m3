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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementGetHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)
	mockPlacementService := placement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	handler := NewPlacementGetHandler(mockClient)
	w := httptest.NewRecorder()

	req, err := http.NewRequest("GET", "/placement/get", nil)
	require.NoError(t, err)

	mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil)
	mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).Return(mockPlacementService, nil)
	mockPlacementService.EXPECT().Placement().Return(placement.NewPlacement(), 0, nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{}}", string(body))
}
