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
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3cluster/placement"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementDeleteHandler(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewDeleteHandler(mockClient, config.Configuration{})

	// Test remove success
	w := httptest.NewRecorder()
	req := httptest.NewRequest("DELETE", "/placement/host1", nil)
	req = mux.SetURLVars(req, map[string]string{"id": "host1"})
	require.NotNil(t, req)
	mockPlacementService.EXPECT().RemoveInstances([]string{"host1"}).Return(placement.NewPlacement(), nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{\"instances\":{},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":0}", string(body))

	// Test remove failure
	w = httptest.NewRecorder()
	req = httptest.NewRequest("DELETE", "/placement/nope", nil)
	req = mux.SetURLVars(req, map[string]string{"id": "nope"})
	require.NotNil(t, req)
	mockPlacementService.EXPECT().RemoveInstances([]string{"nope"}).Return(placement.NewPlacement(), errors.New("ID does not exist"))
	handler.ServeHTTP(w, req)

	resp = w.Result()
	body, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	assert.Equal(t, "{\"error\":\"ID does not exist\"}\n", string(body))
}
