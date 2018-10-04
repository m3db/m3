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
	"github.com/m3db/m3cluster/shard"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementAddHandler_Force(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewAddHandler(mockClient, config.Configuration{})

	// Test add failure
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/placement", strings.NewReader("{\"force\": true, \"instances\":[]}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().AddInstances(gomock.Any()).Return(placement.NewPlacement(), nil, errors.New("no new instances found in the valid zone"))
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "{\"error\":\"no new instances found in the valid zone\"}\n", string(body))

	// Test add success
	w = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/placement", strings.NewReader("{\"force\": true, \"instances\":[{\"id\": \"host1\",\"isolation_group\": \"rack1\",\"zone\": \"test\",\"weight\": 1,\"endpoint\": \"http://host1:1234\",\"hostname\": \"host1\",\"port\": 1234}]}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).Return(placement.NewPlacement(), nil, nil)
	handler.ServeHTTP(w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{\"instances\":{},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":0}", string(body))
}

func TestPlacementAddHandler_SafeErr(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewAddHandler(mockClient, config.Configuration{})

	// Test add failure
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/placement", strings.NewReader("{\"instances\":[]}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().Placement().Return(placement.NewPlacement(), 0, errors.New("no new instances found in the valid zone"))
	mockPlacementService.EXPECT().AddInstances(gomock.Any()).Return(placement.NewPlacement(), nil, errors.New("no new instances found in the valid zone"))
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
	assert.Equal(t, "{\"error\":\"no new instances found in the valid zone\"}\n", string(body))

	// Current placement has initializing shards
	w = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/placement", strings.NewReader("{\"instances\":[{\"id\": \"host1\",\"isolation_group\": \"rack1\",\"zone\": \"test\",\"weight\": 1,\"endpoint\": \"http://host1:1234\",\"hostname\": \"host1\",\"port\": 1234}]}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().Placement().Return(newInitPlacement(), 0, nil)
	mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).Return(placement.NewPlacement(), nil, nil)
	handler.ServeHTTP(w, req)

	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, `{"error":"instances [A,B] do not have all shards available"}`+"\n", string(body))
}

func TestPlacementAddHandler_SafeOK(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewAddHandler(mockClient, config.Configuration{})

	// Test add success
	w := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/placement", strings.NewReader("{\"instances\":[{\"id\": \"host1\",\"isolation_group\": \"rack1\",\"zone\": \"test\",\"weight\": 1,\"endpoint\": \"http://host1:1234\",\"hostname\": \"host1\",\"port\": 1234}]}"))
	require.NotNil(t, req)

	mockPlacementService.EXPECT().Placement().Return(placement.NewPlacement(), 0, nil)
	mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).Return(placement.NewPlacement(), nil, nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{\"instances\":{},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":0}", string(body))
}

func newPlacement(state shard.State) placement.Placement {
	shards := shard.NewShards([]shard.Shard{
		shard.NewShard(1).SetState(state),
	})

	instA := placement.NewInstance().SetShards(shards).SetID("A")
	instB := placement.NewInstance().SetShards(shards).SetID("B")
	return placement.NewPlacement().SetInstances([]placement.Instance{instA, instB})
}

func newInitPlacement() placement.Placement {
	return newPlacement(shard.Initializing)
}

func newAvailPlacement() placement.Placement {
	return newPlacement(shard.Available)
}

func TestValidateNoInitializing(t *testing.T) {
	p := placement.NewPlacement()
	assert.NoError(t, validateAllAvailable(p))

	p = newAvailPlacement()
	assert.NoError(t, validateAllAvailable(p))

	p = newInitPlacement()
	assert.Error(t, validateAllAvailable(p))
}
