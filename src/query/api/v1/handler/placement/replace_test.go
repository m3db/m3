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
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/cmd/services/m3query/config"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func newReplaceRequest(body string) *http.Request {
	rb := strings.NewReader(body)
	return httptest.NewRequest(ReplaceHTTPMethod, M3DBReplaceURL, rb)
}

func TestPlacementReplaceHandler_Force(t *testing.T) {
	runForAllAllowedServices(func(s string) {
		testPlacementReplaceHandlerForce(t, s)
	})
}

func TestPlacementReplaceHandler_Safe_Err(t *testing.T) {
	runForAllAllowedServices(func(s string) {
		testPlacementReplaceHandlerSafeErr(t, s)
	})
}

func TestPlacementReplaceHandler_Safe_Ok(t *testing.T) {
	runForAllAllowedServices(func(s string) {
		testPlacementReplaceHandlerSafeOk(t, s)
	})
}

func testPlacementReplaceHandlerForce(t *testing.T, serviceName string) {
	var (
		mockClient, mockPlacementService = SetupPlacementTest(t)
		handlerOpts                      = NewHandlerOptions(mockClient, config.Configuration{}, nil)
		handler                          = NewReplaceHandler(handlerOpts)
	)
	handler.nowFn = func() time.Time { return time.Unix(0, 0) }

	w := httptest.NewRecorder()
	req := newReplaceRequest(`{"force": true, "leavingInstanceIDs": []}`)

	mockPlacementService.EXPECT().ReplaceInstances([]string{}, gomock.Any()).Return(placement.NewPlacement(), nil, errors.New("test"))
	handler.ServeHTTP(serviceName, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, `{"error":"test"}`+"\n", string(body))
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	w = httptest.NewRecorder()
	req = newReplaceRequest(`{"force": true, "leavingInstanceIDs": ["a"]}`)
	mockPlacementService.EXPECT().ReplaceInstances([]string{"a"}, gomock.Not(nil)).Return(placement.NewPlacement(), nil, nil)
	handler.ServeHTTP(serviceName, w, req)
	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":0}`, string(body))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func testPlacementReplaceHandlerSafeErr(t *testing.T, serviceName string) {
	var (
		mockClient, mockPlacementService = SetupPlacementTest(t)
		handlerOpts                      = NewHandlerOptions(mockClient, config.Configuration{}, nil)
		handler                          = NewReplaceHandler(handlerOpts)
	)
	handler.nowFn = func() time.Time { return time.Unix(0, 0) }

	w := httptest.NewRecorder()
	req := newReplaceRequest("{}")

	mockPlacementService.EXPECT().Placement().Return(newInitPlacement(), nil)
	if serviceName == M3CoordinatorServiceName {
		mockPlacementService.EXPECT().CheckAndSet(gomock.Any(), 0)
	}
	handler.ServeHTTP(serviceName, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	switch serviceName {
	case M3CoordinatorServiceName:
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	default:
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, `{"error":"instances [A,B] do not have all shards available"}`+"\n", string(body))
	}
}

type placementReplaceMatcher struct{}

func (placementReplaceMatcher) Matches(x interface{}) bool {
	pl := x.(placement.Placement)

	instA, ok := pl.Instance("A")
	if !ok {
		return false
	}

	instC, ok := pl.Instance("C")
	if !ok {
		return false
	}

	return instA.Shards().NumShardsForState(shard.Leaving) == 1 &&
		instC.Shards().NumShardsForState(shard.Initializing) == 1
}

func (placementReplaceMatcher) String() string {
	return "matches if the placement has instance A leaving and C initializing"
}

func newPlacementReplaceMatcher() gomock.Matcher {
	return placementReplaceMatcher{}
}

func testPlacementReplaceHandlerSafeOk(t *testing.T, serviceName string) {
	var (
		mockClient, mockPlacementService = SetupPlacementTest(t)
		handlerOpts                      = NewHandlerOptions(mockClient, config.Configuration{}, nil)
		handler                          = NewReplaceHandler(handlerOpts)
	)
	handler.nowFn = func() time.Time { return time.Unix(0, 0) }

	pl := newAvailPlacement()

	matcher := gomock.Any()
	switch serviceName {
	case M3DBServiceName:
		pl = pl.SetIsSharded(true)
		matcher = newPlacementReplaceMatcher()
	case M3AggregatorServiceName:
		pl = pl.SetIsSharded(true).SetIsMirrored(true)
		matcher = newPlacementReplaceMatcher()
	default:
	}

	instances := pl.Instances()
	for i, inst := range instances {
		newInst := inst.SetIsolationGroup("r1").SetZone("z1").SetWeight(1)
		if serviceName == M3CoordinatorServiceName {
			newInst = newInst.SetShards(shard.NewShards([]shard.Shard{}))
		}
		instances[i] = newInst
	}
	pl = pl.SetInstances(instances)

	w := httptest.NewRecorder()
	req := newReplaceRequest(`
	{
		"leavingInstanceIDs": ["A"],
		"candidates": [
			{
				"id": "C",
				"zone": "z1",
				"isolation_group": "r1",
				"weight": 1
			}
		]
	}
	`)

	mockPlacementService.EXPECT().Placement().Return(pl, nil)
	mockPlacementService.EXPECT().CheckAndSet(matcher, 1).Return(2, nil)
	handler.ServeHTTP(serviceName, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	switch serviceName {
	case M3CoordinatorServiceName:
		exp := `{"placement":{"instances":{"B":{"id":"B","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[],"shardSetId":0,"hostname":"","port":0},"C":{"id":"C","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[],"shardSetId":0,"hostname":"","port":0}},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":2}`
		assert.Equal(t, exp, string(body))
	case M3DBServiceName:
		exp := `{"placement":{"instances":{"A":{"id":"A","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"LEAVING","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0},"B":{"id":"B","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"AVAILABLE","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0},"C":{"id":"C","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"INITIALIZING","sourceId":"A","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0}},"replicaFactor":0,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":2}`
		assert.Equal(t, exp, string(body))
	case M3AggregatorServiceName:
		exp := `{"placement":{"instances":{"A":{"id":"A","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"LEAVING","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0},"B":{"id":"B","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"AVAILABLE","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0},"C":{"id":"C","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"INITIALIZING","sourceId":"A","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0}},"replicaFactor":0,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":true,"maxShardSetId":0},"version":2}`
		assert.Equal(t, exp, string(body))
	default:
		t.Errorf("unknown service name %s", serviceName)
	}

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
