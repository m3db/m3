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
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReplaceRequest(body string) *http.Request {
	rb := strings.NewReader(body)
	return httptest.NewRequest(ReplaceHTTPMethod, M3DBReplaceURL, rb)
}

func TestPlacementReplaceHandler_Force(t *testing.T) {
	runForAllAllowedServices(func(s string) {
		t.Run(s, func(t *testing.T) {
			testPlacementReplaceHandlerForce(t, s)
		})
	})
}

func TestPlacementReplaceHandler_Safe_Err(t *testing.T) {
	runForAllAllowedServices(func(s string) {
		t.Run(s, func(t *testing.T) {
			testPlacementReplaceHandlerSafeErr(t, s)
		})
	})
}

func TestPlacementReplaceHandler_Safe_Ok(t *testing.T) {
	runForAllAllowedServices(func(s string) {
		t.Run(s, func(t *testing.T) {
			testPlacementReplaceHandlerSafeOk(t, s)
		})
	})
}

func testPlacementReplaceHandlerForce(t *testing.T, serviceName string) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
	handlerOpts, err := NewHandlerOptions(mockClient, config.Configuration{}, nil, instrument.NewOptions())
	require.NoError(t, err)
	handler := NewReplaceHandler(handlerOpts)
	handler.nowFn = func() time.Time { return time.Unix(0, 0) }

	w := httptest.NewRecorder()
	req := newReplaceRequest(`{"force": true, "leavingInstanceIDs": []}`)

	svcDefaults := handleroptions.ServiceNameAndDefaults{
		ServiceName: serviceName,
	}

	mockPlacementService.EXPECT().ReplaceInstances([]string{}, gomock.Any()).Return(placement.NewPlacement(), nil, errors.New("test"))
	handler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.JSONEq(t, `{"status":"error","error":"test"}`, string(body))
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	w = httptest.NewRecorder()
	req = newReplaceRequest(`{"force": true, "leavingInstanceIDs": ["a"]}`)
	mockPlacementService.EXPECT().ReplaceInstances([]string{"a"}, gomock.Not(nil)).Return(placement.NewPlacement(), nil, nil)
	handler.ServeHTTP(svcDefaults, w, req)
	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":0}`, string(body))
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func testPlacementReplaceHandlerSafeErr(t *testing.T, serviceName string) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
	handlerOpts, err := NewHandlerOptions(mockClient, config.Configuration{}, nil, instrument.NewOptions())
	require.NoError(t, err)
	handler := NewReplaceHandler(handlerOpts)
	handler.nowFn = func() time.Time { return time.Unix(0, 0) }

	w := httptest.NewRecorder()
	req := newReplaceRequest("{}")

	svcDefaults := handleroptions.ServiceNameAndDefaults{
		ServiceName: serviceName,
	}

	mockPlacementService.EXPECT().Placement().Return(newInitPlacement(), nil)
	if serviceName == handleroptions.M3CoordinatorServiceName {
		mockPlacementService.EXPECT().CheckAndSet(gomock.Any(), 0).
			Return(newInitPlacement().SetVersion(1), nil)
	}

	handler.ServeHTTP(svcDefaults, w, req)
	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	switch serviceName {
	case handleroptions.M3CoordinatorServiceName:
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	default:
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.JSONEq(t,
			`{"status":"error","error":"instances do not have all shards available: [A, B]"}`,
			string(body))
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
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
	handlerOpts, err := NewHandlerOptions(mockClient, config.Configuration{}, nil, instrument.NewOptions())
	require.NoError(t, err)
	handler := NewReplaceHandler(handlerOpts)
	handler.nowFn = func() time.Time { return time.Unix(0, 0) }

	pl := newAvailPlacement()

	matcher := gomock.Any()
	switch serviceName {
	case handleroptions.M3DBServiceName:
		pl = pl.SetIsSharded(true)
		matcher = newPlacementReplaceMatcher()
	case handleroptions.M3AggregatorServiceName:
		pl = pl.SetIsSharded(true).SetIsMirrored(true)
		matcher = newPlacementReplaceMatcher()
	default:
	}

	instances := pl.Instances()
	for i, inst := range instances {
		newInst := inst.SetIsolationGroup("r1").SetZone("z1").SetWeight(1)
		if serviceName == handleroptions.M3CoordinatorServiceName {
			newInst = newInst.SetShards(shard.NewShards([]shard.Shard{}))
		}
		instances[i] = newInst
	}

	pl = pl.SetInstances(instances).SetVersion(1)
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

	mockPlacementService.EXPECT().Placement().Return(pl.Clone(), nil)

	returnPl := pl.Clone()

	newInst := placement.NewInstance().
		SetID("C").
		SetZone("z1").
		SetIsolationGroup("r1").
		SetWeight(1)

	if !isStateless(serviceName) {
		newInst = newInst.SetShards(shard.NewShards([]shard.Shard{
			shard.NewShard(1).
				SetState(shard.Initializing).
				SetSourceID("A"),
		}))
	}

	instances = append(returnPl.Instances(), newInst)

	instances[0].Shards().Remove(1)
	instances[0].Shards().Add(shard.NewShard(1).SetState(shard.Leaving))

	if isStateless(serviceName) {
		instances = instances[1:]
	}

	returnPl = returnPl.
		SetInstances(instances).
		SetVersion(2)

	svcDefaults := handleroptions.ServiceNameAndDefaults{
		ServiceName: serviceName,
	}

	mockPlacementService.EXPECT().CheckAndSet(matcher, 1).Return(returnPl, nil)
	handler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)

	switch serviceName {
	case handleroptions.M3CoordinatorServiceName:
		exp := `{"placement":{"instances":{"B":{"id":"B","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}},"C":{"id":"C","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}}},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":2}`
		assert.Equal(t, exp, string(body))
	case handleroptions.M3DBServiceName:
		exp := `{"placement":{"instances":{"A":{"id":"A","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"LEAVING","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}},"B":{"id":"B","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"AVAILABLE","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}},"C":{"id":"C","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"INITIALIZING","sourceId":"A","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}}},"replicaFactor":0,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":2}`
		assert.Equal(t, exp, string(body))
	case handleroptions.M3AggregatorServiceName:
		exp := `{"placement":{"instances":{"A":{"id":"A","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"LEAVING","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}},"B":{"id":"B","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"AVAILABLE","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}},"C":{"id":"C","isolationGroup":"r1","zone":"z1","weight":1,"endpoint":"","shards":[{"id":1,"state":"INITIALIZING","sourceId":"A","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}}},"replicaFactor":0,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":true,"maxShardSetId":0},"version":2}`
		assert.Equal(t, exp, string(body))
	default:
		t.Errorf("unknown service name %s", serviceName)
	}

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
