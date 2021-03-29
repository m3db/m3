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
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementAddHandler_Force(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)

		handlerOpts, err := NewHandlerOptions(
			mockClient, config.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)

		handler := NewAddHandler(handlerOpts)
		handler.nowFn = func() time.Time { return time.Unix(0, 0) }

		// Test add failure
		var (
			w   = httptest.NewRecorder()
			req *http.Request
		)
		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL,
				strings.NewReader(`{"force": true, "instances":[]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL,
				strings.NewReader(`{"force": true, "instances":[]}`))
		}
		require.NotNil(t, req)

		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}
		mockPlacementService.EXPECT().AddInstances(gomock.Any()).Return(
			placement.NewPlacement(),
			nil,
			errors.New("no new instances found in the valid zone"))
		handler.ServeHTTP(svcDefaults, w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		assert.JSONEq(t,
			`{"status":"error","error":"no new instances found in the valid zone"}`,
			string(body))
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

		// Test add success
		w = httptest.NewRecorder()
		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL,
				strings.NewReader(`{"force": true, "instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL,
				strings.NewReader(`{"force": true, "instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		}
		require.NotNil(t, req)

		mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).
			Return(placement.NewPlacement(), nil, nil)
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		body, _ = ioutil.ReadAll(resp.Body)
		assert.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":0}`, string(body))
		assert.Equal(t, http.StatusOK, resp.StatusCode)

	})
}

func TestPlacementAddHandler_SafeErr_NoNewInstance(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := setupPlacementTest(t, ctrl, newValidAvailPlacement())
		handlerOpts, err := NewHandlerOptions(
			mockClient, config.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)
		handler := NewAddHandler(handlerOpts)

		// Test add failure
		var (
			w   = httptest.NewRecorder()
			req *http.Request
		)

		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3AggAddURL, strings.NewReader(`{"instances":[]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[]}`))
		}

		require.NotNil(t, req)
		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		handler.ServeHTTP(svcDefaults, w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		assert.JSONEq(t,
			`{"status":"error","error":"no new instances found in the valid zone"}`,
			string(body))
	})
}

func TestPlacementAddHandler_SafeErr_NotAllAvailable(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient := setupPlacementTest(t, ctrl, newValidInitPlacement())
		handlerOpts, err := NewHandlerOptions(
			mockClient, config.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)
		handler := NewAddHandler(handlerOpts)

		// Test add failure
		var (
			w   = httptest.NewRecorder()
			req *http.Request
		)
		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3AggAddURL,
				strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL,
				strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		}

		require.NotNil(t, req)
		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		handler.ServeHTTP(svcDefaults, w, req)
		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.JSONEq(t,
			`{"status":"error","error":"instances do not have all shards available: [A, B]"}`,
			string(body))
	})
}

func TestPlacementAddHandler_SafeOK(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
		handlerOpts, err := NewHandlerOptions(
			mockClient, config.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)
		handler := NewAddHandler(handlerOpts)
		handler.nowFn = func() time.Time { return time.Unix(0, 0) }

		// Test add error
		var (
			w   = httptest.NewRecorder()
			req *http.Request
		)

		switch serviceName {
		case handleroptions.M3AggregatorServiceName:
			req = httptest.NewRequest(AddHTTPMethod, M3AggAddURL,
				strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		default:
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL,
				strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		}

		require.NotNil(t, req)
		var (
			existingPlacement = placement.NewPlacement().
						SetIsSharded(true)
			newPlacement = placement.NewPlacement().
					SetIsSharded(true)
		)

		switch serviceName {
		case handleroptions.M3CoordinatorServiceName:
			existingPlacement = existingPlacement.
				SetIsSharded(false).
				SetReplicaFactor(1)
			newPlacement = existingPlacement.
				SetIsSharded(false).
				SetReplicaFactor(1)
		case handleroptions.M3AggregatorServiceName:
			existingPlacement = existingPlacement.
				SetIsMirrored(true).
				SetReplicaFactor(1)
			newPlacement = newPlacement.
				SetIsMirrored(true).
				SetReplicaFactor(1)
		}

		mockPlacementService.EXPECT().AddInstances(gomock.Any()).
			Return(nil, nil, errors.New("test err"))
		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		handler.ServeHTTP(svcDefaults, w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		require.JSONEq(t, `{"status":"error","error":"test err"}`, string(body))

		w = httptest.NewRecorder()
		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3AggAddURL,
				strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL,
				strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		}

		require.NotNil(t, req)
		newInst := placement.NewInstance().
			SetID("host1").
			SetIsolationGroup("rack1").
			SetZone("test").
			SetWeight(1).
			SetEndpoint("http://host1:1234").
			SetHostname("host1").
			SetPort(1234)

		returnPlacement := newPlacement.Clone().SetInstances([]placement.Instance{
			newInst,
		})

		if serviceName == handleroptions.M3CoordinatorServiceName {
			mockPlacementService.EXPECT().AddInstances(gomock.Any()).
				Return(returnPlacement.SetVersion(1), nil, nil)
		} else {
			mockPlacementService.EXPECT().AddInstances(gomock.Any()).
				Return(existingPlacement.Clone().SetVersion(1), nil, nil)
		}
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		body, _ = ioutil.ReadAll(resp.Body)

		switch serviceName {
		case handleroptions.M3CoordinatorServiceName:
			require.Equal(t, `{"placement":{"instances":{"host1":{"id":"host1","isolationGroup":"rack1","zone":"test","weight":1,"endpoint":"http://host1:1234","shards":[],"shardSetId":0,"hostname":"host1","port":1234,"metadata":{"debugPort":0}}},"replicaFactor":1,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":1}`, string(body))
		case handleroptions.M3AggregatorServiceName:
			require.Equal(t, `{"placement":{"instances":{},"replicaFactor":1,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":true,"maxShardSetId":0},"version":1}`, string(body))
		default:
			require.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":1}`, string(body))
		}

		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}
