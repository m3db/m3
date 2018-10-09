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

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3cluster/placement"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementAddHandler_Force(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		var (
			mockClient, mockPlacementService = SetupPlacementTest(t)
			handlerOpts                      = NewHandlerOptions(
				mockClient, config.Configuration{}, nil)
			handler = NewAddHandler(handlerOpts)
		)
		handler.nowFn = func() time.Time { return time.Unix(0, 0) }

		// Test add failure
		var (
			w   = httptest.NewRecorder()
			req *http.Request
		)
		if serviceName == M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"force": true, "instances":[]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"force": true, "instances":[]}`))
		}
		require.NotNil(t, req)

		mockPlacementService.EXPECT().AddInstances(gomock.Any()).Return(placement.NewPlacement(), nil, errors.New("no new instances found in the valid zone"))
		handler.ServeHTTP(serviceName, w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		assert.Equal(t, "{\"error\":\"no new instances found in the valid zone\"}\n", string(body))
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

		// Test add success
		w = httptest.NewRecorder()
		if serviceName == M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"force": true, "instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"force": true, "instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		}
		require.NotNil(t, req)

		mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).Return(placement.NewPlacement(), nil, nil)
		handler.ServeHTTP(serviceName, w, req)

		resp = w.Result()
		body, _ = ioutil.ReadAll(resp.Body)
		assert.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":0}`, string(body))
		assert.Equal(t, http.StatusOK, resp.StatusCode)

	})
}

func TestPlacementAddHandler_SafeErr(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		var (
			mockClient, mockPlacementService = SetupPlacementTest(t)
			handlerOpts                      = NewHandlerOptions(
				mockClient, config.Configuration{}, nil)
			handler = NewAddHandler(handlerOpts)
		)

		// Test add failure
		var (
			w   = httptest.NewRecorder()
			req *http.Request
		)
		if serviceName == M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[]}`))
		}
		require.NotNil(t, req)

		mockPlacementService.EXPECT().Placement().Return(placement.NewPlacement(), 0, errors.New("no new instances found in the valid zone"))
		mockPlacementService.EXPECT().AddInstances(gomock.Any()).Return(placement.NewPlacement(), nil, errors.New("no new instances found in the valid zone"))
		handler.ServeHTTP(serviceName, w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		assert.Equal(t, "{\"error\":\"no new instances found in the valid zone\"}\n", string(body))

		// Current placement has initializing shards
		w = httptest.NewRecorder()
		if serviceName == M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		}
		require.NotNil(t, req)

		mockPlacementService.EXPECT().Placement().Return(newInitPlacement(), 0, nil)
		mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).Return(placement.NewPlacement(), nil, nil)
		handler.ServeHTTP(serviceName, w, req)

		resp = w.Result()
		body, _ = ioutil.ReadAll(resp.Body)
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		assert.Equal(t, `{"error":"instances [A,B] do not have all shards available"}`+"\n", string(body))
	})
}

func TestPlacementAddHandler_SafeOK(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		var (
			mockClient, mockPlacementService = SetupPlacementTest(t)
			handlerOpts                      = NewHandlerOptions(
				mockClient, config.Configuration{}, nil)
			handler = NewAddHandler(handlerOpts)
		)
		handler.nowFn = func() time.Time { return time.Unix(0, 0) }

		// Test add error
		var (
			w   = httptest.NewRecorder()
			req *http.Request
		)
		if serviceName == M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		}
		require.NotNil(t, req)

		var (
			existingPlacement = placement.NewPlacement().
						SetIsSharded(true)
			newPlacement = placement.NewPlacement().
					SetIsSharded(true)
		)

		if serviceName == M3AggregatorServiceName {
			existingPlacement = existingPlacement.
				SetIsMirrored(true).
				SetReplicaFactor(1)
			newPlacement = newPlacement.
				SetIsMirrored(true).
				SetReplicaFactor(1)
		}
		mockPlacementService.EXPECT().Placement().Return(existingPlacement, 0, nil)
		mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).Return(newPlacement, nil, nil)
		mockPlacementService.EXPECT().CheckAndSet(gomock.Any(), 0).Return(errors.New("test err"))
		handler.ServeHTTP(serviceName, w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		require.Equal(t, `{"error":"test err"}`+"\n", string(body))

		w = httptest.NewRecorder()
		if serviceName == M3AggregatorServiceName {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		} else {
			req = httptest.NewRequest(AddHTTPMethod, M3DBAddURL, strings.NewReader(`{"instances":[{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234}]}`))
		}
		require.NotNil(t, req)

		mockPlacementService.EXPECT().Placement().Return(existingPlacement, 0, nil)
		mockPlacementService.EXPECT().AddInstances(gomock.Not(nil)).Return(newPlacement, nil, nil)
		mockPlacementService.EXPECT().CheckAndSet(gomock.Any(), 0).Return(nil)
		handler.ServeHTTP(serviceName, w, req)

		resp = w.Result()
		body, _ = ioutil.ReadAll(resp.Body)

		if serviceName == M3AggregatorServiceName {
			require.Equal(t, `{"placement":{"instances":{},"replicaFactor":1,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":true,"maxShardSetId":0},"version":1}`, string(body))
		} else {
			require.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":1}`, string(body))
		}
		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}
