// Copyright (c) 2021 Uber Technologies, Inc.
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

package placementhandler

import (
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/x/instrument"
)

func TestPlacementRemoveHandler_Force(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)

		handlerOpts, err := NewHandlerOptions(
			mockClient, placement.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)

		handler := NewRemoveHandler(handlerOpts)
		handler.nowFn = func() time.Time { return time.Unix(0, 0) }

		// Test remove failure
		w := httptest.NewRecorder()
		req := httptest.NewRequest(RemoveHTTPMethod, M3DBRemoveURL,
			strings.NewReader(`{"force": true, "instanceIds":[]}`))
		require.NotNil(t, req)

		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}
		mockPlacementService.EXPECT().RemoveInstances(gomock.Any()).Return(
			placement.NewPlacement(),
			errors.New("no new instances found in the valid zone"))
		handler.ServeHTTP(svcDefaults, w, req)

		resp := w.Result()
		defer resp.Body.Close()

		body, _ := ioutil.ReadAll(resp.Body)
		assert.JSONEq(t,
			`{"status":"error","error":"no new instances found in the valid zone"}`,
			string(body))
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)

		// Test remove success
		w = httptest.NewRecorder()
		req = httptest.NewRequest(RemoveHTTPMethod, M3DBRemoveURL,
			strings.NewReader(`{"force": true, "instanceIds":["host2"]}`))
		require.NotNil(t, req)

		mockPlacementService.EXPECT().RemoveInstances(gomock.Not(nil)).
			Return(placement.NewPlacement(), nil)
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		defer resp.Body.Close()

		body, _ = ioutil.ReadAll(resp.Body)
		//nolint: lll
		assert.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":0}`, string(body))
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})
}

func TestPlacementRemoveHandler_SafeOK(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
		handlerOpts, err := NewHandlerOptions(
			mockClient, placement.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)
		handler := NewRemoveHandler(handlerOpts)
		handler.nowFn = func() time.Time { return time.Unix(0, 0) }

		// Test remove error
		w := httptest.NewRecorder()
		req := httptest.NewRequest(RemoveHTTPMethod, M3DBRemoveURL,
			strings.NewReader(`{"instanceIds":["host2"]}`))

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

		mockPlacementService.EXPECT().RemoveInstances(gomock.Any()).
			Return(nil, errors.New("test err"))
		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		handler.ServeHTTP(svcDefaults, w, req)

		resp := w.Result()
		defer resp.Body.Close()

		body, _ := ioutil.ReadAll(resp.Body)
		require.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		require.JSONEq(t, `{"status":"error","error":"test err"}`, string(body))

		w = httptest.NewRecorder()
		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(RemoveHTTPMethod, M3AggRemoveURL,
				strings.NewReader(`{"instanceIds":["host2"]}`))
		} else {
			req = httptest.NewRequest(RemoveHTTPMethod, M3DBRemoveURL,
				strings.NewReader(`{"instanceIds":["host2"]}`))
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
			mockPlacementService.EXPECT().RemoveInstances(gomock.Any()).
				Return(returnPlacement.SetVersion(1), nil)
		} else {
			mockPlacementService.EXPECT().RemoveInstances(gomock.Any()).
				Return(existingPlacement.Clone().SetVersion(1), nil)
		}
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		defer resp.Body.Close()

		body, _ = ioutil.ReadAll(resp.Body)

		switch serviceName {
		case handleroptions.M3CoordinatorServiceName:
			//nolint: lll
			require.Equal(t, `{"placement":{"instances":{"host1":{"id":"host1","isolationGroup":"rack1","zone":"test","weight":1,"endpoint":"http://host1:1234","shards":[],"shardSetId":0,"hostname":"host1","port":1234,"metadata":{"debugPort":0}}},"replicaFactor":1,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":1}`, string(body))
		case handleroptions.M3AggregatorServiceName:
			//nolint: lll
			require.Equal(t, `{"placement":{"instances":{},"replicaFactor":1,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":true,"maxShardSetId":0},"version":1}`, string(body))
		default:
			//nolint: lll
			require.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":1}`, string(body))
		}

		require.Equal(t, http.StatusOK, resp.StatusCode)
	})
}
