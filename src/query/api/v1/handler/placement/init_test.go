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

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	initTestPlacementProto = &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"host1": &placementpb.Instance{
				Id:             "host1",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host1:1234",
				Hostname:       "host1",
				Port:           1234,
				Metadata: &placementpb.InstanceMetadata{
					DebugPort: 0,
				},
			},
			"host2": &placementpb.Instance{
				Id:             "host2",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host2:1234",
				Hostname:       "host2",
				Port:           1234,
				Metadata: &placementpb.InstanceMetadata{
					DebugPort: 0,
				},
			},
		},
	}
)

func TestPlacementInitHandler(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
		handlerOpts, err := NewHandlerOptions(
			mockClient, config.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)
		handler := NewInitHandler(handlerOpts)

		// Test placement init success
		var (
			w   = httptest.NewRecorder()
			req *http.Request
		)
		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(InitHTTPMethod, M3DBInitURL, strings.NewReader(`{"instances": [{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234, "metadata": {"debugPort":0}},{"id": "host2","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host2:1234","hostname": "host2","port": 1234, "metadata": {"debugPort":0}}],"num_shards": 16,"replication_factor": 1}`))
		} else {
			req = httptest.NewRequest(InitHTTPMethod, M3DBInitURL, strings.NewReader(`{"instances": [{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host1:1234","hostname": "host1","port": 1234, "metadata": {"debugPort":0}},{"id": "host2","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host2:1234","hostname": "host2","port": 1234, "metadata": {"debugPort":0}}],"num_shards": 16,"replication_factor": 1}`))
		}

		require.NotNil(t, req)

		newPlacement, err := placement.NewPlacementFromProto(initTestPlacementProto)
		require.NoError(t, err)

		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Not(nil), 16, 1).Return(newPlacement, nil)
		handler.ServeHTTP(svcDefaults, w, req)
		resp := w.Result()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, `{"placement":{"instances":{"host1":{"id":"host1","isolationGroup":"rack1","zone":"test","weight":1,"endpoint":"http://host1:1234","shards":[],"shardSetId":0,"hostname":"host1","port":1234,"metadata":{"debugPort":0}},"host2":{"id":"host2","isolationGroup":"rack1","zone":"test","weight":1,"endpoint":"http://host2:1234","shards":[],"shardSetId":0,"hostname":"host2","port":1234,"metadata":{"debugPort":0}}},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":0}`, string(body))

		// Test error response
		w = httptest.NewRecorder()
		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(InitHTTPMethod, M3DBInitURL, strings.NewReader(`{"instances": [{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "host1:1234","hostname": "host1","port": 1234, "metadata": {"debugPort":0}},{"id": "host2","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host2:1234","hostname": "host2","port": 1234, "metadata": {"debugPort":0}}],"num_shards": 64,"replication_factor": 2}`))
		} else {
			req = httptest.NewRequest(InitHTTPMethod, M3DBInitURL, strings.NewReader(`{"instances": [{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "host1:1234","hostname": "host1","port": 1234, "metadata": {"debugPort":0}},{"id": "host2","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host2:1234","hostname": "host2","port": 1234, "metadata": {"debugPort":0}}],"num_shards": 64,"replication_factor": 2}`))
		}
		require.NotNil(t, req)

		switch serviceName {
		case handleroptions.M3CoordinatorServiceName:
			mockPlacementService.EXPECT().
				BuildInitialPlacement(gomock.Not(nil), 64, 1).
				Return(nil, errors.New("unable to build initial placement"))
		default:
			mockPlacementService.EXPECT().
				BuildInitialPlacement(gomock.Not(nil), 64, 2).
				Return(nil, errors.New("unable to build initial placement"))
		}

		handler.ServeHTTP(svcDefaults, w, req)
		resp = w.Result()
		body, err = ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
		assert.JSONEq(t,
			`{"status":"error","error":"unable to build initial placement"}`,
			string(body))

		// Test error response
		w = httptest.NewRecorder()
		if serviceName == handleroptions.M3AggregatorServiceName {
			req = httptest.NewRequest(InitHTTPMethod, M3DBInitURL, strings.NewReader(`{"instances": [{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "host1:1234","hostname": "host1","port": 1234, "metadata": {"debugPort":0}},{"id": "host2","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host2:1234","hostname": "host2","port": 1234, "metadata": {"debugPort":0}}],"num_shards": 64,"replication_factor": 2}`))
		} else {
			req = httptest.NewRequest(InitHTTPMethod, M3DBInitURL, strings.NewReader(`{"instances": [{"id": "host1","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "host1:1234","hostname": "host1","port": 1234, "metadata": {"debugPort":0}},{"id": "host2","isolation_group": "rack1","zone": "test","weight": 1,"endpoint": "http://host2:1234","hostname": "host2","port": 1234, "metadata": {"debugPort":0}}],"num_shards": 64,"replication_factor": 2}`))
		}

		require.NotNil(t, req)
		switch serviceName {
		case handleroptions.M3CoordinatorServiceName:
			mockPlacementService.EXPECT().
				BuildInitialPlacement(gomock.Not(nil), 64, 1).
				Return(nil, kv.ErrAlreadyExists)
		default:
			mockPlacementService.EXPECT().
				BuildInitialPlacement(gomock.Not(nil), 64, 2).
				Return(nil, kv.ErrAlreadyExists)
		}

		handler.ServeHTTP(svcDefaults, w, req)
		resp = w.Result()
		_, err = ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		assert.Equal(t, http.StatusConflict, resp.StatusCode)
	})
}
