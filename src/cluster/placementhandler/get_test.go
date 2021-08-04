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

package placementhandler

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/service"
	"github.com/m3db/m3/src/cluster/placement/storage"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func SetupPlacementTest(t *testing.T, ctrl *gomock.Controller) (*client.MockClient, *placement.MockService) {
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)

	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)

	mockPlacementService := placement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	mockClient.EXPECT().Services(gomock.Any()).Return(mockServices, nil).AnyTimes()
	mockServices.EXPECT().PlacementService(gomock.Any(), gomock.Any()).Return(mockPlacementService, nil).AnyTimes()

	return mockClient, mockPlacementService
}

func setupPlacementTest(t *testing.T, ctrl *gomock.Controller, initPlacement placement.Placement) *client.MockClient {
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)

	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)

	mockClient.EXPECT().Services(gomock.Any()).Return(mockServices, nil).AnyTimes()
	mockServices.EXPECT().PlacementService(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ interface{}, opts placement.Options) (placement.Service, error) {
			ps := service.NewPlacementService(
				storage.NewPlacementStorage(mem.NewStore(), "", opts),
				service.WithPlacementOptions(opts))
			if initPlacement != nil {
				_, err := ps.Set(initPlacement)
				require.NoError(t, err)
			}
			return ps, nil
		},
	).AnyTimes()

	return mockClient
}

func TestPlacementGetHandler(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
		handlerOpts, err := NewHandlerOptions(
			mockClient, placement.Configuration{}, nil, instrument.NewOptions())
		require.NoError(t, err)
		handler := NewGetHandler(handlerOpts)

		// Test successful get
		w := httptest.NewRecorder()
		req := httptest.NewRequest(GetHTTPMethod, M3DBGetURL, nil)
		require.NotNil(t, req)

		placementProto := &placementpb.Placement{
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
						DebugPort: 1,
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
						DebugPort: 2,
					},
				},
			},
		}

		const placementJSON = `{"placement":{"instances":{"host1":{"id":"host1","isolationGroup":"rack1","zone":"test","weight":1,"endpoint":"http://host1:1234","shards":[],"shardSetId":0,"hostname":"host1","port":1234,"metadata":{"debugPort":1}},"host2":{"id":"host2","isolationGroup":"rack1","zone":"test","weight":1,"endpoint":"http://host2:1234","shards":[],"shardSetId":0,"hostname":"host2","port":1234,"metadata":{"debugPort":2}}},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":%d}`

		placementObj, err := placement.NewPlacementFromProto(placementProto)
		require.NoError(t, err)

		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		mockPlacementService.EXPECT().Placement().Return(placementObj, nil)
		handler.ServeHTTP(svcDefaults, w, req)

		resp := w.Result()
		body, _ := ioutil.ReadAll(resp.Body)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, fmt.Sprintf(placementJSON, 0), string(body))

		// Test error case
		w = httptest.NewRecorder()
		req = httptest.NewRequest(GetHTTPMethod, M3DBGetURL, nil)
		require.NotNil(t, req)

		mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)

		// With bad version request
		w = httptest.NewRecorder()
		req = httptest.NewRequest(GetHTTPMethod, "/placement/get?version=foo", nil)
		require.NotNil(t, req)

		handler.ServeHTTP(svcDefaults, w, req)
		resp = w.Result()
		assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

		// With valid version request
		w = httptest.NewRecorder()
		req = httptest.NewRequest(GetHTTPMethod, "/placement/get?version=12", nil)
		require.NotNil(t, req)

		mockPlacementService.EXPECT().PlacementForVersion(12).Return(placementObj.Clone().SetVersion(12), nil)

		handler.ServeHTTP(svcDefaults, w, req)
		resp = w.Result()
		body, _ = ioutil.ReadAll(resp.Body)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		assert.Equal(t, fmt.Sprintf(placementJSON, 12), string(body))
	})
}
