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
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func SetupPlacementTest(t *testing.T) (*client.MockClient, *placement.MockService) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

func TestPlacementGetHandler(t *testing.T) {
	mockClient, mockPlacementService := SetupPlacementTest(t)
	handler := NewGetHandler(mockClient, config.Configuration{})

	// Test successful get
	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/placement/get", nil)
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
			},
			"host2": &placementpb.Instance{
				Id:             "host2",
				IsolationGroup: "rack1",
				Zone:           "test",
				Weight:         1,
				Endpoint:       "http://host2:1234",
				Hostname:       "host2",
				Port:           1234,
			},
		},
	}

	const placementJSON = "{\"placement\":{\"instances\":{\"host1\":{\"id\":\"host1\",\"isolationGroup\":\"rack1\",\"zone\":\"test\",\"weight\":1,\"endpoint\":\"http://host1:1234\",\"shards\":[],\"shardSetId\":0,\"hostname\":\"host1\",\"port\":1234},\"host2\":{\"id\":\"host2\",\"isolationGroup\":\"rack1\",\"zone\":\"test\",\"weight\":1,\"endpoint\":\"http://host2:1234\",\"shards\":[],\"shardSetId\":0,\"hostname\":\"host2\",\"port\":1234}},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":%d}"

	placementObj, err := placement.NewPlacementFromProto(placementProto)
	require.NoError(t, err)

	mockPlacementService.EXPECT().Placement().Return(placementObj, 0, nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, fmt.Sprintf(placementJSON, 0), string(body))

	// Test error case
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/placement/get", nil)
	require.NotNil(t, req)

	mockPlacementService.EXPECT().Placement().Return(nil, 0, errors.New("key not found"))
	handler.ServeHTTP(w, req)

	resp = w.Result()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// With bad version request
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/placement/get?version=foo", nil)
	require.NotNil(t, req)

	handler.ServeHTTP(w, req)
	resp = w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	// With valid version request
	w = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/placement/get?version=12", nil)
	require.NotNil(t, req)

	mockPlacementService.EXPECT().PlacementForVersion(12).Return(placementObj, nil)

	handler.ServeHTTP(w, req)
	resp = w.Result()
	body, _ = ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, fmt.Sprintf(placementJSON, 12), string(body))
}
