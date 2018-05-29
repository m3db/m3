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

package database

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/config"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler/namespace"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func SetupDatabaseTest(t *testing.T) (*client.MockClient, *kv.MockStore, *placement.MockService) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockKV := kv.NewMockStore(ctrl)
	require.NotNil(t, mockKV)
	mockPlacementService := placement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)
	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)

	mockServices.EXPECT().PlacementService(gomock.Any(), gomock.Any()).Return(mockPlacementService, nil).AnyTimes()
	mockClient.EXPECT().KV().Return(mockKV, nil).AnyTimes()
	mockClient.EXPECT().Services(gomock.Any()).Return(mockServices, nil).AnyTimes()

	return mockClient, mockKV, mockPlacementService
}

func TestLocalType(t *testing.T) {
	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t)
	createHandler := NewCreateHandler(mockClient, config.Configuration{})
	w := httptest.NewRecorder()

	jsonInput := `
        {
            "namespaceName": "testNamespace",
            "type": "local"
        }
    `

	req := httptest.NewRequest("POST", "/database/create", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
	mockKV.EXPECT().CheckAndSet(namespace.M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"localhost": &placementpb.Instance{
				Id:             "localhost",
				IsolationGroup: "local",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://localhost:9000",
				Hostname:       "localhost",
				Port:           9000,
			},
		},
	}
	newPlacement, err := placement.NewPlacementFromProto(placementProto)
	require.NoError(t, err)
	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Any(), 16, 1).Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"namespace\":{\"registry\":{\"namespaces\":{\"testNamespace\":{\"bootstrapEnabled\":true,\"flushEnabled\":true,\"writesToCommitLog\":true,\"cleanupEnabled\":true,\"repairEnabled\":true,\"retentionOptions\":{\"retentionPeriodNanos\":\"172800000000000\",\"blockSizeNanos\":\"7200000000000\",\"bufferFutureNanos\":\"600000000000\",\"bufferPastNanos\":\"600000000000\",\"blockDataExpiry\":true,\"blockDataExpiryAfterNotAccessPeriodNanos\":\"300000000000\"},\"snapshotEnabled\":false,\"indexOptions\":{\"enabled\":true,\"blockSizeNanos\":\"7200000000000\"}}}}},\"placement\":{\"placement\":{\"instances\":{\"localhost\":{\"id\":\"localhost\",\"isolationGroup\":\"local\",\"zone\":\"embedded\",\"weight\":1,\"endpoint\":\"http://localhost:9000\",\"shards\":[],\"shardSetId\":0,\"hostname\":\"localhost\",\"port\":9000}},\"replicaFactor\":0,\"numShards\":0,\"isSharded\":false,\"cutoverTime\":\"0\",\"isMirrored\":false,\"maxShardSetId\":0},\"version\":0}}", string(body))
}

func TestBadType(t *testing.T) {
	mockClient, _, _ := SetupDatabaseTest(t)
	createHandler := NewCreateHandler(mockClient, config.Configuration{})
	w := httptest.NewRecorder()

	jsonInput := `
		{
			"namespaceName": "testNamespace",
			"type": "badtype"
		}
	`
	req := httptest.NewRequest("POST", "/database/create", strings.NewReader(jsonInput))
	require.NotNil(t, req)
	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, "{\"error\":\"invalid database type\"}\n", string(body))
}
