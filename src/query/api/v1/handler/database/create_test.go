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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/fake"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/validators"
	"github.com/m3db/m3/src/x/instrument"
	xjson "github.com/m3db/m3/src/x/json"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	listenAddress = "0.0.0.0:9000"
	testDBCfg     = &dbconfig.DBConfiguration{
		ListenAddress: &listenAddress,
	}

	svcDefaultOptions = []handleroptions.ServiceOptionsDefault{
		func(o handleroptions.ServiceOptions) handleroptions.ServiceOptions {
			return o
		},
	}
)

func SetupDatabaseTest(
	t *testing.T,
	ctrl *gomock.Controller,
) (*client.MockClient, *kv.MockStore, *placement.MockService) {
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
	testLocalType(t, "local", false)
}

func TestLocalTypePlacementAlreadyExists(t *testing.T) {
	testLocalType(t, "local", true)
}

func TestLocalTypePlacementAlreadyExistsNoTypeProvided(t *testing.T) {
	testLocalType(t, "", true)
}

func testLocalType(t *testing.T, providedType string, placementExists bool) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil).AnyTimes()
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          providedType,
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound).Times(2)
	mockKV.EXPECT().CheckAndSet(namespace.M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"localhost": &placementpb.Instance{
				Id:             "m3db_local",
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

	if placementExists {
		mockPlacementService.EXPECT().Placement().Return(newPlacement, nil)
	} else {
		mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
		mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Any(), 64, 1).Return(newPlacement, nil)
	}

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `
	{
		"namespace": {
			"registry": {
				"namespaces": {
					"testNamespace": {
						"aggregationOptions": {
							"aggregations": [
								{
									"aggregated": false,
									"attributes": null
								}
							]
						},
						"bootstrapEnabled": true,
						"cacheBlocksOnRetrieve": false,
						"flushEnabled": true,
						"writesToCommitLog": true,
						"cleanupEnabled": true,
						"repairEnabled": false,
						"retentionOptions": {
							"retentionPeriodNanos": "86400000000000",
							"blockSizeNanos": "3600000000000",
							"bufferFutureNanos": "120000000000",
							"bufferPastNanos": "600000000000",
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
							"futureRetentionPeriodNanos": "0"
						},
						"snapshotEnabled": true,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "3600000000000"
						},
						"runtimeOptions": null,
						"schemaOptions": null,
						"coldWritesEnabled": false,
						"extendedOptions": null,
						"stagingState": {
							"status": "UNKNOWN"
						}
					}
				}
			}
		},
		"placement": {
			"placement": {
				"instances": {
					"m3db_local": {
						"id": "m3db_local",
						"isolationGroup": "local",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://localhost:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "localhost",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					}
				},
				"replicaFactor": 0,
				"numShards": 0,
				"isSharded": false,
				"cutoverTime": "0",
				"isMirrored": false,
				"maxShardSetId": 0
			},
			"version": 0
		}
	}
	`

	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestLocalTypeClusteredPlacementAlreadyExists(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, _, mockPlacementService := SetupDatabaseTest(t, ctrl)
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "local",
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"localhost": &placementpb.Instance{
				Id:             "m3db_not_local",
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

	mockPlacementService.EXPECT().Placement().Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	_, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestLocalTypeWithNumShards(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil).AnyTimes()
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)

	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "local",
		"numShards":     51,
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound).Times(2)
	mockKV.EXPECT().CheckAndSet(namespace.M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"localhost": &placementpb.Instance{
				Id:             "m3db_local",
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
	mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Any(), 51, 1).Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `
	{
		"namespace": {
			"registry": {
				"namespaces": {
					"testNamespace": {
						"aggregationOptions": {
							"aggregations": [
								{
									"aggregated": false,
									"attributes": null
								}
							]
						},
						"bootstrapEnabled": true,
						"cacheBlocksOnRetrieve": false,
						"flushEnabled": true,
						"writesToCommitLog": true,
						"cleanupEnabled": true,
						"repairEnabled": false,
						"retentionOptions": {
							"retentionPeriodNanos": "86400000000000",
							"blockSizeNanos": "3600000000000",
							"bufferFutureNanos": "120000000000",
							"bufferPastNanos": "600000000000",
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
							"futureRetentionPeriodNanos": "0"
						},
						"snapshotEnabled": true,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "3600000000000"
						},
						"runtimeOptions": null,
						"schemaOptions": null,
						"coldWritesEnabled": false,
						"extendedOptions": null,
						"stagingState": {
							"status": "UNKNOWN"
						}
					}
				}
			}
		},
		"placement": {
			"placement": {
				"instances": {
					"m3db_local": {
						"id": "m3db_local",
						"isolationGroup": "local",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://localhost:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "localhost",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					}
				},
				"replicaFactor": 0,
				"numShards": 0,
				"isSharded": false,
				"cutoverTime": "0",
				"isMirrored": false,
				"maxShardSetId": 0
			},
			"version": 0
		}
	}
	`
	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestLocalWithBlockSizeNanos(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil).AnyTimes()
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "local",
		"blockSize":     xjson.Map{"time": "3h"},
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound).Times(2)
	mockKV.EXPECT().CheckAndSet(namespace.M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"localhost": &placementpb.Instance{
				Id:             DefaultLocalHostID,
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
	mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Any(), 64, 1).Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `
	{
		"namespace": {
			"registry": {
				"namespaces": {
					"testNamespace": {
						"aggregationOptions": {
							"aggregations": [
								{
									"aggregated": false,
									"attributes": null
								}
							]
						},
						"bootstrapEnabled": true,
						"cacheBlocksOnRetrieve": false,
						"flushEnabled": true,
						"writesToCommitLog": true,
						"cleanupEnabled": true,
						"repairEnabled": false,
						"retentionOptions": {
							"retentionPeriodNanos": "86400000000000",
							"blockSizeNanos": "10800000000000",
							"bufferFutureNanos": "120000000000",
							"bufferPastNanos": "600000000000",
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
							"futureRetentionPeriodNanos": "0"
						},
						"snapshotEnabled": true,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "10800000000000"
						},
						"runtimeOptions": null,
						"schemaOptions": null,
						"coldWritesEnabled": false,
						"extendedOptions": null,
						"stagingState": {
							"status": "UNKNOWN"
						}
					}
				}
			}
		},
		"placement": {
			"placement": {
				"instances": {
					"m3db_local": {
						"id": "m3db_local",
						"isolationGroup": "local",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://localhost:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "localhost",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					}
				},
				"replicaFactor": 0,
				"numShards": 0,
				"isSharded": false,
				"cutoverTime": "0",
				"isMirrored": false,
				"maxShardSetId": 0
			},
			"version": 0
		}
	}
	`
	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestLocalWithBlockSizeExpectedSeriesDatapointsPerHour(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil).AnyTimes()
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	min := minRecommendCalculateBlockSize
	desiredBlockSize := min + 5*time.Minute

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "local",
		"blockSize": xjson.Map{
			"expectedSeriesDatapointsPerHour": int64(float64(blockSizeFromExpectedSeriesScalar) / float64(desiredBlockSize)),
		},
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound).Times(2)
	mockKV.EXPECT().CheckAndSet(namespace.M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"localhost": &placementpb.Instance{
				Id:             DefaultLocalHostID,
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
	mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Any(), 64, 1).Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := fmt.Sprintf(`
	{
		"namespace": {
			"registry": {
				"namespaces": {
					"testNamespace": {
						"aggregationOptions": {
							"aggregations": [
								{
									"aggregated": false,
									"attributes": null
								}
							]
						},
						"bootstrapEnabled": true,
						"cacheBlocksOnRetrieve": false,
						"flushEnabled": true,
						"writesToCommitLog": true,
						"cleanupEnabled": true,
						"repairEnabled": false,
						"retentionOptions": {
							"retentionPeriodNanos": "86400000000000",
							"blockSizeNanos": "%d",
							"bufferFutureNanos": "120000000000",
							"bufferPastNanos": "600000000000",
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
							"futureRetentionPeriodNanos": "0"
						},
						"snapshotEnabled": true,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "%d"
						},
						"runtimeOptions": null,
						"schemaOptions": null,
						"coldWritesEnabled": false,
						"extendedOptions": null,
						"stagingState": {
							"status": "UNKNOWN"
						}
					}
				}
			}
		},
		"placement": {
			"placement": {
				"instances": {
					"m3db_local": {
						"id": "m3db_local",
						"isolationGroup": "local",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://localhost:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "localhost",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					}
				},
				"replicaFactor": 0,
				"numShards": 0,
				"isSharded": false,
				"cutoverTime": "0",
				"isMirrored": false,
				"maxShardSetId": 0
			},
			"version": 0
		}
	}
	`, desiredBlockSize, desiredBlockSize)

	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestClusterTypeHosts(t *testing.T) {
	testClusterTypeHosts(t, false)
}

func TestClusterTypeHostsNotProvided(t *testing.T) {
	testClusterTypeHosts(t, true)
}

func TestClusterTypeHostsPlacementAlreadyExistsHostsProvided(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, _, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(nil, nil).AnyTimes()
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "cluster",
		"hosts":         xjson.Array{xjson.Map{"id": "host1"}, xjson.Map{"id": "host2"}},
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"host1": &placementpb.Instance{
				Id:             "host1",
				IsolationGroup: "cluster",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://host1:9000",
				Hostname:       "host1",
				Port:           9000,
			},
			"host2": &placementpb.Instance{
				Id:             "host2",
				IsolationGroup: "cluster",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://host2:9000",
				Hostname:       "host2",
				Port:           9000,
			},
		},
	}
	newPlacement, err := placement.NewPlacementFromProto(placementProto)
	require.NoError(t, err)

	mockPlacementService.EXPECT().Placement().Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	_, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestClusterTypeHostsPlacementAlreadyExistsExistingIsLocal(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, _, mockPlacementService := SetupDatabaseTest(t, ctrl)
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "cluster",
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"localhost": &placementpb.Instance{
				Id:             DefaultLocalHostID,
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

	mockPlacementService.EXPECT().Placement().Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	_, err = ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func testClusterTypeHosts(t *testing.T, placementExists bool) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil).AnyTimes()
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	var jsonInput xjson.Map

	if placementExists {
		jsonInput = xjson.Map{
			"namespaceName": "testNamespace",
			"type":          "cluster",
		}
	} else {
		jsonInput = xjson.Map{
			"namespaceName": "testNamespace",
			"type":          "cluster",
			"hosts":         xjson.Array{xjson.Map{"id": "host1"}, xjson.Map{"id": "host2"}},
		}
	}

	reqBody := bytes.NewBuffer(nil)
	require.NoError(t, json.NewEncoder(reqBody).Encode(jsonInput))

	req := httptest.NewRequest("POST", "/database/create", reqBody)
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound).Times(2)
	mockKV.EXPECT().CheckAndSet(namespace.M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"host1": &placementpb.Instance{
				Id:             "host1",
				IsolationGroup: "cluster",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://host1:9000",
				Hostname:       "host1",
				Port:           9000,
			},
			"host2": &placementpb.Instance{
				Id:             "host2",
				IsolationGroup: "cluster",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://host2:9000",
				Hostname:       "host2",
				Port:           9000,
			},
		},
	}
	newPlacement, err := placement.NewPlacementFromProto(placementProto)
	require.NoError(t, err)

	if placementExists {
		mockPlacementService.EXPECT().Placement().Return(newPlacement, nil)
	} else {
		mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
		mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Any(), 128, 3).Return(newPlacement, nil)
	}

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `
	{
		"namespace": {
			"registry": {
				"namespaces": {
					"testNamespace": {
						"aggregationOptions": {
							"aggregations": [
								{
									"aggregated": false,
									"attributes": null
								}
							]
						},
						"bootstrapEnabled": true,
						"cacheBlocksOnRetrieve": false,
						"flushEnabled": true,
						"writesToCommitLog": true,
						"cleanupEnabled": true,
						"repairEnabled": false,
						"retentionOptions": {
							"retentionPeriodNanos": "86400000000000",
							"blockSizeNanos": "3600000000000",
							"bufferFutureNanos": "120000000000",
							"bufferPastNanos": "600000000000",
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
							"futureRetentionPeriodNanos": "0"
						},
						"snapshotEnabled": true,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "3600000000000"
						},
						"runtimeOptions": null,
						"schemaOptions": null,
						"coldWritesEnabled": false,
						"extendedOptions": null,
						"stagingState": {
							"status": "UNKNOWN"
						}
					}
				}
			}
		},
		"placement": {
			"placement": {
				"instances": {
					"host1": {
						"id": "host1",
						"isolationGroup": "cluster",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://host1:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "host1",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					},
					"host2": {
						"id": "host2",
						"isolationGroup": "cluster",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://host2:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "host2",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					}
				},
				"replicaFactor": 0,
				"numShards": 0,
				"isSharded": false,
				"cutoverTime": "0",
				"isMirrored": false,
				"maxShardSetId": 0
			},
			"version": 0
		}
	}
	`

	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestClusterTypeHostsWithIsolationGroup(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(mockKV, nil).AnyTimes()

	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "cluster",
		"hosts": xjson.Array{
			xjson.Map{"id": "host1", "isolationGroup": "group1"},
			xjson.Map{"id": "host2", "isolationGroup": "group2"},
		},
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound).Times(2)
	mockKV.EXPECT().CheckAndSet(namespace.M3DBNodeNamespacesKey, gomock.Any(), gomock.Not(nil)).Return(1, nil)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"host1": &placementpb.Instance{
				Id:             "host1",
				IsolationGroup: "group1",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://host1:9000",
				Hostname:       "host1",
				Port:           9000,
			},
			"host2": &placementpb.Instance{
				Id:             "host2",
				IsolationGroup: "group2",
				Zone:           "embedded",
				Weight:         1,
				Endpoint:       "http://host2:9000",
				Hostname:       "host2",
				Port:           9000,
			},
		},
	}
	newPlacement, err := placement.NewPlacementFromProto(placementProto)
	require.NoError(t, err)
	mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Any(), 128, 3).Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `
	{
		"namespace": {
			"registry": {
				"namespaces": {
					"testNamespace": {
						"aggregationOptions": {
							"aggregations": [
								{
									"aggregated": false,
									"attributes": null
								}
							]
						},
						"bootstrapEnabled": true,
						"cacheBlocksOnRetrieve": false,
						"flushEnabled": true,
						"writesToCommitLog": true,
						"cleanupEnabled": true,
						"repairEnabled": false,
						"retentionOptions": {
							"retentionPeriodNanos": "86400000000000",
							"blockSizeNanos": "3600000000000",
							"bufferFutureNanos": "120000000000",
							"bufferPastNanos": "600000000000",
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
							"futureRetentionPeriodNanos": "0"
						},
						"snapshotEnabled": true,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "3600000000000"
						},
						"runtimeOptions": null,
						"schemaOptions": null,
						"coldWritesEnabled": false,
						"extendedOptions": null,
						"stagingState": {
							"status": "UNKNOWN"
						}
					}
				}
			}
		},
		"placement": {
			"placement": {
				"instances": {
					"host1": {
						"id": "host1",
						"isolationGroup": "group1",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://host1:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "host1",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					},
					"host2": {
						"id": "host2",
						"isolationGroup": "group2",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://host2:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "host2",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					}
				},
				"replicaFactor": 0,
				"numShards": 0,
				"isSharded": false,
				"cutoverTime": "0",
				"isMirrored": false,
				"maxShardSetId": 0
			},
			"version": 0
		}
	}
	`

	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}
func TestClusterTypeMissingHostnames(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, _, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)

	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "cluster",
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t,
		xtest.MustPrettyJSONMap(t,
			xjson.Map{
				"status": "error",
				"error":  "missing required field",
			},
		),
		xtest.MustPrettyJSONString(t, string(body)))
}

func TestBadType(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, _, mockPlacementService := SetupDatabaseTest(t, ctrl)
	mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)

	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		nil, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "badtype",
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)
	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t,
		xtest.MustPrettyJSONMap(t,
			xjson.Map{
				"status": "error",
				"error":  "invalid database type",
			},
		),
		xtest.MustPrettyJSONString(t, string(body)))
}

func TestLocalTypeWithAggregatedNamespace(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, _, mockPlacementService := SetupDatabaseTest(t, ctrl)
	fakeKV := fake.NewStore()
	mockClient.EXPECT().Store(gomock.Any()).Return(fakeKV, nil).AnyTimes()
	createHandler, err := NewCreateHandler(mockClient, config.Configuration{},
		testDBCfg, svcDefaultOptions, instrument.NewOptions(), validators.NamespaceValidator)
	require.NoError(t, err)

	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"namespaceName": "testNamespace",
		"type":          "local",
		"aggregatedNamespace": xjson.Map{
			"name":          "testAggregatedNamespace",
			"resolution":    "5m",
			"retentionTime": "2440h",
		},
	}

	req := httptest.NewRequest("POST", "/database/create",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	placementProto := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"localhost": &placementpb.Instance{
				Id:             "m3db_local",
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
	mockPlacementService.EXPECT().Placement().Return(nil, kv.ErrNotFound)
	mockPlacementService.EXPECT().BuildInitialPlacement(gomock.Any(), 64, 1).Return(newPlacement, nil)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `
	{
		"namespace": {
			"registry": {
				"namespaces": {
					"testNamespace": {
						"aggregationOptions": {
							"aggregations": [
								{
									"aggregated": false,
									"attributes": null
								}
							]
						},
						"bootstrapEnabled": true,
						"cacheBlocksOnRetrieve": false,
						"flushEnabled": true,
						"writesToCommitLog": true,
						"cleanupEnabled": true,
						"repairEnabled": false,
						"retentionOptions": {
							"retentionPeriodNanos": "86400000000000",
							"blockSizeNanos": "3600000000000",
							"bufferFutureNanos": "120000000000",
							"bufferPastNanos": "600000000000",
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
							"futureRetentionPeriodNanos": "0"
						},
						"snapshotEnabled": true,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "3600000000000"
						},
						"runtimeOptions": null,
						"schemaOptions": null,
						"coldWritesEnabled": false,
						"extendedOptions": null,
						"stagingState": {
							"status": "UNKNOWN"
						}
					},
					"testAggregatedNamespace": {
						"aggregationOptions": {
							"aggregations": [
								{
									"aggregated": true,
									"attributes": {
										"resolutionNanos": "300000000000",
										"downsampleOptions": {
											"all": true
										}
									}
								}
							]
						},
						"bootstrapEnabled": true,
						"cacheBlocksOnRetrieve": false,
						"flushEnabled": true,
						"writesToCommitLog": true,
						"cleanupEnabled": true,
						"repairEnabled": false,
						"retentionOptions": {
							"retentionPeriodNanos": "8784000000000000",
							"blockSizeNanos": "86400000000000",
							"bufferFutureNanos": "120000000000",
							"bufferPastNanos": "600000000000",
							"blockDataExpiry": true,
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000",
							"futureRetentionPeriodNanos": "0"
						},
						"snapshotEnabled": true,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "86400000000000"
						},
						"runtimeOptions": null,
						"schemaOptions": null,
						"coldWritesEnabled": false,
						"extendedOptions": null,
						"stagingState": {
							"status": "UNKNOWN"
						}
					}
				}
			}
		},
		"placement": {
			"placement": {
				"instances": {
					"m3db_local": {
						"id": "m3db_local",
						"isolationGroup": "local",
						"zone": "embedded",
						"weight": 1,
						"endpoint": "http://localhost:9000",
						"shards": [],
						"shardSetId": 0,
						"hostname": "localhost",
						"port": 9000,
						"metadata": {
							"debugPort": 0
						}
					}
				},
				"replicaFactor": 0,
				"numShards": 0,
				"isSharded": false,
				"cutoverTime": "0",
				"isMirrored": false,
				"maxShardSetId": 0
			},
			"version": 0
		}
	}
	`
	expected := xtest.MustPrettyJSONString(t, expectedResponse)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}
