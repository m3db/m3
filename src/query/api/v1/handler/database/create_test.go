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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"unicode"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/config"
	dbconfig "github.com/m3db/m3db/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3db/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3db/src/query/util/logging"
	xtest "github.com/m3db/m3db/src/dbnode/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testDBCfg = &dbconfig.DBConfiguration{
		ListenAddress: "0.0.0.0:9000",
	}
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
	createHandler := NewCreateHandler(mockClient, config.Configuration{}, testDBCfg)
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
						"bootstrapEnabled": true,
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
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000"
						},
						"snapshotEnabled": false,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "3600000000000"
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
						"port": 9000
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
	assert.Equal(t, stripAllWhitespace(expectedResponse), string(body),
		xtest.Diff(mustPrettyJSON(t, expectedResponse), mustPrettyJSON(t, string(body))))
}

func TestLocalWithBlockSizeNanos(t *testing.T) {
	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t)
	createHandler := NewCreateHandler(mockClient, config.Configuration{}, testDBCfg)
	w := httptest.NewRecorder()

	jsonInput := `
		{
			"namespaceName": "testNamespace",
			"type": "local",
			"blockSize": {"time": "3h"}
		}
	`

	req := httptest.NewRequest("POST", "/database/create", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
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
						"bootstrapEnabled": true,
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
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000"
						},
						"snapshotEnabled": false,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "10800000000000"
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
						"port": 9000
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
	assert.Equal(t, stripAllWhitespace(expectedResponse), string(body),
		xtest.Diff(mustPrettyJSON(t, expectedResponse), mustPrettyJSON(t, string(body))))
}

func TestLocalWithBlockSizeExpectedSeriesDatapointsPerHour(t *testing.T) {
	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t)
	createHandler := NewCreateHandler(mockClient, config.Configuration{}, testDBCfg)
	w := httptest.NewRecorder()

	min := minRecommendCalculateBlockSize
	desiredBlockSize := min + 5*time.Minute

	jsonInput := fmt.Sprintf(`
		{
			"namespaceName": "testNamespace",
			"type": "local",
			"blockSize": {"expectedSeriesDatapointsPerHour": %d}
		}
	`, int64(float64(blockSizeFromExpectedSeriesScalar)/float64(desiredBlockSize)))

	req := httptest.NewRequest("POST", "/database/create", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
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
						"bootstrapEnabled": true,
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
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000"
						},
						"snapshotEnabled": false,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "%d"
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
						"port": 9000
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

	assert.Equal(t, stripAllWhitespace(expectedResponse), string(body),
		xtest.Diff(mustPrettyJSON(t, expectedResponse), mustPrettyJSON(t, string(body))))
}

func TestClusterTypeHosts(t *testing.T) {
	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t)
	createHandler := NewCreateHandler(mockClient, config.Configuration{}, testDBCfg)
	w := httptest.NewRecorder()

	jsonInput := `
		{
			"namespaceName": "testNamespace",
			"type": "cluster",
			"hosts": [{"id": "host1"}, {"id": "host2"}]
		}
	`

	req := httptest.NewRequest("POST", "/database/create", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
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
						"bootstrapEnabled": true,
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
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000"
						},
						"snapshotEnabled": false,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "3600000000000"
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
						"port": 9000
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
						"port": 9000
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
	assert.Equal(t, stripAllWhitespace(expectedResponse), string(body),
		xtest.Diff(mustPrettyJSON(t, expectedResponse), mustPrettyJSON(t, string(body))))
}

func TestClusterTypeHostsWithIsolationGroup(t *testing.T) {
	mockClient, mockKV, mockPlacementService := SetupDatabaseTest(t)
	createHandler := NewCreateHandler(mockClient, config.Configuration{}, testDBCfg)
	w := httptest.NewRecorder()

	jsonInput := `
		{
			"namespaceName": "testNamespace",
			"type": "cluster",
			"hosts": [{"id":"host1", "isolationGroup":"group1"}, {"id":"host2", "isolationGroup":"group2"}]
		}
	`

	req := httptest.NewRequest("POST", "/database/create", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	mockKV.EXPECT().Get(namespace.M3DBNodeNamespacesKey).Return(nil, kv.ErrNotFound)
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
						"bootstrapEnabled": true,
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
							"blockDataExpiryAfterNotAccessPeriodNanos": "300000000000"
						},
						"snapshotEnabled": false,
						"indexOptions": {
							"enabled": true,
							"blockSizeNanos": "3600000000000"
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
						"port": 9000
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
						"port": 9000
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
	assert.Equal(t, stripAllWhitespace(expectedResponse), string(body),
		xtest.Diff(mustPrettyJSON(t, expectedResponse), mustPrettyJSON(t, string(body))))
}
func TestClusterTypeMissingHostnames(t *testing.T) {
	mockClient, _, _ := SetupDatabaseTest(t)
	createHandler := NewCreateHandler(mockClient, config.Configuration{}, testDBCfg)
	w := httptest.NewRecorder()

	jsonInput := `
		{
			"namespaceName": "testNamespace",
			"type": "cluster"
		}
	`

	req := httptest.NewRequest("POST", "/database/create", strings.NewReader(jsonInput))
	require.NotNil(t, req)

	createHandler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	assert.Equal(t, withEndline(`{"error":"missing required field"}`), string(body))
}

func TestBadType(t *testing.T) {
	mockClient, _, _ := SetupDatabaseTest(t)
	createHandler := NewCreateHandler(mockClient, config.Configuration{}, nil)
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
	assert.Equal(t, withEndline(`{"error":"invalid database type"}`), string(body))
}

func stripAllWhitespace(str string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, str)
}

func mustPrettyJSON(t *testing.T, str string) string {
	var unmarshalled map[string]interface{}
	err := json.Unmarshal([]byte(str), &unmarshalled)
	require.NoError(t, err)
	pretty, err := json.MarshalIndent(unmarshalled, "", "  ")
	require.NoError(t, err)
	return string(pretty)
}

func withEndline(str string) string {
	return str + "\n"
}
