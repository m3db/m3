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
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementDeleteHandler_Force(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
		handlerOpts, err := NewHandlerOptions(mockClient,
			config.Configuration{}, nil, instrument.NewOptions())
		handler := NewDeleteHandler(handlerOpts)

		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		// Test remove success
		inst := placement.NewInstance().
			SetID("host1").
			SetEndpoint("host1:123").
			SetHostname("host1").
			SetPort(123).
			SetIsolationGroup("host1-group").
			SetWeight(1).
			SetZone("default")
		if serviceName == handleroptions.M3AggregatorServiceName {
			inst = inst.SetShardSetID(0)
		}

		existing := placement.NewPlacement().
			SetInstances([]placement.Instance{inst})

		mockKVStore := kv.NewMockStore(ctrl)

		w := httptest.NewRecorder()
		req := httptest.
			NewRequest(DeleteHTTPMethod, "/placement/host1?force=true", nil)
		req = mux.SetURLVars(req, map[string]string{"id": "host1"})
		require.NotNil(t, req)
		mockPlacementService.EXPECT().Placement().Return(existing, nil)
		mockPlacementService.EXPECT().RemoveInstances([]string{"host1"}).
			Return(placement.NewPlacement(), nil)
		if serviceName == handleroptions.M3AggregatorServiceName {
			flushTimesMgrOpts := aggregator.NewFlushTimesManagerOptions()
			electionMgrOpts := aggregator.NewElectionManagerOptions()
			mockClient.EXPECT().Store(gomock.Any()).Return(mockKVStore, nil)
			mockKVStore.EXPECT().
				Get(gomock.Any()).
				DoAndReturn(func(k string) (kv.Value, error) {
					switch k {
					case fmt.Sprintf(flushTimesMgrOpts.FlushTimesKeyFmt(), inst.ShardSetID()):
						return nil, nil
					case fmt.Sprintf(electionMgrOpts.ElectionKeyFmt(), inst.ShardSetID()):
						return nil, nil
					}
					return nil, errors.New("unexpected")
				}).
				AnyTimes()
			mockKVStore.EXPECT().
				Delete(gomock.Any()).
				DoAndReturn(func(k string) (kv.Value, error) {
					switch k {
					case fmt.Sprintf(flushTimesMgrOpts.FlushTimesKeyFmt(), inst.ShardSetID()):
						return nil, nil
					case fmt.Sprintf(electionMgrOpts.ElectionKeyFmt(), inst.ShardSetID()):
						return nil, nil
					}
					return nil, errors.New("unexpected")
				}).
				AnyTimes()
		}

		handler.ServeHTTP(svcDefaults, w, req)

		resp := w.Result()
		body, err := ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":0}`, string(body))

		// Test remove failure
		w = httptest.NewRecorder()
		req = httptest.NewRequest(DeleteHTTPMethod, "/placement/nope?force=true", nil)
		req = mux.SetURLVars(req, map[string]string{"id": "nope"})
		require.NotNil(t, req)
		mockPlacementService.EXPECT().Placement().Return(existing, nil)
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		body, err = ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
		require.JSONEq(t, `{"status":"error","error":"instance not found: nope"}`, string(body))
	})
}

func TestPlacementDeleteHandler_Safe(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		t.Run(serviceName, func(t *testing.T) {
			testDeleteHandlerSafe(t, serviceName)
		})
	})
}

func testDeleteHandlerSafe(t *testing.T, serviceName string) {
	ctrl := xtest.NewController(t)
	ctrl = gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockPlacementService := SetupPlacementTest(t, ctrl)
	handlerOpts, err := NewHandlerOptions(
		mockClient,
		config.Configuration{},
		&handleroptions.M3AggServiceOptions{
			WarmupDuration:           time.Minute,
			MaxAggregationWindowSize: 5 * time.Minute,
		},
		instrument.NewOptions())
	require.NoError(t, err)

	mockKVStore := kv.NewMockStore(ctrl)
	shardSetIDs := []uint32{0, 1}

	if serviceName == handleroptions.M3AggregatorServiceName {
		flushTimesMgrOpts := aggregator.NewFlushTimesManagerOptions()
		electionMgrOpts := aggregator.NewElectionManagerOptions()
		mockClient.EXPECT().Store(gomock.Any()).Return(mockKVStore, nil).AnyTimes()
		mockKVStore.EXPECT().
			Get(gomock.Any()).
			DoAndReturn(func(k string) (kv.Value, error) {
				for _, shardSetID := range shardSetIDs {
					switch k {
					case fmt.Sprintf(flushTimesMgrOpts.FlushTimesKeyFmt(), shardSetID):
						return nil, nil
					case fmt.Sprintf(electionMgrOpts.ElectionKeyFmt(), shardSetID):
						return nil, nil
					}
				}
				return nil, errors.New("unexpected")
			}).
			AnyTimes()
		mockKVStore.EXPECT().
			Delete(gomock.Any()).
			DoAndReturn(func(k string) (kv.Value, error) {
				for _, shardSetID := range shardSetIDs {
					switch k {
					case fmt.Sprintf(flushTimesMgrOpts.FlushTimesKeyFmt(), shardSetID):
						return nil, nil
					case fmt.Sprintf(electionMgrOpts.ElectionKeyFmt(), shardSetID):
						return nil, nil
					}
				}
				return nil, errors.New("unexpected")
			}).
			AnyTimes()
	}

	var (
		handler = NewDeleteHandler(handlerOpts)

		basePlacement = placement.NewPlacement().
				SetIsSharded(true)

		// Test remove absent host
		w   = httptest.NewRecorder()
		req = httptest.NewRequest(DeleteHTTPMethod, "/placement/host1", nil)
	)
	handler.nowFn = func() time.Time { return time.Unix(0, 0) }

	switch serviceName {
	case handleroptions.M3CoordinatorServiceName:
		basePlacement = basePlacement.
			SetIsSharded(false).
			SetReplicaFactor(1)
	case handleroptions.M3AggregatorServiceName:
		basePlacement = basePlacement.
			SetIsMirrored(true)
	}

	svcDefaults := handleroptions.ServiceNameAndDefaults{
		ServiceName: serviceName,
	}

	req = mux.SetURLVars(req, map[string]string{"id": "host1"})
	require.NotNil(t, req)
	mockPlacementService.EXPECT().Placement().Return(basePlacement, nil)
	handler.ServeHTTP(svcDefaults, w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "instance not found: host1")
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// Test remove host when placement unsafe
	basePlacement = basePlacement.SetInstances([]placement.Instance{
		placement.NewInstance().
			SetID("host1").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(2).SetState(shard.Available),
			})).
			SetShardSetID(shardSetIDs[0]),
		placement.NewInstance().
			SetID("host2").
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(1).SetState(shard.Leaving),
			})).
			SetShardSetID(shardSetIDs[1]),
	})

	switch serviceName {
	case handleroptions.M3CoordinatorServiceName:
		// M3Coordinator placement changes are alway safe because it is stateless
	default:
		w = httptest.NewRecorder()
		req = httptest.NewRequest(DeleteHTTPMethod, "/placement/host1", nil)
		req = mux.SetURLVars(req, map[string]string{"id": "host1"})
		require.NotNil(t, req)
		mockPlacementService.EXPECT().Placement().Return(basePlacement, nil)
		handler.ServeHTTP(svcDefaults, w, req)

		resp = w.Result()
		body, err = ioutil.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, resp.StatusCode)
		require.JSONEq(t,
			`{"status":"error","error":"instances do not have all shards available: [host2]"}`,
			string(body))
	}

	// Test OK
	basePlacement = basePlacement.SetReplicaFactor(2).SetMaxShardSetID(2).SetInstances([]placement.Instance{
		placement.NewInstance().SetID("host1").SetIsolationGroup("a").SetWeight(10).
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(0).SetState(shard.Available),
			})),
		placement.NewInstance().SetID("host2").SetIsolationGroup("b").SetWeight(10).
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(0).SetState(shard.Available),
				shard.NewShard(1).SetState(shard.Available),
			})),
		placement.NewInstance().SetID("host3").SetIsolationGroup("c").SetWeight(10).
			SetShards(shard.NewShards([]shard.Shard{
				shard.NewShard(1).SetState(shard.Available),
			})),
	})

	var returnPlacement placement.Placement

	switch serviceName {
	case handleroptions.M3CoordinatorServiceName:
		basePlacement.
			SetIsSharded(false).
			SetReplicaFactor(1).
			SetShards(nil).
			SetInstances([]placement.Instance{placement.NewInstance().SetID("host1")})
		mockPlacementService.EXPECT().Placement().Return(basePlacement, nil)
		mockPlacementService.EXPECT().
			RemoveInstances([]string{"host1"}).
			Return(placement.NewPlacement(), nil)
	case handleroptions.M3AggregatorServiceName:
		// Need to be mirrored in M3Agg case
		basePlacement.SetReplicaFactor(1).SetMaxShardSetID(2).SetInstances([]placement.Instance{
			placement.NewInstance().SetID("host1").SetIsolationGroup("a").SetWeight(10).SetShardSetID(0).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
				})),
			placement.NewInstance().SetID("host2").SetIsolationGroup("b").SetWeight(10).SetShardSetID(1).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(1).SetState(shard.Available),
				})),
		})

		returnPlacement = basePlacement.Clone().SetInstances([]placement.Instance{
			placement.NewInstance().SetID("host1").SetIsolationGroup("a").SetWeight(10).SetShardSetID(0).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Leaving).
						SetCutoffNanos(300000000000),
				})),
			placement.NewInstance().SetID("host2").SetIsolationGroup("b").SetWeight(10).SetShardSetID(1).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Initializing).
						SetCutoverNanos(300000000000).
						SetSourceID("host1"),
					shard.NewShard(1).SetState(shard.Available),
				})),
		}).SetVersion(2)
	case handleroptions.M3DBServiceName:
		returnPlacement = basePlacement.Clone().SetInstances([]placement.Instance{
			placement.NewInstance().SetID("host1").SetIsolationGroup("a").SetWeight(10).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Leaving),
				})),
			placement.NewInstance().SetID("host2").SetIsolationGroup("b").SetWeight(10).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Available),
					shard.NewShard(1).SetState(shard.Available),
				})),
			placement.NewInstance().SetID("host3").SetIsolationGroup("c").SetWeight(10).
				SetShards(shard.NewShards([]shard.Shard{
					shard.NewShard(0).SetState(shard.Initializing).SetSourceID("host1"),
					shard.NewShard(1).SetState(shard.Available),
				})),
		}).SetVersion(2)
	}

	w = httptest.NewRecorder()
	req = httptest.NewRequest(DeleteHTTPMethod, "/placement/host1", nil)
	req = mux.SetURLVars(req, map[string]string{"id": "host1"})
	require.NotNil(t, req)

	if !isStateless(serviceName) {
		mockPlacementService.EXPECT().Placement().Return(basePlacement, nil)
		mockPlacementService.EXPECT().CheckAndSet(gomock.Any(), 0).Return(returnPlacement, nil)
	}

	handler.ServeHTTP(svcDefaults, w, req)

	resp = w.Result()
	body, err = ioutil.ReadAll(resp.Body)
	require.NoError(t, err)
	switch serviceName {
	case handleroptions.M3CoordinatorServiceName:
		require.Equal(t, `{"placement":{"instances":{},"replicaFactor":0,"numShards":0,"isSharded":false,"cutoverTime":"0","isMirrored":false,"maxShardSetId":0},"version":0}`, string(body))
	case handleroptions.M3AggregatorServiceName:
		require.Equal(t, `{"placement":{"instances":{"host1":{"id":"host1","isolationGroup":"a","zone":"","weight":10,"endpoint":"","shards":[{"id":0,"state":"LEAVING","sourceId":"","cutoverNanos":"0","cutoffNanos":"300000000000"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}},"host2":{"id":"host2","isolationGroup":"b","zone":"","weight":10,"endpoint":"","shards":[{"id":0,"state":"INITIALIZING","sourceId":"host1","cutoverNanos":"300000000000","cutoffNanos":"0"},{"id":1,"state":"AVAILABLE","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":1,"hostname":"","port":0,"metadata":{"debugPort":0}}},"replicaFactor":1,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":true,"maxShardSetId":2},"version":2}`, string(body))
	default:
		require.Equal(t, `{"placement":{"instances":{"host1":{"id":"host1","isolationGroup":"a","zone":"","weight":10,"endpoint":"","shards":[{"id":0,"state":"LEAVING","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}},"host2":{"id":"host2","isolationGroup":"b","zone":"","weight":10,"endpoint":"","shards":[{"id":0,"state":"AVAILABLE","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"},{"id":1,"state":"AVAILABLE","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}},"host3":{"id":"host3","isolationGroup":"c","zone":"","weight":10,"endpoint":"","shards":[{"id":0,"state":"INITIALIZING","sourceId":"host1","cutoverNanos":"0","cutoffNanos":"0"},{"id":1,"state":"AVAILABLE","sourceId":"","cutoverNanos":"0","cutoffNanos":"0"}],"shardSetId":0,"hostname":"","port":0,"metadata":{"debugPort":0}}},"replicaFactor":2,"numShards":0,"isSharded":true,"cutoverTime":"0","isMirrored":false,"maxShardSetId":2},"version":2}`, string(body))
	}
}
