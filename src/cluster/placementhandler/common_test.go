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
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
)

func TestPlacementService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runForAllAllowedServices(func(serviceName string) {
		mockClient := client.NewMockClient(ctrl)
		require.NotNil(t, mockClient)
		mockServices := services.NewMockServices(ctrl)
		require.NotNil(t, mockServices)
		mockPlacementService := placement.NewMockService(ctrl)
		require.NotNil(t, mockPlacementService)

		mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil)
		mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).
			Return(mockPlacementService, nil)

		svcDefaults := handleroptions.ServiceNameAndDefaults{
			ServiceName: serviceName,
		}

		placementService, algo, err := ServiceWithAlgo(mockClient,
			handleroptions.NewServiceOptions(svcDefaults, nil, nil),
			placement.Configuration{},
			time.Time{}, nil)
		assert.NoError(t, err)
		assert.NotNil(t, placementService)
		assert.NotNil(t, algo)

		// Test Services returns error
		mockClient.EXPECT().Services(gomock.Not(nil)).
			Return(nil, errors.New("dummy service error"))
		placementService, err = Service(mockClient,
			handleroptions.NewServiceOptions(svcDefaults, nil, nil),
			placement.Configuration{},
			time.Time{}, nil)
		assert.Nil(t, placementService)
		assert.EqualError(t, err, "dummy service error")

		// Test PlacementService returns error
		mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil)
		mockServices.EXPECT().
			PlacementService(gomock.Not(nil), gomock.Not(nil)).
			Return(nil, errors.New("dummy placement error"))
		placementService, err = Service(mockClient,
			handleroptions.NewServiceOptions(svcDefaults, nil, nil),
			placement.Configuration{},
			time.Time{}, nil)
		assert.Nil(t, placementService)
		assert.EqualError(t, err, "dummy placement error")
	})
}

func TestPlacementServiceWithClusterHeaders(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	runForAllAllowedServices(func(serviceName string) {
		mockClient := client.NewMockClient(ctrl)
		require.NotNil(t, mockClient)
		mockServices := services.NewMockServices(ctrl)
		require.NotNil(t, mockServices)
		mockPlacementService := placement.NewMockService(ctrl)
		require.NotNil(t, mockPlacementService)

		mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil)

		var actual services.ServiceID
		mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).
			DoAndReturn(func(
				serviceID services.ServiceID,
				_ placement.Options,
			) (placement.Service, error) {
				actual = serviceID
				return mockPlacementService, nil
			})

		var (
			serviceValue = handleroptions.M3DBServiceName
			svcDefaults  = handleroptions.ServiceNameAndDefaults{
				ServiceName: handleroptions.M3DBServiceName,
			}
			environmentValue = "bar_env"
			zoneValue        = "baz_zone"
			opts             = handleroptions.NewServiceOptions(svcDefaults, nil, nil)
		)

		opts.ServiceEnvironment = environmentValue
		opts.ServiceZone = zoneValue

		placementService, err := Service(
			mockClient,
			opts,
			placement.Configuration{},
			time.Time{},
			nil,
		)
		require.NoError(t, err)
		require.NotNil(t, placementService)

		require.NotNil(t, actual)
		require.Equal(t, serviceValue, actual.Name())
		require.Equal(t, environmentValue, actual.Environment())
		require.Equal(t, zoneValue, actual.Zone())
	})
}

func TestConvertInstancesProto(t *testing.T) {
	runForAllAllowedServices(func(serviceName string) {
		instances, err := ConvertInstancesProto([]*placementpb.Instance{})
		require.NoError(t, err)
		require.Equal(t, 0, len(instances))

		instances, err = ConvertInstancesProto([]*placementpb.Instance{
			&placementpb.Instance{
				Id:             "i1",
				IsolationGroup: "r1",
				Weight:         1,
				Endpoint:       "i1:1234",
				Hostname:       "i1",
				Port:           1234,
				Metadata: &placementpb.InstanceMetadata{
					DebugPort: 4231,
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(instances))
		require.Equal(t, "Instance[ID=i1, IsolationGroup=r1, Zone=, Weight=1, Endpoint=i1:1234, Hostname=i1, Port=1234, ShardSetID=0, Shards=[Initializing=[], Available=[], Leaving=[]], Metadata={DebugPort:4231}]", instances[0].String())

		instances, err = ConvertInstancesProto([]*placementpb.Instance{
			&placementpb.Instance{
				Id:             "i1",
				IsolationGroup: "r1",
				Weight:         1,
				Endpoint:       "i1:1234",
				Hostname:       "i1",
				Port:           1234,
				ShardSetId:     1,
				Shards: []*placementpb.Shard{
					&placementpb.Shard{
						Id:       1,
						State:    placementpb.ShardState_AVAILABLE,
						SourceId: "s1",
					},
					&placementpb.Shard{
						Id:       2,
						State:    placementpb.ShardState_AVAILABLE,
						SourceId: "s1",
					},
				},
				Metadata: &placementpb.InstanceMetadata{
					DebugPort: 1,
				},
			},
			&placementpb.Instance{
				Id:             "i2",
				IsolationGroup: "r1",
				Weight:         1,
				Endpoint:       "i2:1234",
				Hostname:       "i2",
				Port:           1234,
				ShardSetId:     1,
				Shards: []*placementpb.Shard{
					&placementpb.Shard{
						Id:       1,
						State:    placementpb.ShardState_AVAILABLE,
						SourceId: "s2",
					},
					&placementpb.Shard{
						Id:       1,
						State:    placementpb.ShardState_AVAILABLE,
						SourceId: "s2",
					},
				},
				Metadata: &placementpb.InstanceMetadata{
					DebugPort: 2,
				},
			},
			&placementpb.Instance{
				Id:             "i3",
				IsolationGroup: "r2",
				Weight:         2,
				Endpoint:       "i3:1234",
				Hostname:       "i3",
				Port:           1234,
				ShardSetId:     2,
				Shards: []*placementpb.Shard{
					&placementpb.Shard{
						Id:           1,
						State:        placementpb.ShardState_INITIALIZING,
						SourceId:     "s1",
						CutoverNanos: 2,
						CutoffNanos:  3,
					},
				},
				Metadata: &placementpb.InstanceMetadata{
					DebugPort: 3,
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, 3, len(instances))
		require.Equal(t, "Instance[ID=i1, IsolationGroup=r1, Zone=, Weight=1, Endpoint=i1:1234, Hostname=i1, Port=1234, ShardSetID=1, Shards=[Initializing=[], Available=[1 2], Leaving=[]], Metadata={DebugPort:1}]", instances[0].String())
		require.Equal(t, "Instance[ID=i2, IsolationGroup=r1, Zone=, Weight=1, Endpoint=i2:1234, Hostname=i2, Port=1234, ShardSetID=1, Shards=[Initializing=[], Available=[1], Leaving=[]], Metadata={DebugPort:2}]", instances[1].String())
		require.Equal(t, "Instance[ID=i3, IsolationGroup=r2, Zone=, Weight=2, Endpoint=i3:1234, Hostname=i3, Port=1234, ShardSetID=2, Shards=[Initializing=[1], Available=[], Leaving=[]], Metadata={DebugPort:3}]", instances[2].String())

		_, err = ConvertInstancesProto([]*placementpb.Instance{
			&placementpb.Instance{
				Id:             "i1",
				IsolationGroup: "r1",
				Weight:         1,
				Endpoint:       "i1:1234",
				Hostname:       "i1",
				Port:           1234,
				ShardSetId:     1,
				Shards: []*placementpb.Shard{
					&placementpb.Shard{
						Id:       1,
						State:    9999,
						SourceId: "s1",
					},
				},
			},
		})
		require.EqualError(t, err, "invalid proto shard state")
	})
}

func newValidPlacement(state shard.State) placement.Placement {
	shards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetState(state),
	})

	instA := placement.NewInstance().SetShards(shards).SetID("A").SetEndpoint("A")
	instB := placement.NewInstance().SetShards(shards).SetID("B").SetEndpoint("B")
	return placement.NewPlacement().
		SetInstances([]placement.Instance{instA, instB}).
		SetIsSharded(true).
		SetShards([]uint32{0}).
		SetReplicaFactor(2)
}

func newValidInitPlacement() placement.Placement {
	return newValidPlacement(shard.Initializing)
}

func newValidAvailPlacement() placement.Placement {
	return newValidPlacement(shard.Available)
}

func newPlacement(state shard.State) placement.Placement {
	shards := shard.NewShards([]shard.Shard{
		shard.NewShard(1).SetState(state),
	})

	instA := placement.NewInstance().SetShards(shards).SetID("A")
	instB := placement.NewInstance().SetShards(shards).SetID("B")
	return placement.NewPlacement().SetInstances([]placement.Instance{instA, instB})
}

func newInitPlacement() placement.Placement {
	return newPlacement(shard.Initializing)
}

func newAvailPlacement() placement.Placement {
	return newPlacement(shard.Available)
}

func TestValidateAllAvailable(t *testing.T) {
	p := placement.NewPlacement()
	assert.NoError(t, validateAllAvailable(p))

	p = newAvailPlacement()
	assert.NoError(t, validateAllAvailable(p))

	p = newInitPlacement()
	assert.Error(t, validateAllAvailable(p))
}

func runForAllAllowedServices(f func(service string)) {
	for _, service := range handleroptions.AllowedServices() {
		f(service)
	}
}

func TestIsStateless(t *testing.T) {
	for _, s := range []string{
		handleroptions.M3CoordinatorServiceName,
	} {
		assert.True(t, isStateless(s))
	}

	for _, s := range []string{
		handleroptions.M3AggregatorServiceName,
		handleroptions.M3DBServiceName,
	} {
		assert.False(t, isStateless(s))
	}
}
