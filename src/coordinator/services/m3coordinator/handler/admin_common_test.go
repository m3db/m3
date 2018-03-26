package handler

import (
	"errors"
	"testing"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementService(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)
	mockPlacementService := placement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil)
	mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).Return(mockPlacementService, nil)

	placementService, err := PlacementService(mockClient)
	assert.NoError(t, err)
	assert.NotNil(t, placementService)

	// Test Services returns error
	mockClient.EXPECT().Services(gomock.Not(nil)).Return(nil, errors.New("dummy service error"))
	placementService, err = PlacementService(mockClient)
	assert.Nil(t, placementService)
	assert.EqualError(t, err, "dummy service error")

	// Test PlacementService returns error
	mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil)
	mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).Return(nil, errors.New("dummy placement error"))
	placementService, err = PlacementService(mockClient)
	assert.Nil(t, placementService)
	assert.EqualError(t, err, "dummy placement error")
}

func TestConvertInstancesProto(t *testing.T) {
	instances, err := ConvertInstancesProto([]*placementpb.Instance{})
	require.NoError(t, err)
	require.Equal(t, 0, len(instances))

	instances, err = ConvertInstancesProto([]*placementpb.Instance{
		&placementpb.Instance{
			Id:       "i1",
			Rack:     "r1",
			Weight:   1,
			Endpoint: "i1:1234",
			Hostname: "i1",
			Port:     1234,
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(instances))
	require.Equal(t, "Instance[ID=i1, Rack=r1, Zone=, Weight=1, Endpoint=i1:1234, Hostname=i1, Port=1234, ShardSetID=0, Shards=[Initializing=[], Available=[], Leaving=[]]]", instances[0].String())

	instances, err = ConvertInstancesProto([]*placementpb.Instance{
		&placementpb.Instance{
			Id:         "i1",
			Rack:       "r1",
			Weight:     1,
			Endpoint:   "i1:1234",
			Hostname:   "i1",
			Port:       1234,
			ShardSetId: 1,
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
		},
		&placementpb.Instance{
			Id:         "i2",
			Rack:       "r1",
			Weight:     1,
			Endpoint:   "i2:1234",
			Hostname:   "i2",
			Port:       1234,
			ShardSetId: 1,
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
		},
		&placementpb.Instance{
			Id:         "i3",
			Rack:       "r2",
			Weight:     2,
			Endpoint:   "i3:1234",
			Hostname:   "i3",
			Port:       1234,
			ShardSetId: 2,
			Shards: []*placementpb.Shard{
				&placementpb.Shard{
					Id:           1,
					State:        placementpb.ShardState_INITIALIZING,
					SourceId:     "s1",
					CutoverNanos: 2,
					CutoffNanos:  3,
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(instances))
	require.Equal(t, "Instance[ID=i1, Rack=r1, Zone=, Weight=1, Endpoint=i1:1234, Hostname=i1, Port=1234, ShardSetID=1, Shards=[Initializing=[], Available=[1 2], Leaving=[]]]", instances[0].String())
	require.Equal(t, "Instance[ID=i2, Rack=r1, Zone=, Weight=1, Endpoint=i2:1234, Hostname=i2, Port=1234, ShardSetID=1, Shards=[Initializing=[], Available=[1], Leaving=[]]]", instances[1].String())
	require.Equal(t, "Instance[ID=i3, Rack=r2, Zone=, Weight=2, Endpoint=i3:1234, Hostname=i3, Port=1234, ShardSetID=2, Shards=[Initializing=[1], Available=[], Leaving=[]]]", instances[2].String())

	_, err = ConvertInstancesProto([]*placementpb.Instance{
		&placementpb.Instance{
			Id:         "i1",
			Rack:       "r1",
			Weight:     1,
			Endpoint:   "i1:1234",
			Hostname:   "i1",
			Port:       1234,
			ShardSetId: 1,
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
}
