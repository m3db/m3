// Copyright (c) 2017 Uber Technologies, Inc.
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

package util

import (
	"testing"

	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3cluster/shard"
	"github.com/stretchr/testify/assert"
)

func TestConvertBetweenProtoAndService(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	sid := services.NewServiceID().
		SetName("test_service").
		SetEnvironment("test_env").
		SetZone("test_zone")
	p := placementproto.Placement{
		Instances: map[string]*placementproto.Instance{
			"i1": &placementproto.Instance{
				Id:       "i1",
				Rack:     "r1",
				Zone:     "z1",
				Endpoint: "e1",
				Weight:   1,
				Shards:   protoShards,
			},
			"i2": &placementproto.Instance{
				Id:       "i2",
				Rack:     "r2",
				Zone:     "z1",
				Endpoint: "e2",
				Weight:   1,
				Shards:   protoShards,
			},
		},
		ReplicaFactor: 2,
		NumShards:     3,
		IsSharded:     true,
	}

	s, err := ServiceFromProto(p, sid)
	assert.NoError(t, err)
	assert.Equal(t, 2, s.Replication().Replicas())
	assert.Equal(t, 3, s.Sharding().NumShards())
	assert.True(t, s.Sharding().IsSharded())

	i1, err := s.Instance("i1")
	assert.NoError(t, err)
	assert.Equal(t, "i1", i1.InstanceID())
	assert.Equal(t, "e1", i1.Endpoint())
	assert.Equal(t, 3, i1.Shards().NumShards())
	assert.Equal(t, sid, i1.ServiceID())
	assert.True(t, i1.Shards().Contains(0))
	assert.True(t, i1.Shards().Contains(1))
	assert.True(t, i1.Shards().Contains(2))

	i2, err := s.Instance("i2")
	assert.NoError(t, err)
	assert.Equal(t, "i2", i2.InstanceID())
	assert.Equal(t, "e2", i2.Endpoint())
	assert.Equal(t, 3, i2.Shards().NumShards())
	assert.Equal(t, sid, i2.ServiceID())
	assert.True(t, i2.Shards().Contains(0))
	assert.True(t, i2.Shards().Contains(1))
	assert.True(t, i2.Shards().Contains(2))
}

func TestConvertBetweenProtoAndPlacement(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	placementProto := placementproto.Placement{
		Instances: map[string]*placementproto.Instance{
			"i1": &placementproto.Instance{
				Id:       "i1",
				Rack:     "r1",
				Zone:     "z1",
				Endpoint: "e1",
				Weight:   1,
				Shards:   protoShards,
			},
			"i2": &placementproto.Instance{
				Id:       "i2",
				Rack:     "r2",
				Zone:     "z1",
				Endpoint: "e2",
				Weight:   1,
				Shards:   protoShards,
			},
		},
		ReplicaFactor: 2,
		NumShards:     3,
		IsSharded:     true,
	}

	p, err := PlacementFromProto(placementProto)
	assert.NoError(t, err)
	assert.Equal(t, 2, p.NumInstances())
	assert.Equal(t, 2, p.ReplicaFactor())
	assert.True(t, p.IsSharded())
	assert.Equal(t, []uint32{0, 1, 2}, p.Shards())

	placementProtoNew, err := PlacementToProto(p)
	assert.NoError(t, err)
	assert.Equal(t, placementProto.ReplicaFactor, placementProtoNew.ReplicaFactor)
	assert.Equal(t, placementProto.NumShards, placementProtoNew.NumShards)
	for id, h := range placementProto.Instances {
		i1 := placementProtoNew.Instances[id]
		assert.Equal(t, h.Id, i1.Id)
		assert.Equal(t, h.Rack, i1.Rack)
		assert.Equal(t, h.Zone, i1.Zone)
		assert.Equal(t, h.Weight, i1.Weight)
		assert.Equal(t, h.Shards, i1.Shards)
	}
}

func TestPlacementInstanceFromProto(t *testing.T) {
	protoShardsUnsorted := getProtoShards([]uint32{2, 1, 0})

	instanceProto := placementproto.Instance{
		Id:       "i1",
		Rack:     "r1",
		Zone:     "z1",
		Endpoint: "e1",
		Weight:   1,
		Shards:   protoShardsUnsorted,
	}

	instance, err := PlacementInstanceFromProto(instanceProto)
	assert.NoError(t, err)

	instanceShards := instance.Shards()

	// assert.Equal can't compare shards due to pointer types, we check them
	// manually
	instance.SetShards(shard.NewShards(nil))

	expShards := shard.NewShards([]shard.Shard{
		shard.NewShard(0).SetSourceID("i1").SetState(shard.Available),
		shard.NewShard(1).SetSourceID("i1").SetState(shard.Available),
		shard.NewShard(2).SetSourceID("i1").SetState(shard.Available),
	})

	expInstance := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("e1").
		SetWeight(1)

	assert.Equal(t, expInstance, instance)
	assert.Equal(t, expShards.AllIDs(), instanceShards.AllIDs())

	instanceProto.Shards[0].State = placementproto.ShardState(1000)
	instance, err = PlacementInstanceFromProto(instanceProto)
	assert.Error(t, err)
	assert.Nil(t, instance)
}

func TestPlacementInstanceToProto(t *testing.T) {
	shards := shard.NewShards([]shard.Shard{
		shard.NewShard(2).SetSourceID("i1").SetState(shard.Available),
		shard.NewShard(1).SetSourceID("i1").SetState(shard.Available),
		shard.NewShard(0).SetSourceID("i1").SetState(shard.Available),
	})

	instance := placement.NewInstance().
		SetID("i1").
		SetRack("r1").
		SetZone("z1").
		SetEndpoint("e1").
		SetWeight(1).
		SetShards(shards)

	instanceProto, err := PlacementInstanceToProto(instance)
	assert.NoError(t, err)

	protoShards := getProtoShards([]uint32{0, 1, 2})
	for _, s := range protoShards {
		s.SourceId = "i1"
	}

	expInstance := &placementproto.Instance{
		Id:       "i1",
		Rack:     "r1",
		Zone:     "z1",
		Endpoint: "e1",
		Weight:   1,
		Shards:   protoShards,
	}

	assert.Equal(t, expInstance, instanceProto)
}

func getProtoShards(ids []uint32) []*placementproto.Shard {
	r := make([]*placementproto.Shard, len(ids))
	for i, id := range ids {
		r[i] = &placementproto.Shard{
			Id:    id,
			State: placementproto.ShardState_AVAILABLE,
		}
	}
	return r
}
