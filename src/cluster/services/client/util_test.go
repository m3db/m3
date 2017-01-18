// Copyright (c) 2016 Uber Technologies, Inc.
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

package client

import (
	"testing"

	placementproto "github.com/m3db/m3cluster/generated/proto/placement"
	"github.com/m3db/m3cluster/services"
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

	s, err := serviceFromProto(p, sid)
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

func TestConvertBetweenProtoAndPlacementWithError(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	// wrong number of shards, should be 3
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
		NumShards:     2,
	}
	_, err := PlacementFromProto(p)
	assert.Error(t, err)

	// wrong replica factor, should be 2
	p = placementproto.Placement{
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
		ReplicaFactor: 3,
		NumShards:     3,
	}
	_, err = PlacementFromProto(p)
	assert.Error(t, err)

	// invalid shards, should be 0, 1, 2
	p = placementproto.Placement{
		Instances: map[string]*placementproto.Instance{
			"i1": &placementproto.Instance{
				Id:       "i1",
				Rack:     "r1",
				Zone:     "z1",
				Endpoint: "e1",
				Weight:   1,
				Shards:   getProtoShards([]uint32{1, 2, 3}),
			},
			"i2": &placementproto.Instance{
				Id:       "i2",
				Rack:     "r2",
				Zone:     "z1",
				Endpoint: "e2",
				Weight:   1,
				Shards:   getProtoShards([]uint32{1, 2, 3}),
			},
		},
		ReplicaFactor: 2,
		NumShards:     3,
	}
	_, err = PlacementFromProto(p)
	assert.Error(t, err)

	// invalid shards, should be 0, 1, 2
	p = placementproto.Placement{
		Instances: map[string]*placementproto.Instance{
			"i1": &placementproto.Instance{
				Id:       "i1",
				Rack:     "r1",
				Zone:     "z1",
				Endpoint: "e1",
				Weight:   1,
				Shards:   getProtoShards([]uint32{0, 1}),
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
	}
	_, err = PlacementFromProto(p)
	assert.Error(t, err)
}

func TestKeys(t *testing.T) {
	sid := services.NewServiceID().SetName("m3db").SetEnvironment("production")
	assert.Equal(t, "production/m3db", serviceKey(sid))
	assert.Equal(t, "_sd.placement/production/m3db", placementKey(sid))
	assert.Equal(t, "_sd.metadata/production/m3db", metadataKey(sid))
	assert.Equal(t, "production/m3db/instance1", adKey(sid, "instance1"))
}

func getProtoShards(ids []uint32) []*placementproto.Shard {
	r := make([]*placementproto.Shard, len(ids))
	for i, id := range ids {
		r[i] = &placementproto.Shard{
			Id:    id,
			State: placementproto.ShardState_Available,
		}
	}
	return r
}
