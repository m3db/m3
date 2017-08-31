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

package services

import (
	"testing"

	"github.com/m3db/m3cluster/generated/proto/placementpb"

	"github.com/stretchr/testify/assert"
)

func TestOptions(t *testing.T) {
	opts := NewOptions()
	assert.Equal(t, NewNamespaceOptions(), opts.NamespaceOptions())

	nOpts := NewNamespaceOptions().SetPlacementNamespace("p")
	opts = opts.SetNamespaceOptions(nOpts)
	assert.Equal(t, nOpts, opts.NamespaceOptions())
}

func TestNamespaceOptions(t *testing.T) {
	opts := NewNamespaceOptions()
	assert.Empty(t, opts.PlacementNamespace())
	assert.Empty(t, opts.MetadataNamespace())

	opts = opts.SetPlacementNamespace("p").SetMetadataNamespace("m")
	assert.Equal(t, "p", opts.PlacementNamespace())
	assert.Equal(t, "m", opts.MetadataNamespace())
}

func TestConvertBetweenProtoAndService(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	sid := NewServiceID().
		SetName("test_service").
		SetEnvironment("test_env").
		SetZone("test_zone")
	p := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"i1": &placementpb.Instance{
				Id:       "i1",
				Rack:     "r1",
				Zone:     "z1",
				Endpoint: "e1",
				Weight:   1,
				Shards:   protoShards,
			},
			"i2": &placementpb.Instance{
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

	s, err := NewServiceFromProto(p, sid)
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

func getProtoShards(ids []uint32) []*placementpb.Shard {
	r := make([]*placementpb.Shard, len(ids))
	for i, id := range ids {
		r[i] = &placementpb.Shard{
			Id:    id,
			State: placementpb.ShardState_AVAILABLE,
		}
	}
	return r
}
