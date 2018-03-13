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

package storage

import (
	"testing"

	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/kv/mem"

	"github.com/stretchr/testify/require"
)

func TestPlacementHelper(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	proto1 := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"i1": &placementpb.Instance{
				Id:             "i1",
				IsolationGroup: "r1",
				Zone:           "z1",
				Endpoint:       "e1",
				Weight:         1,
				Shards:         protoShards,
				ShardSetId:     0,
			},
			"i2": &placementpb.Instance{
				Id:             "i2",
				IsolationGroup: "r2",
				Zone:           "z1",
				Endpoint:       "e2",
				Weight:         1,
				Shards:         protoShards,
				ShardSetId:     1,
			},
		},
		ReplicaFactor: 2,
		NumShards:     3,
		IsSharded:     true,
		CutoverTime:   1000,
		MaxShardSetId: 1,
	}
	key := "key"
	store := mem.NewStore()
	_, err := store.Set(key, proto1)
	require.NoError(t, err)

	helper := newPlacementHelper(store, key)

	p, v, err := helper.Placement()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	_, err = helper.PlacementForVersion(0)
	require.Error(t, err)

	_, err = helper.PlacementForVersion(2)
	require.Error(t, err)

	h, err := helper.PlacementForVersion(1)
	require.NoError(t, err)
	require.Equal(t, p, h)

	m, err := helper.GenerateProto(p)
	require.NoError(t, err)

	newProto := m.(*placementpb.Placement)
	require.Equal(t, proto1, newProto)

	err = helper.ValidateProto(m)
	require.NoError(t, err)

	err = helper.ValidateProto(&placementpb.Instance{Id: "id"})
	require.Error(t, err)
	require.Equal(t, errInvalidProtoForSinglePlacement, err)
}

func TestPlacementSnapshotsHelper(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	proto1 := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"i1": &placementpb.Instance{
				Id:             "i1",
				IsolationGroup: "r1",
				Zone:           "z1",
				Endpoint:       "e1",
				Weight:         1,
				Shards:         protoShards,
				ShardSetId:     0,
			},
			"i2": &placementpb.Instance{
				Id:             "i2",
				IsolationGroup: "r2",
				Zone:           "z1",
				Endpoint:       "e2",
				Weight:         1,
				Shards:         protoShards,
				ShardSetId:     1,
			},
		},
		ReplicaFactor: 2,
		NumShards:     3,
		IsSharded:     true,
		CutoverTime:   1000,
	}
	proto2 := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"i1": &placementpb.Instance{
				Id:             "i1",
				IsolationGroup: "r1",
				Zone:           "z1",
				Endpoint:       "e1",
				Weight:         1,
				Shards:         protoShards,
				ShardSetId:     0,
			},
		},
		ReplicaFactor: 1,
		NumShards:     3,
		IsSharded:     true,
		CutoverTime:   2000,
	}
	ps := &placementpb.PlacementSnapshots{Snapshots: []*placementpb.Placement{proto1, proto2}}
	key := "key"
	store := mem.NewStore()
	_, err := store.Set(key, ps)
	require.NoError(t, err)

	helper := newStagedPlacementHelper(store, key)
	p, v, err := helper.Placement()
	require.NoError(t, err)
	require.Equal(t, 1, v)

	_, err = helper.PlacementForVersion(0)
	require.Error(t, err)

	_, err = helper.PlacementForVersion(2)
	require.Error(t, err)

	h, err := helper.PlacementForVersion(1)
	require.NoError(t, err)
	require.Equal(t, p, h)

	_, err = helper.GenerateProto(p)
	require.Error(t, err)

	newCutoverTime := p.CutoverNanos() + 1
	m, err := helper.GenerateProto(p.SetCutoverNanos(newCutoverTime))
	require.NoError(t, err)

	newProto := m.(*placementpb.PlacementSnapshots)
	require.Equal(
		t,
		&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				proto1,
				proto2,
				&placementpb.Placement{
					Instances: map[string]*placementpb.Instance{
						"i1": &placementpb.Instance{
							Id:             "i1",
							IsolationGroup: "r1",
							Zone:           "z1",
							Endpoint:       "e1",
							Weight:         1,
							Shards:         protoShards,
							ShardSetId:     0,
						},
					},
					ReplicaFactor: 1,
					NumShards:     3,
					IsSharded:     true,
					CutoverTime:   newCutoverTime,
				},
			},
		},
		newProto,
	)

	err = helper.ValidateProto(m)
	require.NoError(t, err)

	err = helper.ValidateProto(proto1)
	require.Error(t, err)
	require.Equal(t, errInvalidProtoForPlacementSnapshots, err)

	_, err = store.Set(key, &placementpb.PlacementSnapshots{})
	require.NoError(t, err)

	_, _, err = helper.Placement()
	require.Error(t, err)
	require.Equal(t, errNoPlacementInTheSnapshots, err)

	_, err = helper.PlacementForVersion(2)
	require.Error(t, err)
	require.Equal(t, errNoPlacementInTheSnapshots, err)

	_, err = store.Set(key, newProto)
	require.NoError(t, err)

	h, err = helper.PlacementForVersion(3)
	require.NoError(t, err)
	require.Equal(t, p.SetVersion(3), h)
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
