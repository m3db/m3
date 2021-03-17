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

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestPlacementHelper(t *testing.T) {
	protoShards := getProtoShards([]uint32{0, 1, 2})
	proto1 := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"i1": newTestProtoInstance("i1", 0, protoShards),
			"i2": newTestProtoInstance("i2", 1, protoShards),
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
			"i1": newTestProtoInstance("i1", 0, protoShards),
			"i2": newTestProtoInstance("i2", 1, protoShards),
		},
		ReplicaFactor: 2,
		NumShards:     3,
		IsSharded:     true,
		CutoverTime:   1000,
	}
	proto2 := &placementpb.Placement{
		Instances: map[string]*placementpb.Instance{
			"i1": newTestProtoInstance("i1", 0, protoShards),
		},
		ReplicaFactor: 1,
		NumShards:     3,
		IsSharded:     true,
		CutoverTime:   2000,
	}

	setupHelper := func(proto proto.Message) helper {
		key := "key"
		store := mem.NewStore()
		compressFlag := false
		_, err := store.Set(key, proto)
		require.NoError(t, err)

		return newStagedPlacementHelper(store, key, compressFlag)
	}

	t.Run("version_is_respected", func(t *testing.T) {
		helper := setupHelper(&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				proto1,
				proto2,
			},
		})

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
	})

	t.Run("generates_snapshots_with_single_specified_placement", func(t *testing.T) {
		helper := setupHelper(&placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				proto1,
				proto2,
			},
		})

		p, v, err := helper.Placement()
		require.NoError(t, err)
		require.Equal(t, 1, v)
		require.Equal(t, proto2.GetCutoverTime(), p.CutoverNanos())

		p1 := p.SetCutoverNanos(p.CutoverNanos() + 1)
		m, err := helper.GenerateProto(p1)
		require.NoError(t, err)
		require.NoError(t, helper.ValidateProto(m))

		actualProto := m.(*placementpb.PlacementSnapshots)
		require.Equal(t, 1, len(actualProto.Snapshots))

		proto2.CutoverTime = 0
		expectedProto := &placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				proto2,
			},
		}

		require.Equal(t, expectedProto, actualProto)
	})

	t.Run("proto_of_wrong_type_is_invalid", func(t *testing.T) {
		helper := setupHelper(proto1)
		err := helper.ValidateProto(proto1)
		require.Error(t, err)
		require.Equal(t, errInvalidProtoForPlacementSnapshots, err)
	})

	t.Run("empty_proto", func(t *testing.T) {
		helper := setupHelper(&placementpb.PlacementSnapshots{})

		_, _, err := helper.Placement()
		require.Error(t, err)
		require.Contains(t, err.Error(), "placement snapshots is empty")

		_, err = helper.PlacementForVersion(1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "placement snapshots is empty")
	})

	t.Run("generates_compressed_proto", func(t *testing.T) {
		expected, err := placement.NewPlacementFromProto(proto1)
		require.NoError(t, err)

		key := "key"
		store := mem.NewStore()
		compressFlag := true
		helper := newStagedPlacementHelper(store, key, compressFlag)

		m, err := helper.GenerateProto(expected)
		require.NoError(t, err)
		require.NoError(t, helper.ValidateProto(m))

		proto := m.(*placementpb.PlacementSnapshots)
		require.NotNil(t, proto)

		require.Equal(t, placementpb.CompressMode_ZSTD, proto.CompressMode)
		require.Equal(t, 0, len(proto.Snapshots))

		ps, err := placement.NewPlacementsFromProto(proto)
		require.NoError(t, err)
		actual := ps.Latest()
		require.Equal(t, expected.String(), actual.String())
	})
}

func newTestProtoInstance(id string, shardSetID uint32, shards []*placementpb.Shard) *placementpb.Instance {
	return &placementpb.Instance{
		Id:             id,
		IsolationGroup: "g-" + id,
		Zone:           "z-" + id,
		Endpoint:       "e-" + id,
		Weight:         1,
		Shards:         shards,
		ShardSetId:     shardSetID,
		Metadata: &placementpb.InstanceMetadata{
			DebugPort: 1,
		},
	}
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
