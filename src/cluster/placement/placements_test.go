// Copyright (c) 2021 Uber Technologies, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
)

var (
	testLastPlacementProto = &placementpb.Placement{
		NumShards:   30,
		CutoverTime: 789,
		Instances: map[string]*placementpb.Instance{
			"i1": testInstanceProto("i1", 1),
			"i2": testInstanceProto("i2", 1),
			"i3": testInstanceProto("i3", 2),
			"i4": testInstanceProto("i4", 2),
			"i5": testInstanceProto("i5", 3),
			"i6": testInstanceProto("i6", 3),
		},
	}

	// Snapshots listed intentionally out of sorted order by CutoverTime
	testPlacementsProto = []*placementpb.Placement{
		{
			NumShards:   20,
			CutoverTime: 456,
			Instances: map[string]*placementpb.Instance{
				"i1": testInstanceProto("i1", 1),
				"i2": testInstanceProto("i2", 1),
				"i3": testInstanceProto("i3", 2),
				"i4": testInstanceProto("i4", 2),
			},
		},
		testLastPlacementProto,
		{
			NumShards:   10,
			CutoverTime: 123,
			Instances: map[string]*placementpb.Instance{
				"i1": testInstanceProto("i1", 1),
				"i2": testInstanceProto("i2", 1),
			},
		},
	}
	testStagedPlacementProto = &placementpb.PlacementSnapshots{
		Snapshots: testPlacementsProto,
	}
)

func TestPlacements(t *testing.T) {
	t.Run("new_fails_for_nil", func(t *testing.T) {
		_, err := NewPlacementsFromProto(nil)
		require.Equal(t, errNilPlacementSnapshotsProto, err)
	})
	t.Run("new_fails_for_empty", func(t *testing.T) {
		emptyProto := &placementpb.PlacementSnapshots{}
		_, err := NewPlacementsFromProto(emptyProto)
		require.Equal(t, errEmptyPlacementSnapshots, err)
	})
	t.Run("new_keeps_only_latest_by_cutover_time", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)
		require.NotNil(t, ps)

		actual, err := ps.Proto()
		require.NoError(t, err)
		require.Equal(t, 1, len(actual.Snapshots))
		require.Equal(t, testLastPlacementProto, actual.Snapshots[0])
	})
	t.Run("backward_compatible_latest_returns_latest_by_cutover_time", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)

		expected, err := NewPlacementFromProto(testLastPlacementProto)
		require.NoError(t, err)
		require.Equal(t, expected, ps.Latest())
	})
	t.Run("compatible_with_single_snapshot_in_the_proto", func(t *testing.T) {
		singleSnapshotProto := &placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				testLastPlacementProto,
			},
		}
		ps, err := NewPlacementsFromProto(singleSnapshotProto)
		require.NoError(t, err)

		expected, err := NewPlacementFromProto(testLastPlacementProto)
		require.NoError(t, err)
		require.Equal(t, expected, ps.Latest())
	})
}

func testLastPlacement(t *testing.T) Placement {
	t.Helper()
	p, err := NewPlacementFromProto(testLastPlacementProto)
	require.NoError(t, err)

	return p
}

func testInstanceProto(id string, shardSetID uint32) *placementpb.Instance {
	instance := NewInstance().
		SetID(id).
		SetShardSetID(shardSetID).
		SetIsolationGroup("rack-" + id).
		SetEndpoint("endpoint-" + id).
		SetMetadata(InstanceMetadata{DebugPort: 80}).
		SetWeight(1)

	result, err := instance.Proto()
	if err != nil {
		panic(err)
	}

	return result
}
