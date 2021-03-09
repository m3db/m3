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
	testEarliestPlacementProto = &placementpb.Placement{
		NumShards:   10,
		CutoverTime: 0,
		Instances: map[string]*placementpb.Instance{
			"i1": testInstanceProto("i1", 1),
			"i2": testInstanceProto("i2", 1),
		},
	}
	testMiddlePlacementProto = &placementpb.Placement{
		NumShards:   20,
		CutoverTime: 456,
		Instances: map[string]*placementpb.Instance{
			"i1": testInstanceProto("i1", 1),
			"i2": testInstanceProto("i2", 1),
			"i3": testInstanceProto("i3", 2),
			"i4": testInstanceProto("i4", 2),
		},
	}
	testLatestPlacementProto = &placementpb.Placement{
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
		testMiddlePlacementProto,
		testLatestPlacementProto,
		testEarliestPlacementProto,
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
	t.Run("new_falls_back_to_snapshots_field", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)

		expected, err := NewPlacementFromProto(testLatestPlacementProto)
		require.NoError(t, err)
		require.Equal(t, expected, ps.Latest())
	})
	t.Run("proto_returns_latest_by_cutover_time", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)
		require.NotNil(t, ps)

		actual, err := ps.Proto()
		require.NoError(t, err)
		require.Equal(t, 1, len(actual.Snapshots))
		require.Equal(t, testLatestPlacementProto, actual.Snapshots[0])
	})
	t.Run("backward_compatible_latest_returns_latest_by_cutover_time", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)

		expected, err := NewPlacementFromProto(testLatestPlacementProto)
		require.NoError(t, err)
		require.Equal(t, expected, ps.Latest())
	})
	t.Run("compatible_with_single_snapshot_in_the_snapshots_field", func(t *testing.T) {
		singleSnapshotProto := &placementpb.PlacementSnapshots{
			Snapshots: []*placementpb.Placement{
				testLatestPlacementProto,
			},
		}
		ps, err := NewPlacementsFromProto(singleSnapshotProto)
		require.NoError(t, err)

		expected, err := NewPlacementFromProto(testLatestPlacementProto)
		require.NoError(t, err)
		require.Equal(t, expected, ps.Latest())
	})
	t.Run("new_decompresses_compressed_placement", func(t *testing.T) {
		compressed, err := compressPlacementProto(testLatestPlacementProto)
		require.NoError(t, err)
		compressedProto := &placementpb.PlacementSnapshots{
			CompressMode:        placementpb.CompressMode_ZSTD,
			CompressedPlacement: compressed,
		}

		ps, err := NewPlacementsFromProto(compressedProto)
		require.NoError(t, err)
		require.NotNil(t, ps)
		actual := ps.Latest()
		require.NotNil(t, actual)

		expected, err := NewPlacementFromProto(testLatestPlacementProto)
		require.NoError(t, err)

		require.Equal(t, expected, actual)
	})
	t.Run("new_fails_to_decompress_invalid_proto", func(t *testing.T) {
		invalidProto := &placementpb.PlacementSnapshots{
			CompressMode:        placementpb.CompressMode_ZSTD,
			CompressedPlacement: nil,
		}

		_, err := NewPlacementsFromProto(invalidProto)
		require.Equal(t, errNilValue, err)
	})
	t.Run("new_fails_to_decompress_corrupt_proto", func(t *testing.T) {
		compressed, err := compressPlacementProto(testLatestPlacementProto)
		require.NoError(t, err)

		i := len(compressed) / 2
		compressed[i] = (compressed[i] + 1) % 255 // corrupt
		corruptProto := &placementpb.PlacementSnapshots{
			CompressMode:        placementpb.CompressMode_ZSTD,
			CompressedPlacement: compressed,
		}

		_, err = NewPlacementsFromProto(corruptProto)
		require.Error(t, err)
	})
	t.Run("proto_compressed_compresses_placement", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)
		require.NotNil(t, ps)

		compressedProto, err := ps.ProtoCompressed()
		require.NoError(t, err)
		require.Equal(t, placementpb.CompressMode_ZSTD, compressedProto.CompressMode)
		require.Equal(t, 0, len(compressedProto.Snapshots))

		decompressedProto, err := decompressPlacementProto(compressedProto.CompressedPlacement)
		require.NoError(t, err)

		require.Equal(t, testLatestPlacementProto.String(), decompressedProto.String())
	})
}

func testLatestPlacement(t *testing.T) Placement {
	t.Helper()
	p, err := NewPlacementFromProto(testLatestPlacementProto)
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
