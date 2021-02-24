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
	t.Run("new_process_all_snapshots", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)
		require.Equal(
			t,
			len(testPlacementsProto),
			len(ps))

		sortedFirst, err := ps[0].Proto()
		require.NoError(t, err)
		require.Equal(t, testPlacementsProto[2], sortedFirst)

		sortedSecond, err := ps[1].Proto()
		require.NoError(t, err)
		require.Equal(t, testPlacementsProto[0], sortedSecond)

		sortedLast, err := ps[2].Proto()
		require.NoError(t, err)
		require.Equal(t, testPlacementsProto[1], sortedLast)
	})
	t.Run("proto_process_all_snapshots", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)

		proto, err := ps.Proto()
		require.NoError(t, err)
		require.Equal(
			t,
			len(testPlacementsProto),
			len(proto.Snapshots))

		require.Equal(t, testPlacementsProto[2], proto.Snapshots[0])
		require.Equal(t, testPlacementsProto[0], proto.Snapshots[1])
		require.Equal(t, testPlacementsProto[1], proto.Snapshots[2])
	})
	t.Run("last_returns_last_by_cutover_time", func(t *testing.T) {
		ps, err := NewPlacementsFromProto(testStagedPlacementProto)
		require.NoError(t, err)

		expected, err := NewPlacementFromProto(testLastPlacementProto)
		require.NoError(t, err)

		actual, err := ps.Last()
		require.NoError(t, err)
		require.Equal(t, expected, actual)
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
