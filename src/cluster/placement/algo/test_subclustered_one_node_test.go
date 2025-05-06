package algo

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/placement"
)

// nolint
func TestOneNodeAtATime(t *testing.T) {
	i1 := placement.NewEmptyInstance("a", "r1", "z1", "endpoint", 1)
	i2 := placement.NewEmptyInstance("b", "r2", "z1", "endpoint", 1)
	i3 := placement.NewEmptyInstance("c", "r3", "z1", "endpoint", 1)
	i4 := placement.NewEmptyInstance("d", "r1", "z1", "endpoint", 1)
	i5 := placement.NewEmptyInstance("e", "r2", "z1", "endpoint", 1)
	i6 := placement.NewEmptyInstance("f", "r3", "z1", "endpoint", 1)
	i7 := placement.NewEmptyInstance("g", "r1", "z1", "endpoint", 1)
	i8 := placement.NewEmptyInstance("h", "r2", "z1", "endpoint", 1)
	i9 := placement.NewEmptyInstance("i", "r3", "z1", "endpoint", 1)
	i11 := placement.NewEmptyInstance("j", "r1", "z1", "endpoint", 1)
	i12 := placement.NewEmptyInstance("k", "r2", "z1", "endpoint", 1)
	i13 := placement.NewEmptyInstance("l", "r3", "z1", "endpoint", 1)
	i14 := placement.NewEmptyInstance("m", "r1", "z1", "endpoint", 1)
	i15 := placement.NewEmptyInstance("n", "r2", "z1", "endpoint", 1)
	i16 := placement.NewEmptyInstance("o", "r3", "z1", "endpoint", 1)
	i17 := placement.NewEmptyInstance("p", "r1", "z1", "endpoint", 1)
	i18 := placement.NewEmptyInstance("q", "r2", "z1", "endpoint", 1)
	i19 := placement.NewEmptyInstance("r", "r3", "z1", "endpoint", 1)

	numShards := 64

	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	opts := placement.NewOptions().SetHasSubClusters(true).SetIsSharded(true).SetInstancesPerSubCluster(6)
	algo := newSubclusteredv2(opts)
	p, err := algo.InitialPlacement([]placement.Instance{i1, i2, i3, i4, i5, i6}, ids, 3)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NoError(t, placement.Validate(p))
	p, _, err = algo.MarkAllShardsAvailable(p)
	assert.NoError(t, err)
	instances := p.Instances()
	sort.Sort(BySubClusterIDInstanceID(instances))
	for _, instance := range instances {
		shards := instance.Shards().AllIDs()
		sort.Sort(UInts(shards))
		//fmt.Println(instance.ID(), instance.SubClusterID(), instance.IsolationGroup(), " Shards: ", shards)
		fmt.Println(instance.ID(), instance.SubClusterID(), instance.IsolationGroup(), len(shards), " Shards: ", shards)
	}
	require.NoError(t, validateSubClusteredPlacement(p))
	fmt.Println("============================================================================")
	instanceToAdd := []placement.Instance{i14, i15, i16, i17, i18, i19}
	for _, instance := range instanceToAdd {
		p, err = algo.AddInstances(p, []placement.Instance{instance})
		require.NoError(t, err)
		require.NoError(t, placement.Validate(p))
		p, _, err = algo.MarkAllShardsAvailable(p)
		assert.NoError(t, err)
		instances = p.Instances()
		sort.Sort(BySubClusterIDInstanceID(instances))
		for _, instance := range instances {
			shards := instance.Shards().AllIDs()
			sort.Sort(UInts(shards))
			fmt.Println(instance.ID(), instance.SubClusterID(), instance.IsolationGroup(), len(shards), " Shards: ", shards)
		}
		fmt.Println("============================================================================")
	}

	require.NoError(t, validateSubClusteredPlacement(p))
	instanceToAdd = []placement.Instance{i7, i8, i9, i11, i12, i13}

	for _, instance := range instanceToAdd {
		p, err = algo.AddInstances(p, []placement.Instance{instance})
		require.NoError(t, err)
		require.NoError(t, placement.Validate(p))
		p, _, err = algo.MarkAllShardsAvailable(p)
		assert.NoError(t, err)
		instances = p.Instances()
		sort.Sort(BySubClusterIDInstanceID(instances))
		for _, instance := range instances {
			shards := instance.Shards().AllIDs()
			sort.Sort(UInts(shards))
			fmt.Println(instance.ID(), instance.SubClusterID(), instance.IsolationGroup(), len(shards), " Shards: ", shards)
		}
		fmt.Println("============================================================================")
	}
	require.NoError(t, validateSubClusteredPlacement(p))

	//i27 := placement.NewEmptyInstance("y", "r3", "z1", "endpoint", 1)
	i28 := placement.NewEmptyInstance("aa", "r1", "z1", "endpoint", 1)
	//i29 := placement.NewEmptyInstance("aa", "r2", "z1", "endpoint", 1)
	p, err = algo.ReplaceInstances(p, []string{"a"}, []placement.Instance{i28})
	require.NoError(t, err)
	require.NoError(t, placement.Validate(p))
	p, _, err = algo.MarkAllShardsAvailable(p)
	assert.NoError(t, err)
	instances = p.Instances()
	sort.Sort(BySubClusterIDInstanceID(instances))
	for _, instance := range instances {
		shards := instance.Shards().AllIDs()
		sort.Sort(UInts(shards))
		fmt.Println(instance.ID(), instance.SubClusterID(), instance.IsolationGroup(), len(shards), " Shards: ", shards)
	}
	require.NoError(t, validateSubClusteredPlacement(p))
	fmt.Println("============================================================================")
	instancesToBemoved := []string{"aa", "b", "c", "d", "e", "f"}

	for _, i := range instancesToBemoved {
		p, err = algo.RemoveInstances(p, []string{i})
		require.NoError(t, err)
		require.NoError(t, placement.Validate(p))
		p, _, err = algo.MarkAllShardsAvailable(p)
		assert.NoError(t, err)
		instances = p.Instances()
		sort.Sort(BySubClusterIDInstanceID(instances))
		for _, instance := range instances {
			shards := instance.Shards().AllIDs()
			sort.Sort(UInts(shards))
			fmt.Println(instance.ID(), instance.SubClusterID(), instance.IsolationGroup(), len(shards), " Shards: ", shards)
		}
		fmt.Println("============================================================================")
	}

}
