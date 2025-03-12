package algo

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

type UInts []uint32

func (u UInts) Len() int { return len(u) }

func (u UInts) Less(i, j int) bool { return u[i] < u[j] }

func (u UInts) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
}

type BySubClusterIDInstanceID []placement.Instance

func (a BySubClusterIDInstanceID) Len() int { return len(a) }

func (a BySubClusterIDInstanceID) Less(i, j int) bool {
	if a[i].SubClusterID() == a[j].SubClusterID() {
		return a[i].ID() < a[j].ID()
	}
	return a[i].SubClusterID() < a[j].SubClusterID()
}

func (a BySubClusterIDInstanceID) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// nolint
func TestGoodCaseSubClusters(t *testing.T) {
	i1 := placement.NewEmptyInstance("a", "r1", "z1", "endpoint", 1).SetSubClusterID(1)
	i2 := placement.NewEmptyInstance("b", "r2", "z1", "endpoint", 1).SetSubClusterID(1)
	i3 := placement.NewEmptyInstance("c", "r3", "z1", "endpoint", 1).SetSubClusterID(1)
	i4 := placement.NewEmptyInstance("d", "r1", "z1", "endpoint", 1).SetSubClusterID(1)
	i5 := placement.NewEmptyInstance("e", "r2", "z1", "endpoint", 1).SetSubClusterID(1)
	//i6 := placement.NewEmptyInstance("f", "r3", "z1", "endpoint", 1).SetSubClusterID(1)
	i7 := placement.NewEmptyInstance("g", "r1", "z1", "endpoint", 1).SetSubClusterID(2)
	i8 := placement.NewEmptyInstance("h", "r2", "z1", "endpoint", 1).SetSubClusterID(2)
	i9 := placement.NewEmptyInstance("i", "r3", "z1", "endpoint", 1).SetSubClusterID(2)
	i11 := placement.NewEmptyInstance("j", "r1", "z1", "endpoint", 1).SetSubClusterID(2)
	//i12 := placement.NewEmptyInstance("k", "r2", "z1", "endpoint", 1).SetSubClusterID(2)
	i13 := placement.NewEmptyInstance("l", "r3", "z1", "endpoint", 1).SetSubClusterID(2)
	i14 := placement.NewEmptyInstance("m", "r1", "z1", "endpoint", 1).SetSubClusterID(3)
	i15 := placement.NewEmptyInstance("n", "r2", "z1", "endpoint", 1).SetSubClusterID(3)
	i16 := placement.NewEmptyInstance("o", "r3", "z1", "endpoint", 1).SetSubClusterID(3)
	i17 := placement.NewEmptyInstance("p", "r1", "z1", "endpoint", 1).SetSubClusterID(3)
	i18 := placement.NewEmptyInstance("q", "r2", "z1", "endpoint", 1).SetSubClusterID(3)
	i19 := placement.NewEmptyInstance("r", "r3", "z1", "endpoint", 1).SetSubClusterID(3)

	//subClusterToInstanceMap := make(map[uint32][]placement.Instance)
	//subClusterToInstanceMap[1] = []placement.Instance{i1, i3, i5, i2, i4, i6}
	////subClusterToInstanceMap[2] = []placement.Instance{i2, i4, i6}
	//subClusterToInstanceMap[3] = []placement.Instance{i7, i8, i9}

	numShards := 64
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	//helper, err := newSubClusterInitHelper(subClusterToInstanceMap, ids)
	//require.NoError(t, err)
	//subClusterToShardMap, err := helper.placeShards(ids)
	//require.NoError(t, err)
	//require.NotNil(t, subClusterToShardMap)
	opts := placement.NewOptions().SetHasSubClusters(true).SetIsSharded(true).SetInstancesPerSubCluster(6)
	algo := newSubClusteredAlgorithm(opts)
	p, err := algo.InitialPlacement([]placement.Instance{i1, i2, i3, i4, i5, i7, i8, i9, i11, i13}, ids, 3)
	fmt.Println(p)
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
	////
	i21 := placement.NewEmptyInstance("s", "r1", "z1", "endpoint", 1).SetSubClusterID(4)
	i22 := placement.NewEmptyInstance("t", "r2", "z1", "endpoint", 1).SetSubClusterID(4)
	i23 := placement.NewEmptyInstance("u", "r3", "z1", "endpoint", 1).SetSubClusterID(4)
	//i24 := placement.NewEmptyInstance("v", "r1", "z1", "endpoint", 1).SetSubClusterID(4)
	i25 := placement.NewEmptyInstance("w", "r2", "z1", "endpoint", 1).SetSubClusterID(4)
	i26 := placement.NewEmptyInstance("x", "r3", "z1", "endpoint", 1).SetSubClusterID(4)

	p, err = algo.AddInstances(p, []placement.Instance{i21, i22, i23, i25, i26})
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
	p, err = algo.AddInstances(p, []placement.Instance{i19, i15, i16, i17, i18, i14})
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
	p, err = algo.RemoveInstances(p, []string{"a", "b", "c", "d", "e"})
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
	i27 := placement.NewEmptyInstance("y", "r3", "z1", "endpoint", 1).SetSubClusterID(4)
	p, err = algo.ReplaceInstances(p, []string{"x"}, []placement.Instance{i27})
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
}

func validateSubClusteredPlacement(p placement.Placement) error {
	shardToSubCluster := make(map[uint32]uint32)
	shardToIGMap := make(map[uint32]map[string]int)
	for _, instance := range p.Instances() {
		for _, s := range instance.Shards().All() {
			if s.State() == shard.Leaving {
				continue
			}
			if _, ok := shardToIGMap[s.ID()]; !ok {
				shardToIGMap[s.ID()] = make(map[string]int)
			}
			shardToIGMap[s.ID()][instance.IsolationGroup()]++
			if _, ok := shardToSubCluster[s.ID()]; !ok {
				shardToSubCluster[s.ID()] = instance.SubClusterID()
				continue
			}
			if shardToSubCluster[s.ID()] != instance.SubClusterID() {
				return fmt.Errorf("shardToSubCluster[%d]: expected %d, actual %d", s.ID(), shardToSubCluster[s.ID()], instance.SubClusterID())
			}
		}
	}
	for shard, igs := range shardToIGMap {
		if len(igs) != 3 {
			return fmt.Errorf("shardToIGMap[%d]: expected 3, actual %d", shard, len(igs))
		}
	}
	return nil
}
