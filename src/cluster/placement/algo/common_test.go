package algo

import (
	"fmt"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

type UInts []uint32

func (u UInts) Len() int { return len(u) }

func (u UInts) Less(i, j int) bool { return u[i] < u[j] }

func (u UInts) Swap(i, j int) {
	u[i], u[j] = u[j], u[i]
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
		if len(igs) != p.ReplicaFactor() {
			return fmt.Errorf("shardToIGMap[%d]: expected %d, actual %d", shard, p.ReplicaFactor(), len(igs))
		}
	}
	return nil
}
