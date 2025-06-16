package algo

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
)

// Global variables for random instance name generation
var (
	usedInstanceNames = make(map[string]struct{})
	nameRng           = rand.New(rand.NewSource(time.Now().UnixNano()))
	nameMutex         sync.Mutex
)

// generateRandomInstanceName generates a random unique instance name
func generateRandomInstanceName() string {
	nameMutex.Lock()
	defer nameMutex.Unlock()

	for {
		// Generate a random name using timestamp and random number
		name := fmt.Sprintf("instance_%d_%d", time.Now().UnixNano(), nameRng.Int63())
		if _, exists := usedInstanceNames[name]; !exists {
			usedInstanceNames[name] = struct{}{}
			return name
		}
	}
}

// generatePrefixedRandomInstanceName generates a random instance name with a prefix
func generatePrefixedRandomInstanceName(prefix string) string {
	return fmt.Sprintf("%s_%s", prefix, generateRandomInstanceName())
}

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
