package algo

import (
	"fmt"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/placement"
)

// generateInstances creates n instances with random names and round-robin rack assignments
func generateInstances(n int, startIdx int) []placement.Instance {
	instances := make([]placement.Instance, n)
	for i := 0; i < n; i++ {
		rackID := fmt.Sprintf("r%d", (i%3)+1) // r1, r2, r3 in round-robin
		instances[i] = placement.NewEmptyInstance(
			fmt.Sprintf("I%d%d", startIdx, i),
			rackID,
			"z1",
			"endpoint",
			1,
		)
	}
	return instances
}

func TestLargeSubclusterPlacement(t *testing.T) {

	numShards := 4196
	instancesPerBatch := 6
	totalSubclusters := 70
	totalInstances := totalSubclusters * instancesPerBatch

	// Initialize shard IDs
	ids := make([]uint32, numShards)
	for i := 0; i < len(ids); i++ {
		ids[i] = uint32(i)
	}

	// Initialize placement options
	opts := placement.NewOptions().
		SetHasSubClusters(true).
		SetIsSharded(true).
		SetInstancesPerSubCluster(instancesPerBatch)

	algo := newSubclusteredShardedAlgorithm(opts)

	// Generate initial batch of instances
	initialInstances := generateInstances(instancesPerBatch, 0)

	// Create initial placement
	p, err := algo.InitialPlacement(initialInstances, ids, 3)
	require.NoError(t, err)
	require.NotNil(t, p)
	require.NoError(t, placement.Validate(p))

	p, _, err = algo.MarkAllShardsAvailable(p)
	require.NoError(t, err)

	fmt.Printf("Initial placement created with %d instances\n", len(p.Instances()))
	printPlacementAndValidate(t, p)

	// Add remaining instances in batches
	remainingBatches := (totalInstances / instancesPerBatch) - 1
	for i := 1; i <= remainingBatches; i++ {
		newInstances := generateInstances(instancesPerBatch, i)
		printInstances(newInstances)

		p, err = algo.AddInstances(p, newInstances)
		require.NoError(t, err)
		require.NoError(t, placement.Validate(p))

		p, _, err = algo.MarkAllShardsAvailable(p)
		require.NoError(t, err)

		p, err = algo.BalanceShards(p)
		require.NoError(t, err)

		p, _, err = algo.MarkAllShardsAvailable(p)
		require.NoError(t, err)

		fmt.Printf("Added batch %d/%d - Total instances: %d\n", i+2, remainingBatches+1, len(p.Instances()))
		printPlacementAndValidate(t, p)
	}
	printPlacementAndValidate(t, p)
}

func printPlacementAndValidate(t *testing.T, p placement.Placement) {
	printPlacement(p)
	require.NoError(t, validateSubClusteredPlacement(p))
}

func printPlacement(p placement.Placement) {
	instances := p.Instances()
	sort.Sort(BySubClusterIDInstanceID(instances))
	for _, instance := range instances {
		shards := instance.Shards().AllIDs()
		sort.Sort(UInts(shards))
		fmt.Println(instance.ID(), instance.SubClusterID(), instance.IsolationGroup(), len(shards), " Shards: ")
	}
	fmt.Println("============================================================================")
}

func printInstances(instances []placement.Instance) {
	for _, instance := range instances {
		fmt.Println(instance.ID(), instance.SubClusterID(), instance.IsolationGroup())
	}
}
