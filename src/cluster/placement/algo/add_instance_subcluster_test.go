package algo

import (
	"fmt"
	"testing"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/require"
)

// getMaxShardDiffInSubclusters returns skew information for all subclusters and prints summary statistics
func getMaxShardDiffInSubclusters(p placement.Placement) map[uint32]int {
	// Map to store shard counts per instance in each subcluster
	subclusterShardCounts := make(map[uint32]map[string]int)

	// Count shards for each instance in each subcluster
	for _, instance := range p.Instances() {
		subclusterID := instance.SubClusterID()
		if _, exists := subclusterShardCounts[subclusterID]; !exists {
			subclusterShardCounts[subclusterID] = make(map[string]int)
		}
		subclusterShardCounts[subclusterID][instance.ID()] = instance.Shards().NumShards()
	}

	// Calculate skew for each subcluster
	subclusterSkews := make(map[uint32]int)
	var globalMaxSkew int

	for subclusterID, instanceCounts := range subclusterShardCounts {
		var minCount, maxCount int
		first := true
		for _, count := range instanceCounts {
			if first {
				minCount = count
				maxCount = count
				first = false
				continue
			}
			if count < minCount {
				minCount = count
			}
			if count > maxCount {
				maxCount = count
			}
		}

		skew := maxCount - minCount
		subclusterSkews[subclusterID] = skew
		if skew > globalMaxSkew {
			globalMaxSkew = skew
		}
	}

	// Count how many subclusters have the maximum skew
	subclustersWithMaxSkew := 0
	for _, skew := range subclusterSkews {
		if skew == globalMaxSkew {
			subclustersWithMaxSkew++
		}
	}

	// Print summary statistics only if max skew > 2
	if globalMaxSkew > 2 {
		fmt.Printf("=== Subcluster Skew Analysis ===\n")
		fmt.Printf("Maximum skew among all subclusters: %d\n", globalMaxSkew)
		fmt.Printf("Number of subclusters with maximum skew: %d\n", subclustersWithMaxSkew)
		fmt.Printf("Total subclusters: %d\n", len(subclusterSkews))

		// Print detailed skew information for each subcluster (only those with skew > 2)
		fmt.Printf("Detailed skew by subcluster (skew > 2):\n")
		for subclusterID, skew := range subclusterSkews {
			fmt.Printf("  Subcluster %d: skew = %d\n", subclusterID, skew)
		}
		fmt.Printf("================================\n")
	}

	return subclusterSkews
}

func TestSubclusteredV2AddInstances(t *testing.T) {
	tests := []struct {
		name                          string
		rf                            int
		instancesPerSub               int
		subclustersToAdd              int
		shards                        int
		subclusterToAddAfterRebalance int
	}{
		{
			name:                          "RF=3, 6 instances per subcluster, start with 12 add 6",
			rf:                            3,
			instancesPerSub:               6,
			subclustersToAdd:              28,
			shards:                        8192,
			subclusterToAddAfterRebalance: 2,
		},
		// {
		// 	name:                          "RF=3, 9 instances per subcluster, start with 18 add 9",
		// 	rf:                            3,
		// 	instancesPerSub:               9,
		// 	subclustersToAdd:              6,
		// 	shards:                        4096,
		// 	subclusterToAddAfterRebalance: 5,
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create initial test instances
			instances := make([]placement.Instance, tt.instancesPerSub)
			for i := 0; i < tt.instancesPerSub; i++ {
				instances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", i)).
					SetIsolationGroup(fmt.Sprintf("R%d", i%tt.rf)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", i)).
					SetShards(shard.NewShards(nil))
			}

			// Generate shard IDs from 0 to shards-1
			shardIDs := make([]uint32, tt.shards)
			for i := 0; i < tt.shards; i++ {
				shardIDs[i] = uint32(i)
			}

			// Create algorithm
			opts := placement.NewOptions().
				SetValidZone("zone1").
				SetIsSharded(true).
				SetInstancesPerSubCluster(tt.instancesPerSub).
				SetHasSubClusters(true)
			algo := newSubclusteredShardedAlgorithm(opts)

			// Perform initial placement
			p, err := algo.InitialPlacement(instances, shardIDs, tt.rf)
			require.NoError(t, err)
			require.NotNil(t, p)

			p, marked, err := algo.MarkAllShardsAvailable(p)
			require.NoError(t, err)
			require.True(t, marked)

			// Verify initial placement
			require.NoError(t, placement.Validate(p))
			require.NoError(t, validateSubClusteredPlacement(p))

			// Create new instances to add
			newInstances := make([]placement.Instance, tt.instancesPerSub*tt.subclustersToAdd)
			for i := 0; i < len(newInstances); i++ {
				newInstances[i] = placement.NewInstance().
					SetID(fmt.Sprintf("I%d", tt.instancesPerSub+i)).
					SetIsolationGroup(fmt.Sprintf("R%d", (tt.instancesPerSub+i)%tt.rf)).
					SetWeight(1).
					SetEndpoint(fmt.Sprintf("E%d", tt.instancesPerSub+i)).
					SetShards(shard.NewShards(nil))
			}

			// Add instances one by one
			currentPlacement := p
			instanceCount := 0

			for i := 0; i < len(newInstances); i++ {
				newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{newInstances[i]})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)
				currentPlacement = newPlacement
				instanceCount++
				if instanceCount%tt.instancesPerSub == 0 {
					t.Logf("Added %d instances", instanceCount)
					require.NoError(t, placement.Validate(currentPlacement))
					require.NoError(t, validateSubClusteredPlacement(currentPlacement))
				}
			}

			// Verify the placement after addition

			// Verify the placement after addition
			require.NoError(t, placement.Validate(currentPlacement))

			currentPlacement, marked, err = algo.MarkAllShardsAvailable(currentPlacement)
			require.NoError(t, err)
			require.True(t, marked)

			require.NoError(t, validateSubClusteredPlacement(currentPlacement))
			// printPlacement(currentPlacement)

			// Get max shard differences before rebalancing
			subclusterSkews := getMaxShardDiffInSubclusters(currentPlacement)
			// Find the maximum skew and its subcluster ID
			var maxDiffSubclusterID uint32
			var maxBeforeDiff int
			for subclusterID, skew := range subclusterSkews {
				if skew > maxBeforeDiff {
					maxBeforeDiff = skew
					maxDiffSubclusterID = subclusterID
				}
			}
			t.Logf("Maximum shard difference before rebalancing: %d (subcluster %d)", maxBeforeDiff, maxDiffSubclusterID)
			balancedplacement := currentPlacement.Clone()

			balancedplacement, err = algo.BalanceShards(balancedplacement)
			require.NoError(t, err)
			require.NotNil(t, balancedplacement)

			balancedplacement, _, err = algo.MarkAllShardsAvailable(balancedplacement)
			require.NoError(t, err)

			// Get max shard differences after rebalancing
			subclusterSkewsAfter := getMaxShardDiffInSubclusters(balancedplacement)
			// Find the maximum skew and its subcluster ID after rebalancing
			var maxDiffSubclusterIDAfter uint32
			var maxAfterDiff int
			for subclusterID, skew := range subclusterSkewsAfter {
				if skew > maxAfterDiff {
					maxAfterDiff = skew
					maxDiffSubclusterIDAfter = subclusterID
				}
			}
			t.Logf("Maximum shard difference after rebalancing: %d (subcluster %d)", maxAfterDiff, maxDiffSubclusterIDAfter)

			// Final validation after all additions
			require.NoError(t, placement.Validate(balancedplacement))
			require.NoError(t, validateSubClusteredPlacement(balancedplacement))

			if tt.subclusterToAddAfterRebalance > 0 {
				instancesToAdd := make([]placement.Instance, tt.instancesPerSub*tt.subclusterToAddAfterRebalance)
				for i := 0; i < (tt.instancesPerSub * tt.subclusterToAddAfterRebalance); i++ {
					instancesToAdd[i] = placement.NewInstance().
						SetID(fmt.Sprintf("RI%d", tt.instancesPerSub+i)).
						SetIsolationGroup(fmt.Sprintf("R%d", (tt.instancesPerSub+i)%tt.rf)).
						SetWeight(1).
						SetEndpoint(fmt.Sprintf("E%d", tt.instancesPerSub+i)).
						SetShards(shard.NewShards(nil))
				}
				newPlacement, err := algo.AddInstances(balancedplacement, instancesToAdd)
				require.NoError(t, err)
				require.NotNil(t, newPlacement)
				balancedplacement = newPlacement

				balancedplacement, marked, err = algo.MarkAllShardsAvailable(balancedplacement)
				require.NoError(t, err)
				require.True(t, marked)

				require.NoError(t, validateSubClusteredPlacement(balancedplacement))
				printPlacement(balancedplacement)

				subclusterSkewsFinal := getMaxShardDiffInSubclusters(currentPlacement)
				// Find the maximum skew and its subcluster ID in final placement
				var maxDiffSubclusterIDFinal uint32
				var maxFinalDiff int
				for subclusterID, skew := range subclusterSkewsFinal {
					if skew > maxFinalDiff {
						maxFinalDiff = skew
						maxDiffSubclusterIDFinal = subclusterID
					}
				}
				t.Logf("Maximum shard difference after final addition: %d (subcluster %d)", maxFinalDiff, maxDiffSubclusterIDFinal)

			}
		})
	}
}
