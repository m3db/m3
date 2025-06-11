package algo

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/stretchr/testify/require"
)

// getMaxShardDiffInSubclusters returns skew information for all subclusters and prints summary statistics
func getMaxShardDiffInSubclusters(p placement.Placement) (map[uint32]int, int, int) {
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
	subclustersWithMaxSkewGTTwo := 0
	for _, skew := range subclusterSkews {
		if skew > 2 {
			subclustersWithMaxSkewGTTwo++
		}
	}

	// Print summary statistics only if max skew > 2
	// if globalMaxSkew > 2 {
	// 	fmt.Printf("=== Subcluster Skew Analysis ===\n")
	// 	fmt.Printf("Maximum skew among all subclusters: %d\n", globalMaxSkew)
	// 	fmt.Printf("Number of subclusters with skew > 2: %d\n", subclustersWithMaxSkewGTTwo)
	// 	fmt.Printf("Total subclusters: %d\n", len(subclusterSkews))

	// 	// Print detailed skew information for each subcluster (only those with skew > 2)
	// 	fmt.Printf("Detailed skew by subcluster (skew > 2):\n")
	// 	for subclusterID, skew := range subclusterSkews {
	// 		if skew > 2 {
	// 			fmt.Printf("  Subcluster %d: skew = %d\n", subclusterID, skew)
	// 		}
	// 	}
	// 	fmt.Printf("================================\n")
	// }

	return subclusterSkews, globalMaxSkew, subclustersWithMaxSkewGTTwo
}

func TestSubclusteredV2AddInstances(t *testing.T) {
	tests := []struct {
		name                string
		rf                  int
		instancesPerSub     int
		subclustersToAdd    int
		shards              int
		subclustersToRemove int
	}{
		{
			name:                "RF=3, 6 instances per subcluster, start with 12 add 6",
			rf:                  3,
			instancesPerSub:     6,
			subclustersToAdd:    100,
			shards:              4096,
			subclustersToRemove: 10,
		},
		// {
		// 	name:             "RF=3, 9 instances per subcluster, start with 18 add 9",
		// 	rf:               3,
		// 	instancesPerSub:  9,
		// 	subclustersToAdd: 30,
		// 	shards:           8192,
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
				newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)
				currentPlacement = newPlacement
				instanceCount++
				if instanceCount%tt.instancesPerSub == 0 {
					t.Logf("Added %d instances", instanceCount)
					require.NoError(t, placement.Validate(currentPlacement))
					require.NoError(t, validateSubClusteredPlacement(currentPlacement))
					// Get max shard differences before rebalancing
					_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(currentPlacement)
					// Find the maximum skew and its subcluster ID
					t.Logf("Maximum shard difference before rebalancing: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)
				}
			}

			// Verify the placement after addition

			// Verify the placement after addition
			require.NoError(t, placement.Validate(currentPlacement))

			require.NoError(t, validateSubClusteredPlacement(currentPlacement))
			//printPlacement(currentPlacement)

			// Get max shard differences before rebalancing
			_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(currentPlacement)
			// Find the maximum skew and its subcluster ID
			t.Logf("Maximum shard difference before rebalancing: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)

			// Get instances from random N subclusters
			instancesToRemove := make([]placement.Instance, 0, tt.instancesPerSub*tt.subclustersToRemove)

			// Create a list of all available subcluster IDs
			availableSubclusters := make(map[uint32]bool)
			for _, instance := range currentPlacement.Instances() {
				availableSubclusters[instance.SubClusterID()] = true
			}

			subclusterIDs := make([]uint32, 0, len(availableSubclusters))
			for id := range availableSubclusters {
				subclusterIDs = append(subclusterIDs, id)
			}

			// Randomly select subclusters to remove
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			rng.Shuffle(len(subclusterIDs), func(i, j int) {
				subclusterIDs[i], subclusterIDs[j] = subclusterIDs[j], subclusterIDs[i]
			})

			selectedSubclusters := subclusterIDs[:tt.subclustersToRemove]

			for _, subClusterID := range selectedSubclusters {
				for _, instance := range currentPlacement.Instances() {
					if instance.SubClusterID() == subClusterID {
						instancesToRemove = append(instancesToRemove, instance)
					}
				}
			}
			require.Equal(t, tt.instancesPerSub*tt.subclustersToRemove, len(instancesToRemove),
				"Should have correct number of instances to remove")

			// Remove instances one by one
			for i, instance := range instancesToRemove {
				t.Logf("Removing instance %s (%d/%d)", instance.ID(), i+1, len(instancesToRemove))

				// Remove the instance
				newPlacement, err := algo.RemoveInstances(currentPlacement, []string{instance.ID()})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)
				// printPlacement(newPlacement)

				newPlacement, marked, err := algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)

				// Verify the placement after removal
				require.NoError(t, placement.Validate(newPlacement))

				currentPlacement = newPlacement
			}

			// Final validation after all removals
			require.NoError(t, placement.Validate(currentPlacement))
			//printPlacementAndValidate(t, currentPlacement)

			_, globalMaxSkew, subclustersWithMaxSkewGTTwo = getMaxShardDiffInSubclusters(currentPlacement)
			t.Logf("Maximum shard difference after removals: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)

			for i := 0; i < len(instancesToRemove); i++ {
				instance := instancesToRemove[i].SetShards(shard.NewShards(nil))
				newPlacement, err := algo.AddInstances(currentPlacement, []placement.Instance{instance})
				require.NoError(t, err)
				require.NotNil(t, newPlacement)
				newPlacement, marked, err = algo.MarkAllShardsAvailable(newPlacement)
				require.NoError(t, err)
				require.True(t, marked)
				currentPlacement = newPlacement
				instanceCount++
				if instanceCount%tt.instancesPerSub == 0 {
					t.Logf("Added %d instances", instanceCount)
					require.NoError(t, placement.Validate(currentPlacement))
					require.NoError(t, validateSubClusteredPlacement(currentPlacement))
					// Get max shard differences before rebalancing
					_, globalMaxSkew, subclustersWithMaxSkewGTTwo := getMaxShardDiffInSubclusters(currentPlacement)
					// Find the maximum skew and its subcluster ID
					t.Logf("Maximum shard difference before rebalancing: %d (subcluster %d)", globalMaxSkew, subclustersWithMaxSkewGTTwo)
				}
			}

		})
	}
}
